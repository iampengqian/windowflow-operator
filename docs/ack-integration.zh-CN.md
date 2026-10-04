# ACK / PyTorch / Megatron 接入路径

目标是让训练平台负责窗口管理，算法代码继续消费一个 batch iterator。这需要平台层的一次适配；仅安装 Operator 无法让任意既有 Dataset 自动改变采样范围。

本文是实验版本的接入说明。真实 ACK、CPFS、PPU 和你们的 Megatron 分支尚未验证。

## 1. 先确定数据窗口与容量

2 PB 按十进制均分十份，每份是 200 TB。两个完整窗口已经占用 400 TB **文件内容**，所以购买恰好 400 TB 的 CPFS 并没有给元数据、分配开销和运维留空间。可以增加 CPFS 容量，或改为更多、更小的窗口；最终以源端清单和实际文件系统占用为准。

v0.1 的窗口必须对应一个现成的目录或 OSS prefix，例如 `windows/0000`。`expectedBytes` 是这个目录内全部普通文件的精确内容字节数之和。Operator 不替你扫描 2 PB 数据、生成均衡分片，也不把任意视频清单自动重排为目录。若目前视频散布在无关 prefix 下，优先补 manifest 后端，不能假装改一份 YAML 就能使用任意清单。

构造窗口时要保持训练所需的数据分布。按来源、日期或类别直接分十份，可能把数据偏差引入训练。窗口内采样不等价于全量 shuffle，训练收敛效果需要单独评估。

## 2. 训练平台创建一次 WindowPlan

每次独立的训练尝试创建一个 plan。不要把失败后重启的作业直接当成原尝试继续运行。

| 平台输入 | WindowPlan / SDK 配置 |
| --- | --- |
| 共享 CPFS PVC | `spec.pvcName`；worker 可写，训练 Pod 只读挂载 |
| 窗口数量及顺序 | `spec.windows`；每项包含唯一 `id`、相对 `source`、`expectedBytes` |
| 允许同时驻留的窗口 | `spec.slots`，范围 2–64 |
| 逻辑工作集预算 | `spec.capacityBytes`；不等于底层硬配额 |
| 允许并行导入的数量 | `spec.maxConcurrentLoads`，初始建议 1 |
| 真正执行文件读取的进程 | `spec.readers` 中的固定 ID 列表 |
| 恢复位置 | 新 plan 的 `spec.startWindow`，来自训练 checkpoint |
| CPFS DataFlow / 凭据 | `spec.dataFlow`，参考 sample；凭据仅注入 stage Job |

同一 PVC 一次只交给一个 plan。不要通过另外一个 PVC 名称重复暴露同一缓存目录；锁无法识别这种别名。

## 3. 在 Dataset 工厂外包一层窗口迭代器

平台可以提供以下内部接口，算法工程师继续使用原有的 `get_batch` 和训练循环：

```text
平台窗口迭代器
  acquire(window i)
  用 handle.root 创建现有 VideoDataset
  用 Megatron 的 DP sampler 创建有限 DataLoader
  持续 yield 已完成解码的 batch
  DataLoader 正常耗尽，worker/decoder 全部退出
  release(window i)
  acquire(window i+1)
  ...
```

可参考 [examples/megatron_adapter.py](../examples/megatron_adapter.py) 中的 `continuous_window_batches`。这是接入草图，需对齐你们固定版本的 Megatron external dataloader hook；它没有在 PPU 上执行过。

`dataset_factory(handle)` 的输入是当前窗口根目录。如果既有索引里存的是 OSS URL 或 CPFS 全局绝对路径，需要在 Dataset 的路径解析层换成当前窗口内的相对路径。算法、loss、optimizer 不需要知道 CPFS 导入或 Kubernetes API。

示例采用每个窗口固定、有限的样本预算，并要求预算对齐完整 global batch。窗口内索引次序必须在同一 DP replica 的读取进程间一致。它不实现变长 packing、弹性 DP、Megatron rerun 或数据增强 RNG 的精确恢复；这些不能通过简单加一个 barrier 补齐。

## 4. 分开“样本分片身份”和“安全释放身份”

**样本分片使用 DP rank/DP world size。** 不要把 global world size 传给普通 DistributedSampler 后就认为完成了 TP/PP 适配。

**租约默认给每个实际打开视频文件的进程一个独立 ID。** 举例：若 8 个 DP replica 各只有一个 TP rank/PP stage 读文件，则声明 8 个 reader；若每个 replica 的两个 PP stage 都独立读文件，则声明 16 个。实际数量由你们的读取拓扑决定，不能只看卡数。

只接收已经解码 tensor 的进程不必持有文件租约。若平台希望一个 ID 代表多个进程，就必须自己确认这些进程的文件 I/O 全部结束后才释放。

`release()` 表示允许删除文件。DataLoader 的预取、持久 worker、懒解码句柄、主进程 decoder 和异步读取必须都结束。单纯完成 optimizer step 或执行一次分布式 barrier 不足以证明这些条件成立。异常和取消不能走无条件 `finally: release()`。

平台还要处理正常训练结束：固定步数的训练循环可能在最后一次 `yield` 后直接退出，尚未让 Python 迭代器走到结束。确认已经消费完整计划的样本预算后，应再推进一次 iterator 并要求得到 `StopIteration`，让最后一个 DataLoader 正常结束并释放最后窗口。提前停止时不要用这个步骤强行释放；保留租约并走安全恢复。

## 5. checkpoint 与恢复

checkpoint 保存模型状态、原始窗口顺序/版本、样本预算、已消费样本偏移和所需 RNG 状态。租约和缓存状态只描述文件生命周期，不描述哪些样本参与了已经提交的训练步。

恢复时先完全隔离旧训练尝试及其云端导入，再按[运维恢复流程](operations.md#failure-and-recovery)处理旧 plan。创建新的 plan UID，保留原始窗口列表，把 `startWindow` 设为 checkpoint 所在窗口，并从相同序号开始 acquire。窗口内部的偏移只应用一次，避免外层框架和适配器重复跳过。

已经完整结束全部窗口的 checkpoint 不需要再创建用于恢复数据的 plan。已释放窗口需要重放时，通过新 plan 重新导入，不能把旧租约改回未释放。

## 6. 上线前的最小验收

先运行 `./scripts/kind-smoke.sh` 验证通用编排，再用 ACK 上三个很小的视频目录做真实 CPFS 试验。需要记录：实际导入路径、全部文件字节数、两个 reader 的释放行为、旧目录清理、控制器重启，以及失败任务保留数据的行为。

之后才扩大到代表性视频窗口，测量同时训练读取时的导入和清理速度。稳定轮换至少要求：从槽位可复用开始，清理与装载下一窗口的总耗时能被剩余训练时间覆盖。增加槽位可以增加预取余量，但无法弥补长期平均补给速度低于消费速度。

这轮验收不需要先迁移完整 2 PB。先让数据生命周期和当前训练拓扑对上，再决定容量、并发和窗口划分。
