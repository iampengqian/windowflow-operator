# 窗口计划与 PyTorch 接入（SDK 0.2.0a1）

本版在训练平台入口增加计划生成、预热查询、窗口生命周期封装。兼容现有
`data.windowflow.io/v1alpha1` API 和 v0.1.0 Operator，无需迁移 CRD。
这是源码中的实验性 SDK 版本，尚未发布到 PyPI。

## 1. 从已有目录生成有限计划

先将数据组织成不可变 OSS 前缀或源 PVC 目录。生成器不扫描 OSS，也不搬移、
重新分片或校验原始视频。`expectedBytes` 必须是该目录普通文件的实际内容字节数。

```sh
pip install './sdk/python[kubernetes,torch]'
windowflow-plan \
  --catalog examples/window-catalog.json \
  --template examples/plan-template.json \
  --num-cycles 3 --seed 42 --shuffle-windows \
  --output /tmp/training-plan.json \
  --schedule-output /tmp/training-schedule.json
kubectl apply -f /tmp/training-plan.json
```

输出文件必须不存在；命令拒绝覆盖输入、现有文件、模板内的 `windows` 或保留注解。
未安装 console script 时可用 `PYTHONPATH=sdk/python python -m windowflow.plan_cli`。
示例模板是本地 PVC 后端；CPFS 使用相同 catalog 格式，并按
[CPFS 示例](../config/samples/cpfs-plan.yaml) 配置模板的 backend/dataFlow，省略 windows。

执行清单保存完整 catalog、摘要、生成器版本、seed、轮次、实际顺序和自身摘要。
按 SHA-256 排序生成每轮顺序，不依赖 Python 的随机数实现。用
`validate_schedule(json.load(...))` 可以重新校验清单。
摘要检查元数据一致性，不验证 OSS 内容真实性。源数据仍须保持不可变。

三窗口 × 三轮生成九次访问，各次访问具有不同 window ID/generation。最多
1,024 次访问；重复轮次会重新导入，不保证复用此前驻留的数据。
`sampleCount` 是元数据，不是 Runner 自动执行的样本预算。

## 2. 平台预热完成后启动训练

平台只需 WindowPlan 的 GET 权限，无需 reader ID、CPFS 挂载或租约写权限：

```python
from windowflow import WindowObserver

observer = WindowObserver("generated-local-demo", "default", timeout=7200)
status = observer.wait_ready(window_index=0)
print(status.plan_uid, status.phase)
# 在你们的平台中启动训练任务；每个实际读取进程随后独立 acquire。
```

`get_status()` 返回冻结状态快照，包括失败原因、槽位、窗口序号和 worker Job 名称。
`wait_ready()` 只做 GET，不预约数据、不创建 WindowLease，也不返回可释放的 handle。
快照可能立即过期；训练仍必须 acquire。计划失败、被替换、删除或目标已经回收时会报错。
当前不提供导入百分比、吞吐估计或逐个未释放 reader 的诊断。

## 3. 平台接入一次，算法代码继续使用 Dataset

```python
from windowflow import TorchEpochFactory, WindowClient, WindowRunner

client = WindowClient("generated-local-demo", "default", "dp0", "/cache")
factory = TorchEpochFactory(
    lambda context: ExistingVideoDataset(root=context.handle.root),
    batch_size=8,
    num_workers=4,
    shuffle=True,
    seed=42,
    multiprocessing_context="spawn",
)
runner = WindowRunner(client, factory, epochs_per_window=2)

def train_batch(batch, context):
    # 现有同步训练一步：forward / backward / optimizer.step。
    train_step(batch)

runner.run(train_batch)
```

`ExistingVideoDataset` 和 `train_step` 是平台已有代码，不是 SDK 内置对象。
可运行的 CPU 小样例见 [train_torch.py](../examples/train_torch.py)，支持普通 PyTorch
和纯 DDP；它读取小文本并训练 Linear 模型，不代表真实视频解码或 Megatron 验证。

Runner 每个窗口内执行指定遍数，每遍重建 Dataset/DataLoader。最后一遍完成、worker
正常退出、主进程 Dataset 的可选 `close()` 成功后才释放窗口。整个 Runner 正常耗尽
会处理最后一个窗口，避免固定步数训练忘记触发最后一次释放。

- `epochs_per_window=2`：当前窗口读两遍。
- 计划的 `num_cycles=3`：全量窗口目录走三轮。
- 第一版 Runner 只支持完整、非空 epoch，不支持截断到任意全局样本数。

生产 DDP 应通过 `sampler_factory(dataset, context)` 设置 DP rank/size 与 epoch。
有 sampler_factory 时必须 `shuffle=False`；自定义 sampler 必须给出准确长度，且
每个同步训练 rank 的 batch 数应一致。`DistributedSampler` 的尾部填充/丢弃由平台选择。
TP/PP 的数据广播、真正读取数据的进程、reader ID 列表仍由 Megatron 接入层确定，
不能按 global rank 随意切分数据。现有 [Megatron 草图](../examples/megatron_adapter.py)
仍是独立示例，未宣称已经与新 Runner 或实际 Megatron 分支联调。

Dataset 必须是有限 map-style 数据集，返回完全读取的 CPU 数据。
不要返回视频句柄、惰性解码器、依赖源文件的 mmap/tensor view，或启动脱离 worker
生命周期的后台读取。此适配器不支持 persistent workers、IterableDataset 或外部异步
解码服务。需要这些能力时实现自定义 `WindowEpoch(batches, drain, abort)`，并让 drain
明确等待全部存储 I/O 结束。worker 中 Dataset 的 close 不等于主进程 Dataset 的 close。

## 4. 固定步数、失败和恢复

外部迭代器入口可使用 `iter(runner)`。训练完成全部计划的最后一个 batch 后，调用
`runner.finish()` 验证真正耗尽并释放。finish 仅允许最终窗口的最终 epoch；若发现
还有 batch，则保留该 batch 并报错，不会丢弃它或释放窗口。

提前退出时调用 `runner.close()`，只停止消费并尽力清理本进程资源，**不释放租约**。
它不读取剩余全窗数据来完成取消。PyTorch 没有公开的 DataLoader 提前关闭并 join API；
丢弃 iterator 引用不是 worker fencing，平台仍需按[恢复手册](operations.md)确认旧进程树
停止后处理失败计划。调用方自行 break 也不会自动释放。Runner 不能跨线程共享或在
训练 callback 内重入 next/finish/run/close。

factory、迭代、训练 callback、drain、release 的异常会停止 Runner；不自动继续下窗。
Torch 适配器还检查实际 batch 数与 `len(loader)` 一致，防止 Dataset 错误抛出
StopIteration 被当作成功训练完毕。

Runner 的 state/context 是运行状态，不是模型 checkpoint。已输出/预取的 batch 不等于
模型已提交训练进度。本版没有新增 state_dict、样本级精确恢复、训练自动重启或部分
替换 `replace_rate`。平台可从已有 checkpoint 创建新 attempt，设置 spec.startWindow
和 Runner 的 start_window；只有完整窗口边界才无需另做窗内 sampler/RNG 恢复。

## 来源与验证范围

借鉴 [MONAI 1.6.1 SmartCacheHandler](https://github.com/Project-MONAI/MONAI/blob/1.6.1/monai/handlers/smartcache_handler.py)
的生命周期接入思路，未复制 MONAI 实现。窗口文件仍按共享存储读者租约回收。
PyTorch 正常迭代结束时的 worker 行为见[官方文档](https://docs.pytorch.org/docs/2.14/data.html#multi-process-data-loading)。

真实 ACK/CPFS、PPU、Megatron TP/PP、PB 规模吞吐和训练收敛仍需实际环境验证。
