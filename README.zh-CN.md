# WindowFlow Operator

WindowFlow 把不可变数据集按目录组织成窗口，在 Kubernetes 共享 PVC 上保留有限数量的窗口。训练读取当前窗口时，后台 Job 可以准备后续窗口；**只有所有声明的读取者显式释放后，Operator 才能清理该窗口**。

适用场景是：全量数据放对象存储，少量工作集放共享训练文件系统，训练任务接受在当前窗口内采样。v0.1 提供本地 PVC 复制和阿里云 CPFS 数据流导入两种后端。原始视频可以保留目录结构，无需为本项目转换格式。

[English](README.md) · [架构](docs/architecture.md) · [运维](docs/operations.md) · [贡献指南](CONTRIBUTING.md)

**当前状态：实验性 v0.1。** 这是参考实现，没有 PB 规模、真实 ACK/CPFS 或 PPU 生产验证。CPFS 后端必须在真实环境做冒烟测试。SDK 和示例不提供完整的 Megatron 断点恢复能力。计划使用的镜像名为 `ghcr.io/iampengqian/windowflow-operator:v0.1.0`，本文不表示镜像已经成功发布；在确认 registry 构建成功前，请自行构建。

## 功能与边界

- `WindowPlan` 固定一个训练尝试的数据窗口、读取者、槽位数和逻辑容量。
- Operator 通过 Kubernetes Jobs 执行 `stage` 和 `clean`。
- 导入成功且文件总字节数符合 `expectedBytes` 后，窗口才进入 `Ready`。
- 每个 `WindowLease` 绑定 plan UID、窗口序号、generation 和 reader ID；旧任务的释放记录不能授权新任务清理。
- Python SDK 提供显式 `acquire(index)` 和 `release(handle)`，不逐样本调用 Kubernetes API。
- 不根据超时、心跳消失或进程退出猜测数据已经无人使用。

**一个活动计划独占一个缓存 PVC。** v0.1 不支持跨作业共享缓存、弹性读取者、自动源端清单、任意 CSV manifest、全量随机采样或失败训练的自动恢复。`capacityBytes` 仅是调度时预留的文件内容字节预算，不是文件系统硬配额。

## 一条命令验证轮换

安装 Docker、kind 和 kubectl 后，运行 `./scripts/kind-smoke.sh`。脚本创建独立的单节点临时集群，构建 Operator 和 SDK 读取者镜像，用两个读取者、两个槽位消费三个小数据窗口，完成后删除自己创建的集群。无需 GPU 或云凭据；设置 `KEEP_CLUSTER=1` 可以保留集群检查。已有同名集群时脚本拒绝覆盖。

## 构建与部署

准备 Go 1.26、Python 3.10 或更新版本、容器构建工具，以及具有 CRD/RBAC 安装权限的 Kubernetes 1.32+ 集群和 `kubectl`。缓存 PVC 必须支持 worker 和所有训练读取者实际需要的同时挂载方式；跨节点训练通常需要共享存储。

```sh
git clone https://github.com/iampengqian/windowflow-operator.git
cd windowflow-operator
make test
make manifests
make build

docker build -t ghcr.io/iampengqian/windowflow-operator:v0.1.0 .
# 将镜像推送到集群可访问的 registry，或加载到本地集群。
# kind 集群示例：
kind load docker-image ghcr.io/iampengqian/windowflow-operator:v0.1.0

kubectl apply -k config/default
pip install ./sdk/python
```

若使用不同镜像名，同时修改部署配置中的 controller image 和计划中的 `spec.workerImage`。已有 WindowPlan 不可变；更新 controller 镜像不会改变已有计划的 worker 镜像。

Go 二进制提供 `windowflow manager`、`windowflow stage`、`windowflow clean` 三个子命令。worker Job 使用 `WINDOWFLOW_WORKER_CONFIG` 接收 JSON 配置，普通训练客户端无需直接调用 worker 命令。

## 先运行本地后端

使用 [local-plan.yaml](config/samples/local-plan.yaml) 验证存储轮转和释放协议，再连接 CPFS。

1. 在 `default` namespace 创建缓存 PVC `training-cache` 和独立的源 PVC `source-data`。
2. 按 sample 中的 `source` 准备源目录，并确认每个窗口的普通文件内容字节数之和与 `expectedBytes` 完全相等。提交后源数据保持不可变。
3. 确保 worker 使用的 UID/GID `1000:1000` 能读取源目录、写入缓存目录。
4. 提交计划：

```sh
kubectl apply -f config/samples/local-plan.yaml
kubectl get windowplans -n default -w
kubectl get jobs -n default
```

示例名是 `local-demo`，包含 `dp0`、`dp1` 两个读取者。每个窗口都需要两者分别 acquire 和 release；少一个释放，窗口就保留。本地后端拒绝路径逃逸、符号链接和特殊文件，不删除源数据。

## 接入现有训练代码

将缓存 PVC 挂到 `/cache`，并配置训练 ServiceAccount 的 CRD 读取与租约写入权限。基础安装 `pip install ./sdk/python` 不安装可选的 Kubernetes 客户端；实际连接集群时使用：

```sh
pip install "./sdk/python[kubernetes]"
```

权限与凭据配置见[运维文档](docs/operations.md)。在负责协调的读取进程中创建客户端，不要将客户端传给 DataLoader worker。

```python
from windowflow import WindowClient

client = WindowClient(
    plan_name="local-demo",
    namespace="default",
    reader_id="dp0",  # 第二个声明的读取者使用 dp1
    mount_path="/cache",
)

for window_index in range(3):
    handle = client.acquire(window_index)
    # handle.root 是 pathlib.Path，指向当前窗口的文件目录。
    # 在此创建原有 Dataset，并完成当前窗口训练。
    # 释放前停止迭代器，排空并关闭 DataLoader worker、decoder、
    # 预取队列与异步读取；不得再有任何参与者访问该窗口。
    client.release(handle)
```

上面的注释位置需要填入你们的训练流程；单独运行这个循环不会训练模型。**不要在无条件 `finally` 中 release。** 异常发生时可能仍有读取者运行，而 release 就是允许清理。SDK 超时不会自动释放，已经释放的窗口也不能在同一计划中重新获取。

Megatron TP/PP 场景应保留原有视频 Dataset、采样器以及 `get_batch`/广播路径，只在统一的窗口边界接入。默认给**每个实际打开文件的进程分配独立 reader ID**，包括额外承担读取的 TP/PP 进程。样本划分使用 Megatron 的 DP rank，租约 ID 是另一种身份。只有平台明确等待所有下属读取进程结束时，才可以用一个协调者 ID 代表多个进程。SDK 不替训练框架执行分布式 collective。

## 接入 CPFS 数据流

[cpfs-plan.yaml](config/samples/cpfs-plan.yaml) 是配置模板。CPFS 文件系统、PVC 挂载和 OSS 数据流需要事先创建。本项目不会创建云端存储资源。

- 填写真实的 region、filesystem/data-flow ID、源目录和精确 `expectedBytes`。
- `fileSystemPath` 是数据流关联的 CPFS 根路径，`pvcPath` 是 PVC 对应的实际文件系统路径；两者都不是容器内 `/cache`。`pvcPath` 必须位于关联根目录内。
- 在计划所在 namespace 创建凭据 Secret，键为 `ALIBABA_CLOUD_ACCESS_KEY_ID`、`ALIBABA_CLOUD_ACCESS_KEY_SECRET`，可选 `ALIBABA_CLOUD_SECURITY_TOKEN`。不要把值提交到 Git。
- 后端按目录导入元数据和数据，等待 provider 任务完成，再核验导入文件总字节数。它不读取 CSV 对象清单，也不自动发现全量数据。

首次使用先执行[真实 CPFS 冒烟测试](docs/operations.md#cpfs-setup)。本地测试成功不能证明云端导入路径、权限和清理行为正确。

## 容量、采样与恢复

以十进制估算，2 PB 分成十份，每份 200 TB，两槽需要最多 400 TB 的逻辑数据容量，另留文件系统余量。这只是算术示例。实际容量还受窗口大小偏差、元数据、checkpoint、清理延迟及 PVC 上其他内容影响。

应在训练并发读取时测量有效导入带宽，把清理、Job 启动、校验与重试余量计入窗口切换预算。首次装载存在冷启动。窗口内随机采样不等于全量随机；窗口构成和切换顺序需要通过训练效果验证。

stage/clean Job 失败会冻结计划，不会自动回收或重试。删除 WindowPlan 后，PVC 锁和数据仍保留。恢复前必须确认所有训练读取者和云端任务已经停止，安全清理对应目录，再手动释放锁、创建新的 plan。详见[恢复流程](docs/operations.md#failure-and-recovery)。

模型 checkpoint 需要记录窗口序号、采样器状态和训练进度；租约记录不能代替 checkpoint。保存 checkpoint 不等于 release，release 也不保存训练进度。隔离旧训练尝试后，新 plan 保留完整窗口列表，把 `spec.startWindow` 设为 checkpoint 对应的窗口序号；SDK/示例的起始序号保持一致。Operator 跳过前面的窗口，窗口内部的采样偏移和 RNG 状态仍由训练平台恢复。`completedWindows` 只统计当前尝试实际清理的窗口。完整且可复现的 Megatron 恢复不属于 v0.1 范围。

接入位置可参考 [Megatron 适配草图](examples/megatron_adapter.py)，运行协议可参考 [CPU 文件读取示例](examples/train_local.py)。

## 开源与来源

开发验证入口为 `make test`、`make manifests`、`make build`；提交变更需说明实际执行的测试，见 [CONTRIBUTING.md](CONTRIBUTING.md)。

窗口轮换思路参考了 [MONAI SmartCacheDataset](https://github.com/Project-MONAI/MONAI/blob/dev/monai/data/dataset.py)。MONAI 缓存进程内的变换结果，本项目协调共享 PVC 的目录驻留；未复制 MONAI 源码。

项目采用 [Apache License 2.0](LICENSE)。安全问题处理方式见 [SECURITY.md](SECURITY.md)。
