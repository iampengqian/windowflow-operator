# WindowFlow Python SDK

Reader-side protocol for [WindowFlow Operator](https://github.com/iampengqian/windowflow-operator).
The source SDK is **0.2.0a1**, compatible with the v0.1.0 Operator and unchanged CRDs.
Install from the repository with `pip install './sdk/python[kubernetes]'`.
This package is not advertised as published to PyPI.

The SDK now includes:

- `generate_schedule` / `build_plan` and `windowflow-plan`: deterministic finite
  cycles of existing directory windows, with recorded order and digests.
- `WindowObserver.get_status()` / `wait_ready(index)`: read-only prewarm queries,
  requiring no reader identity, mount, or lease write permission.
- `WindowRunner` / `WindowEpoch`: continuous batches over full window-local
  epochs, explicit drain before release, and retained leases on failure/cancel.
- `TorchEpochFactory`: optional map-style, nonpersistent PyTorch DataLoaders.
  Install with `pip install './sdk/python[kubernetes,torch]'`.

```python
from windowflow import WindowObserver

ready = WindowObserver("training", "default").wait_ready(0)
# Start training resources. Each real reader must still acquire its own lease.
```

```python
from windowflow import WindowClient, WindowRunner, TorchEpochFactory

client = WindowClient("training", "default", "dp0", "/cache")
factory = TorchEpochFactory(
    lambda context: ExistingDataset(context.handle.root),  # application code
    batch_size=8, num_workers=4, shuffle=True, seed=42,
    multiprocessing_context="spawn",
)
runner = WindowRunner(client, factory, epochs_per_window=2)
runner.run(lambda batch, context: train_step(batch))  # synchronous application code
```

ExistingDataset/train_step above are application placeholders. DDP sharding is
supplied with `sampler_factory(dataset, context)`, with `shuffle=False`; TP/PP
reader placement/broadcast remains the framework's responsibility. Return owned
CPU data, not lazy video readers, mmap views or external asynchronous readers.
The built-in adapter requires full nonempty epochs and accurate DataLoader lengths.

For fixed-step external iterators, call `runner.finish()` after the final planned
batch to confirm true exhaustion. Early `runner.close()` never releases its lease
and never scans the remaining dataset. PyTorch iterator cleanup is not a fencing
guarantee. State/context are not model checkpoint progress. This version does not
implement global sample budgets, partial replacement, persistent-worker hot swaps,
or exact Megatron checkpoint recovery.

See the [platform integration guide](https://github.com/iampengqian/windowflow-operator/blob/main/docs/sdk-training.zh-CN.md)
and [CPU PyTorch/DDP example](https://github.com/iampengqian/windowflow-operator/blob/main/examples/train_torch.py).
The low-level API remains available:

```python
from windowflow import WindowClient

client = WindowClient("local-demo", "default", "dp0", "/cache")
handle = client.acquire(0)
# Open the existing Dataset at handle.root; consume the finite window.
# Stop and join every covered DataLoader worker, decoder and asynchronous read.
client.release(handle)
```

Release permits deletion. Never release unconditionally in `finally` after a
failure, and never reuse a reader ID while its old process can still read.
Acquire/release calls occur at window boundaries, with no per-sample Kubernetes
traffic or distributed collectives. Each actual file-reading process normally
has its own reader ID. Model-parallel sample assignment stays in the framework.

See the repository's architecture, recovery runbook, and Megatron sketch for the
full contract. The SDK is experimental and does not provide automatic checkpoint
recovery or elastic reader membership.
