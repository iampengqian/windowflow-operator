# WindowFlow Python SDK

Reader-side protocol for [WindowFlow Operator](https://github.com/iampengqian/windowflow-operator).
Install from the repository with `pip install './sdk/python[kubernetes]'`.
This package is not advertised as published to PyPI.

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
full contract. v0.1 is experimental and does not provide automatic checkpoint
recovery or elastic reader membership.
