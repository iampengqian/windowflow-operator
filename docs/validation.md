# v0.1 validation record

The first complete [GitHub CI run](https://github.com/iampengqian/windowflow-operator/actions/runs/37210716853) passed on 2026-10-04. It tested commit `84f1549`. Later runs are available in [Actions](https://github.com/iampengqian/windowflow-operator/actions). Check the run's commit before using it as evidence for a later revision.

| Check actually executed | Result and scope |
| --- | --- |
| `make test vet build` on Linux | Go race tests, static analysis, binary build, and 25 Python SDK tests passed. |
| `make manifests` and generated-file diff | CRD/deepcopy regeneration matched checked-in files. |
| `make test-envtest`, Kubernetes API 1.35.0 | Real API admission rejected plan-spec mutations, lease identity changes, and release rollback. A three-window flow completed against the API with locally executed workers. Envtest has no kubelet/Job controller. |
| `./scripts/kind-smoke.sh`, Kubernetes 1.32.2 | Real controller Deployment, PVC mounts, two SDK-reader Pods, and six stage/clean Jobs completed three windows over two slots. Both readers hashed the staged files. |
| `python -m build sdk/python` | SDK source distribution and wheel built. The subsequent local packaging check included the Apache-2.0 license and SDK README. |

The Go controller tests include missing readers, stale plan releases, controller reconstruction at every transition, reservation limits, failed-worker retention, PVC ownership after plan deletion, and checkpoint suffix staging. Worker tests cover actual local copying/cleanup, ownership rejection, traversal/symlinks, cancellation, and a fake CPFS HTTP endpoint. SDK tests cover pinned identities, conflicts, explicit release, timeout retention, malformed paths, replacement plans, and skipped checkpoint windows.

**Not tested:** a real OSS-to-CPFS import; ACK CSI mounts or PPU execution; the full Megatron sketch with real TP/PP training; video decoding performance; convergence under window-local sampling; PB-scale throughput, filesystem occupancy, or control-plane scalability.

The kind demonstration uses tiny text files and a CPU file reader. It does not train a model. Fake CPFS responses are protocol tests, not evidence of provider compatibility. Use the [live CPFS runbook](operations.md#cpfs-setup) before promoting this reference implementation to a real dataset.
