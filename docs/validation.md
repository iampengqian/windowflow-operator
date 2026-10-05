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

## Published release

The final v0.1.0 code at `9c7b7665b0de9c091950a54d9af4627311cb1d84` also passed the complete [release CI](https://github.com/iampengqian/windowflow-operator/actions/runs/37211171949), including both test and kind jobs. The subsequent README/validation edits only record these results.

The [image publication workflow](https://github.com/iampengqian/windowflow-operator/actions/runs/37211173600) passed and published Linux amd64/arm64 manifests. Anonymous registry authentication and image-index retrieval were verified without a GitHub credential.

```text
ghcr.io/iampengqian/windowflow-operator:v0.1.0
sha256:690e4b0e1cd0a75f92d82b21c626338ba248c15bb438895b4b2134859754a0aa
```

The index also contains provenance attestations; their `unknown/unknown` entries are not additional executable architectures. These checks establish registry availability and declared platforms, not an arm64 or PPU training execution test.

SDK wheel, source archive, and SHA256SUMS are attached to the [v0.1.0 experimental release](https://github.com/iampengqian/windowflow-operator/releases/tag/v0.1.0).
