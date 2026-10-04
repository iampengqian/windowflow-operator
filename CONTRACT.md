# Implementation contract (v0.1)

This file is the shared implementation specification. The public README will explain usage.

Module: `github.com/iampengqian/windowflow-operator`. Go 1.26, controller-runtime v0.25.2. Apache-2.0.

## Kubernetes API

Group/version: `data.windowflow.io/v1alpha1`. Namespaced CRDs: `WindowPlan` (`windowplans`) and `WindowLease` (`windowleases`). One immutable WindowPlan = one attempt of one distributed training job. No elastic reader membership. Exactly one active plan owns a PVC; a Kubernetes Lease lock is held for the plan lifetime and is never stolen on timeout. Completed plans release the lock after cleanup. Failed/deleted plans require an explicit recovery procedure; fail closed.

WindowPlan spec (immutable):

```yaml
pvcName: training-cache
slots: 2
maxConcurrentLoads: 1
startWindow: 0 # new checkpoint-recovery attempts can skip earlier ordinals
capacityBytes: 100000000
readers: [dp0, dp1]
workerImage: ghcr.io/iampengqian/windowflow-operator:v0.1.0
backend: local # or cpfs-dataflow
sourcePVC: source-data # required for local; mounted read-only at /source
dataFlow: # required for cpfs-dataflow
  region: cn-wulanchabu
  fileSystemId: bmcpfs-example
  dataFlowId: df-example
  fileSystemPath: /the/path/linked/by/dataflow/
  pvcPath: /the/actual/filesystem/path/of/the/pvc/
  credentialsSecret: aliyun-dataflow
windows:
  - id: window-0000
    source: windows/0000 # relative source dir: /source for local, DataFlow source for CPFS
    expectedBytes: 1000 # sum of all regular-file content bytes, exact; trusted immutable source
```

Status:
`phase` (Pending/Running/Completed/Failed), `message`, `nextWindow` (next unassigned ordinal), `completedWindows`, `observedGeneration`, `slots` array.
Slot: `index` (int), `windowIndex` (int), `windowId`, `generation` (unique within plan, `w000000`), `phase` (Loading/Ready/Reclaiming/Failed), `relativePath` (`windowflow/<plan UID>/w000000` within PVC), `reservedBytes`, `stageJob`, `cleanJob`.

WindowLease spec:
`planName`, `planUID`, `readerID`, `windowIndex` (int), `generation`, `released` (bool).
Tuple is immutable. `released` may transition false->true only. Create while slot Ready; unique deterministic name = `wl-` + first 40 hex chars of SHA256(planUID + "\n" + readerID + "\n" + decimal windowIndex + "\n" + generation). No heartbeat expiry implies permission to delete.
Operator only reclaims a Ready slot after a released matching lease from EVERY spec.readers entry. Leases from wrong plan UID/generation/index or unknown reader never count. Absence of a lease blocks reclamation. SDK must create/acquire before yielding a window handle. No per-sample API calls, no distributed collectives in SDK.

## Worker jobs

Same Go binary has `manager`, `stage`, `clean` subcommands. Manager delegates both stage and clean to Jobs that mount the single cache PVC at `/cache` (local also mounts sourcePVC at `/source`, with distinct PVC required). Jobs are deterministic names tied to plan UID, ordinal and action. A replacement worker uses the same destination and a persisted provider task ID; an ambiguous submission intent blocks automatic resubmission; a failed job freezes the plan and does not reclaim files. Jobs have backoffLimit 0, no TTL while plan active.

Worker receives a JSON config in env `WINDOWFLOW_WORKER_CONFIG`:

```json
{"action":"stage","backend":"local","planUID":"uuid","generation":"w000000","relativePath":"windowflow/uuid/w000000","source":"windows/0000","expectedBytes":1000,"dataFlow":{"region":"cn-wulanchabu","fileSystemId":"bmcpfs-example","dataFlowId":"df-example","fileSystemPath":"/linked/","pvcPath":"/pv/"}}
```

Define this in package `internal/worker`: `Config`, `DataFlowConfig`; export `Run(ctx context.Context, cfg Config, cacheRoot, sourceRoot string) error` and `Validate(cfg Config) error`.
Credential env names `ALIBABA_CLOUD_ACCESS_KEY_ID`, `ALIBABA_CLOUD_ACCESS_KEY_SECRET`, optional `ALIBABA_CLOUD_SECURITY_TOKEN` injected from a Secret with matching keys for CPFS. Never log config credentials. Use official Alibaba SDK or correctly signed RPC; no shells. DataFlow uses Import + MetaAndData + Directory + DstDirectory, maps CPFS path relative to dataFlow.fileSystemPath (MUST reject pvcPath outside linked root), CreateDirIfNotExist, deterministic ClientToken. Poll success and report partial failures; verify imported file bytes against expectedBytes before success. Actual CPFS integration is NOT claimed tested.

Local stage securely copies a relative directory from sourceRoot into cacheRoot/relativePath. Reject traversal, symlinks, special files and source tree mutations detectable during copy. Sum sizes BEFORE copying and require == expectedBytes. No shell, never delete source. Ownership marker in destination binds planUID+generation; stage retry can only touch a matching owned dir. Clean must verify owned marker before removing only that generation. Refuse symlink/path escapes. Never delete PVC or source. Do not rename a nonempty staging directory (CPFS constraint).

## Python SDK

Package `windowflow`, stdlib + optional `kubernetes` client dependency; version 0.1.0.
`WindowClient(plan_name, namespace, reader_id, mount_path, timeout=..., poll_interval=..., api=None)` supports dependency-injected CustomObjectsApi. Load in-cluster config with kubeconfig fallback ONLY if api absent.
`acquire(window_index)` waits for matching Ready slot, validates membership, creates lease, returns immutable handle (plan UID/index/id/generation/root). Validate CR data, root relativePath and nonnegative indices; path cannot escape mount. Pin UID on first contact and fail if same-name plan replaced. Lease conflicts require verifying tuple and released status. Never reacquire a released lease. `release(handle)` is explicit and idempotent, patches released=true with resourceVersion conflict handling. No __exit__ auto-release on exceptions. No blanket retries for RBAC/validation errors. A timeout never means release.
`iter_windows` optional; do not auto-release merely when iterator yields a handle. Training example drains workers/decoder before release; sample progress is stored in model checkpoint, not inferred from lease.
SDK tests use fake API; no real cluster needed. Megatron example must use DP rank, not global rank, and is an integration sketch, not an advertised production adapter.

## Scope and honesty

Working v0.1 reference implementation, not certified for PB workloads or PPU. Tests: Go unit and Kubernetes API/envtest if runtime obtainable, Python unit, local worker end-to-end (at least 3 windows over 2 slots). No credentials committed. No MONAI source copied; credit design inspiration. Raw videos supported by directory-based windows; arbitrary CSV manifests, auto source inventory, global random sampling, multi-job cache sharing, elastic training and exactly reproducible Megatron checkpoint integration are out of v0.1 scope. Source data must remain immutable. capacityBytes is logical reserved file bytes, not a physical filesystem quota; CPFS must have independent free-space margin. Need real ACK/CPFS smoke test before production.
