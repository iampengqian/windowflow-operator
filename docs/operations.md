# Operations

This runbook covers the experimental v0.1 implementation. Start with expendable local data, then run a small real CPFS test. No local unit or API test establishes PB-scale throughput, PPU compatibility, or provider correctness.

## Storage preparation

The plan, its PVCs, credential Secret, worker Jobs, and reader leases belong to the same namespace. Use `default` for the provided samples.

Provision PVCs appropriate to the environment before applying a plan:

| PVC | Local backend | CPFS backend |
| --- | --- | --- |
| Cache | `training-cache`; writable by workers; read-only for readers | Existing CPFS volume; writable by workers and readable by training |
| Source | `source-data`; distinct PVC mounted read-only by stage Jobs | OSS source associated with the existing data flow; no source PVC |

For multi-node readers, confirm the storage supports the required concurrent mounts. Requesting a PVC access mode does not create storage-provider support. Keep independent free-space headroom for metadata, filesystem allocation, ownership markers, checkpoints, and unrelated files.

Workers run as UID/GID `1000:1000`. Prepare directory permissions through your storage provisioner or an authorized administrative setup step. Do not recursively change ownership of an existing shared dataset without reviewing its other users. A writable container mount alone does not prove filesystem permission.

For every source window, inventory regular-file content sizes and set the exact sum in `expectedBytes`. The local backend rejects symlinks and special files. Freeze source contents for the entire plan attempt; the copy checks do not make mutable sources safe.

## Deployment and image access

```sh
make test
make manifests
make build
docker build -t ghcr.io/iampengqian/windowflow-operator:v0.1.0 .
kubectl apply -k config/default
```

Before the last command, make the image available to every node that may run the controller or a worker. For a local kind cluster, `kind load docker-image ghcr.io/iampengqian/windowflow-operator:v0.1.0` loads the build. For other clusters, use a registry and any necessary pull credentials. The documented GHCR tag is a planned release location, not proof that a release exists.

The controller and worker images are separate configuration references even though they use the same binary. Update the controller deployment and `spec.workerImage` together for a new installation. An existing plan's spec cannot be edited to change its image.

Check the applied Deployment, Pod events, and image-pull status before submitting a plan. If `kubectl apply -k config/default` fails because CRDs or RBAC cannot be created, resolve the cluster permission issue rather than granting training Pods cluster-admin.

## Training service account

The Python SDK needs to read `windowplans` and create/read/patch `windowleases` in the plan namespace. It does not need Secret read access, worker Job creation, or permission to delete plans/PVCs.

For a training ServiceAccount named `windowflow-reader` in `default`, the following is a namespace-scoped starting point:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: windowflow-reader
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: windowflow-reader
  namespace: default
rules:
  - apiGroups: [data.windowflow.io]
    resources: [windowplans]
    verbs: [get]
  - apiGroups: [data.windowflow.io]
    resources: [windowleases]
    verbs: [get, create, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: windowflow-reader
  namespace: default
subjects:
  - kind: ServiceAccount
    name: windowflow-reader
    namespace: default
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: windowflow-reader
```

Set `serviceAccountName: windowflow-reader` on the training Pod. The SDK loads in-cluster credentials, with kubeconfig fallback when used outside a Pod; a dependency-injected API client can be used for tests.

RBAC here scopes resource access, not individual reader identities. Do not place mutually untrusted training clients in one namespace with broad lease write access and assume a `readerID` is an authenticated identity. A false release can authorize deletion. Use trusted clients and appropriate namespace/access isolation.

## Local smoke test

1. Prepare `source-data` and `training-cache`, permissions, and the source directories declared by [the local sample](../config/samples/local-plan.yaml).
2. Apply the sample with `kubectl apply -f config/samples/local-plan.yaml`.
3. Start SDK clients for both `dp0` and `dp1`, with the cache PVC mounted at `/cache`.
4. Acquire window 0 from both clients and check file contents. Release only `dp0`: window 0 must remain present. Release `dp1` only after both readers have stopped all reads.
5. Consume all three windows. With two slots, verify that a cleaned slot is reused for a later window, that old generation directories disappear only after all releases, and that source files remain unchanged.
6. Wait for `Completed` and verify generation directories are cleaned and the PVC lock has been released.

This exercises storage coordination. It does not train a model or validate decoder throughput. Stop on unexpected cleanup or identity behavior; do not continue to a valuable dataset.

## CPFS setup

Create and test the CPFS filesystem mount and OSS data flow separately. The backend expects an existing association; it does not provision Alibaba Cloud infrastructure. Use [Alibaba Cloud's data-flow documentation](https://help.aliyun.com/zh/cpfs/bmcpfs/user-guide/create-and-manage-data-flows) to verify product support, permissions, path rules, and current provider limits for your environment.

Edit [the CPFS sample](../config/samples/cpfs-plan.yaml) before applying it:

| Field | Meaning |
| --- | --- |
| `dataFlow.region` | Region containing the configured CPFS data flow |
| `fileSystemId`, `dataFlowId` | Existing provider resource IDs |
| `fileSystemPath` | CPFS filesystem root associated with the data flow |
| `pvcPath` | Actual CPFS filesystem directory exposed as the cache PVC root |
| `credentialsSecret` | Secret in the plan namespace |
| `windows[].source` | Relative directory beneath the data-flow source |
| `windows[].expectedBytes` | Exact regular-file content bytes expected for this window |

For example, if the data-flow root is `/datasets/` and the PVC exposes `/datasets/cache/`, the generated filesystem destination belongs under `/datasets/cache/windowflow/<plan UID>/<generation>/`. Do not set `pvcPath: /cache` merely because that is the Pod mount point. The worker maps the destination relative to the data-flow root and refuses a PVC outside it.

Create credentials from securely prepared local files so secret values are not placed in shell history. Replace the paths below with files containing only the corresponding value:

```sh
kubectl create secret generic aliyun-dataflow -n default \
  --from-file=ALIBABA_CLOUD_ACCESS_KEY_ID=/secure/path/access-key-id \
  --from-file=ALIBABA_CLOUD_ACCESS_KEY_SECRET=/secure/path/access-key-secret
```

For temporary credentials, add `--from-file=ALIBABA_CLOUD_SECURITY_TOKEN=/secure/path/security-token`. Ensure validity covers the operation. The initial backend consumes configured credentials; do not assume automatic credential refresh. Do not attach Secret contents to issues or logs.

Use the minimum cloud permissions required by the existing data-flow import and polling operations. Verify that policy with a small import in your actual account. Training Pods should not receive these credentials.

Before real training, run a live smoke test with at least three small, immutable source directories and two cache slots. Confirm:

- Each destination is in the expected CPFS directory, including non-root data-flow/PVC mappings.
- Provider completion and expected bytes agree with mounted file contents; a partial or failed task does not become `Ready`.
- One unreleased reader prevents cleanup; complete release permits cleanup only of the matching generation.
- Cleanup leaves source OSS objects, unrelated CPFS files, and the PVC intact.
- A deliberately failed import freezes the plan and the recovery procedure preserves active data.

Record provider request/task IDs, region, filesystem configuration, image digest, source sizes, and actual results. A fake API test does not establish these properties against CPFS.

## Inspect a running plan

```sh
kubectl get windowplans,windowleases -n default
kubectl get windowplan local-demo -n default -o yaml
kubectl get jobs,pods -n default
kubectl get leases.coordination.k8s.io -n default -o yaml
```

Use `status.slots[].stageJob` and `cleanJob` to identify the relevant Job, then inspect that exact resource:

```sh
kubectl describe job JOB_NAME -n default
kubectl logs job/JOB_NAME -n default --all-containers=true
```

Check slot phase, window index, generation, reserved bytes, and reader lease tuples together. A released lease from another generation is not evidence that the current files can be deleted. Jobs have no active-plan TTL so their outcomes remain available to reconciliation and diagnosis.

## Failure and recovery

Recovery is manual in v0.1. A failed Job uses `backoffLimit: 0` and freezes the plan. Reapplying the same YAML or editing status is not a supported retry mechanism. Deleting a `WindowPlan` does not delete its data or its PVC lock.

Follow this order:

1. **Preserve evidence.** Save the plan/status, matching custom leases, relevant Job events/logs, plan UID, generation paths, PVC lock details, and any cloud task IDs. Do not export Secrets into the incident bundle.
2. **Stop training.** Terminate or quiesce all readers covered by the plan, including DataLoader workers, decoders, TP/PP participants, and asynchronous reads. Disable external job restart loops.
3. **Stop storage work.** Verify stage/clean Jobs and Pods are no longer modifying the affected directories. For CPFS, inspect the provider task and confirm it is terminal or successfully cancelled. Deleting a Pod does not prove the remote task stopped.
4. **Inspect owned output.** Verify each candidate generation path and its ownership marker against the saved plan UID/generation. Confirm no other reader relies on it. If ownership is missing or inconsistent, leave it in place for investigation. Never delete the PVC, source directory, or a broad shared parent directory as a shortcut.
5. **Clean the verified generations.** Use the worker's ownership-checked cleanup path under administrator control, with a configuration matching the saved identity. The JSON worker contract is documented in [CONTRACT.md](../CONTRACT.md). Recheck source and unrelated files after cleanup.
6. **Release the PVC lock last.** After preserving evidence and verifying cleanup, remove the old plan and confirm its workloads cannot resume. Identify the exact `coordination.k8s.io` Lease belonging to the affected PVC/plan and delete only that lock. Do not delete all Lease objects in the namespace. Do not remove the lock while the old attempt can still resume work.
7. **Create a new plan attempt.** Use a new name and fresh plan UID, fixed reader membership, and an explicitly chosen training restart point. Start readers only after the old attempt is fully fenced off.

There is no claim that this procedure reconstructs a Megatron checkpoint. If a checkpoint needs a window already released by the old attempt, restage the required data through a new plan. Do not mutate old leases to `released: false`.

## Common symptoms

| Symptom | What to check |
| --- | --- |
| Plan cannot acquire its PVC | Existing plan or retained lock; investigate its ownership before recovery. |
| Slot stays `Loading` | Job scheduling, image pull, PVC mount/permissions, source size, provider task status. |
| Slot stays `Ready` after training | Every declared reader must have a released lease matching this UID/index/generation. |
| SDK cannot acquire | Namespace/plan identity, reader membership, RBAC, slot readiness, timeout, or previously released lease. |
| Later window does not stage | Slot/byte reservation still held, earlier failure, or all-reader release not complete. |
| `Completed` but physical use is higher than expected | Other PVC contents, filesystem accounting, metadata, and storage-provider behavior; logical capacity is not quota. |

## Upgrade and uninstall

There is no promised in-place migration compatibility for this experimental API. Finish active plans where possible and preserve CR/status/checkpoint evidence before changing controller or CRD versions. Validate a replacement build with new expendable plans before resuming valuable workloads.

Deleting the controller deployment stops orchestration; it does not safely stop training or cloud imports. Do not remove CRDs while active or failed plans still require recovery, because that discards the resource records needed to reason about ownership. Recover or complete each plan first, retain evidence, and only then remove the installation according to your cluster policy.

## Additional v0.1 limits

- Cache PVC exclusivity is by **namespace and PVC name**. Do not create aliases of the same filesystem path through multiple PVCs or expose an active cache to another WindowFlow installation. The lock cannot detect those aliases.
- Mount training volumes read-only. Use one live process per reader ID; reattaching an unreleased lease is allowed only after the previous process has been fenced off. This SDK is not a process-fencing service.
- For checkpoint recovery, set a new plan's `startWindow` to the checkpoint ordinal and start readers at the same index. Keep the full window schedule and restore the within-window sampler offset separately. Earlier ordinals will not stage or need releases.
- A window is one source directory/prefix. Repeated passes can use repeated sources with distinct window IDs. No implicit infinite cycling occurs.
- Source names beginning `.windowflow-` are reserved. Do not enable an independent automatic import/export or synchronization process against these cache directories.
- This CPFS adapter requests `CreateDirIfNotExist=true`, so its target is CPFS 智算版 **2.6.0+**, and it requires filesystem support for advisory `flock`. Verify both in the actual deployment.
- The worker writes an import intent and task-ID receipt beside the generation directory. A receipt with no task ID represents an ambiguous submission: inspect the provider; never erase the receipt to force an automatic resubmit. Completed receipts and small lock files remain for audit after data cleanup.
- A nonempty failed/skipped report link fails closed, even if the provider may have generated an empty report. Missing progress counters, unexpected response fields used for identity, and transient API errors also stop the worker. This conservative adapter favors manual diagnosis over availability. No cloud request cancellation or credential refresh is implemented.
- Local validation and cleanup currently retain O(file count) metadata in memory and walk whole directories. Benchmark representative video window sizes before choosing worker memory and time budgets; PB-scale throughput has not been measured. `expectedBytes` does not stop a provider from importing oversized/mutated input before the post-import check.
- A completed Job is retained for audit. Avoid external Job TTL/cleanup policies while its plan is active. The controller relies on Kubernetes terminal Job conditions (test targets 1.32 and 1.35).

API references: [CreateDataFlowTask](https://help.aliyun.com/zh/cpfs/bmcpfs/developer-reference/api-nas-2017-06-26-createdataflowtask-bmcpfs), [DescribeDataFlowTasks](https://help.aliyun.com/zh/cpfs/bmcpfs/developer-reference/api-nas-2017-06-26-describedataflowtasks-bmcpfs), and [NAS RPC signatures](https://help.aliyun.com/zh/nas/request-signatures).
