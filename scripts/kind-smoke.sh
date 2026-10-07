#!/usr/bin/env bash
# A disposable, CPU-only integration smoke test; no cloud credentials or GPUs.
set -euo pipefail
cd "$(dirname "$0")/.."
for required_tool in python3 kind docker kubectl; do
  if ! command -v "$required_tool" >/dev/null 2>&1; then
    echo "Required command not found: $required_tool" >&2
    exit 1
  fi
done
cluster=windowflow-smoke
if kind get clusters | grep -qx "$cluster"; then
  echo "Refusing to replace existing cluster $cluster" >&2
  exit 1
fi
smoke_dir=$(mktemp -d "${TMPDIR:-/tmp}/windowflow-smoke.XXXXXX")
export KUBECONFIG="$smoke_dir/kubeconfig"
cluster_created=0
finish() {
  result=$?
  set +e
  if [ "$result" -ne 0 ] && [ "$cluster_created" = 1 ]; then
    kubectl get pods,jobs,windowplans,windowleases -A -o wide || true
    kubectl describe pods -n default || true
    kubectl logs -n windowflow-system deployment/windowflow-controller || true
    for job in $(kubectl get jobs -o name); do kubectl logs "$job" --all-containers=true || true; done
  fi
  if [ "$cluster_created" = 1 ] && [ "${KEEP_CLUSTER:-0}" != 1 ]; then kind delete cluster --name "$cluster"; fi
  # Remove only this invocation's known files; never recursively delete TMPDIR.
  rm -f -- "$smoke_dir/kubeconfig" "$smoke_dir/plan.json" "$smoke_dir/schedule.json"
  rmdir -- "$smoke_dir"
  exit "$result"
}
trap finish EXIT
PYTHONPATH="$PWD/sdk/python" python3 -m windowflow.plan_cli \
  --catalog examples/window-catalog.json --template examples/plan-template.json \
  --output "$smoke_dir/plan.json" --schedule-output "$smoke_dir/schedule.json" \
  --seed 42 --num-cycles 3 --shuffle-windows
docker build -t ghcr.io/iampengqian/windowflow-operator:v0.1.0 .
docker build -f examples/Dockerfile.reader -t windowflow-reader:smoke .
cluster_created=1
kind create cluster --name "$cluster" --image kindest/node:v1.32.2 --wait 120s
kind load docker-image --name "$cluster" ghcr.io/iampengqian/windowflow-operator:v0.1.0 windowflow-reader:smoke
kubectl apply -k config/default
kubectl wait --for=condition=Established crd/windowplans.data.windowflow.io crd/windowleases.data.windowflow.io --timeout=90s
kubectl rollout status -n windowflow-system deployment/windowflow-controller --timeout=120s
kubectl apply -f config/samples/kind-storage.yaml
kubectl wait --for=condition=Complete job/windowflow-prepare --timeout=120s
kubectl apply -f config/samples/reader-rbac.yaml
kubectl apply -f "$smoke_dir/plan.json"
# Before any readers run, ordinal 0 occupies the first slot and cannot be
# reclaimed. This gates reader startup on real controller/worker prewarming.
kubectl wait --for=jsonpath='{.status.slots[0].phase}'=Ready windowplan/generated-local-demo --timeout=180s
test "$(kubectl get windowplan generated-local-demo -o jsonpath='{.status.slots[0].windowIndex}')" = 0
kubectl apply -f config/samples/kind-readers.yaml
kubectl wait --for=condition=Complete job/windowflow-readers --timeout=1200s
kubectl wait --for=jsonpath='{.status.phase}'=Completed windowplan/generated-local-demo --timeout=120s
test "$(kubectl get windowplan generated-local-demo -o jsonpath='{.status.completedWindows}')" = 9
test "$(kubectl get jobs -l app.kubernetes.io/name=windowflow-worker -o name | wc -l | tr -d ' ')" = 18
kubectl logs job/windowflow-readers --all-pods=true --prefix=true
echo "PASS: two WindowRunner readers completed two epochs per visit across nine visits (three catalog cycles), two slots, and eighteen real stage/clean Jobs."
