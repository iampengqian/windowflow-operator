#!/usr/bin/env bash
# A disposable, CPU-only integration smoke test; no cloud credentials or GPUs.
set -euo pipefail
cd "$(dirname "$0")/.."
cluster=windowflow-smoke
if kind get clusters | grep -qx "$cluster"; then
  echo "Refusing to replace existing cluster $cluster" >&2
  exit 1
fi
docker build -t ghcr.io/iampengqian/windowflow-operator:v0.1.0 .
docker build -f examples/Dockerfile.reader -t windowflow-reader:smoke .
export KUBECONFIG
KUBECONFIG=$(mktemp)
finish() {
  result=$?
  if [ "$result" -ne 0 ]; then
    kubectl get pods,jobs,windowplans,windowleases -A -o wide || true
    kubectl describe pods -n default || true
    kubectl logs -n windowflow-system deployment/windowflow-controller || true
    for job in $(kubectl get jobs -o name); do kubectl logs "$job" --all-containers=true || true; done
  fi
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then kind delete cluster --name "$cluster"; fi
  rm -f "$KUBECONFIG"
  exit "$result"
}
trap finish EXIT
kind create cluster --name "$cluster" --image kindest/node:v1.32.2 --wait 120s
kind load docker-image --name "$cluster" ghcr.io/iampengqian/windowflow-operator:v0.1.0 windowflow-reader:smoke
kubectl apply -k config/default
kubectl wait --for=condition=Established crd/windowplans.data.windowflow.io crd/windowleases.data.windowflow.io --timeout=90s
kubectl rollout status -n windowflow-system deployment/windowflow-controller --timeout=120s
kubectl apply -f config/samples/kind-storage.yaml
kubectl wait --for=condition=Complete job/windowflow-prepare --timeout=120s
kubectl apply -f config/samples/reader-rbac.yaml
kubectl apply -f config/samples/local-plan.yaml
kubectl apply -f config/samples/kind-readers.yaml
kubectl wait --for=condition=Complete job/windowflow-readers --timeout=600s
kubectl wait --for=jsonpath='{.status.phase}'=Completed windowplan/local-demo --timeout=120s
test "$(kubectl get windowplan local-demo -o jsonpath='{.status.completedWindows}')" = 3
test "$(kubectl get jobs -l app.kubernetes.io/name=windowflow-worker -o name | wc -l | tr -d ' ')" = 6
kubectl logs job/windowflow-readers --all-pods=true --prefix=true
echo "PASS: two SDK readers consumed three windows through two slots; six real worker Jobs completed."
