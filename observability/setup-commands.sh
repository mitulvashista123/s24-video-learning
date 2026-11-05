#!/usr/bin/env bash
# Observability stack for S24 on EKS:
#   * kube-prometheus-stack : Prometheus, Alertmanager, Grafana, node-exporter, kube-state-metrics
#   * Loki (single binary)   : centralised log store, gateway exposed on an internal AWS LB
#                              so the Flink job on EMR can push its logs too
#   * Grafana Alloy          : collects logs of every pod in the cluster and ships them to Loki
#
# Prerequisite: kubeconfig pointing at the cluster:
#   $(terraform -chdir=terraform output -raw kubeconfig_command)
set -euo pipefail

NAMESPACE=monitoring
DIR="$(cd "$(dirname "$0")" && pwd)"

KPS_VERSION="${KPS_VERSION:-65.1.1}"
LOKI_VERSION="${LOKI_VERSION:-6.16.0}"
ALLOY_VERSION="${ALLOY_VERSION:-0.9.1}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-s24-grafana-admin}"

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo ">> [1/3] Prometheus + Grafana (kube-prometheus-stack ${KPS_VERSION})"
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace "$NAMESPACE" --version "$KPS_VERSION" \
  --values "$DIR/values/kube-prometheus-stack.yaml" \
  --set grafana.adminPassword="$GRAFANA_ADMIN_PASSWORD" \
  --wait --timeout 15m

echo ">> [2/3] Loki (${LOKI_VERSION})"
helm upgrade --install loki grafana/loki \
  --namespace "$NAMESPACE" --version "$LOKI_VERSION" \
  --values "$DIR/values/loki.yaml" \
  --wait --timeout 15m

echo ">> [3/3] Grafana Alloy log collector (${ALLOY_VERSION})"
helm upgrade --install alloy grafana/alloy \
  --namespace "$NAMESPACE" --version "$ALLOY_VERSION" \
  --values "$DIR/values/alloy.yaml" \
  --wait --timeout 10m

echo ">> waiting for the internal load balancer of the Loki gateway"
for _ in $(seq 1 60); do
  LOKI_HOST="$(kubectl -n "$NAMESPACE" get svc loki-gateway -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
  [[ -n "$LOKI_HOST" ]] && break
  sleep 10
done

GRAFANA_HOST="$(kubectl -n "$NAMESPACE" get svc kube-prometheus-stack-grafana -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"

cat <<MSG

Observability stack installed.

  Grafana  : http://${GRAFANA_HOST:-<pending>}   (admin / ${GRAFANA_ADMIN_PASSWORD})
             dashboard "S24 Online Video Learning – Overview" is provisioned by ArgoCD
             (ConfigMap s24-grafana-dashboard, label grafana_dashboard=1)
  Prometheus: kubectl -n ${NAMESPACE} port-forward svc/kube-prometheus-stack-prometheus 9090
  Loki push : http://${LOKI_HOST:-<pending>}/loki/api/v1/push

Ship the Flink (EMR) logs to the same Loki:
  terraform -chdir=terraform apply -var "loki_push_url=http://${LOKI_HOST:-<pending>}/loki/api/v1/push" \\
            -var "gitops_repo_url=<your repo url>"
  (the EMR bootstrap action then installs Grafana Alloy on the Flink nodes; bootstrap actions
   run at cluster creation, so taint & re-create the cluster:
   terraform -chdir=terraform apply -replace=aws_emr_cluster.flink ...)
MSG
