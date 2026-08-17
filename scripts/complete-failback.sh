#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL="microk8s kubectl"

echo "== Completing fail-back: primary becomes active again =="

$KUBECTL patch configmap active-cluster-config -n kafka-clients \
  --type merge -p '{"data":{"ACTIVE_CLUSTER":"primary"}}'
echo "ACTIVE_CLUSTER set to primary."

echo "Removing the dr -> primary resync leg (its job is done)..."
$KUBECTL delete -f "$DIR/../strimzi/03-mirror/mm2-dr-to-primary.yaml" --ignore-not-found

echo "Restoring steady-state mirroring: primary -> dr..."
$KUBECTL apply -f "$DIR/../strimzi/03-mirror/mm2-primary-to-dr.yaml"

rm -f "$DIR/.failover-watermark.json"

echo
echo "Fail-back complete. primary is active, dr is on standby, mirroring runs primary -> dr again."
echo "Run ./status.sh to confirm."
