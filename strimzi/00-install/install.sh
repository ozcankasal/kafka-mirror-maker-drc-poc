#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL="microk8s kubectl"

$KUBECTL apply -f "$DIR/strimzi-crds-1.1.0.yaml"
$KUBECTL apply -f "$DIR/strimzi-cluster-operator-1.1.0.yaml" -n kafka-primary
$KUBECTL apply -f "$DIR/strimzi-cluster-operator-dr-rbac.yaml"

echo "Waiting for the operator to become available..."
$KUBECTL wait deployment/strimzi-cluster-operator -n kafka-primary --for=condition=Available --timeout=180s
$KUBECTL get pods -n kafka-primary
