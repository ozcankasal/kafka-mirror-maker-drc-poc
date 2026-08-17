#!/usr/bin/env bash
set -euo pipefail

KUBECTL="microk8s kubectl"
PRIMARY_POD="primary-cluster-primary-pool-0"
DR_POD="dr-cluster-dr-pool-0"

echo "== Active cluster =="
$KUBECTL get configmap active-cluster-config -n kafka-clients -o jsonpath='{.data.ACTIVE_CLUSTER}'
echo
echo

echo "== Primary cluster broker pods (kafka-primary) =="
$KUBECTL get pods -n kafka-primary -l strimzi.io/cluster=primary-cluster

echo
echo "== DR cluster broker pods (kafka-dr) =="
$KUBECTL get pods -n kafka-dr -l strimzi.io/cluster=dr-cluster

echo
echo "== Producer/consumer pods (kafka-clients) =="
$KUBECTL get pods -n kafka-clients -l 'app in (producer,consumer)'

echo
echo "== MirrorMaker 2 instances =="
$KUBECTL get kafkamirrormaker2 -n kafka-dr 2>&1 || true
$KUBECTL get kafkamirrormaker2 -n kafka-primary 2>&1 || true

echo
echo "== MirrorMaker 2 connector states (source/checkpoint, embedded in the CR status) =="
$KUBECTL get kafkamirrormaker2 -n kafka-dr -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.connectors[*]}  {.name}: {.connector.state}{"\n"}{end}{end}' 2>&1 || true
$KUBECTL get kafkamirrormaker2 -n kafka-primary -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.connectors[*]}  {.name}: {.connector.state}{"\n"}{end}{end}' 2>&1 || true

echo
echo "== Consumer group lag: cg-drc on primary =="
$KUBECTL exec "$PRIMARY_POD" -n kafka-primary -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cg-drc 2>&1 || true

echo
echo "== Consumer group lag: cg-drc on dr =="
$KUBECTL exec "$DR_POD" -n kafka-dr -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cg-drc 2>&1 || true
