#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL="microk8s kubectl"
WATERMARK_FILE="$DIR/.failover-watermark.json"
DR_POD="dr-cluster-dr-pool-0"
TOPIC="drc-demo-topic"

echo "== Simulating a disaster: primary is declared lost, dr is promoted to active =="

echo "Stopping primary -> dr mirroring (source is being failed over; leaving it running"
echo "would let writes made on dr after failover get mirrored back into a dead cluster,"
echo "and blocks the reverse leg from being started safely later)..."
$KUBECTL delete -f "$DIR/../strimzi/03-mirror/mm2-primary-to-dr.yaml" --ignore-not-found

echo "Recording dr's per-partition end offsets right now — this is the watermark"
echo "fail-back will use later to mirror back only what's written to dr AFTER this point,"
echo "instead of re-mirroring dr's existing copy of primary's own pre-failover data."
OFFSETS=$($KUBECTL exec "$DR_POD" -n kafka-dr -- \
  bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC")
echo "$OFFSETS" | awk -F: -v topic="$TOPIC" '
  BEGIN { printf "{\"offsets\":[" }
  { if (NR>1) printf ","; printf "{\"partition\":{\"cluster\":\"dr\",\"partition\":%s,\"topic\":\"%s\"},\"offset\":{\"offset\":%s}}", $2, topic, $3 }
  END { printf "]}\n" }
' > "$WATERMARK_FILE"
echo "Watermark saved to $WATERMARK_FILE:"
cat "$WATERMARK_FILE"

$KUBECTL patch configmap active-cluster-config -n kafka-clients \
  --type merge -p '{"data":{"ACTIVE_CLUSTER":"dr"}}'

echo
echo "ACTIVE_CLUSTER set to dr."
echo "Note: kubelet's ConfigMap volume sync can take up to ~60s; the producer/consumer"
echo "pick up the new cluster and reconnect without a restart."
echo
echo "Next: ./status.sh to watch the cutover, then ./scripts/start-failback-resync.sh"
echo "once primary is healthy again."
