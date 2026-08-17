#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL="microk8s kubectl"
WATERMARK_FILE="$DIR/.failover-watermark.json"
CONNECT_POD="mm2-dr-to-primary-mirrormaker2-0"
CONNECTOR="dr-%3Eprimary.MirrorSourceConnector"
MANIFEST="$DIR/../strimzi/03-mirror/mm2-dr-to-primary.yaml"
NS="kafka-primary"

if [ ! -f "$WATERMARK_FILE" ]; then
  echo "No watermark file at $WATERMARK_FILE — run ./scripts/failover.sh first." >&2
  exit 1
fi

echo "== Starting fail-back resync: dr -> primary =="
READY=$($KUBECTL get kafka primary-cluster -n kafka-primary -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
if [ "$READY" != "True" ]; then
  echo "primary-cluster is not Ready yet (status: '${READY:-unknown}'). Aborting." >&2
  exit 1
fi

connector_state() {
  $KUBECTL exec "$CONNECT_POD" -n "$NS" -- \
    curl -s "http://localhost:8083/connectors/$CONNECTOR/status" 2>/dev/null \
    | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || echo ""
}

wait_for_state() {
  local want="$1"
  for i in $(seq 1 40); do
    STATE=$(connector_state)
    echo "  attempt $i: connector state=${STATE:-(not up yet)}"
    [ "$STATE" = "$want" ] && return 0
    sleep 5
  done
  return 1
}

# Kafka Connect always starts a newly-created connector RUNNING; Strimzi's
# `state: stopped` is only applied via a *follow-up* REST call once it
# reconciles, so there's an unavoidable window right after creation where the
# connector is genuinely live. If it were pointed at drc-demo-topic during
# that window, it would replay dr's entire backlog — including the old
# primary-origin data already mirrored there before failover — straight back
# into primary. See docs/dr-runbook.md for how this was found.
#
# The fix: bring the connector up against a topic pattern that matches
# nothing real ("^$"), so it's harmless no matter how long that window is.
# Only once it's confirmed STOPPED do we correct its offsets and switch it
# to the real topic pattern.
echo "Phase 1/3: bringing up the connector against a no-op topic filter (safe regardless of timing)..."
sed 's/topicsPattern: "drc-demo-topic"/topicsPattern: "^$"/' "$MANIFEST" | $KUBECTL apply -f -

for i in $(seq 1 40); do
  R=$($KUBECTL get kafkamirrormaker2 mm2-dr-to-primary -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
  [ "$R" = "True" ] && break
  sleep 5
done

echo "Waiting for the source connector to settle into STOPPED..."
wait_for_state STOPPED || { echo "Source connector never reached STOPPED — aborting before touching offsets." >&2; exit 1; }

echo
echo "Phase 2/3: correcting starting offsets to the failover watermark"
echo "(this is safe now: the connector has never matched a real topic yet):"
cat "$WATERMARK_FILE"
$KUBECTL exec "$CONNECT_POD" -n "$NS" -- \
  curl -s -X PATCH "http://localhost:8083/connectors/$CONNECTOR/offsets" \
    -H "Content-Type: application/json" \
    -d "$(cat "$WATERMARK_FILE")"
echo

echo
echo "Phase 3/3: switching to the real topic filter and resuming..."
$KUBECTL apply -f "$MANIFEST"
sleep 5
STATE=$(connector_state)
if [ "$STATE" != "STOPPED" ]; then
  echo "connector state is '$STATE' after the config switch, not STOPPED as expected — stopping it again before resuming, to be safe."
  $KUBECTL exec "$CONNECT_POD" -n "$NS" -- curl -s -X PUT "http://localhost:8083/connectors/$CONNECTOR/stop" -o /dev/null
  sleep 3
fi

echo "Setting the connector's desired state to running (both live, via the REST API,"
echo "and in the CR, so the operator doesn't stop it again on its next reconcile)..."
$KUBECTL exec "$CONNECT_POD" -n "$NS" -- \
  curl -s -X PUT "http://localhost:8083/connectors/$CONNECTOR/resume" -o /dev/null -w "resume HTTP:%{http_code}\n"
$KUBECTL patch kafkamirrormaker2 mm2-dr-to-primary -n "$NS" --type=json \
  -p '[{"op":"replace","path":"/spec/mirrors/0/sourceConnector/state","value":"running"}]'

echo
echo "Resync running. Watch it catch up by comparing the latest offset on each side:"
echo "  microk8s kubectl exec dr-cluster-dr-pool-0 -n kafka-dr -- \\"
echo "    bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic drc-demo-topic"
echo "  microk8s kubectl exec primary-cluster-primary-pool-0 -n kafka-primary -- \\"
echo "    bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic drc-demo-topic"
echo
echo "Once every partition's offsets match, run ./scripts/complete-failback.sh."
