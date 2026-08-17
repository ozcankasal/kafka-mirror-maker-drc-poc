#!/usr/bin/env bash
set -euo pipefail

KUBECTL="microk8s kubectl"
STATE_FILE="$(dirname "$0")/.original-replicas"

# format: namespace:name:known-original-replicas
# (the "known original" is recorded here rather than read live, because these
# deployments may already be sitting at 0 from an earlier PoC session)
TARGETS=(
  "audit-demo:audit-service:1"
  "audit-demo:echo-app:1"
  "audit-demo:waypoint:1"
  "default:mcp-server:1"
  "default:wasm-server:1"
)

echo "Scaling down unrelated deployments to free up resources for the Kafka DR PoC..."
: > "$STATE_FILE"
for target in "${TARGETS[@]}"; do
  ns="$(echo "$target" | cut -d: -f1)"
  name="$(echo "$target" | cut -d: -f2)"
  original="$(echo "$target" | cut -d: -f3)"
  if ! $KUBECTL get deployment "$name" -n "$ns" >/dev/null 2>&1; then
    echo "  skipping: $ns/$name not found"
    continue
  fi
  echo "$ns:$name:$original" >> "$STATE_FILE"
  echo "  $ns/$name: -> 0 (restores to $original)"
  $KUBECTL scale deployment "$name" -n "$ns" --replicas=0
done

echo "Done. Original replica counts saved to $STATE_FILE."
echo "To restore: cluster-prep/restore-unrelated.sh"
