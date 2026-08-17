#!/usr/bin/env bash
set -euo pipefail

KUBECTL="microk8s kubectl"
STATE_FILE="$(dirname "$0")/.original-replicas"

if [ ! -f "$STATE_FILE" ]; then
  echo "State file not found: $STATE_FILE (scale-down-unrelated.sh may never have been run)"
  exit 1
fi

while IFS=: read -r ns name replicas; do
  [ -z "$ns" ] && continue
  echo "  $ns/$name: 0 -> $replicas"
  $KUBECTL scale deployment "$name" -n "$ns" --replicas="$replicas"
done < "$STATE_FILE"

echo "Done."
