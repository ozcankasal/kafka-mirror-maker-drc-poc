#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$DIR/../app"

microk8s kubectl create configmap client-app-code \
  --namespace kafka-clients \
  --from-file="$APP_DIR/common.py" \
  --from-file="$APP_DIR/producer.py" \
  --from-file="$APP_DIR/consumer.py" \
  --dry-run=client -o yaml | cat > "$DIR/app-code-configmap.yaml"

echo "$DIR/app-code-configmap.yaml updated."
