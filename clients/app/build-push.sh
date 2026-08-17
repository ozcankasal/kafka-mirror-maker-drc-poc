#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="localhost:32000/kafka-mirror-client:latest"

docker build -t "$IMAGE" "$DIR"
docker push "$IMAGE"
