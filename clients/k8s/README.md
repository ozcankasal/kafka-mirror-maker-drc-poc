# clients/k8s

Producer/consumer Deployments and the ConfigMap that carries the `ACTIVE_CLUSTER` parameter, all in the `kafka-clients` namespace.

One producer, one consumer — not one pair per zone/cluster like `kafka-multi`. Since the two Kafka clusters here are genuinely separate (different bootstrap addresses), a single client that reconnects to whichever cluster is active is both simpler and more realistic: it's what a real application's client library does during a DR failover.

## Why there's no custom image

Same constraint as `kafka-multi`: no `docker`/`buildah`/`nerdctl` in this environment, and `microk8s ctr` requires interactive `sudo`. The application code lives in a `ConfigMap` (`client-app-code`) mounted at `/app`; the container uses the stock `python:3.12-slim` image and `pip install`s `confluent-kafka` on startup (see `args` in `producer.yaml`/`consumer.yaml`). `clients/app/Dockerfile` + `build-push.sh` are the real/production path, kept in the repo for when a registry push is possible.

The `client-app-code` ConfigMap isn't edited by hand — regenerate it from `clients/app/*.py` whenever the source changes:

```bash
./generate-app-configmap.sh
microk8s kubectl apply -f app-code-configmap.yaml
microk8s kubectl rollout restart deploy/producer deploy/consumer -n kafka-clients
```

## Resources

| File | Contents |
|---|---|
| `active-cluster-configmap.yaml` | The `ACTIVE_CLUSTER` parameter (starts as `primary`) |
| `app-code-configmap.yaml` | (generated, see above) producer/consumer/common Python code |
| `producer.yaml`, `consumer.yaml` | The two client Deployments |

## Verified behavior

```
producer: produced cluster=primary partition=... offset=...
consumer: consumed cluster=primary partition=... produced_by=primary ...
```

Failover / fail-back: [`../../scripts/`](../../scripts) and [`../../docs/dr-runbook.md`](../../docs/dr-runbook.md).
