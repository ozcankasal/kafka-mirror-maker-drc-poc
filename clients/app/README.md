# clients/app

Simple Python (confluent-kafka) producer and consumer scripts. Both live in the same image; which role runs is decided by the `command` in the k8s Deployment.

## Active/passive logic

Unlike `kafka-multi` (one cluster, clients that pick *which zone's identity to use*), here the two clusters are genuinely separate — so the client has to pick *which cluster to connect to*, and reconnect when that changes.

- `ACTIVE_CLUSTER_FILE` (default `/etc/active-cluster/ACTIVE_CLUSTER`) — a file mounted from the `active-cluster-config` ConfigMap, containing a single line, `primary` or `dr`.
- `PRIMARY_BOOTSTRAP_SERVERS` / `DR_BOOTSTRAP_SERVERS` — fixed env vars pointing at each cluster's bootstrap service.
- The file is re-read on every loop iteration (kubelet syncs ConfigMap volumes roughly every ~60s, no restart required).
- `producer.py`: produces to whichever cluster `ACTIVE_CLUSTER` currently names, rebuilding its `Producer` (new `bootstrap.servers`) whenever that value changes.
- `consumer.py`: consumes from whichever cluster `ACTIVE_CLUSTER` currently names, closing and rebuilding its `Consumer` (leaving the old cluster's group, joining the new cluster's group) whenever that value changes. Because `mm2-primary-to-dr`'s `MirrorCheckpointConnector` mirrors `cg-drc`'s offsets, the consumer picks up close to where it left off after a failover, rather than replaying from the beginning.

## Build & push

The registry addon runs on `localhost:32000` in microk8s:

```bash
./build-push.sh
```

## Environment variables

| Variable | Description |
|---|---|
| `PRIMARY_BOOTSTRAP_SERVERS` | required, e.g. `primary-cluster-kafka-bootstrap.kafka-primary.svc:9092` |
| `DR_BOOTSTRAP_SERVERS` | required, e.g. `dr-cluster-kafka-bootstrap.kafka-dr.svc:9092` |
| `TOPIC` | default `drc-demo-topic` |
| `CONSUMER_GROUP` | default `cg-drc` (consumer only) |
| `ACTIVE_CLUSTER_FILE` | default `/etc/active-cluster/ACTIVE_CLUSTER` |
| `PRODUCE_INTERVAL_SECONDS` / `POLL_INTERVAL_SECONDS` | default `1` |
| `STANDBY_LOG_EVERY_SECONDS` | default `10` |
