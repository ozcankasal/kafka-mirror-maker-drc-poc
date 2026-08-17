# strimzi/01-clusters

Two genuinely independent KRaft Kafka 4.3.0 clusters — not two zones of one cluster (see [`../../docs/architecture.md`](../../docs/architecture.md) for how this differs from the earlier `kafka-multi` stretched-cluster PoC):

| Cluster | Namespace | Nodes | Role |
|---|---|---|---|
| `primary-cluster` | `kafka-primary` | 3 (broker+controller, combined) | Normally active |
| `dr-cluster` | `kafka-dr` | 3 (broker+controller, combined) | Standby, kept warm by [MirrorMaker 2](../03-mirror) |

Each cluster is its own `KafkaNodePool` + `Kafka` custom resource, with its own KRaft controller quorum, its own storage, its own topic metadata. Nothing is shared between them except the data MirrorMaker 2 copies across.

3 nodes per cluster (not 6 as in the stretched-cluster PoC) is also an **odd-sized KRaft quorum** — the "recommended odd number of controller nodes" warning that `kafka-multi` deliberately accepted doesn't apply here.

## Replication

`default.replication.factor=3`, `min.insync.replicas=2` on both clusters — normal in-cluster replication, tolerating the loss of one broker within a cluster. This is orthogonal to the mirroring between clusters, which is what actually provides the DR value here.

## Apply

```bash
microk8s kubectl apply -f primary-pool.yaml -f primary-cluster.yaml
microk8s kubectl apply -f dr-pool.yaml -f dr-cluster.yaml
microk8s kubectl get kafka -n kafka-primary -w
microk8s kubectl get kafka -n kafka-dr -w
```
