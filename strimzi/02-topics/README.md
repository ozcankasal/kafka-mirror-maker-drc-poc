# strimzi/02-topics

`drc-demo-topic` is only declared once, on the primary cluster: 3 partitions, RF=3, `min.insync.replicas=2`.

There is deliberately **no `KafkaTopic` manifest for the DR cluster**. `drc-demo-topic` gets created there automatically by the [`mm2-primary-to-dr`](../03-mirror) `MirrorSourceConnector`, the first time it mirrors a record — with the same name (thanks to `IdentityReplicationPolicy`, see the mirror README) and `replication.factor: 3` to match the DR cluster's 3 brokers.

```bash
microk8s kubectl apply -f primary-topic.yaml
microk8s kubectl exec primary-cluster-primary-pool-0 -n kafka-primary -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic drc-demo-topic

# after MirrorMaker 2 is running and has mirrored at least one record:
microk8s kubectl exec dr-cluster-dr-pool-0 -n kafka-dr -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic drc-demo-topic
```
