# strimzi/03-mirror

`KafkaMirrorMaker2` is what makes this a *mirrored two-cluster* DR simulation rather than a stretched cluster: `primary-cluster` and `dr-cluster` share no infrastructure, no controller quorum, no storage — the only thing keeping `dr-cluster` warm is a continuous, asynchronous, one-way copy of every record.

## `mm2-primary-to-dr` (steady state, always running)

Runs a Kafka Connect worker in `kafka-dr` (colocated with its target cluster — standard MirrorMaker 2 practice, and it means the mirror survives even if `kafka-primary` disappears entirely) with two connectors:

- **`MirrorSourceConnector`**: reads `drc-demo-topic` from `primary-cluster` and writes it to `dr-cluster`, creating the topic there on first write (RF=3, matching `dr-cluster`'s broker count).
- **`MirrorCheckpointConnector`**: mirrors consumer group offsets (for groups matching `cg-drc`) so that, in principle, a consumer resuming on the DR side can start close to where it left off on the primary side, translated to DR-side offsets.

### `IdentityReplicationPolicy`

By default, MirrorMaker 2 renames mirrored topics with a source-cluster prefix (`primary.drc-demo-topic`) — this is what lets *bidirectional* active/active mirroring run safely without an infinite replication loop, but it means consumers have to know a different topic name depending on which cluster they're talking to.

For an active/**passive** DR pair, that's the wrong trade-off: after failover, clients should be able to reconnect to `dr-cluster` and read `drc-demo-topic` — the exact same name — without any reconfiguration beyond the bootstrap address. `replication.policy.class: org.apache.kafka.connect.mirror.IdentityReplicationPolicy` (set identically on both connectors) does that: the topic name and consumer group IDs are preserved as-is across the mirror.

The trade-off: identity replication drops MM2's built-in loop protection (which normally recognizes and skips already-mirrored, prefixed topics). That's why **the two mirrors in this directory are never meant to run at the same time** — see below.

## `mm2-dr-to-primary` (fail-back leg, applied only on demand)

The reverse leg. Not applied by default. Only brought up during fail-back, after `mm2-primary-to-dr` has already been torn down (see [`docs/dr-runbook.md`](../../docs/dr-runbook.md)) — running both directions concurrently with identity replication would loop `drc-demo-topic` back and forth between the clusters forever.

## Verifying

```bash
microk8s kubectl get kafkamirrormaker2 -n kafka-dr
microk8s kubectl get kafkaconnector -n kafka-dr   # per-connector status (source + checkpoint)
microk8s kubectl logs -n kafka-dr -l strimzi.io/cluster=mm2-primary-to-dr --tail=50
```
