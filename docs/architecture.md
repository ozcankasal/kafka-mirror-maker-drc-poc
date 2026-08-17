# Architecture

## Overview

Two independent KRaft Kafka 4.3.0 clusters run on a single microk8s node:

- `primary-cluster` (namespace `kafka-primary`): 3 nodes (broker+controller, combined mode), its own KRaft controller quorum, its own storage.
- `dr-cluster` (namespace `kafka-dr`): 3 nodes, same shape, entirely separate KRaft quorum and storage.

They share nothing infrastructural. What connects them is [`KafkaMirrorMaker2`](../strimzi/03-mirror) (`mm2-primary-to-dr`), continuously copying `drc-demo-topic` and the `cg-drc` consumer group's offsets from `primary-cluster` to `dr-cluster`. A single producer/consumer pair (namespace `kafka-clients`) follows an `ACTIVE_CLUSTER` parameter to decide which cluster to talk to.

This is a deliberate contrast to the earlier `kafka-multi` PoC (a sibling repo), which simulated DR with **one stretched cluster** spread across two zones via rack-aware replication. Both are legitimate Kafka DR patterns, with different trade-offs:

| | `kafka-multi` (stretched cluster) | `kafka-mirror` (this repo) |
|---|---|---|
| Topology | One Kafka cluster, one KRaft quorum, brokers split across zones | Two independent Kafka clusters, two KRaft quorums |
| Replication mechanism | Kafka's own in-cluster (Raft-backed) partition replication | MirrorMaker 2 — an external, asynchronous Kafka Connect pipeline |
| RPO (data loss on failover) | ~0 — every in-sync replica already has the data by definition | > 0 — bounded by mirror lag, not zero. A message produced to primary an instant before it's lost may never reach dr. |
| Realistic use case | Stretched/metro cluster across two nearby, low-latency AZs | Cross-region or cross-provider DR, where one KRaft quorum can't span the distance |
| Failover cost | Restart-free; clients just point at the same cluster, from the other zone | Clients reconnect to a different cluster (different bootstrap address) |
| Fail-back cost | Automatic — same cluster the whole time | Requires an explicit reverse mirror leg to resync what was written to dr, see [dr-runbook.md](dr-runbook.md) |

The stretched-cluster approach only works when the network between "zones" is fast and reliable enough to be a single Raft quorum's fabric — in practice, one metro area. Real cross-region or cross-cloud-provider DR (the actual "disaster" in "disaster recovery" — losing an entire cluster, region, or provider) needs two independent clusters and asynchronous replication between them. That's what this repo simulates.

## Why `IdentityReplicationPolicy`

MirrorMaker 2's default replication policy prefixes mirrored topics with the source cluster's alias (`primary.drc-demo-topic`), so bidirectional active/active mirroring can safely avoid infinite loops (a mirrored, prefixed topic is recognizably "already mirrored" and skipped). That's the wrong trade-off for an active/**passive** pair: after failover, a client should be able to point at `dr-cluster` and read `drc-demo-topic` — the identical name — without touching its configuration beyond the bootstrap address.

`IdentityReplicationPolicy` (set in [`mm2-primary-to-dr.yaml`](../strimzi/03-mirror/mm2-primary-to-dr.yaml)) keeps topic and group names unchanged across the mirror, at the cost of losing that built-in loop protection — which is why the forward (`mm2-primary-to-dr`) and reverse (`mm2-dr-to-primary`) mirror legs are never run at the same time (see [dr-runbook.md](dr-runbook.md)).

## Consumers must be idempotent

MirrorMaker 2 — like Kafka Connect generally — is at-least-once, not exactly-once. Verified while building this repo: even with careful use of Kafka Connect's offset-management API to skip dr's copy of primary's own pre-failover data (see [dr-runbook.md](dr-runbook.md#appendix-the-fail-back-duplication-finding)), the fail-back leg still replayed that backlog once into primary before the correction could take effect. That's not a bug in this repo's scripts so much as a real, documented characteristic of Kafka Connect source connectors — config/lifecycle changes can cause a task to re-read from a stored offset that doesn't reflect a correction made moments earlier.

The production-correct answer isn't to chase a perfect zero-replay connector dance; it's what any consumer reading through a Kafka Connect pipeline should do anyway: **be idempotent**. `clients/app/consumer.py` tracks the highest `seq` seen per origin cluster (the payload carries which site originally produced it, preserved by `IdentityReplicationPolicy`) and drops anything at or below that watermark. This is what actually makes fail-back safe in this repo, not the offset correction alone — the offset correction reduces how much gets replayed and bounds it to a single pass; the idempotent consumer is what guarantees the replay is harmless.

## Deliberate trade-offs (PoC scope)

| Decision | Why | What production would do |
|---|---|---|
| Combined broker+controller (3 nodes per cluster) | Saves resources on a single node | Dedicated controller quorum, separate from the brokers, per cluster |
| No TLS on the internal listener, or between clusters for MirrorMaker 2 | PoC, not exposed outside the cluster | `tls: true` + `authentication` on both the Kafka listeners and the MM2 cluster configs |
| No `kafkaExporter` on either cluster | `status.sh` already gets consumer-group lag straight from `kafka-consumer-groups.sh`; an exporter+Prometheus stack is extra moving parts this PoC doesn't need | Deploy Prometheus + `kafkaExporter` per cluster for dashboards/alerting |
| ConfigMap + runtime `pip install` instead of a client image | No docker/buildah in this environment | An image built from [`clients/app/Dockerfile`](../clients/app/Dockerfile) and pushed to the registry |
| Single-replica producer/consumer, single MM2 task per connector | PoC, throughput/HA not required | Multiple MM2 tasks (`tasksMax`), multiple client replicas + `PodDisruptionBudget` |
| Fail-back is a 2-step manual script, not automatic | Mirrors real operational practice — an automatic, unattended fail-back after a real disaster is rarely what you actually want | Same, in practice — this is genuinely how MM2-based DR fail-back works |

## Kafka 4.3 features

Same KRaft/Kafka 4.3.0 feature set as `kafka-multi` — mandatory KRaft, the new consumer-group protocol (KIP-848), Eligible Leader Replicas (KIP-966), transaction v2 (KIP-890) — now running twice, once per independent cluster.

## Resource usage

2 clusters × 3 Kafka pods × (request 200m CPU / 512Mi, limit 400m / 768Mi) + 2 topic-operators + 1 cluster operator (200m/384Mi) + 1 MirrorMaker 2 Connect pod (200m/512Mi request) + 2 client pods (50m/128Mi) — roughly ~2 CPU / ~4.6Gi requested in total.
