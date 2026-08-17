<div align="center">

# kafka-mirror

**A two-cluster, MirrorMaker 2 active/passive DRC (Disaster Recovery Center) simulation for Apache Kafka 4.3, running on a single-node Kubernetes (microk8s) cluster.**

![Kafka](https://img.shields.io/badge/Kafka-4.3.0-231F20?logo=apachekafka&logoColor=white)
![Strimzi](https://img.shields.io/badge/Strimzi-1.1.0-1E88E5)
![KRaft](https://img.shields.io/badge/mode-KRaft%20(no%20ZooKeeper)-6E56CF)
![MirrorMaker 2](https://img.shields.io/badge/replication-MirrorMaker%202-orange)
![Kubernetes](https://img.shields.io/badge/Kubernetes-microk8s-326CE5?logo=kubernetes&logoColor=white)
![Status](https://img.shields.io/badge/status-deployed%20%26%20verified-brightgreen)

</div>

---

## Overview

This is the follow-up to [`kafka-multi`](https://github.com/ozcankasal/kafka-drc-simulation), a sibling PoC that simulated DR with a single Kafka cluster stretched across two zones. That approach only works when both "zones" are close enough to share one KRaft quorum. This repo simulates the other real Kafka DR pattern: **two genuinely independent clusters**, kept in sync by asynchronous replication, standing in for a real cross-region or cross-provider disaster recovery pair.

It builds two separate **KRaft-mode Kafka 4.3.0 clusters** (Strimzi-managed) — `primary-cluster` and `dr-cluster` — with no shared infrastructure between them, and a [`KafkaMirrorMaker2`](strimzi/03-mirror) pipeline continuously mirroring the demo topic and its consumer group's offsets from primary to dr. In normal operation, a producer/consumer pair reads and writes against `primary-cluster`. On a simulated disaster, a script stops the mirror, flips a config value, and the same clients reconnect to `dr-cluster` — which already has the data, thanks to the mirror. Failing back is a deliberate two-step, documented process, not automatic — because in the real world it shouldn't be either.

Every step here was applied against a real cluster and verified live, including a genuinely hard-won finding about MirrorMaker 2's fail-back behavior — see [docs/dr-runbook.md](docs/dr-runbook.md#appendix-the-fail-back-duplication-finding).

## Table of contents

- [Highlights](#highlights)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Running the DR simulation](#running-the-dr-simulation)
- [Documentation](#documentation)
- [Design notes & trade-offs](#design-notes--trade-offs)

## Highlights

- **Two real, independent Kafka clusters**, not one cluster wearing two hats — separate `KafkaNodePool`s, separate KRaft controller quorums, separate storage, separate namespaces (`kafka-primary` / `kafka-dr`), connected by nothing but [MirrorMaker 2](strimzi/03-mirror).
- **`IdentityReplicationPolicy`**, so a mirrored topic keeps its exact name across the failover — a client can fail over by changing its bootstrap address alone, no topic-name remapping. See [docs/architecture.md](docs/architecture.md#why-identityreplicationpolicy) for the loop-protection trade-off that comes with it.
- **One operator watching two namespaces.** A single Strimzi Cluster Operator manages both clusters via Strimzi's multi-namespace RBAC pattern (`strimzi/00-install`), rather than running two operators.
- **A real, verified fail-back subtlety**: naive reverse mirroring re-injects already-shared data, and even Kafka Connect's own offset-management API doesn't fully close that gap on its own (Connect always starts a new connector `RUNNING` before Strimzi can stop it). The actual fix — a race-free 3-phase resync plus an idempotent consumer — is implemented, tested, and documented rather than assumed. See [docs/dr-runbook.md](docs/dr-runbook.md#appendix-the-fail-back-duplication-finding).
- **Odd-sized KRaft quorums.** 3 nodes per cluster (not the 6-node even quorum `kafka-multi` deliberately accepted) — no "recommended odd number of nodes" warning this time.

## Architecture

```
   kafka-primary namespace              kafka-dr namespace
  ┌───────────────────────┐            ┌───────────────────────┐
  │   primary-cluster      │            │      dr-cluster        │
  │   (3-node KRaft,       │  MirrorMaker 2  (3-node KRaft,      │
  │    broker+controller)  │ ───────────▶│   broker+controller)  │
  │   drc-demo-topic       │  primary -> dr │   drc-demo-topic    │
  └───────────────────────┘            └───────────────────────┘
             ▲                                     ▲
             │                                     │
             └───────────────┐       ┌─────────────┘
                              │       │
                    kafka-clients namespace
                  producer / consumer (single pair)
                  follow ACTIVE_CLUSTER: primary | dr
```

Full write-up, including the contrast with `kafka-multi`'s stretched-cluster approach: [docs/architecture.md](docs/architecture.md).

## Repository layout

| Path | Contents |
|---|---|
| [`cluster-prep/`](cluster-prep) | Scripts that prepare the cluster for the PoC (scale down unrelated workloads, create the three namespaces) |
| [`strimzi/00-install/`](strimzi/00-install) | Strimzi Cluster Operator install manifests (single operator, two watched namespaces) |
| [`strimzi/01-clusters/`](strimzi/01-clusters) | `KafkaNodePool` + `Kafka` custom resources for `primary-cluster` and `dr-cluster` |
| [`strimzi/02-topics/`](strimzi/02-topics) | The demo `KafkaTopic` (primary side only — the DR side is created by the mirror) |
| [`strimzi/03-mirror/`](strimzi/03-mirror) | `KafkaMirrorMaker2` resources — steady-state (`primary -> dr`) and fail-back (`dr -> primary`) |
| [`clients/app/`](clients/app) | The producer/consumer Python application |
| [`clients/k8s/`](clients/k8s) | Client `Deployment` and `ConfigMap` manifests |
| [`scripts/`](scripts) | Operational scripts — `status.sh`, `failover.sh`, `start-failback-resync.sh`, `complete-failback.sh` |
| [`notebook/`](notebook) | Jupyter notebook that runs and records a full failover/fail-back cycle |
| [`docs/`](docs) | Architecture notes and the DR runbook |

## Prerequisites

- A Kubernetes cluster — this was built and tested against [microk8s](https://microk8s.io/) on a single node (4 vCPU / 16 GB RAM), with the `dns`, `storage`/`hostpath-storage`, and `registry` addons enabled.
- `kubectl` access (the scripts assume `microk8s kubectl`; swap in your own if you're on a different distribution).
- Roughly 2 vCPU / 5 GB of allocatable capacity free for both Kafka clusters, the mirror, and the clients.
- `curl` and `jq`-free shell tooling only — the fail-back scripts talk to the Kafka Connect REST API via `kubectl exec ... curl` and parse JSON with `grep`/`sed`, no extra tooling needed inside the cluster.

## Quick start

```bash
git clone git@github.com:ozcankasal/kafka-mirror-maker-drc-poc.git
cd kafka-mirror-maker-drc-poc

# 1. Free up cluster resources (optional — only if you're low on capacity) and create namespaces
./cluster-prep/scale-down-unrelated.sh
microk8s kubectl apply -f cluster-prep/namespaces.yaml

# 2. Install the Strimzi operator (watches both kafka-primary and kafka-dr)
./strimzi/00-install/install.sh

# 3. Bring up both Kafka clusters
microk8s kubectl apply -f strimzi/01-clusters/primary-pool.yaml -f strimzi/01-clusters/primary-cluster.yaml
microk8s kubectl apply -f strimzi/01-clusters/dr-pool.yaml -f strimzi/01-clusters/dr-cluster.yaml

# 4. Create the demo topic and start steady-state mirroring
microk8s kubectl apply -f strimzi/02-topics/primary-topic.yaml
microk8s kubectl apply -f strimzi/03-mirror/mm2-primary-to-dr.yaml

# 5. Deploy the producer/consumer
microk8s kubectl apply -f clients/k8s/active-cluster-configmap.yaml -f clients/k8s/app-code-configmap.yaml
microk8s kubectl apply -f clients/k8s/producer.yaml -f clients/k8s/consumer.yaml

# 6. Check everything is healthy
./scripts/status.sh
```

## Running the DR simulation

```bash
# Disaster: dr becomes active
./scripts/failover.sh
./scripts/status.sh

# Once primary is healthy again: resync what dr wrote while it was active...
./scripts/start-failback-resync.sh
# ...watch it catch up (see docs/dr-runbook.md), then:
./scripts/complete-failback.sh
```

For the full walkthrough — including real captured log output from an actual failover/fail-back cycle, and the fail-back duplication finding — see [docs/dr-runbook.md](docs/dr-runbook.md).

Prefer to see it running rather than read about it? [`notebook/drc-simulation.ipynb`](notebook/drc-simulation.ipynb) drives the exact same scripts against a live cluster and captures the real output of a full primary → dr → primary cycle (recorded run: producer cutover in ~78s, consumer cutover on fail-back in ~63s, 185 replayed duplicates correctly dropped by the client's dedup logic).

## Documentation

- [docs/architecture.md](docs/architecture.md) — how the two clusters and MirrorMaker 2 fit together, the contrast with `kafka-multi`'s stretched-cluster approach, `IdentityReplicationPolicy`, why consumers must be idempotent, resource sizing.
- [docs/dr-runbook.md](docs/dr-runbook.md) — step-by-step failover/fail-back runbook with real, captured verification output, plus a full write-up of the fail-back duplication finding.
- [notebook/drc-simulation.ipynb](notebook/drc-simulation.ipynb) — the same scenario, run live and recorded end to end.

## Design notes & trade-offs

This is a PoC, and the shortcuts are called out explicitly rather than hidden:

| Decision | Why | Production alternative |
|---|---|---|
| Combined broker+controller roles (3 nodes per cluster) | Saves resources on a single node | Dedicated, odd-sized controller quorum per cluster |
| No TLS anywhere (Kafka listeners or MirrorMaker 2) | Not exposed outside the cluster | `tls: true` + `authentication` throughout |
| No `kafkaExporter` | `status.sh` gets lag straight from `kafka-consumer-groups.sh` | Prometheus + `kafkaExporter` per cluster |
| Client code shipped via `ConfigMap` + runtime `pip install` | No container build tooling available in the dev environment | Image built from [`clients/app/Dockerfile`](clients/app/Dockerfile) and pushed to a registry |
| Single-replica producer/consumer, single MM2 task | HA/throughput not required for a demo | Multiple replicas/tasks + `PodDisruptionBudget` |
| Fail-back is a manual, 2-step, scripted process | Real MM2 fail-back genuinely works this way — see the runbook appendix | Same, in practice |

See [docs/architecture.md](docs/architecture.md#deliberate-trade-offs-poc-scope) for the full breakdown.

## Known operational quirk: namespace teardown order

Deleting `kafka-primary` in one shot (which also deletes the Strimzi operator running inside it) can leave the namespace stuck `Terminating`, because the `KafkaTopic`'s `strimzi.io/topic-operator` finalizer needs the (now-gone) topic operator to clear it. If that happens:

```bash
microk8s kubectl patch kafkatopic drc-demo-topic -n kafka-primary --type=merge -p '{"metadata":{"finalizers":[]}}'
```
