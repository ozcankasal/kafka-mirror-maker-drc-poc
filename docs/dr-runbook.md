# DR Runbook: Failover and Fail-back

## Normal state

- `ACTIVE_CLUSTER=primary` (default, `clients/k8s/active-cluster-configmap.yaml`).
- `mm2-primary-to-dr` continuously mirrors `drc-demo-topic` and the `cg-drc` group's offsets from `primary-cluster` to `dr-cluster`.
- The producer writes to whichever cluster `ACTIVE_CLUSTER` names; the consumer reads from the same one, in the same `cg-drc` group either side.

## Scenario: primary is lost, dr takes over

```bash
./scripts/failover.sh
./scripts/status.sh
```

What it does, and what was actually observed running it against a live cluster:

1. **Stops `mm2-primary-to-dr`.** Left running, it would mirror writes made directly to dr after failover back toward a cluster that's supposed to be dead, and it blocks the reverse leg from starting safely later (see the [appendix](#appendix-the-fail-back-duplication-finding)).
2. **Captures dr's current per-partition end offsets** to `scripts/.failover-watermark.json` — the high-water mark fail-back will later use to tell "dr's copy of primary's own old data" apart from "genuinely new data written directly to dr." Real captured output:
   ```
   {"offsets":[{"partition":{"cluster":"dr","partition":0,"topic":"drc-demo-topic"},"offset":{"offset":0}},{"partition":{"cluster":"dr","partition":1,"topic":"drc-demo-topic"},"offset":{"offset":74}},{"partition":{"cluster":"dr","partition":2,"topic":"drc-demo-topic"},"offset":{"offset":0}}]}
   ```
3. **Patches `ACTIVE_CLUSTER` to `dr`.** As with `kafka-multi`'s zone switch, kubelet's ConfigMap sync means the producer and consumer pods each notice on their own schedule, not atomically — in the recorded run, the producer picked it up in well under a minute and reconnected without a restart:
   ```
   [17:14:53] produced cluster=dr partition=2 offset=9 seq=140
   ```
   and the consumer followed shortly after, leaving `cg-drc` on primary and re-joining `cg-drc` on dr — picking up mid-stream from dr's already-mirrored copy of the group's checkpointed offsets, not from the beginning:
   ```
   [17:54:41] active cluster changed primary -> dr, leaving old group
   [17:54:41] connecting consumer to cluster=dr (...), group=cg-drc
   [17:54:45] consumed cluster=dr partition=2 offset=20 produced_by=dr seq=234
   ```

At this point primary can be considered lost; dr is fully serving reads and writes, with whatever primary produced in the few seconds between mirroring being stopped and the client cutover **not** present on dr — this is the real RPO cost of asynchronous mirroring (contrast with `kafka-multi`'s stretched cluster, where that gap is ~0 by construction).

## Fail-back, once primary recovers

Two steps, deliberately not one — an unattended, automatic fail-back the moment primary comes back is rarely what you actually want operationally.

### 1. Start the resync

```bash
./scripts/start-failback-resync.sh
```

Brings up `mm2-dr-to-primary` (the reverse leg) and corrects its starting offsets to the watermark from step 2 above, so it mirrors back only what was genuinely written to dr while it was active — not dr's own mirrored copy of primary's pre-failover data. See the [appendix](#appendix-the-fail-back-duplication-finding) for why this needs a 3-phase dance and doesn't fully eliminate a one-time replay on its own.

Watch it catch up:

```bash
microk8s kubectl exec dr-cluster-dr-pool-0 -n kafka-dr -- \
  bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic drc-demo-topic
microk8s kubectl exec primary-cluster-primary-pool-0 -n kafka-primary -- \
  bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic drc-demo-topic
```

### 2. Complete the fail-back

Once primary's offsets are tracking dr's closely:

```bash
./scripts/complete-failback.sh
```

Flips `ACTIVE_CLUSTER` back to `primary`, removes the now-finished `mm2-dr-to-primary` leg, and re-applies steady-state `mm2-primary-to-dr` mirroring.

**Real captured output** — the consumer rejoining `cg-drc` on primary, immediately hitting old, already-consumed dr-origin records that the resync had (as expected, see below) replayed into primary, and correctly dropping every one of them:

```
[17:56:30] active cluster changed dr -> primary, leaving old group
[17:56:31] connecting consumer to cluster=primary (...), group=cg-drc
[17:56:35] dropped duplicate cluster=primary partition=2 offset=322 produced_by=dr seq=480
[17:56:35] dropped duplicate cluster=primary partition=2 offset=323 produced_by=dr seq=481
...  (122 duplicates dropped in this run)
[17:56:54] consumed cluster=primary partition=1 offset=231 produced_by=primary seq=548
[17:56:55] consumed cluster=primary partition=1 offset=232 produced_by=primary seq=549
```

122 duplicates, zero data loss, zero bad output reaching application logic past the dedup check — the fail-back completed cleanly.

## Appendix: the fail-back duplication finding

This is worth documenting honestly rather than glossing over, the same way `kafka-multi` documented Strimzi's node-pool scale-down refusal rather than hiding it.

**The naive approach fails.** Just applying `mm2-dr-to-primary` and letting it mirror dr's whole copy of `drc-demo-topic` back to primary re-injects everything `mm2-primary-to-dr` had already mirrored forward *before* failover — because `IdentityReplicationPolicy` (needed for transparent failover) gives up the topic-prefix marker that would otherwise let MM2 recognize "this data is already a mirror, don't loop it." First attempt, verified: primary's pre-failover partition grew by exactly the size of dr's mirrored backlog, and the replayed records were confirmed byte-for-byte identical to the originals (`seq: 0`, `cluster: primary`, re-appearing at new offsets).

**Kafka Connect's offset-management API is the documented fix — but it's not enough by itself.** Kafka Connect (KIP-875) lets you `PATCH /connectors/{name}/offsets` while a connector is `STOPPED`, to set where it starts reading. `start-failback-resync.sh` uses exactly this, via the same REST endpoint Strimzi's `KafkaMirrorMaker2` wraps. The catch, found by testing this directly against the live connector: **Kafka Connect always starts a brand-new connector `RUNNING`** — Strimzi's `sourceConnector.state: stopped` in the CR is applied via a *follow-up* REST call once the operator reconciles, not at creation time, because Connect's `POST /connectors` has no "create pre-stopped" option. That leaves a real window where the connector is live before Strimzi's stop call lands.

`start-failback-resync.sh` closes the dangerous part of that window with a 3-phase sequence:

1. **Phase 1**: create the connector with `topicsPattern: "^$"` (matches no real topic) — so however long it runs before being stopped, it's harmless.
2. **Phase 2**: once confirmed `STOPPED`, `PATCH` its offsets to the failover watermark.
3. **Phase 3**: switch `topicsPattern` to the real value (`drc-demo-topic`) and resume.

Phases 1–2 worked cleanly and reproducibly in testing — zero replay while the connector only ever pointed at a fictitious topic. But **Phase 3's config switch itself reopened the gap**: applying the new `topicsPattern` to an already-offset-corrected, stopped connector caused it to briefly run again and replay the same backlog once — confirmed twice, in both cases the replay was bounded to exactly the size of the watermarked (already-known) backlog, with no further duplication once it caught up to its corrected offset, and no infinite loop.

**Given that, the fix that actually matters is downstream, not upstream**: the offset-watermark mechanism is real and does meaningful work (it prevents the loop from being unbounded and stops the connector from being an active source of *ongoing* duplication), but it can't be relied on alone to guarantee zero replay given how Kafka Connect's own connector lifecycle behaves. `clients/app/consumer.py`'s per-origin `seq` dedup (see [architecture.md](architecture.md#consumers-must-be-idempotent)) is what actually makes this safe end to end — confirmed in the run captured above, dropping all 122 replayed records without any of them reaching application-visible "consumed" output.

The general lesson, consistent with how Kafka Connect and MirrorMaker 2 are documented upstream: **treat MM2 as at-least-once, and make consumers idempotent** — don't rely on connector-level tricks to guarantee exactly-once across a fail-back.
