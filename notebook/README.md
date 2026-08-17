# notebook

`drc-simulation.ipynb` drives the same active/passive DR scenario documented in [`docs/dr-runbook.md`](../docs/dr-runbook.md) against the live cluster and records the real output of every step.

It doesn't reimplement any logic. It shells out to the same `scripts/status.sh`, `scripts/failover.sh`, `scripts/start-failback-resync.sh` and `scripts/complete-failback.sh` used from the command line, plus a few `kubectl exec` / `kubectl logs` calls to show what the producer, consumer and MirrorMaker 2 are actually doing. This keeps the notebook and the CLI scripts as a single source of truth.

## What it shows

1. Baseline: `primary-cluster` active, `mm2-primary-to-dr` mirroring, a producer/consumer pair reading and writing `drc-demo-topic`.
2. Triggering a failover (`failover.sh`) and watching the producer/consumer reconnect to `dr-cluster` without a restart (recorded run: producer picked it up after ~78s, consumer after the file sync had already landed on that pod).
3. `dr-cluster` serving fresh writes on its own, with no mirror running.
4. Starting the fail-back resync (`start-failback-resync.sh`) and watching `dr-cluster`'s new data get copied back to `primary-cluster`.
5. Completing the fail-back (`complete-failback.sh`) and watching the consumer reconnect to `primary-cluster` — including the duplicate records the resync reintroduces (185 in the recorded run) and the client's own idempotent dedup dropping every one of them.

## Running it

The notebook only needs a Python 3 kernel (it shells out to `microk8s kubectl` and the repo's own scripts via `subprocess`, no Kafka client library needed) plus access to the live cluster:

```bash
python3 -m pip install --user nbformat nbclient ipykernel
python3 -m ipykernel install --user --name python3 --display-name "Python 3"
jupyter notebook notebook/drc-simulation.ipynb   # or: jupyter lab
```

Re-running all cells performs a real primary → dr → primary failover/fail-back cycle against whatever cluster is currently reachable via `microk8s kubectl`. It assumes the stack from the [Quick start](../README.md#quick-start) is already deployed and `primary` is the active cluster when it starts.
