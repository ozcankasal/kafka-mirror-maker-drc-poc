# cluster-prep

Preparation steps needed to run the Kafka DR PoC comfortably on a single-node microk8s cluster.

## Namespaces

`namespaces.yaml` creates three namespaces, one per "site" in the simulation:

| Namespace | Role |
|---|---|
| `kafka-primary` | The primary Kafka cluster (normally active) |
| `kafka-dr` | The DR (standby) Kafka cluster, kept warm via mirroring |
| `kafka-clients` | The producer/consumer application, independent of either cluster |

Modeling them as separate namespaces (rather than one shared namespace) mirrors how a real active/passive DR pair is usually two independent environments — potentially different clusters, regions, or accounts — not two labels inside one deployment.

## Scaled-down (replicas=0) workloads

`scale-down-unrelated.sh` scales the following deployments — unrelated to this PoC — to `replicas=0` (not deleted), to free up capacity on the single node:

| Namespace | Deployment | Original replicas |
|---|---|---|
| audit-demo | audit-service | 1 |
| audit-demo | echo-app | 1 |
| audit-demo | waypoint | 1 |
| default | mcp-server | 1 |
| default | wasm-server | 1 |

The following namespaces are **left untouched** because they host microk8s core addons / cluster infrastructure: `kube-system`, `cert-manager`, `container-registry`, `istio-system`, `observability`.

The original replica counts are recorded in the script itself (not read live), because these deployments may already be at `replicas=0` from a previous PoC session in this environment. `restore-unrelated.sh` reads `.original-replicas` (local state, not committed to git) to restore them.

## Usage

```bash
./scale-down-unrelated.sh
microk8s kubectl apply -f namespaces.yaml
```

## Restoring

```bash
./restore-unrelated.sh
```
