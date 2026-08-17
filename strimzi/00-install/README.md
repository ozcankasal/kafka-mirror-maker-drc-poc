# strimzi/00-install

Strimzi Cluster Operator v1.1.0 install manifests (the same version used in the earlier `kafka-multi` stretched-cluster PoC — vendored here again for reproducibility, unmodified apart from namespace substitution).

## Single operator, two watched namespaces

Unlike a typical single-cluster Strimzi install, this PoC needs **one operator watching two namespaces** (`kafka-primary` and `kafka-dr`) — it manages the primary `Kafka` cluster, the DR `Kafka` cluster, and the `KafkaMirrorMaker2` resources that mirror between them.

The operator itself is deployed in `kafka-primary`, with:

- `STRIMZI_NAMESPACE=kafka-primary,kafka-dr` in `strimzi-cluster-operator-1.1.0.yaml` (Strimzi's "several namespaces" mode — a literal comma-separated list instead of the single-namespace default of deriving it from `metadata.namespace`).
- `strimzi-cluster-operator-dr-rbac.yaml`: the vendored bundle only creates the `RoleBinding`s the operator needs in its own namespace (`kafka-primary`). Watching a second namespace means creating equivalent `RoleBinding`s in `kafka-dr`, pointing at the same `ServiceAccount` (`strimzi-cluster-operator.kafka-primary`). The `ClusterRole`s are already cluster-scoped and shared, so only the `RoleBinding`s need duplicating — this is Strimzi's documented multi-namespace RBAC pattern.

CRDs are cluster-scoped, so they're applied without a namespace and are shared by both watched namespaces.

## Install

```bash
./install.sh
```

or manually:

```bash
microk8s kubectl apply -f strimzi-crds-1.1.0.yaml
microk8s kubectl apply -f strimzi-cluster-operator-1.1.0.yaml -n kafka-primary
microk8s kubectl apply -f strimzi-cluster-operator-dr-rbac.yaml
```
