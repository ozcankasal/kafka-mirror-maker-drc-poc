import json
import time

from confluent_kafka import Consumer, KafkaError

from common import bootstrap_servers_by_cluster, env, log, read_active_cluster

TOPIC = env("TOPIC", "drc-demo-topic")
CONSUMER_GROUP = env("CONSUMER_GROUP", "cg-drc")
ACTIVE_CLUSTER_FILE = env("ACTIVE_CLUSTER_FILE", "/etc/active-cluster/ACTIVE_CLUSTER")
POLL_INTERVAL_SECONDS = float(env("POLL_INTERVAL_SECONDS", "1"))
STANDBY_LOG_EVERY_SECONDS = float(env("STANDBY_LOG_EVERY_SECONDS", "10"))
BOOTSTRAP = bootstrap_servers_by_cluster()


def build_consumer(cluster):
    log(f"connecting consumer to cluster={cluster} ({BOOTSTRAP[cluster]}), group={CONSUMER_GROUP}")
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP[cluster],
            "client.id": f"consumer-{cluster}",
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC])
    return consumer


def main():
    log(f"consumer started: group={CONSUMER_GROUP} topic={TOPIC}")

    current_cluster = None
    consumer = None
    last_standby_log = 0.0
    # MirrorMaker 2 (like Kafka Connect generally) is at-least-once: a
    # fail-back resync can replay records this consumer already saw before
    # failover, mirrored back around through the reverse leg. `seq` is
    # monotonically increasing per origin cluster (see producer.py), so
    # tracking the highest seq seen per produced_by cluster is enough to
    # drop replays without needing them to be prevented upstream.
    last_seq_by_origin = {}

    while True:
        active_cluster = read_active_cluster(ACTIVE_CLUSTER_FILE)

        if active_cluster not in BOOTSTRAP:
            if consumer is not None:
                log(f"no valid active cluster (file says '{active_cluster or '(empty)'}'), leaving group")
                consumer.close()
                consumer = None
                current_cluster = None
            now = time.time()
            if now - last_standby_log > STANDBY_LOG_EVERY_SECONDS:
                log(f"standby: no valid active cluster set (file says '{active_cluster or '(empty)'}')")
                last_standby_log = now
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if active_cluster != current_cluster:
            if consumer is not None:
                log(f"active cluster changed {current_cluster} -> {active_cluster}, leaving old group")
                consumer.close()
            consumer = build_consumer(active_cluster)
            current_cluster = active_cluster

        msg = consumer.poll(timeout=POLL_INTERVAL_SECONDS)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log(f"consumer error: {msg.error()}")
            continue
        payload = json.loads(msg.value())
        origin = payload["cluster"]
        seq = payload["seq"]
        if seq <= last_seq_by_origin.get(origin, -1):
            log(
                f"dropped duplicate cluster={current_cluster} partition={msg.partition()} "
                f"offset={msg.offset()} produced_by={origin} seq={seq}"
            )
            continue
        last_seq_by_origin[origin] = seq
        log(
            f"consumed cluster={current_cluster} partition={msg.partition()} "
            f"offset={msg.offset()} produced_by={origin} seq={seq}"
        )


if __name__ == "__main__":
    main()
