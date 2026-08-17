import json
import time

from confluent_kafka import Producer

from common import bootstrap_servers_by_cluster, env, log, read_active_cluster

TOPIC = env("TOPIC", "drc-demo-topic")
ACTIVE_CLUSTER_FILE = env("ACTIVE_CLUSTER_FILE", "/etc/active-cluster/ACTIVE_CLUSTER")
PRODUCE_INTERVAL_SECONDS = float(env("PRODUCE_INTERVAL_SECONDS", "1"))
STANDBY_LOG_EVERY_SECONDS = float(env("STANDBY_LOG_EVERY_SECONDS", "10"))
BOOTSTRAP = bootstrap_servers_by_cluster()


def make_on_delivery(cluster):
    def on_delivery(err, msg):
        if err is not None:
            log(f"delivery error (cluster={cluster}): {err}")
            return
        log(
            f"produced cluster={cluster} partition={msg.partition()} "
            f"offset={msg.offset()} seq={json.loads(msg.value())['seq']}"
        )

    return on_delivery


def build_producer(cluster):
    log(f"connecting producer to cluster={cluster} ({BOOTSTRAP[cluster]})")
    return Producer({"bootstrap.servers": BOOTSTRAP[cluster], "client.id": f"producer-{cluster}"})


def main():
    log(f"producer started: topic={TOPIC}")

    seq = 0
    current_cluster = None
    producer = None
    last_standby_log = 0.0

    while True:
        active_cluster = read_active_cluster(ACTIVE_CLUSTER_FILE)

        if active_cluster not in BOOTSTRAP:
            now = time.time()
            if now - last_standby_log > STANDBY_LOG_EVERY_SECONDS:
                log(f"standby: no valid active cluster set (file says '{active_cluster or '(empty)'}')")
                last_standby_log = now
            time.sleep(PRODUCE_INTERVAL_SECONDS)
            continue

        if active_cluster != current_cluster:
            if producer is not None:
                log(f"active cluster changed {current_cluster} -> {active_cluster}, flushing old producer")
                producer.flush(5)
            producer = build_producer(active_cluster)
            current_cluster = active_cluster

        payload = {"seq": seq, "cluster": current_cluster, "produced_at": time.time()}
        producer.produce(
            TOPIC,
            key=current_cluster.encode(),
            value=json.dumps(payload).encode(),
            callback=make_on_delivery(current_cluster),
        )
        producer.poll(0)
        seq += 1
        time.sleep(PRODUCE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
