import os
import sys
import time


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        sys.exit(f"required environment variable missing: {name}")
    return value


def log(msg):
    print(f"[{time.strftime('%Y-%m-%dT%H:%M:%S')}] {msg}", flush=True)


def read_active_cluster(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def bootstrap_servers_by_cluster():
    return {
        "primary": env("PRIMARY_BOOTSTRAP_SERVERS", required=True),
        "dr": env("DR_BOOTSTRAP_SERVERS", required=True),
    }
