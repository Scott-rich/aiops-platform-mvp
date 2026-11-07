"""Read-only Linux /proc collector. Run on the host for host-level metrics."""

import os
import socket
import time
import uuid
from pathlib import Path
from typing import Any

from .contracts import now_ms


def cpu_ticks() -> Any:
    values = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
    # user,nice,system,idle,iowait,irq,softirq,steal; exclude double-counted guest.
    return sum(values), values[3] + values[4]


def memory_percent() -> Any:
    mem = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        mem[key] = int(value.split()[0])
    return 100 * (1 - mem["MemAvailable"] / mem["MemTotal"])


def main() -> Any:
    from kafka import KafkaProducer

    from .contracts import canonical

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092").split(","),
        value_serializer=lambda e: canonical(e).encode(),
        acks="all",
        retries=5,
    )
    service = os.getenv("AGENT_SERVICE", "linux-node")
    previous = cpu_ticks()
    try:
        while True:
            time.sleep(5)
            current = cpu_ticks()
            total, idle = current[0] - previous[0], current[1] - previous[1]
            previous = current
            event = {
                "schema_version": 1,
                "event_id": uuid.uuid4().hex,
                "event_type": "host",
                "event_time": now_ms(),
                "service": service,
                "instance_id": socket.gethostname(),
                "trace_id": "",
                "cpu_percent": max(0, min(100, 100 * (1 - idle / max(total, 1)))),
                "memory_percent": memory_percent(),
                "load1": os.getloadavg()[0],
                "scope": "proc-visible-node",
            }
            producer.send("telemetry.metrics", key=service.encode(), value=event).get(timeout=10)
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
