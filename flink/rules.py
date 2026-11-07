"""Versioned, deterministic window rules shared by Flink and the offline demo."""
from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Iterable

RULE_VERSION = "window-rules-v1"


from wire_contract import validate_metric


def evaluate_window(events: Iterable[dict], service: str, start: int, end: int) -> list[dict]:
    """Return alerts for [start,end). P95 uses the exact nearest-rank method.

    Duplicate event IDs inside this window count once. Window materialization is
    intentionally bounded by time, not by event count: this is a demo workload.
    """
    unique = {event["event_id"]: event for event in events
              if event.get("service") == service and start <= event["event_time"] < end}
    requests = [event for event in unique.values() if event["event_type"] == "request"]
    hosts = [event for event in unique.values() if event["event_type"] == "host"]
    alerts = []

    def emit(rule_id: str, evidence: dict) -> None:
        fingerprint = f"{service}|{rule_id}|{start}|{end}"
        alerts.append({
            "schema_version": 1,
            "event_type": "alert",
            "alert_id": hashlib.sha256(fingerprint.encode()).hexdigest()[:24],
            "service": service,
            "rule_id": rule_id,
            "rule_version": RULE_VERSION,
            "severity": "warning",
            "window_start": start,
            "window_end": end,
            "detected_at": int(time.time() * 1000),
            "evidence": evidence,
        })

    if len(requests) >= 20:
        latencies = sorted(event["latency_ms"] for event in requests)
        p95 = latencies[math.ceil(len(latencies) * 0.95) - 1]
        error_rate = sum(event["status_code"] >= 500 for event in requests) / len(requests)
        stats = {"sample_count": len(requests), "p95_latency_ms": p95,
                 "error_rate": round(error_rate, 6), "p95_method": "nearest_rank"}
        if p95 > 800:
            emit("high_latency", {**stats, "threshold": 800})
        if error_rate > 0.2:
            emit("high_error_rate", {**stats, "threshold": 0.2})
    if len(hosts) >= 3:
        cpu = sum(event["cpu_percent"] for event in hosts) / len(hosts)
        memory = sum(event["memory_percent"] for event in hosts) / len(hosts)
        stats = {"sample_count": len(hosts), "avg_cpu_percent": round(cpu, 3),
                 "avg_memory_percent": round(memory, 3)}
        if cpu > 90:
            emit("high_cpu", {**stats, "threshold": 90})
        if memory > 90:
            emit("high_memory", {**stats, "threshold": 90})
    return alerts
