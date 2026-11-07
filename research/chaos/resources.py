"""Derive resource measurements without equating requested traffic with telemetry EPS."""

from __future__ import annotations

import re
from typing import Any


def derive(rows: list[dict[str, Any]]) -> dict[str, Any]:
    points = []
    previous: dict[str, Any] | None = None
    for row in sorted(rows, key=lambda item: item["observed_at_ms"]):
        current: dict[str, Any] = {
            "at_ms": row["observed_at_ms"],
            "database_bytes": None,
            "kafka_lag": None,
            "telemetry_eps": None,
        }
        text = row.get("database")
        match = (
            re.search(r"^aiops_database_bytes ([0-9.eE+-]+)$", text, re.M)
            if isinstance(text, str)
            else None
        )
        if match:
            current["database_bytes"] = float(match[1])
        kafka = row.get("kafka")
        if kafka and all(p.get("committed") is not None for p in kafka):
            current["kafka_lag"] = sum(max(0, p["end_offset"] - p["committed"]) for p in kafka)
        offsets = {
            (p["topic"], p["partition"]): p["end_offset"]
            for p in kafka or []
            if p["topic"].startswith("telemetry.")
        }
        current["offsets"] = offsets
        if previous and offsets and offsets.keys() == previous["offsets"].keys():
            delta = sum(offsets[key] - previous["offsets"][key] for key in offsets)
            elapsed = (current["at_ms"] - previous["at_ms"]) / 1000
            if delta >= 0 and elapsed > 0:
                current["telemetry_eps"] = delta / elapsed
        for container in ("api", "worker"):
            data = row.get(container) or {}
            rss = re.search(r"^VmRSS:\s+(\d+)\s+kB", data.get("/proc/1/status") or "", re.M)
            cpu = re.search(r"^usage_usec\s+(\d+)", data.get("/sys/fs/cgroup/cpu.stat") or "", re.M)
            current[container + "_pid1_rss_bytes"] = int(rss[1]) * 1024 if rss else None
            current[container + "_cpu_usage_usec"] = int(cpu[1]) if cpu else None
            current[container + "_cpu_cores"] = None
            if previous and cpu and previous.get(container + "_cpu_usage_usec") is not None:
                delta = int(cpu[1]) - previous[container + "_cpu_usage_usec"]
                elapsed_us = (current["at_ms"] - previous["at_ms"]) * 1000
                if elapsed_us > 0 and delta >= 0:
                    current[container + "_cpu_cores"] = delta / elapsed_us
        durations = []
        for checkpoint in (row.get("flink_checkpoints") or {}).values():
            completed = checkpoint.get("latest", {}).get("completed") or {}
            if completed.get("end_to_end_duration") is not None:
                durations.append(completed["end_to_end_duration"])
        current["latest_completed_checkpoint_ms"] = max(durations) if durations else None
        points.append(current)
        previous = current
    # Remove internal tuple-key lookup maps before JSON serialization.
    for point in points:
        point.pop("offsets")
    observed = [p["telemetry_eps"] for p in points if p["telemetry_eps"] is not None]
    return {
        "points": points,
        "n_samples": len(points),
        "n_observed_eps": len(observed),
        "observed_eps_mean": sum(observed) / len(observed) if observed else None,
        "observed_eps_min": min(observed) if observed else None,
        "observed_eps_max": max(observed) if observed else None,
        "limits": "PID1 RSS excludes child processes; cgroup CPU includes the entire selected container. Kafka/Flink JVM RSS and separate LLM processes are not covered.",
    }
