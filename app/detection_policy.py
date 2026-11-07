"""Read validated adaptive alert thresholds without applying them to other services."""

from typing import Any

from .contracts import number

FIELDS = {
    "high_latency": ("latency_ms", "latency"),
    "high_cpu": ("cpu_percent", "cpu"),
    "high_memory": ("memory_percent", "memory"),
}


def adaptive_policy(alert: Any) -> Any:
    evidence = alert.get("evidence", {}) if isinstance(alert, dict) else {}
    if not isinstance(evidence, dict) or evidence.get("detection_mode") != "dynamic":
        return None
    if (
        alert.get("rule_id") not in FIELDS
        or not number(evidence.get("threshold"))
        or evidence["threshold"] <= 0
    ):
        return None
    if (
        not isinstance(alert.get("service"), str)
        or type(alert.get("window_start")) is not int
        or type(alert.get("window_end")) is not int
    ):
        return None
    field, channel = FIELDS[alert["rule_id"]]
    return {
        "service": alert["service"],
        "rule_id": alert["rule_id"],
        "field": field,
        "channel": channel,
        "threshold": evidence["threshold"],
        "window_start": alert["window_start"],
        "window_end": alert["window_end"],
        "config_id": evidence.get("config_id"),
    }
