"""Public validation API; the dependency-free wire contract also runs in Flink."""

from typing import Any

from wire_contract import canonical, digest, now_ms, number, validate_event

__all__ = ["canonical", "digest", "now_ms", "number", "validate_event", "validate_alert"]


def validate_alert(alert: Any) -> Any:
    if not isinstance(alert, dict) or alert.get("schema_version") != 1:
        raise ValueError("invalid alert schema")
    for key in ("alert_id", "service", "rule_id"):
        if not isinstance(alert.get(key), str) or not alert[key]:
            raise ValueError(f"missing {key}")
    for key in ("window_start", "window_end"):
        if not number(alert.get(key)):
            raise ValueError(f"invalid {key}")
    if alert["window_end"] <= alert["window_start"] or not isinstance(alert.get("evidence"), dict):
        raise ValueError("invalid alert window/evidence")
    canonical(alert).encode("utf-8")
    return alert
