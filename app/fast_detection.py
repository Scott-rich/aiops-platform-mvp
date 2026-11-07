"""Fast availability warning alongside the slower event-time confirmation lane."""

import hashlib
from collections import deque
from typing import Any

from .contracts import now_ms


class FastErrorDetector:
    def __init__(self, threshold: Any = 5, max_gap_ms: Any = 5000) -> None:
        self.threshold = threshold
        self.max_gap_ms = max_gap_ms
        self.states: dict[Any, Any] = {}

    def observe(self, event: Any) -> Any:
        if event.get("event_type") != "request":
            return []
        key = (event["service"], event["instance_id"])
        state = self.states.setdefault(
            key, {"last_time": 0, "errors": [], "fired": False, "seen": set(), "recent": deque()}
        )
        event_id = event["event_id"]
        if event_id in state["seen"] or event["event_time"] < state["last_time"]:
            return []
        state["seen"].add(event_id)
        state["recent"].append(event_id)
        if len(state["recent"]) > 1000:
            state["seen"].discard(state["recent"].popleft())
        if event["event_time"] - state["last_time"] > self.max_gap_ms:
            state["errors"], state["fired"] = [], False
        state["last_time"] = event["event_time"]
        if event["status_code"] < 500:
            state["errors"], state["fired"] = [], False
            return []
        if len(state["errors"]) < self.threshold:
            state["errors"].append(event)
        if len(state["errors"]) < self.threshold or state["fired"]:
            return []
        state["fired"] = True
        errors = state["errors"]
        fingerprint = (
            f"fast-errors-v1|{key[0]}|{key[1]}|{errors[0]['event_id']}|{errors[-1]['event_id']}"
        )
        return [
            {
                "schema_version": 1,
                "event_type": "alert",
                "alert_id": hashlib.sha256(fingerprint.encode()).hexdigest()[:24],
                "service": key[0],
                "instance_id": key[1],
                "rule_id": "fast_errors",
                "rule_version": "fast-errors-v1",
                "severity": "critical",
                "window_start": errors[0]["event_time"],
                "window_end": errors[-1]["event_time"] + 1,
                "detected_at": now_ms(),
                "evidence": {
                    "sample_count": len(errors),
                    "consecutive_errors": len(errors),
                    "threshold": self.threshold,
                    "error_rate": 1.0,
                    "event_ids": [e["event_id"] for e in errors],
                    "trace_ids": [e.get("trace_id", "") for e in errors],
                    "trigger": "consecutive-5xx",
                },
            }
        ]
