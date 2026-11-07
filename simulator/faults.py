"""Bounded, reversible request faults and a separate injection audit trail."""
from collections import deque
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import threading
import time
from typing import Callable
import uuid

LOG = logging.getLogger(__name__)
KINDS = {"none", "latency", "error", "timeout", "unavailable"}


@dataclass(frozen=True)
class FaultSnapshot:
    kind: str = "none"
    remaining_seconds: float = 0.0
    latency_ms: int = 0
    error_rate: float = 0.0
    fault_id: str | None = None


class FaultState:
    def __init__(self, clock: Callable[[], float] = time.monotonic, *, service="unknown",
                 audit_path=None, wall_clock=time.time):
        self._clock, self._wall_clock = clock, wall_clock
        self.service, self.audit_path = service, Path(audit_path) if audit_path else None
        self._kind, self._expires = "none", 0.0
        self._latency_ms, self._error_rate = 0, 0.0
        self._active = None
        self._history = deque(maxlen=256)
        self._lock = threading.RLock()

    def _record(self, record):
        # Audit is independent of telemetry: labels must never become RCA evidence.
        if self.audit_path:
            self.audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self.audit_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
        self._history.append(dict(record))
        LOG.info("fault_lifecycle %s", json.dumps(record, ensure_ascii=False))

    def _end(self, reason):
        if self._active is None:
            return
        record = {**self._active, "action": "ended", "ended_at_ms": int(self._wall_clock()*1000),
                  "end_reason": reason}
        # Clear even if audit storage fails; report the error instead of leaving a fault active.
        self._active, self._kind, self._expires = None, "none", 0.0
        self._record(record)

    def set(self, kind: str, duration_seconds: int, latency_ms: int,
            error_rate: float) -> FaultSnapshot:
        if (kind not in KINDS or type(duration_seconds) is not int or not 1 <= duration_seconds <= 600
                or type(latency_ms) is not int or not 0 <= latency_ms <= 10000
                or not isinstance(error_rate, (int, float)) or not 0 <= error_rate <= 1):
            raise ValueError("invalid bounded fault parameters")
        with self._lock:
            self.snapshot()
            self._end("cleared" if kind == "none" else "replaced")
            if kind != "none":
                start = int(self._wall_clock()*1000)
                record = {"fault_id": uuid.uuid4().hex, "service": self.service,
                          "scope": [self.service + ":/work"], "kind": kind, "action": "started",
                          "started_at_ms": start, "scheduled_end_at_ms": start+duration_seconds*1000,
                          "latency_ms": latency_ms, "error_rate": error_rate}
                self._record(record)  # Fail closed if the requested audit cannot be written.
                self._active, self._kind = record, kind
                self._expires = self._clock() + duration_seconds
                self._latency_ms, self._error_rate = latency_ms, error_rate
            return self.snapshot()

    def snapshot(self) -> FaultSnapshot:
        with self._lock:
            remaining = max(0.0, self._expires - self._clock())
            if self._active is not None and not remaining:
                self._end("expired")
            if self._kind == "none":
                return FaultSnapshot()
            return FaultSnapshot(self._kind, round(remaining, 3), self._latency_ms,
                                 self._error_rate, self._active["fault_id"])

    def history(self):
        with self._lock:
            self.snapshot()
            return list(self._history)

    def close(self):
        with self._lock:
            self._end("shutdown")
