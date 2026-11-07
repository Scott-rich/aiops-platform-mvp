"""Incremental, mergeable detector state without retaining complete metric events.

The 25 ms latency histogram has a fixed 2,402 counters. Exact event-ID deduplication
still retains one compact scalar tuple per ID, so total state remains O(unique
events in the window), rather than constant memory. IDs must be immutable: a
duplicate with different detection values is rejected, including during merge.
"""
from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass, field

RULE_VERSION = "window-rules-v2"
BIN_WIDTH_MS = 25
MAX_HISTOGRAM_MS = 60_000
OVERFLOW_BIN = MAX_HISTOGRAM_MS // BIN_WIDTH_MS + 1
BIN_COUNT = OVERFLOW_BIN + 1


def latency_bucket(latency_ms: float) -> int:
    """0 is exact; positive buckets have inclusive upper bounds; last overflows."""
    if latency_ms > MAX_HISTOGRAM_MS:
        return OVERFLOW_BIN
    return math.ceil(latency_ms / BIN_WIDTH_MS)


@dataclass(slots=True)
class WindowAccumulator:
    service: str | None = None
    # Request=(0, latency_ms, is_5xx); host=(1, cpu_percent, memory_percent).
    seen: dict[str, tuple[int, float, float]] = field(default_factory=dict)
    latency_counts: list[int] = field(default_factory=lambda: [0] * BIN_COUNT)
    request_count: int = 0
    error_count: int = 0
    host_count: int = 0
    cpu_sum: float = 0.0
    memory_sum: float = 0.0
    max_latency_ms: float = 0.0
    quarantine_count: int = 0
    quarantine_samples: list[dict] = field(default_factory=list)

    def add(self, event: dict) -> WindowAccumulator:
        """Add an already-validated metric assigned to this service and window."""
        service = event["service"]
        if self.service is not None and service != self.service:
            raise ValueError("cannot aggregate different services in one window")
        if event["event_type"] == "request":
            record = (0, float(event["latency_ms"]), float(event["status_code"] >= 500))
        elif event["event_type"] == "host":
            record = (1, float(event["cpu_percent"]), float(event["memory_percent"]))
        else:
            raise ValueError("unsupported metric event_type")
        self._add_record(event["event_id"], record)
        self.service = service
        return self

    def add_or_quarantine(self, event: dict) -> WindowAccumulator:
        """Runtime policy: retain first detection values and audit conflicts.

        The strict add/merge API stays unchanged for callers that require error
        signalling. This final guard is essential when event time stalls beyond
        the upstream processing-time deduplication TTL.
        """
        previous = self.seen.get(event["event_id"])
        try:
            return self.add(event)
        except ValueError as error:
            if not str(error).startswith("conflicting duplicate event_id:"):
                raise
            self.quarantine_count += 1
            if len(self.quarantine_samples) < 10:
                self.quarantine_samples.append({"event_id": event["event_id"],
                    "origin": event.get("__kafka_origin"), "first_detection_values": previous,
                    "incoming_type": event["event_type"],
                    "incoming_values": {key: event[key] for key in ("latency_ms", "status_code", "cpu_percent", "memory_percent") if key in event}})
            return self

    def _add_record(self, event_id: str, record: tuple[int, float, float]) -> None:
        previous = self.seen.get(event_id)
        if previous is not None:
            if previous != record:
                raise ValueError(f"conflicting duplicate event_id: {event_id}")
            return
        self.seen[event_id] = record
        if record[0] == 0:
            self.request_count += 1
            self.error_count += int(record[2])
            self.latency_counts[latency_bucket(record[1])] += 1
            self.max_latency_ms = max(self.max_latency_ms, record[1])
        else:
            self.host_count += 1
            self.cpu_sum += record[1]
            self.memory_sum += record[2]

    def merge(self, other: WindowAccumulator) -> WindowAccumulator:
        """Merge in place and deduplicate overlaps; reject conflicts atomically.

        Directly adding partial counters would double-count overlapping event IDs.
        Replaying compact tuples makes merges associative for immutable IDs.
        """
        if self is other:
            return self
        if self.service is not None and other.service is not None and self.service != other.service:
            raise ValueError("cannot merge different services")
        # Detect all conflicting overlap before changing the destination.
        for event_id, record in other.seen.items():
            previous = self.seen.get(event_id)
            if previous is not None and previous != record:
                raise ValueError(f"conflicting duplicate event_id: {event_id}")
        for event_id, record in other.seen.items():
            self._add_record(event_id, record)
        if self.service is None:
            self.service = other.service
        return self

    def p95(self) -> tuple[float, bool]:
        """Return nearest-rank bucket upper bound and whether it is an overflow.

        Normal buckets overestimate by less than 25 ms. The overflow bucket uses
        observed maximum as a conservative upper bound with no 25 ms guarantee.
        """
        if not self.request_count:
            return 0.0, False
        rank = (95 * self.request_count + 99) // 100
        cumulative = 0
        for bucket, count in enumerate(self.latency_counts):
            cumulative += count
            if cumulative >= rank:
                if bucket == OVERFLOW_BIN:
                    return self.max_latency_ms, True
                return float(bucket * BIN_WIDTH_MS), False
        raise ValueError("histogram counters do not match request_count")


def evaluate_accumulator(acc: WindowAccumulator, service: str, start: int, end: int) -> list[dict]:
    """Produce v2 alerts with the v1 event/alert identity and threshold contract."""
    if acc.service is not None and acc.service != service:
        raise ValueError("window key does not match accumulator service")
    alerts = []

    def emit(rule_id: str, evidence: dict) -> None:
        fingerprint = f"{service}|{rule_id}|{start}|{end}"
        alerts.append({
            "schema_version": 1, "event_type": "alert",
            "alert_id": hashlib.sha256(fingerprint.encode()).hexdigest()[:24],
            "service": service, "rule_id": rule_id, "rule_version": RULE_VERSION,
            "severity": "warning", "window_start": start, "window_end": end,
            "detected_at": int(time.time() * 1000), "evidence": {**evidence, **({"quarantined_conflict_count": acc.quarantine_count} if acc.quarantine_count else {})},
        })

    if acc.request_count >= 20:
        p95, overflow = acc.p95()
        error_rate = acc.error_count / acc.request_count
        stats = {
            "sample_count": acc.request_count, "p95_latency_ms": p95,
            "error_rate": round(error_rate, 6), "p95_method": "histogram_upper_bound",
            "p95_precision_ms": None if overflow else BIN_WIDTH_MS,
            "p95_overflow": overflow,
        }
        if overflow:
            stats["p95_overflow_lower_bound_ms"] = MAX_HISTOGRAM_MS
        # 800 is an exact bucket boundary, so this trigger is identical to the
        # exact nearest-rank >800 test despite the reported percentile rounding.
        if p95 > 800:
            emit("high_latency", {**stats, "threshold": 800})
        if acc.error_count * 5 > acc.request_count:
            emit("high_error_rate", {**stats, "threshold": 0.2})
    if acc.host_count >= 3:
        cpu = acc.cpu_sum / acc.host_count
        memory = acc.memory_sum / acc.host_count
        stats = {"sample_count": acc.host_count, "avg_cpu_percent": round(cpu, 3),
                 "avg_memory_percent": round(memory, 3)}
        if cpu > 90:
            emit("high_cpu", {**stats, "threshold": 90})
        if memory > 90:
            emit("high_memory", {**stats, "threshold": 90})
    return alerts


def summarize_accumulator(acc: WindowAccumulator) -> dict:
    """Shared dynamic features: identical histogram precision in local and Flink."""
    stats = {'request_count': acc.request_count, 'host_count': acc.host_count}
    if acc.request_count:
        p95, overflow = acc.p95()
        stats.update(p95_latency_ms=p95, error_rate=acc.error_count / acc.request_count,
                     p95_method='histogram_upper_bound', p95_overflow=overflow,
                     p95_precision_ms=None if overflow else BIN_WIDTH_MS)
    if acc.host_count:
        stats.update(avg_cpu_percent=acc.cpu_sum / acc.host_count,
                     avg_memory_percent=acc.memory_sum / acc.host_count)
    if acc.quarantine_count:
        stats['quarantined_conflict_count'] = acc.quarantine_count
    return stats
