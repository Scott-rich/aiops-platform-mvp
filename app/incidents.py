"""Durable, conservative incident correlation and evidence-based recovery.

An alert is an observation, not a new incident every time a sliding window fires.
Cross-service correlation needs an observed shared trace and the same anomalous
leaf service/instance. Similar topology or overlapping wall time is insufficient.
Notifications are an idempotent local outbox; this module never sends messages.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from .contracts import canonical, digest, now_ms, validate_alert
from .detection_policy import adaptive_policy

log = logging.getLogger(__name__)


def _family(rule: Any) -> Any:
    if rule in {"fast_errors", "fast_error_rate", "high_error_rate", "error_burst"}:
        return "availability"
    if rule in {"high_latency", "fast_latency"}:
        return "latency"
    if rule == "high_cpu":
        return "resource:cpu"
    if rule == "high_memory":
        return "resource:memory"
    return "rule:" + rule


def _bad(event: Any, family: Any, policy: Any = None) -> Any:
    threshold = policy["threshold"] if policy and policy["service"] == event["service"] else None
    if family == "availability":
        return event.get("status_code", 0) >= 500
    if family == "latency":
        return (
            event.get("latency_ms", 0) > (800 if threshold is None else threshold)
            or event.get("status_code", 0) >= 500
        )
    if family == "resource:cpu":
        return event.get("cpu_percent", 0) > (90 if threshold is None else threshold)
    if family == "resource:memory":
        return event.get("memory_percent", 0) > (90 if threshold is None else threshold)
    return False


def _aliases(incident: Any) -> Any:
    incident["candidate_service"] = incident["root_anchor"]
    incident["services"] = list(incident["affected_services"])
    incident["opened_at"] = incident["created_at"]
    incident["first_alert_id"] = incident["alert_ids"][0]
    incident["first_event_time"] = incident["first_anomaly_at"]
    incident["last_event_time"] = incident["last_anomaly_at"]
    incident["alert_count"] = len(incident["alert_ids"])
    return incident


def _summary(incident: Any) -> Any:
    keys = (
        "incident_id",
        "family",
        "root_anchor",
        "root_instance_id",
        "candidate_service",
        "affected_services",
        "services",
        "status",
        "created_at",
        "opened_at",
        "updated_at",
        "first_alert_id",
        "alert_count",
        "first_anomaly_at",
        "last_anomaly_at",
        "first_event_time",
        "last_event_time",
        "latest_alert_end",
        "recovery_started_at",
        "resolved_at",
    )
    return {key: incident[key] for key in keys}


class IncidentManager:
    """Incident state with bounded correlation reads.

    Keyword budgets cap records per seed/trace scan, selected trace IDs, bytes per
    scan and SQL page size. Coverage describes only the declared event-time
    ranges. Truncated context keeps the incident open for explicit reanalysis;
    it is never promoted to a complete causal/recovery claim automatically.
    """

    context_page_size: int
    context_event_limit: int
    context_trace_limit: int
    context_byte_limit: int

    def __init__(
        self,
        store: Any,
        correlation_gap_ms: Any = 60_000,
        recovery_hold_ms: Any = 10_000,
        *,
        context_page_size: Any = 256,
        context_event_limit: Any = 20_000,
        context_trace_limit: Any = 1000,
        context_byte_limit: Any = 8 * 1024 * 1024,
    ) -> None:
        self.store = store
        self.correlation_gap_ms = correlation_gap_ms
        self.recovery_hold_ms = recovery_hold_ms
        for name, value, maximum in (
            ("context_page_size", context_page_size, 1000),
            ("context_event_limit", context_event_limit, 100_000),
            ("context_trace_limit", context_trace_limit, 4096),
            ("context_byte_limit", context_byte_limit, 64 * 1024 * 1024),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be an integer in 1..{maximum}")
            setattr(self, name, value)
        store.ensure_schema(
            """
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                    updated_at INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS incident_status_idx ON incidents(status,updated_at);
                CREATE TABLE IF NOT EXISTS incident_alerts (
                    alert_id TEXT PRIMARY KEY, incident_id TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS incident_alert_incident_idx ON incident_alerts(incident_id);
                CREATE TABLE IF NOT EXISTS incident_notifications (
                    notification_id TEXT PRIMARY KEY, incident_id TEXT NOT NULL,
                    kind TEXT NOT NULL, created_at INTEGER NOT NULL, payload TEXT NOT NULL,
                    UNIQUE(incident_id,kind));
            """,
            {
                "incidents",
                "incident_status_idx",
                "incident_alerts",
                "incident_alert_incident_idx",
                "incident_notifications",
            },
            "incidents-v1",
        )

    def _scan_events(self, db: Any, where: Any, arguments: Any, coverage: Any, limit: Any) -> Any:
        """Keyset pages and one decoded row at a time; limits span all query chunks.

        Callers own ``where`` (never user SQL). One extra row detects truncation,
        including an exact-limit page followed by more data. Bytes and row counts
        include healthy observations so a noisy service cannot exhaust memory.
        """
        if coverage["truncated"]:
            return
        after = None
        while True:
            clause = where
            params = list(arguments)
            if after is not None:
                clause += " AND (event_time>? OR (event_time=? AND event_id>?))"
                params.extend((after[0], after[0], after[1]))
            cursor = db.execute(
                "SELECT event_time,event_id,payload FROM events WHERE "
                + clause
                + " ORDER BY event_time,event_id LIMIT ?",
                (*params, self.context_page_size),
            )
            coverage["pages"] += 1
            count = 0
            for row in cursor:
                if coverage["records"] >= limit:
                    coverage.update(truncated=True, truncation_reason="record-limit")
                    return
                size = len(row[2].encode("utf-8"))
                if coverage["payload_bytes"] + size > self.context_byte_limit:
                    coverage.update(truncated=True, truncation_reason="byte-limit")
                    return
                coverage["records"] += 1
                coverage["payload_bytes"] += size
                count += 1
                after = (row[0], row[1])
                yield json.loads(row[2])
            if count < self.context_page_size:
                return

    def _coverage(self, limit: Any) -> Any:
        return {
            "records": 0,
            "pages": 0,
            "payload_bytes": 0,
            "truncated": False,
            "record_limit": limit,
            "byte_limit": self.context_byte_limit,
            "page_size": self.context_page_size,
        }

    def _context(self, db: Any, alert: Any) -> Any:
        family = _family(alert["rule_id"])
        policy = adaptive_policy(alert)
        metric_kind = "host" if family.startswith("resource:") else "request"
        seed_coverage = self._coverage(self.context_event_limit)
        trace_coverage = self._coverage(self.context_event_limit)
        seeds: list[tuple[Any, Any, Any]] = []
        traces: set[str] = set()
        trace_ids_truncated = False
        # Select the alert's own service first. Unrelated busy services cannot
        # displace the evidence before trace correlation gets a chance to run.
        for event in self._scan_events(
            db,
            "service=? AND event_type IN (?,?) AND event_time>=? AND event_time<?",
            (alert["service"], metric_kind, "span", alert["window_start"], alert["window_end"]),
            seed_coverage,
            self.context_event_limit,
        ):
            if not _bad(event, family, policy):
                continue
            seeds.append((event["event_id"], event["event_time"], event["instance_id"]))
            trace = event.get("trace_id")
            if trace and trace not in traces:
                if len(traces) < self.context_trace_limit:
                    traces.add(trace)
                else:
                    trace_ids_truncated = True
        origins, leaf_ids, related_traces = set(), set(), set()
        affected_by_trace: dict[str, set[str]] = {}
        selected_traces = sorted(traces)
        # Small IN lists stay below SQLite variable limits. All chunks share the
        # same bounded trace-record/byte budget; only compact facts are retained.
        for offset in range(0, len(selected_traces), 64):
            chunk = selected_traces[offset : offset + 64]
            where = "trace_id IN (" + ",".join("?" for _ in chunk) + ")"
            where += " AND event_type IN ('request','span') AND event_time>=? AND event_time<=?"
            for event in self._scan_events(
                db,
                where,
                (*chunk, alert["window_start"] - 5000, alert["window_end"] + 5000),
                trace_coverage,
                self.context_event_limit,
            ):
                if not _bad(event, family, policy):
                    continue
                trace = event["trace_id"]
                affected_by_trace.setdefault(trace, set()).add(event["service"])
                # Declared downstream with no observed child is missing evidence,
                # not proof that the caller itself is the anomalous leaf.
                if event["event_type"] == "span" and not event.get("downstream"):
                    origins.add((event["service"], event["instance_id"]))
                    leaf_ids.add(event["event_id"])
                    related_traces.add(trace)
            if trace_coverage["truncated"]:
                break
        truncated = seed_coverage["truncated"] or trace_coverage["truncated"] or trace_ids_truncated
        coverage = {
            "complete": not truncated,
            "truncated": truncated,
            "seeds": seed_coverage,
            "trace_records": trace_coverage,
            "selected_trace_count": len(traces),
            "trace_limit": self.context_trace_limit,
            "trace_ids_truncated": trace_ids_truncated,
            "scope": "alert-service-seeds-and-selected-trace-requests-and-spans",
            "seed_window": {
                "start": alert["window_start"],
                "end": alert["window_end"],
                "end_exclusive": True,
            },
            "trace_window": {
                "start": alert["window_start"] - 5000,
                "end": alert["window_end"] + 5000,
                "end_exclusive": False,
            },
        }
        verified = len(origins) == 1 and not truncated
        if verified:
            root, instance = next(iter(origins))
            origin_ids = leaf_ids
            affected = set().union(*(affected_by_trace[trace] for trace in related_traces))
        else:
            root = alert["service"]
            instances = {seed[2] for seed in seeds}
            instance = next(iter(instances)) if len(instances) == 1 and not truncated else "unknown"
            origin_ids = {seed[0] for seed in seeds}
            affected = {alert["service"]}
        if truncated:
            log.warning(
                "incident_context_truncated alert_id=%s seed_records=%d trace_records=%d trace_ids=%d",
                alert["alert_id"],
                seed_coverage["records"],
                trace_coverage["records"],
                len(traces),
            )
        times = [seed[1] for seed in seeds]
        return {
            "root_anchor": root,
            "root_instance_id": instance,
            "verified": verified,
            "trace_ids": selected_traces,
            "origin_evidence_ids": sorted(origin_ids),
            "affected_services": sorted(affected | {alert["service"], root}),
            "coverage": coverage,
            "first": min(times) if times else alert["window_start"],
            "last": max(times) if times else alert["window_end"],
            "observed_anomaly": bool(times),
            "reason": "context-truncated; service-only"
            if truncated
            else "shared-trace-anomalous-leaf"
            if verified
            else (
                "ambiguous-leaf-origins; service-only"
                if origins
                else "no-leaf-evidence; service-only"
            ),
        }

    def _save(self, db: Any, incident: Any) -> Any:
        _aliases(incident)
        db.execute(
            """INSERT INTO incidents VALUES(?,?,?,?) ON CONFLICT(incident_id) DO UPDATE
                      SET status=excluded.status,updated_at=excluded.updated_at,payload=excluded.payload""",
            (
                incident["incident_id"],
                incident["status"],
                incident["updated_at"],
                canonical(incident),
            ),
        )

    def _notify(self, db: Any, incident: Any, kind: Any, at: Any) -> Any:
        notification_id = digest({"incident_id": incident["incident_id"], "kind": kind})[:24]
        db.execute(
            "INSERT OR IGNORE INTO incident_notifications VALUES(?,?,?,?,?)",
            (
                notification_id,
                incident["incident_id"],
                kind,
                at,
                canonical(
                    {
                        "notification_id": notification_id,
                        "incident_id": incident["incident_id"],
                        "kind": kind,
                        "created_at": at,
                        "payload": _summary(incident),
                    }
                ),
            ),
        )

    def ingest_alert(self, alert: Any, source: Any = None, now: Any = None) -> Any:
        """Attach once, or open a new incident. `now` is UTC epoch milliseconds."""
        validate_alert(alert)
        at = now_ms() if now is None else int(now)
        self.store.alert(alert, source)
        with self.store.db() as db:
            if not db.in_transaction:
                db.execute("BEGIN IMMEDIATE")
            duplicate = db.execute(
                """SELECT i.payload FROM incidents i JOIN incident_alerts a
                                      ON a.incident_id=i.incident_id WHERE a.alert_id=?""",
                (alert["alert_id"],),
            ).fetchone()
            if duplicate:
                return json.loads(duplicate[0])
            context = self._context(db, alert)
            family = _family(alert["rule_id"])
            rows = db.execute("SELECT payload FROM incidents ORDER BY updated_at DESC").fetchall()
            selected = None
            for row in rows:
                incident = json.loads(row[0])
                if incident["family"] != family:
                    continue
                shared = bool(set(incident["trace_ids"]) & set(context["trace_ids"]))
                same_alert_service = alert["service"] in incident["alerted_services"]
                # Raw evidence can expire before an old alert is replayed. An already
                # observed service/window overlapping the closed incident is history;
                # it cannot demonstrate a new post-recovery failure.
                if (
                    incident["status"] == "resolved"
                    and not context["observed_anomaly"]
                    and same_alert_service
                    and alert["window_end"] <= incident["resolved_at"]
                    and alert["window_start"] <= incident["last_anomaly_at"]
                    and alert["window_end"] >= incident["first_anomaly_at"]
                ):
                    incident["historical_attachment_note"] = (
                        "expired-or-missing-evidence; existing-service closed-window overlap"
                    )
                    selected = incident
                    break
                # Upgrade a provisional service-only anchor when its own trace now has a leaf.
                if not incident["correlation_verified"] and context["verified"] and shared:
                    original_row = db.execute(
                        "SELECT payload FROM alerts WHERE alert_id=?", (incident["alert_ids"][0],)
                    ).fetchone()
                    original = (
                        self._context(db, json.loads(original_row[0])) if original_row else None
                    )
                    if (
                        original
                        and original["verified"]
                        and original["root_anchor"] == context["root_anchor"]
                        and original["root_instance_id"] == context["root_instance_id"]
                    ):
                        incident.update(
                            root_anchor=context["root_anchor"],
                            root_instance_id=context["root_instance_id"],
                            correlation_verified=True,
                            correlation_reason=original["reason"],
                        )
                if incident["root_anchor"] != context["root_anchor"]:
                    continue
                if incident["root_instance_id"] != context["root_instance_id"]:
                    continue
                if not same_alert_service and not (
                    shared and context["verified"] and incident["correlation_verified"]
                ):
                    continue
                if incident["status"] == "resolved":
                    # Historic sliding windows can arrive after recovery. They never reopen it.
                    historical = (
                        context["observed_anomaly"]
                        and context["last"] <= incident["last_anomaly_at"]
                    )
                    historical = historical or (
                        not context["observed_anomaly"]
                        and alert["window_end"] <= incident["last_anomaly_at"]
                    )
                    if not historical:
                        continue
                elif context["first"] > incident["last_anomaly_at"] + self.correlation_gap_ms:
                    continue
                selected = incident
                break
            if selected is None:
                selected = {
                    "incident_id": uuid.uuid4().hex,
                    "family": family,
                    "root_anchor": context["root_anchor"],
                    "root_instance_id": context["root_instance_id"],
                    "correlation_verified": context["verified"],
                    "correlation_reason": context["reason"],
                    "origin_evidence_ids": [],
                    "trace_ids": [],
                    "affected_services": [],
                    "alerted_services": [],
                    "alert_ids": [],
                    "status": "open",
                    "created_at": at,
                    "updated_at": at,
                    "first_anomaly_at": context["first"],
                    "last_anomaly_at": context["last"],
                    "latest_alert_end": alert["window_end"],
                    "recovery_started_at": None,
                    "resolved_at": None,
                    "recovery": {"state": "not-evaluated"},
                }
                opened = True
            else:
                opened = False
            policy = adaptive_policy(alert)
            if policy and selected["status"] != "resolved":
                policies = selected.setdefault("adaptive_recovery_policies", {})
                previous = policies.get(alert["service"])
                if previous is None or policy["window_end"] > previous["window_end"]:
                    policies[alert["service"]] = policy
            # This is additive JSON metadata: existing databases need no migration.
            # Once an attached context was incomplete, do not silently certify the
            # whole incident complete merely because a later small window fits.
            selected["correlation_coverage"] = {
                "alert_id": alert["alert_id"],
                **context["coverage"],
            }
            selected["correlation_truncated"] = (
                selected.get("correlation_truncated", False) or context["coverage"]["truncated"]
            )
            selected["alert_ids"].append(alert["alert_id"])
            for field, values in (
                ("trace_ids", context["trace_ids"]),
                ("origin_evidence_ids", context["origin_evidence_ids"]),
                ("affected_services", context["affected_services"]),
                ("alerted_services", [alert["service"]]),
            ):
                selected[field] = sorted(set(selected[field]) | set(values))
            if opened or context["observed_anomaly"]:
                selected["first_anomaly_at"] = min(selected["first_anomaly_at"], context["first"])
            previous_last = selected["last_anomaly_at"]
            if opened or context["observed_anomaly"]:
                selected["last_anomaly_at"] = max(previous_last, context["last"])
            selected["latest_alert_end"] = max(selected["latest_alert_end"], alert["window_end"])
            selected["updated_at"] = at
            if (
                selected["status"] == "recovering"
                and context["observed_anomaly"]
                and context["last"] > (selected["recovery_started_at"] or 0)
            ):
                selected["status"] = "open"
                selected["recovery_started_at"] = None
                selected["recovery"] = {"state": "new-anomaly"}
            self._save(db, selected)
            db.execute(
                "INSERT INTO incident_alerts VALUES(?,?)",
                (alert["alert_id"], selected["incident_id"]),
            )
            if opened:
                self._notify(db, selected, "opened", at)
            return selected

    def _recovery(self, db: Any, incident: Any, at: Any) -> Any:
        family = incident["family"]
        resource = family.startswith("resource:")
        width = 15_000 if resource else 10_000
        minimum = 3 if resource else 10
        kind = "host" if resource else "request"
        evidence = {}
        bad_times: list[int] = []
        ready = (
            bool(incident["affected_services"])
            and not incident.get("correlation_truncated", False)
            and family in {"availability", "latency", "resource:cpu", "resource:memory"}
        )
        for service in incident["affected_services"]:
            metric_coverage = self._coverage(50_000)
            events = list(
                self._scan_events(
                    db,
                    "service=? AND event_type=? AND event_time>=? AND event_time<=?",
                    (service, kind, at - width, at),
                    metric_coverage,
                    50_000,
                )
            )
            span_coverage = self._coverage(50_000)
            bad_spans = []
            if not resource:
                policy = incident.get("adaptive_recovery_policies", {}).get(service)
                for event in self._scan_events(
                    db,
                    "service=? AND event_type='span' AND event_time>=? AND event_time<=?",
                    (service, at - width, at),
                    span_coverage,
                    50_000,
                ):
                    if _bad(event, family, policy):
                        bad_spans.append((event["event_id"], event["event_time"]))
            truncated = metric_coverage["truncated"] or span_coverage["truncated"]
            required_instance = (
                incident.get("root_instance_id") if service == incident["root_anchor"] else None
            )
            if required_instance and required_instance != "unknown":
                events = [e for e in events if e["instance_id"] == required_instance]
            latest = max((e["event_time"] for e in events), default=None)
            policy = incident.get("adaptive_recovery_policies", {}).get(service)
            bad = [e for e in events if _bad(e, family, policy)]
            bad_times.extend(e["event_time"] for e in bad)
            bad_times.extend(timestamp for _, timestamp in bad_spans)
            fresh = latest is not None and at - latest <= 5000
            enough = len(events) >= minimum
            times = sorted({e["event_time"] for e in events})
            # A late burst must not hide a long sampling gap during the hold.
            maximum_gap = max((b - a for a, b in zip(times, times[1:], strict=False)), default=0)
            sampled_across_window = (
                bool(times) and times[0] <= at - width + 5000 and maximum_gap <= 5000
            )
            healthy = (
                enough
                and fresh
                and sampled_across_window
                and not bad
                and not bad_spans
                and not truncated
                and required_instance != "unknown"
            )
            ready = ready and healthy
            evidence[service] = {
                "samples": len(events),
                "required_instance_id": required_instance,
                "truncated": truncated,
                "minimum_samples": minimum,
                "latest_metric_at": latest,
                "fresh": fresh,
                "healthy": healthy,
                "sampled_across_window": sampled_across_window,
                "maximum_sample_gap_ms": maximum_gap,
                "bad_samples": len(bad),
                "adaptive_threshold_policy": policy,
                "bad_span_samples": len(bad_spans),
                "blocking_span_evidence_ids": [event_id for event_id, _ in bad_spans[:10]],
                "blocking_span_ids_truncated": len(bad_spans) > 10,
                "metric_coverage": metric_coverage,
                "span_coverage": span_coverage,
            }
        return (
            ready,
            {
                "checked_at": at,
                "sample_window_ms": width,
                "freshness_limit_ms": 5000,
                "required_healthy_hold_ms": self.recovery_hold_ms,
                "services": evidence,
                "correlation_truncated": incident.get("correlation_truncated", False),
                "scope": "service-metrics-and-error-spans; root-instance-metrics-required",
            },
            bad_times,
        )

    def tick(self, now: Any = None) -> Any:
        """Resolve only after fresh, adequately sampled healthy metrics persist."""
        at = now_ms() if now is None else int(now)
        changed = []
        with self.store.db() as db:
            if not db.in_transaction:
                db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT payload FROM incidents WHERE status!='resolved'").fetchall()
            for row in rows:
                incident = json.loads(row[0])
                before = (
                    incident["status"],
                    incident["last_anomaly_at"],
                    incident["recovery_started_at"],
                )
                ready, details, bad_times = self._recovery(db, incident, at)
                previous_check = incident.get("recovery", {}).get("checked_at")
                if incident["status"] == "recovering" and (
                    previous_check is None
                    or at < previous_check
                    or at - previous_check > details["sample_window_ms"]
                ):
                    incident["status"] = "open"
                    incident["recovery_started_at"] = None
                    details["hold_restarted_reason"] = "observation-gap-or-clock-reversal"
                if bad_times:
                    incident["last_anomaly_at"] = max(incident["last_anomaly_at"], *bad_times)
                if not ready:
                    incident["status"] = "open"
                    incident["recovery_started_at"] = None
                    details["state"] = "unhealthy-or-insufficient-evidence"
                elif incident["status"] == "open":
                    incident["status"] = "recovering"
                    incident["recovery_started_at"] = at
                    details["state"] = "healthy-hold-in-progress"
                elif at - incident["recovery_started_at"] >= self.recovery_hold_ms:
                    incident["status"] = "resolved"
                    incident["resolved_at"] = at
                    details["state"] = "resolved-with-fresh-metrics"
                else:
                    details["state"] = "healthy-hold-in-progress"
                incident["recovery"] = details
                after = (
                    incident["status"],
                    incident["last_anomaly_at"],
                    incident["recovery_started_at"],
                )
                if before != after:
                    incident["updated_at"] = at
                self._save(db, incident)
                if incident["status"] == "resolved":
                    self._notify(db, incident, "resolved", at)
                if before != after:
                    changed.append(incident)
        return changed

    def get(self, incident_id: Any) -> Any:
        with self.store.db() as db:
            row = db.execute(
                "SELECT payload FROM incidents WHERE incident_id=?", (incident_id,)
            ).fetchone()
        if row is None:
            raise KeyError(incident_id)
        return json.loads(row[0])

    def list(self, limit: Any = 100) -> Any:
        with self.store.db() as db:
            rows = db.execute(
                "SELECT payload FROM incidents ORDER BY updated_at DESC LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
        return [_summary(json.loads(row[0])) for row in rows]

    def notifications(self, incident_id: Any = None) -> Any:
        with self.store.db() as db:
            if incident_id is None:
                rows = db.execute(
                    "SELECT payload FROM incident_notifications ORDER BY created_at,notification_id"
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT payload FROM incident_notifications WHERE incident_id=? ORDER BY created_at,notification_id",
                    (incident_id,),
                ).fetchall()
        return [json.loads(row[0]) for row in rows]
