"""Bounded, explicitly partial evidence views for model diagnosis."""

from collections import defaultdict
from typing import Any

from .contracts import canonical, digest
from .llm_config import LLMConfig, ModelError, redact


def anomalous(event: Any) -> Any:
    return (
        event.get("status_code", 0) >= 500
        or event.get("level") == "ERROR"
        or event.get("latency_ms", 0) > 800
    )


def _trace_subset(rows: Any, limit: Any) -> Any:
    spans = {e.get("span_id"): e for e in rows if e["event_type"] == "span" and e.get("span_id")}
    chosen: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(event: Any) -> Any:
        if event["event_id"] not in seen and len(chosen) < limit:
            chosen.append(event)
            seen.add(event["event_id"])

    targets = sorted(
        rows,
        key=lambda e: (
            not anomalous(e),
            e["event_type"] != "span",
            bool(e.get("downstream")),
            e["event_time"],
            e["event_id"],
        ),
    )
    for target in targets:
        if not anomalous(target):
            continue
        add(target)
        # Reserve actual ancestor/paired log/request observations where available.
        ancestor = target
        visited = set()
        while ancestor.get("parent_span_id") in spans and ancestor["parent_span_id"] not in visited:
            visited.add(ancestor["parent_span_id"])
            ancestor = spans[ancestor["parent_span_id"]]
            add(ancestor)
        for paired in rows:
            if paired.get("span_id") == target.get("span_id"):
                add(paired)
        if len(chosen) >= limit:
            return chosen
    for event in targets:
        add(event)
    return chosen


def prompt_records(snapshot: Any, limit: Any = 48) -> Any:
    if limit < 1:
        return []
    bundles: dict[str, list[dict[str, Any]]] = defaultdict(list)
    untraced: list[dict[str, Any]] = []
    for row in snapshot["records"]:
        event = row["event"]
        (bundles[event["trace_id"]] if event.get("trace_id") else untraced).append(event)

    def rank(rows: Any) -> Any:
        leaf_bad = any(
            e["event_type"] == "span" and not e.get("downstream") and anomalous(e) for e in rows
        )
        return (
            not leaf_bad,
            not any(anomalous(e) for e in rows),
            min(e["event_time"] for e in rows),
        )

    ordered = sorted(bundles.values(), key=rank)
    baseline = next(
        (r for r in ordered if not any(anomalous(e) for e in r) and len(r) <= limit // 3), None
    )
    reserve = len(baseline) if baseline else 0
    selected: list[dict[str, Any]] = []
    for rows in ordered:
        if rows is baseline:
            continue
        remaining = limit - reserve - len(selected)
        if len(rows) <= remaining:
            selected.extend(sorted(rows, key=lambda e: (e["event_time"], e["event_id"])))
        elif remaining > 0 and any(anomalous(e) for e in rows):
            selected.extend(_trace_subset(rows, remaining))
    if baseline:
        selected.extend(sorted(baseline, key=lambda e: (e["event_time"], e["event_id"])))
    selected.extend(
        sorted(untraced, key=lambda e: (not anomalous(e), e["event_time"], e["event_id"]))[
            : max(0, limit - len(selected))
        ]
    )
    return selected[:limit]


FIELDS = {
    "schema_version",
    "event_id",
    "event_time",
    "event_type",
    "service",
    "instance_id",
    "trace_id",
    "span_id",
    "parent_span_id",
    "downstream",
    "status_code",
    "latency_ms",
    "error_code",
    "level",
    "message",
    "cpu_percent",
    "memory_percent",
    "load1",
    "scope",
    "method",
    "path",
}


def _project(event: Any) -> Any:
    result = redact({k: v for k, v in event.items() if k in FIELDS})
    truncated = []
    for key in ("message", "path"):
        if isinstance(result.get(key), str) and len(result[key]) > 2000:
            result[key] = result[key][:2000]
            truncated.append(key)
    if len(result.get("downstream", [])) > 20:
        result["downstream"] = result["downstream"][:20]
        truncated.append("downstream")
    return redact(result), {
        "event_id": event["event_id"],
        "truncated_fields": truncated,
        "omitted_extension_field_count": len(set(event) - FIELDS),
    }


def build_context(
    alert: Any,
    snapshot: Any,
    cases: Any,
    observations: Any,
    missing: Any,
    *,
    incident: Any = None,
    related_alerts: Any = None,
    related_alert_total: Any = None,
    prompt_version: Any = "evidence-first-v2",
) -> Any:
    try:
        limits = LLMConfig.from_env()
        config_error = None
    except ModelError as error:
        limits, config_error = LLMConfig(), error.code
    selected = prompt_records(snapshot, limits.max_context_records)
    projected, flags = [], []
    for event in selected:
        value, flag = _project(event)
        projected.append(value)
        flags.append(flag)
    sources = {r["event"]["event_id"]: r.get("source", {}) for r in snapshot["records"]}
    all_traces = defaultdict(set)
    for row in snapshot["records"]:
        event = row["event"]
        if event.get("trace_id"):
            all_traces[event["trace_id"]].add(event["event_id"])
    related = related_alerts if related_alerts is not None else [alert]

    # Keep identity/window/observed statistics, never arbitrary injected control metadata.
    def alert_view(value: Any) -> Any:
        fields = (
            "alert_id",
            "service",
            "rule_id",
            "rule_version",
            "severity",
            "window_start",
            "window_end",
            "detected_at",
            "evidence",
        )
        result = {k: value[k] for k in fields if k in value}
        if len(canonical(result).encode()) > 4096:
            result.pop("evidence", None)
            result["evidence_omitted_due_to_size"] = True
        return result

    safe_incident = {
        k: incident[k]
        for k in (
            "incident_id",
            "status",
            "services",
            "candidate_service",
            "opened_at",
            "first_event_time",
            "last_event_time",
        )
        if incident and k in incident
    }
    if "services" in safe_incident:
        safe_incident["services_total"] = len(safe_incident["services"])
        safe_incident["services"] = safe_incident["services"][:48]
    compact_facts = {
        "services": observations.get("services", [])[:48],
        "trace_coverage": {
            k: v
            for k, v in observations.get("trace_coverage", {}).items()
            if k.endswith("count") or k == "definition"
        },
    }
    payload = {
        "prompt_version": prompt_version,
        "alert": alert_view(alert),
        "incident": safe_incident,
        "coverage": {
            k: v for k, v in snapshot.get("coverage", {}).items() if k != "trace_coverage"
        },
        "observations": compact_facts,
        "missing_evidence": missing[:12],
        "cases": cases[:3],
        "related_alerts": [alert_view(a) for a in related[:20]],
        "related_alerts_total": related_alert_total
        if related_alert_total is not None
        else len(related),
        "related_alerts_truncated": (
            related_alert_total if related_alert_total is not None else len(related)
        )
        > min(len(related), 20),
        "evidence_set": {
            "id": "evidence-" + digest(snapshot),
            "sha256": digest(snapshot),
            "scope": "immutable report snapshot; not a complete source-topic archive",
            "snapshot_record_count": len(snapshot["records"]),
            "snapshot_truncated": bool(snapshot.get("coverage", {}).get("truncated")),
        },
        "records": projected,
        "selection": {
            "strategy": "anomalous-traces-first-with-explicit-partial-subgraphs-v3",
            "configuration_error": config_error,
        },
    }

    def rebuild() -> Any:
        records = payload["records"]
        ids = {e["event_id"] for e in records}
        selected_services = {e["service"] for e in records}
        if "services" in payload["observations"]:
            payload["observations"]["services"] = [
                {
                    **row,
                    "evidence_ids": [ref for ref in row.get("evidence_ids", []) if ref in ids],
                    "observed_error_codes": row.get("observed_error_codes", [])[:8],
                }
                for row in compact_facts["services"]
                if row["service"] in selected_services
            ]
        by_trace = defaultdict(set)
        for e in records:
            if e.get("trace_id"):
                by_trace[e["trace_id"]].add(e["event_id"])
        payload["selection"].update(
            selected_record_count=len(records),
            omitted_record_count=len(snapshot["records"]) - len(records),
            partial_trace_ids=sorted(t for t, refs in by_trace.items() if refs != all_traces[t]),
            record_projections=[
                f
                for f in flags
                if f["event_id"] in ids
                and (f["truncated_fields"] or f["omitted_extension_field_count"])
            ],
        )
        payload["evidence_index"] = {
            key: [e["event_id"] for e in records if e["event_type"] in kinds]
            for key, kinds in (
                ("metrics", ("request", "host")),
                ("logs", ("log",)),
                ("spans", ("span",)),
            )
        }
        payload["event_timeline"] = [
            {
                k: e[k]
                for k in (
                    "event_id",
                    "event_time",
                    "service",
                    "event_type",
                    "status_code",
                    "error_code",
                )
                if k in e
            }
            for e in sorted(records, key=lambda e: (e["event_time"], e["event_id"]))
        ]
        payload["trace_links"] = [
            {k: e.get(k) for k in ("event_id", "trace_id", "span_id", "parent_span_id", "service")}
            for e in records
            if e["event_type"] == "span"
        ]
        payload["record_sources"] = [
            {
                "event_id": e["event_id"],
                "source": {
                    k: v
                    for k, v in (
                        sources[e["event_id"]] if isinstance(sources[e["event_id"]], dict) else {}
                    ).items()
                    if k
                    in {"transport", "topic", "partition", "offset", "kafka_timestamp", "timestamp"}
                },
            }
            for e in records
        ]

    rebuild()
    # Drop low-ranked final records first, with every omission visible. Bound other
    # data too: unusual corpus/alert payloads cannot silently expand prompt cost.
    for field in ("cases", "related_alerts"):
        minimum = 0 if field == "cases" else 1
        while (
            len(canonical(payload).encode()) > limits.max_context_bytes
            and len(payload[field]) > minimum
        ):
            payload[field].pop()
            payload["selection"][field + "_reduced_for_byte_limit"] = True
            if field == "related_alerts":
                payload["related_alerts_truncated"] = True
    if len(canonical(payload).encode()) > limits.max_context_bytes:
        payload["observations"] = {"omitted_due_to_context_budget": True}
        payload["selection"]["observations_reduced_for_byte_limit"] = True
    while len(canonical(payload).encode()) > limits.max_context_bytes and payload["records"]:
        payload["records"].pop()
        rebuild()
    if len(canonical(payload).encode()) > limits.max_context_bytes:
        payload["cases"] = []
        payload["related_alerts"] = [alert_view(alert)]
        payload["observations"] = {"omitted_due_to_context_budget": True}
        payload["missing_evidence"] = missing[:1]
        payload["selection"]["summaries_reduced_for_byte_limit"] = True
        rebuild()
    if len(canonical(payload).encode()) > limits.max_context_bytes:
        raise ModelError("context_too_large")
    payload = redact(payload)
    return payload, payload["records"]
