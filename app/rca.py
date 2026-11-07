"""Deterministic evidence graph RCA; candidate scores are not probabilities.

Call edges point from caller to callee. Error/latency propagation is evaluated
inside one trace, never by time correlation or by joining service-level edges
from different traces. This analyses already-collected evidence; it does not
replace the sample-gated window anomaly detector.
"""

import logging
from collections import defaultdict
from typing import Any

from .analysis_schema import validate_analysis
from .contracts import canonical, digest, number
from .detection_policy import adaptive_policy

RCA_VERSION = "evidence-graph-v1.1"
THRESHOLDS = {"http_error_min": 500, "latency_ms": 800, "host_percent": 90, "host_min_samples": 3}
MAX_RECORDS = 10000
log = logging.getLogger(__name__)


def _records(snapshot: Any) -> Any:
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("records"), list):
        raise ValueError("rca_snapshot_records_required")
    unique: dict[str, Any] = {}
    conflicts: set[str] = set()
    invalid = 0
    for item in snapshot["records"][:MAX_RECORDS]:
        event = item.get("event", item) if isinstance(item, dict) else None
        if not isinstance(event, dict) or not all(
            isinstance(event.get(k), str) and event[k] for k in ("event_id", "service")
        ):
            invalid += 1
            continue
        if event.get("event_type") not in {"request", "span", "host", "log"}:
            invalid += 1
            continue
        try:
            canonical(event)
        except (ValueError, TypeError, RecursionError):
            invalid += 1
            continue
        identifier = event["event_id"]
        if identifier in unique and unique[identifier] != event:
            conflicts.add(identifier)
        unique[identifier] = event
    for identifier in conflicts:
        unique.pop(identifier, None)
    if invalid or conflicts:
        log.warning(
            "RCA ignored invalid_records=%d conflicting_event_ids=%d", invalid, len(conflicts)
        )
    return unique, invalid, conflicts


def _observation_signals(events: Any) -> Any:
    signals = defaultdict(set)
    hosts = defaultdict(list)
    insufficient = set()
    for event in events.values():
        if event["event_type"] in {"span", "request"}:
            status, latency = event.get("status_code"), event.get("latency_ms")
            if type(status) is int and 500 <= status <= 599:
                signals[event["event_id"]].add("http_5xx")
            if number(latency) and latency > THRESHOLDS["latency_ms"]:
                signals[event["event_id"]].add("latency")
        elif event["event_type"] == "host":
            hosts[event["service"]].append(event)
    host_stats: dict[str, dict[str, Any]] = {}
    for service, rows in hosts.items():
        host_stats[service] = {}
        for field, signal in (("cpu_percent", "cpu"), ("memory_percent", "memory")):
            samples = [e for e in rows if number(e.get(field)) and 0 <= e[field] <= 100]
            if len(samples) < THRESHOLDS["host_min_samples"]:
                insufficient.add(service)
                continue
            average = sum(e[field] for e in samples) / len(samples)
            host_stats[service][field] = {
                "sample_count": len(samples),
                "average": round(average, 6),
            }
            if average > THRESHOLDS["host_percent"]:
                for event in samples:
                    if event[field] > THRESHOLDS["host_percent"]:
                        signals[event["event_id"]].add(signal)
    return signals, host_stats, insufficient


def _cycles(parents: Any) -> Any:
    """Cycle membership in a single-parent graph, without recursive traversal."""
    done, cyclic = set(), set()
    for start in parents:
        path: list[Any] = []
        positions: dict[Any, int] = {}
        node = start
        while node is not None and node not in done and node not in positions:
            positions[node] = len(path)
            path.append(node)
            node = parents.get(node)
        if node in positions:
            cyclic.update(path[positions[node] :])
        done.update(path)
    return cyclic


def _healthy_for(event: Any, channels: Any) -> Any:
    if event["event_type"] in {"request", "span"}:
        status, latency = event.get("status_code"), event.get("latency_ms")
        return ("http_5xx" in channels and type(status) is int and 100 <= status < 500) or (
            "latency" in channels and number(latency) and 0 <= latency <= THRESHOLDS["latency_ms"]
        )
    if event["event_type"] == "host":
        return any(
            signal in channels
            and number(event.get(field))
            and 0 <= event[field] <= THRESHOLDS["host_percent"]
            for field, signal in (("cpu_percent", "cpu"), ("memory_percent", "memory"))
        )
    return False


def analyze(
    snapshot: Any,
    alert: Any = None,
    historical_cases: Any = None,
    model_analysis: Any = None,
    model_metadata: Any = None,
) -> Any:
    """Return trace-grounded candidates and their auditable evidence references.

    A candidate is an observed anomalous endpoint, not a proven internal cause.
    Host pressure requires the same >=3 sample and >90 average gates as the
    existing fixed window rules. A lone 5xx/slow span is an observed symptom,
    not a newly raised high-error-rate/window alert.
    """
    events, invalid, conflicts = _records(snapshot)
    try:
        snapshot_hash = digest(snapshot)
    except (ValueError, TypeError, RecursionError, UnicodeError) as error:
        raise ValueError("rca_snapshot_not_canonical_json") from error
    signals, host_stats, insufficient_hosts = _observation_signals(events)
    policy = adaptive_policy(alert)
    if policy:
        # Reclassify only the alert's service/channel; no baseline is inferred for
        # other services and only records inside the observed window can support it.
        scoped = [e for e in events.values() if e["service"] == policy["service"]]
        for event in scoped:
            signals[event["event_id"]].discard(policy["channel"])
        rows = [
            e
            for e in scoped
            if type(e.get("event_time")) is int
            and policy["window_start"] <= e["event_time"] < policy["window_end"]
            and e["event_type"]
            in ({"request", "span"} if policy["channel"] == "latency" else {"host"})
            and number(e.get(policy["field"]))
            and e[policy["field"]] >= 0
        ]
        enough = policy["channel"] == "latency" or len(rows) >= THRESHOLDS["host_min_samples"]
        if enough and (
            policy["channel"] == "latency"
            or sum(e[policy["field"]] for e in rows) / len(rows) > policy["threshold"]
        ):
            for event in rows:
                if event[policy["field"]] > policy["threshold"]:
                    signals[event["event_id"]].add(policy["channel"])
    warnings = ["候选只表示观测到的异常源头，调用关系与同时异常仍不能证明内部因果；评分不是概率"]
    if policy:
        warnings.append(
            "动态阈值仅适用于告警服务及其窗口；其他服务缺少对应基线时，传播分析可能不完整"
        )
    coverage = snapshot.get("coverage") if isinstance(snapshot.get("coverage"), dict) else {}
    incomplete = bool(coverage.get("truncated")) or len(snapshot["records"]) > MAX_RECORDS
    if incomplete:
        warnings.append("证据快照或RCA输入达到上限，未覆盖的异常与调用关系仍未知")
    if invalid or conflicts:
        warnings.append("存在无效记录或冲突事件ID，已排除这些记录")
    if insufficient_hosts:
        warnings.append("部分服务主机指标样本不足，未据此建立资源异常候选")
    spans_by_key = defaultdict(list)
    for event in events.values():
        if (
            event["event_type"] == "span"
            and isinstance(event.get("trace_id"), str)
            and event["trace_id"]
            and isinstance(event.get("span_id"), str)
            and event["span_id"]
        ):
            spans_by_key[(event["trace_id"], event["span_id"])].append(event)
    ambiguous = {key for key, rows in spans_by_key.items() if len(rows) != 1}
    spans = {key: rows[0] for key, rows in spans_by_key.items() if len(rows) == 1}
    parents: dict[Any, Any] = {}
    children: dict[Any, list[Any]] = defaultdict(list)
    missing_services, declared_services = set(), set()
    edge_groups: dict[Any, Any] = {}
    observed_services = {e["service"] for e in events.values()}
    all_services = set(observed_services)

    def add_edge(
        caller: Any, callee: Any, basis: Any, rows: Any, trace_id: Any, channels: Any = ()
    ) -> Any:
        key = (caller, callee, basis)
        value = edge_groups.setdefault(
            key,
            {
                "caller": caller,
                "callee": callee,
                "basis": basis,
                "evidence_ids": set(),
                "trace_ids": set(),
                "anomaly_channels": set(),
                "observed_pairs": [],
            },
        )
        value["evidence_ids"].update(e["event_id"] for e in rows)
        if trace_id:
            value["trace_ids"].add(trace_id)
        value["anomaly_channels"].update(channels)
        if basis == "parent_span":
            value["observed_pairs"].append(
                {
                    "trace_id": trace_id,
                    "caller_event_id": rows[0]["event_id"],
                    "callee_event_id": rows[1]["event_id"],
                }
            )
        all_services.update((caller, callee))

    observed_children = {
        (key[0], e.get("parent_span_id"), e["service"])
        for key, e in spans.items()
        if isinstance(e.get("parent_span_id"), str)
    }
    for key, child in spans.items():
        parent_id = child.get("parent_span_id")
        parent_key = (key[0], parent_id) if isinstance(parent_id, str) and parent_id else None
        if parent_key is not None and parent_key in spans:
            parent = spans[parent_key]
            parents[key] = parent_key
            children[parent_key].append(key)
            shared = (
                signals.get(parent["event_id"], set())
                & signals.get(child["event_id"], set())
                & {"http_5xx", "latency"}
            )
            add_edge(
                parent["service"], child["service"], "parent_span", [parent, child], key[0], shared
            )
        else:
            parents[key] = None
            if parent_key is not None:
                missing_services.add(child["service"])
        downstream = child.get("downstream", [])
        for declared in downstream if isinstance(downstream, list) else []:
            if not isinstance(declared, str) or not declared:
                continue
            found = (key[0], key[1], declared) in observed_children
            if not found:
                add_edge(child["service"], declared, "declared_only", [child], key[0])
                declared_services.add(child["service"])
    cyclic = _cycles(parents)
    cyclic_services = {spans[key]["service"] for key in cyclic}
    if cyclic:
        warnings.append("同一调用链存在循环父引用，相关循环不用于确定传播源头")
    if missing_services or declared_services or ambiguous:
        warnings.append("部分父子span缺失、声明未观测或span标识冲突，相关候选置信度降低")
    eligible: dict[str, set[Any]] = defaultdict(set)
    propagated: dict[str, set[Any]] = defaultdict(set)
    own_traces: dict[str, set[Any]] = defaultdict(set)
    residual_observations: dict[str, list[Any]] = defaultdict(list)
    affected_by = defaultdict(set)
    candidate_channels = defaultdict(set)
    uncertain_services = missing_services | declared_services | cyclic_services
    candidate_uncertainty = set()
    for key in ambiguous:
        uncertain_services.update(e["service"] for e in spans_by_key[key])
    for identifier, channels in signals.items():
        if not channels:
            continue
        event = events[identifier]
        service = event["service"]
        span_key = (event.get("trace_id"), event.get("span_id"))
        if event["event_type"] in {"span", "request"} and span_key in ambiguous:
            continue
        if (
            event["event_type"] in {"span", "request"}
            and span_key in spans
            and spans[span_key]["service"] == service
        ):
            if span_key in cyclic:
                continue
            # Attribute each anomaly channel separately: a slow callee cannot
            # explain a caller's independent 5xx observation, or vice versa.
            local_channels = {
                channel
                for channel in channels
                if not any(
                    channel in signals.get(spans[c]["event_id"], set())
                    for c in children[span_key]
                    if c not in cyclic
                )
            }
            child_rows = [spans[c] for c in children[span_key] if c not in cyclic]
            observed_parent = spans[span_key]
            if (
                "latency" in channels
                and child_rows
                and number(observed_parent.get("latency_ms"))
                and all(number(c.get("latency_ms")) and c["latency_ms"] >= 0 for c in child_rows)
            ):
                # Subtracting summed child durations is conservative when
                # children overlap. This is unaccounted observed latency, not
                # proof of CPU/self-time or a complete exclusive-time measure.
                residual = max(
                    0, observed_parent["latency_ms"] - sum(c["latency_ms"] for c in child_rows)
                )
                residual_threshold = (
                    policy["threshold"]
                    if policy and policy["channel"] == "latency" and policy["service"] == service
                    else THRESHOLDS["latency_ms"]
                )
                if residual > residual_threshold:
                    local_channels.add("latency")
                    observation = {
                        "trace_id": span_key[0],
                        "parent_event_id": observed_parent["event_id"],
                        "child_event_ids": sorted(c["event_id"] for c in child_rows),
                        "observed_latency_residual_ms": round(residual, 6),
                    }
                    if observation not in residual_observations[service]:
                        residual_observations[service].append(observation)
            if not local_channels:
                continue
            eligible[service].add(identifier)
            if signals.get(spans[span_key]["event_id"], set()) & local_channels:
                eligible[service].add(spans[span_key]["event_id"])
            own_traces[service].add(span_key[0])
            candidate_channels[service].update(local_channels)
            previous, current = span_key, parents.get(span_key)
            propagation_channels = local_channels & signals.get(spans[span_key]["event_id"], set())
            visited = {span_key}
            while current is not None and current not in visited and current not in cyclic:
                visited.add(current)
                shared = propagation_channels & signals.get(spans[current]["event_id"], set())
                if not shared:
                    break
                ancestor = spans[current]
                propagated[service].update((ancestor["event_id"], spans[previous]["event_id"]))
                if ancestor["service"] != service:
                    affected_by[service].add(ancestor["service"])
                if ancestor["service"] in uncertain_services:
                    candidate_uncertainty.add(service)
                propagation_channels = shared
                previous, current = current, parents.get(current)
        else:
            eligible[service].add(identifier)
            candidate_channels[service].update(channels)
            if event["event_type"] != "host":
                candidate_uncertainty.add(service)
    uncertain_services |= candidate_uncertainty
    candidates = []
    for service, own_ids in eligible.items():
        channels = candidate_channels[service]
        support = own_ids | propagated[service]
        for observation in residual_observations[service]:
            support.update(observation["child_event_ids"])
        instances = {events[identifier].get("instance_id") for identifier in own_ids}
        against = {
            e["event_id"]
            for e in events.values()
            if e["service"] == service
            and e["event_id"] not in support
            and e.get("instance_id") in instances
            and _healthy_for(e, channels)
            and not signals.get(e["event_id"], set()) & channels
        }
        for event in events.values():
            if (
                event["event_type"] == "log"
                and event["service"] == service
                and event.get("trace_id") in own_traces[service]
                and str(event.get("level", "")).upper() in {"ERROR", "CRITICAL"}
            ):
                support.add(event["event_id"])
        components = {
            "own_anomaly": 30,
            "observed_trace_endpoint": 20 if own_traces[service] else 0,
            "same_trace_propagation": min(30, 10 * len(affected_by[service])),
            "independent_traces": min(10, 5 * max(0, len(own_traces[service]) - 1)),
            "incomplete_relationships": -15 if service in uncertain_services else 0,
            "truncated_snapshot": -10 if incomplete else 0,
            "counterobservations": -5 if against else 0,
        }
        score = max(0, min(100, sum(components.values())))
        uncertainty = ["内部原因未证明；候选分数是固定规则排序分数，不是概率"]
        if service in uncertain_services:
            uncertainty.append("关联span不完整、标识冲突或存在循环，无法排除未观测下游")
        if incomplete:
            uncertainty.append("快照存在截断，可能遗漏更直接的异常源头")
        if against:
            uncertainty.append("同服务同类指标也有正常观测，异常可能是间歇性或局部影响")
        if any(events[identifier]["event_type"] == "host" for identifier in own_ids):
            uncertainty.append("主机资源使用率不能定位进程或证明服务内部根因")
        if residual_observations[service]:
            uncertainty.append(
                "父span减已观测直接子span时长只表示未解释延迟；不是精确自耗时，未观测调用与调度等待仍未知"
            )
        candidates.append(
            {
                "service": service,
                "rank": 0,
                "score": score,
                "score_components": components,
                "confidence": "medium"
                if score >= 60
                and affected_by[service]
                and service not in uncertain_services
                and not incomplete
                else "low",
                "supporting_evidence_ids": sorted(support),
                "against_evidence_ids": sorted(against),
                "own_anomalous_evidence_ids": sorted(own_ids),
                "affected_services": sorted(affected_by[service]),
                "anomaly_channels": sorted(channels),
                "latency_residual_observations": sorted(
                    residual_observations[service],
                    key=lambda row: (row["trace_id"], row["parent_event_id"]),
                ),
                "reasoning": [
                    "存在自身服务的异常观测",
                    "按同一trace、同类异常和未解释延迟定位候选"
                    if own_traces[service]
                    else "缺少可证明传播的调用关系，保留孤立异常候选",
                    f"同链同类异常关联到{len(affected_by[service])}个调用方；未将不同trace按时间拼接",
                ],
                "uncertainties": uncertainty,
                "verification_steps": [
                    f"补齐{service}的父子span、内部依赖指标与部署变更，核对异常先后和自耗时",
                    "复测恢复后的错误与延迟，人工验证候选，避免直接执行未经确认的处置",
                ],
                "historical_case_matches": [],
                "model_hypotheses": [],
            }
        )
    candidates.sort(key=lambda c: (-c["score"], c["service"]))
    previous_score, rank = None, 0
    for index, candidate in enumerate(candidates, 1):
        if candidate["score"] != previous_score:
            rank = index
        candidate["rank"] = rank
        previous_score = candidate["score"]
    candidate_by_service = {c["service"]: c for c in candidates}
    integration: dict[str, Any] = {
        "rules": RCA_VERSION,
        "scores_are_probabilities": False,
        "history_changes_score": False,
        "model_changes_score": False,
        "history_matches": 0,
        "invalid_history_cases": 0,
        "model_status": "unavailable",
        "disagreements": [],
    }
    for entry in historical_cases or []:
        if not isinstance(entry, dict):
            integration["invalid_history_cases"] += 1
            continue
        case = entry.get("case", entry)
        try:
            if (
                not isinstance(case, dict)
                or not isinstance(case.get("case_id"), str)
                or not isinstance(case.get("services"), list)
            ):
                raise ValueError("invalid_case")
            fingerprint = digest(case)
            if entry.get("case_sha256", fingerprint) != fingerprint:
                raise ValueError("case_hash_mismatch")
            for service in (
                set(s for s in case["services"] if isinstance(s, str)) & candidate_by_service.keys()
            ):
                candidate_by_service[service]["historical_case_matches"].append(
                    {
                        "case_id": case["case_id"],
                        "case_sha256": fingerprint,
                        "title": case.get("title", ""),
                        "source": case.get("source", "unknown"),
                        "relation": "service_metadata_match_only_not_causal_evidence",
                    }
                )
                integration["history_matches"] += 1
        except (ValueError, TypeError, RecursionError):
            integration["invalid_history_cases"] += 1
    if model_analysis is not None:
        if not isinstance(model_metadata, dict) or model_metadata.get("model_status") not in {
            "validated",
            "grounded_observations",
        }:
            integration["model_status"] = "ignored_unvalidated"
        else:
            try:
                checked = validate_analysis(model_analysis, list(events.values()), alert=alert)
                integration["model_status"] = model_metadata["model_status"]
                mentioned = set()
                for hypothesis in checked["hypotheses"]:
                    service = hypothesis["service"]
                    mentioned.add(service)
                    if service in candidate_by_service:
                        candidate_by_service[service]["model_hypotheses"].append(hypothesis)
                    else:
                        integration["disagreements"].append(
                            {"kind": "model_candidate_not_rule_endpoint", "service": service}
                        )
                for service in candidate_by_service.keys() - mentioned:
                    integration["disagreements"].append(
                        {"kind": "rule_candidate_not_supported_by_model", "service": service}
                    )
            except (ValueError, TypeError, KeyError):
                integration["model_status"] = "rejected_invalid_structure_or_evidence"
                log.warning("RCA rejected model integration after evidence revalidation")
    direct = {events[identifier]["service"] for identifier, channels in signals.items() if channels}
    affected = (
        set().union(*(set(c["affected_services"]) for c in candidates)) if candidates else set()
    )
    if affected & candidate_by_service.keys():
        warnings.append("同一服务可在不同调用链或异常类型中同时作为候选和受影响服务，角色允许重叠")
    edges = []
    for key in sorted(edge_groups):
        edge = edge_groups[key]
        edges.append(
            {
                **edge,
                "evidence_ids": sorted(edge["evidence_ids"]),
                "trace_ids": sorted(edge["trace_ids"]),
                "anomaly_channels": sorted(edge["anomaly_channels"]),
                "observed_pairs": sorted(
                    edge["observed_pairs"],
                    key=lambda pair: (
                        pair["trace_id"],
                        pair["caller_event_id"],
                        pair["callee_event_id"],
                    ),
                ),
            }
        )
    nodes = [
        {
            "service": service,
            "observed": service in observed_services,
            "direct_anomalous": service in direct,
            "candidate": service in candidate_by_service,
            "affected": service in affected,
        }
        for service in sorted(all_services)
    ]
    status = "candidates" if candidates else "unknown"
    return {
        "rca_version": RCA_VERSION,
        "status": status,
        "evidence_set_id": "evidence-" + snapshot_hash,
        "direct_anomalous_services": sorted(direct),
        "affected_services": sorted(affected),
        "candidates": candidates,
        "graph": {"direction": "caller_to_callee", "nodes": nodes, "edges": edges},
        "uncertainties": warnings,
        "source_integration": integration,
        "summary": (
            "候选异常源头："
            + "、".join(c["service"] for c in candidates)
            + "；内部根因需进一步验证"
        )
        if candidates
        else "证据不足以形成可定位的异常源头，无法确定",
        "thresholds": dict(THRESHOLDS),
        "adaptive_threshold_policy": policy,
        "host_observations": host_stats,
        "input_quality": {
            "input_records": len(snapshot["records"]),
            "usable_records": len(events),
            "invalid_records": invalid,
            "conflicting_event_ids": len(conflicts),
            "ambiguous_span_ids": len(ambiguous),
            "cycle_spans": len(cyclic),
            "truncated": incomplete,
        },
    }
