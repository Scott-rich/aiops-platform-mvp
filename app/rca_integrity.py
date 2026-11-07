"""Reference and hash checks for immutable, evidence-bound RCA attachments."""

from typing import Any

from .contracts import digest


def _reference_check(value: Any, snapshot: Any) -> Any:
    if not isinstance(value, dict):
        return {"valid": False, "errors": ["invalid_rca_attachment"]}
    events = [row.get("event", row) for row in snapshot.get("records", [])]
    by_id = {event["event_id"]: event for event in events}
    trace_ids = {event.get("trace_id") for event in events if event.get("trace_id")}
    errors = []

    def walk(item: Any) -> Any:
        if isinstance(item, dict):
            for key, child in item.items():
                if key in {
                    "evidence_ids",
                    "supporting_evidence_ids",
                    "against_evidence_ids",
                    "own_anomalous_evidence_ids",
                    "child_event_ids",
                }:
                    if not isinstance(child, list) or any(
                        not isinstance(ref, str) or ref not in by_id for ref in child
                    ):
                        errors.append("unknown_evidence_reference")
                elif key in {"caller_event_id", "callee_event_id", "parent_event_id"}:
                    if not isinstance(child, str) or child not in by_id:
                        errors.append("unknown_edge_reference")
                elif key == "trace_ids":
                    if not isinstance(child, list) or any(
                        not isinstance(ref, str) or ref not in trace_ids for ref in child
                    ):
                        errors.append("unknown_trace_reference")
                else:
                    walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)

    if not isinstance(value.get("candidates"), list) or not isinstance(value.get("graph"), dict):
        return {"valid": False, "errors": ["invalid_rca_structure"]}
    if not isinstance(value["graph"].get("edges"), list) or not isinstance(
        value["graph"].get("nodes"), list
    ):
        return {"valid": False, "errors": ["invalid_rca_structure"]}
    if any(
        not isinstance(node, dict)
        or not isinstance(node.get("service"), str)
        or not node["service"]
        for node in value["graph"]["nodes"]
    ):
        return {"valid": False, "errors": ["invalid_rca_structure"]}
    walk(value)
    for edge in value.get("graph", {}).get("edges", []):
        if edge.get("basis") == "parent_span":
            pairs = edge.get("observed_pairs", [])
            if not pairs:
                errors.append("edge_without_observed_pair")
            for pair in pairs:
                parent = by_id.get(pair.get("caller_event_id"), {})
                child = by_id.get(pair.get("callee_event_id"), {})
                if not (
                    parent.get("event_type") == child.get("event_type") == "span"
                    and parent.get("service") == edge.get("caller")
                    and child.get("service") == edge.get("callee")
                    and isinstance(pair.get("trace_id"), str)
                    and bool(pair["trace_id"])
                    and parent.get("trace_id") == child.get("trace_id") == pair.get("trace_id")
                    and parent.get("span_id")
                    and parent.get("span_id") == child.get("parent_span_id")
                ):
                    errors.append("edge_relationship_not_supported")
        elif edge.get("basis") == "declared_only":
            if not any(
                by_id.get(ref, {}).get("service") == edge.get("caller")
                and edge.get("callee") in by_id.get(ref, {}).get("downstream", [])
                for ref in edge.get("evidence_ids", [])
                if isinstance(ref, str)
            ):
                errors.append("declared_edge_not_supported")
        else:
            errors.append("unknown_edge_basis")
    for candidate in value.get("candidates", []):
        if not isinstance(candidate, dict):
            errors.append("invalid_candidate")
            continue
        support = candidate.get("supporting_evidence_ids", [])
        against = candidate.get("against_evidence_ids", [])
        if (
            not isinstance(support, list)
            or not isinstance(against, list)
            or not all(isinstance(ref, str) for ref in support + against)
        ):
            errors.append("invalid_candidate_references")
            continue
        if set(support) & set(against):
            errors.append("contradictory_evidence_reference")
        if not any(
            by_id.get(ref, {}).get("service") == candidate.get("service") for ref in support
        ):
            errors.append("candidate_without_own_service_evidence")
    return {"valid": not errors, "errors": sorted(set(errors))}


def reference_check(value: Any, snapshot: Any) -> Any:
    try:
        return _reference_check(value, snapshot)
    except (AttributeError, TypeError, ValueError, KeyError, RecursionError):
        # Corrupted attachment structure must fail verification, not the API.
        return {"valid": False, "errors": ["invalid_rca_structure"]}


def verify_attachment(report: Any) -> Any:
    value = report.get("rca")
    if "rca" not in report:
        return {
            "rca_sha256_valid": None,
            "rca_evidence_references_valid": None,
            "rca_evidence_set_valid": None,
        }
    snapshot = report["snapshot"]
    if not isinstance(value, dict):
        return {
            "rca_sha256_valid": False,
            "rca_evidence_references_valid": False,
            "rca_evidence_set_valid": False,
        }
    return {
        "rca_sha256_valid": report["audit"].get("rca_sha256") == digest(value),
        "rca_evidence_references_valid": reference_check(value, snapshot)["valid"],
        "rca_evidence_set_valid": value.get("evidence_set_id") == "evidence-" + digest(snapshot),
    }
