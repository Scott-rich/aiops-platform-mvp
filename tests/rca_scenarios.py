"""Fixed synthetic RCA scenarios. Ground-truth labels never enter analyze().

Labels describe the deliberately constructed observed fault source, not a claim
that HTTP evidence proves a CPU/database/process-level root cause. Duplicate and
reordered variants share a family and are not independent production samples.
"""
from copy import deepcopy
from dataclasses import dataclass
import random
import uuid

BASE_TIME = 1_700_000_000_000


@dataclass(frozen=True)
class Scenario:
    case_id: str
    family: str
    description: str
    snapshot: dict
    alert: dict | None
    labels: dict


def identifier(value):
    return uuid.uuid5(uuid.NAMESPACE_URL, "aiops-rca-evaluation/" + value).hex


def trace_records(name, specifications, repetition=0):
    """Each specification: (service, parent_service, code, latency_ms, children)."""
    trace = identifier(f"{name}/trace/{repetition}")
    span_ids = {service: identifier(f"{trace}/{service}")[:16] for service, *_ in specifications}
    output = []
    start = BASE_TIME + repetition * 10_000
    for index, (service, parent, code, latency, children) in enumerate(specifications):
        span_id = span_ids[service]
        common = {"schema_version": 1, "service": service, "instance_id": service + "-fixture",
                  "trace_id": trace, "span_id": span_id, "event_time": start + int(latency) + index * 10}
        for kind in ("span", "request", "log"):
            event = {**common, "event_id": identifier(f"{trace}/{service}/{kind}"), "event_type": kind}
            if kind == "span":
                event.update(parent_span_id=span_ids.get(parent) if parent else None,
                             downstream=list(children), start_time=start + index * 10,
                             end_time=common["event_time"], latency_ms=latency, status_code=code)
            elif kind == "request":
                event.update(latency_ms=latency, status_code=code, error=code >= 500)
            else:
                event.update(level="ERROR" if code >= 500 else "INFO",
                             message="request failed" if code >= 500 else "request completed",
                             error_code=("UPSTREAM_ERROR" if children else "HTTP_ERROR") if code >= 500 else None)
            output.append(event)
    return output


def make_case(case_id, description, events, roots, affected=(), status="candidates", *, family=None,
              healthy=False, forbidden_edges=()):
    events = deepcopy(events)
    times = [event["event_time"] for event in events]
    start, end = min(times) - 1000, max(times) + 1000
    snapshot = {"query": {"start": start, "end": end, "service": events[0]["service"],
                           "relation": "fixed synthetic trace fixture"},
                "coverage": {"range_candidate_count": len(events), "scanned": len(events),
                             "related": len(events), "selected": len(events), "truncated": False},
                "records": [{"event": event, "source": {"transport": "synthetic-fixture"},
                             "ingested_at": event["event_time"] + 25} for event in events]}
    alert = {"schema_version": 1, "event_type": "alert", "alert_id": identifier(case_id + "/alert"),
             "service": events[0]["service"], "rule_id": "labelled_fixture_observation",
             "window_start": start, "window_end": end, "detected_at": end + 1,
             "severity": "warning", "evidence": {"sample_count": len(events)}}
    return Scenario(case_id, family or case_id, description, snapshot, alert,
                    {"root_services": list(roots), "affected_services": list(affected),
                     "expected_status": status, "healthy": healthy,
                     "forbidden_edges": [list(edge) for edge in forbidden_edges]})


def scenarios():
    payment_specs = [("gateway", None, 502, 320, ["order"]),
                     ("order", "gateway", 502, 250, ["payment"]),
                     ("payment", "order", 503, 120, [])]
    payment = [event for repeat in range(3) for event in trace_records("payment-cascade", payment_specs, repeat)]
    cases = [make_case("payment_503_cascade", "Payment emits 503; order and gateway propagate failure in the same trace.",
                       payment, ["payment"], ["gateway", "order"], family="payment_cascade")]
    inventory_specs = [("gateway", None, 200, 1800, ["order"]),
                       ("order", "gateway", 200, 1700, ["inventory"]),
                       ("inventory", "order", 200, 1500, [])]
    inventory = [event for repeat in range(3) for event in trace_records("inventory-latency", inventory_specs, repeat)]
    cases.append(make_case("inventory_latency_cascade", "A slow inventory leaf dominates the same-trace caller durations.",
                           inventory, ["inventory"], ["gateway", "order"]))
    independent = payment + [event for repeat in range(3) for event in trace_records("independent-inventory", [
        ("checkout", None, 502, 260, ["catalog"]), ("catalog", "checkout", 502, 180, ["inventory"]),
        ("inventory", "catalog", 500, 90, [])], repeat)]
    cases.append(make_case("two_independent_roots", "Independent payment and inventory failures both require candidates.",
                           independent, ["payment", "inventory"], ["gateway", "order", "checkout", "catalog"]))
    collision = []
    for repeat in range(3):
        a = trace_records("separate-alpha", [("alpha", None, 500, 100, [])], repeat)
        b = trace_records("separate-beta", [("beta", None, 503, 100, [])], repeat)
        # Deliberate span-id collision across distinct traces must not create a parent edge.
        b[0]["parent_span_id"] = a[0]["span_id"]
        collision.extend(a + b)
    cases.append(make_case("same_time_different_trace", "Simultaneous errors and cross-trace span-ID matches do not imply causality.",
                           collision, ["alpha", "beta"], forbidden_edges=[("alpha", "beta"), ("beta", "alpha")]))
    healthy = [event for repeat in range(3) for event in trace_records("healthy", [
        ("gateway", None, 200, 80, ["order"]), ("order", "gateway", 200, 50, ["payment"]),
        ("payment", "order", 200, 20, [])], repeat)]
    cases.append(make_case("healthy_chain", "Healthy spans cannot yield a fault root merely because an alert context is supplied.",
                           healthy, [], status="unknown", healthy=True))
    missing = trace_records("missing-child", [("order", None, 200, 80, ["payment"])])
    cases.append(make_case("missing_child_insufficient", "A declared payment dependency has no child observation or abnormal evidence.",
                           missing, [], status="unknown"))
    parent_fault = [event for repeat in range(3) for event in trace_records("parent-self-fault", [
        ("gateway", None, 502, 220, ["order"]), ("order", "gateway", 500, 150, ["inventory"]),
        ("inventory", "order", 200, 25, [])], repeat)]
    for event in parent_fault:
        if event["service"] == "order" and event["event_type"] == "log":
            event.update(message="local request handler failed", error_code="INTERNAL_ERROR")
    cases.append(make_case("parent_fault_healthy_child", "Order fails internally while its observed inventory dependency is healthy.",
                           parent_fault, ["order"], ["gateway"]))
    cycle = trace_records("ambiguous-cycle", [("alpha", "beta", 500, 100, ["beta"]),
                                               ("beta", "alpha", 500, 100, ["alpha"])])
    cases.append(make_case("ambiguous_parent_cycle", "A closed parent-reference cycle has no observable source endpoint; do not invent one.",
                           cycle, [], status="unknown"))
    cases.append(make_case("duplicate_payment_events", "Exact duplicate event IDs must not change root ordering or propagation.",
                           payment + deepcopy(payment), ["payment"], ["gateway", "order"], family="payment_cascade"))
    shuffled = deepcopy(payment)
    random.Random(173).shuffle(shuffled)
    cases.append(make_case("out_of_order_payment_events", "Arrival ordering is shuffled with a fixed seed; event identities and traces stay fixed.",
                           shuffled, ["payment"], ["gateway", "order"], family="payment_cascade"))
    host_events = [{"schema_version": 1, "event_id": identifier(f"healthy-host/{i}"), "event_time": BASE_TIME + i * 1000,
                    "service": "worker", "instance_id": "worker-fixture", "event_type": "host", "trace_id": "",
                    "cpu_percent": 30.0, "memory_percent": 40.0} for i in range(3)]
    host_events.append({"schema_version": 1, "event_id": identifier("healthy-host/log"), "event_time": BASE_TIME + 3000,
                        "service": "worker", "instance_id": "worker-fixture", "event_type": "log", "trace_id": "",
                        "level": "INFO", "message": "worker heartbeat"})
    cases.append(make_case("healthy_host_without_traces", "Normal resource samples and INFO logs are insufficient for an anomaly root.",
                           host_events, [], status="unknown", healthy=True))
    orphan = [event for repeat in range(3) for event in trace_records("orphan-payment", [("payment", None, 503, 150, [])], repeat)]
    for event in orphan:
        if event["event_type"] == "span":
            event["parent_span_id"] = identifier("absent-parent")[:16]
    cases.append(make_case("orphan_abnormal_leaf", "A payment 503 is observed despite missing parent spans; never invent a caller service.",
                           orphan, ["payment"]))
    return cases
