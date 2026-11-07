"""Explicit OTLP mappings, with no fabricated missing metric values."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from wire_contract import digest, validate_event


def attributes(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Decode scalar AnyValue entries; nested attributes are intentionally omitted."""
    result: dict[str, Any] = {}
    for item in items:
        value = item.get("value", {})
        for key in ("stringValue", "intValue", "doubleValue", "boolValue"):
            if key in value:
                result[str(item["key"])] = value[key]
                break
    return result


def identity(resource: dict[str, Any]) -> tuple[str, str]:
    """Require a real service identity; do not alias unknown services together."""
    attrs = attributes(resource.get("attributes", []))
    service = attrs.get("service.name")
    instance = attrs.get("service.instance.id") or attrs.get("k8s.pod.uid") or attrs.get("host.id")
    if not service or not instance:
        raise ValueError("OTLP resource requires service.name and an instance/pod/host ID")
    return str(service), str(instance)


def base(
    resource: dict[str, Any], timestamp: Any, raw: dict[str, Any], kind: str
) -> dict[str, Any]:
    """Nanoseconds are converted with integer arithmetic; IDs are replay-stable."""
    service, instance = identity(resource)
    ns = int(timestamp)
    if ns <= 0:
        raise ValueError("missing OTLP timestamp")
    return {
        "schema_version": 1,
        "event_id": digest(["otlp-json-v1", service, instance, kind, raw]),
        "event_time": ns // 1_000_000,
        "event_type": kind,
        "service": service,
        "instance_id": instance,
        "trace_id": str(raw.get("traceId", "")),
        "otel_mapping_version": "otlp-json-v1",
    }


def status(span: dict[str, Any], attrs: dict[str, Any]) -> tuple[int, str]:
    """HTTP status is real when available; otherwise legacy success/error projection."""
    for key in ("http.response.status_code", "http.status_code"):
        if key in attrs:
            return int(attrs[key]), "http_attribute"
    grpc = attrs.get("rpc.grpc.status_code")
    error = int(grpc) != 0 if grpc is not None else int(span.get("status", {}).get("code", 0)) == 2
    return (500 if error else 200), (
        "grpc_projection" if grpc is not None else "otel_status_projection"
    )


def traces(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for resource in payload.get("resourceSpans", []):
        for scope in resource.get("scopeSpans", []):
            for raw in scope.get("spans", []):
                attrs = attributes(raw.get("attributes", []))
                event = base(
                    resource.get("resource", {}), raw.get("endTimeUnixNano", 0), raw, "span"
                )
                duration = int(raw["endTimeUnixNano"]) - int(raw["startTimeUnixNano"])
                if duration < 0 or not raw.get("traceId") or not raw.get("spanId"):
                    raise ValueError("invalid trace identity or time interval")
                code, projection = status(raw, attrs)
                event.update(
                    latency_ms=duration / 1_000_000,
                    status_code=code,
                    span_id=str(raw["spanId"]),
                    parent_span_id=str(raw.get("parentSpanId", "")),
                    status_projection=projection,
                    otel_attributes=attrs,
                    span_kind=raw.get("kind", 0),
                )
                peer = attrs.get("peer.service") or attrs.get("server.address")
                if peer:
                    event["downstream"] = [str(peer)]
                result.append(validate_event(event))
                # Only SERVER spans become requests; CLIENT spans must not double-count traffic.
                if raw.get("kind") in (2, "SPAN_KIND_SERVER"):
                    request = dict(
                        event, event_type="request", event_id=digest([event["event_id"], "request"])
                    )
                    result.append(validate_event(request))
    return result


def logs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for resource in payload.get("resourceLogs", []):
        for scope in resource.get("scopeLogs", []):
            for raw in scope.get("logRecords", []):
                event = base(
                    resource.get("resource", {}),
                    raw.get("timeUnixNano") or raw.get("observedTimeUnixNano", 0),
                    raw,
                    "log",
                )
                body = raw.get("body", {})
                if "stringValue" not in body:
                    raise ValueError("only string log bodies are mapped; retain raw OTLP in DLQ")
                event.update(
                    message=str(body["stringValue"]),
                    level=str(raw.get("severityText", "UNSPECIFIED")),
                    otel_attributes=attributes(raw.get("attributes", [])),
                )
                if raw.get("spanId"):
                    event["span_id"] = str(raw["spanId"])
                result.append(validate_event(event))
    return result


def metrics(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    """Map paired utilization gauges only; retain unsupported metrics upstream."""
    result: list[dict[str, Any]] = []
    unsupported = 0
    for resource in payload.get("resourceMetrics", []):
        groups: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        for scope in resource.get("scopeMetrics", []):
            for metric in scope.get("metrics", []):
                name = metric.get("name")
                if name not in {"system.cpu.utilization", "system.memory.utilization"}:
                    unsupported += 1
                    continue
                for point in metric.get("gauge", {}).get("dataPoints", []):
                    attrs = attributes(point.get("attributes", []))
                    state = attrs.get("state")
                    # Idle CPU may have one sample per core; average rather than sum cores.
                    key = (
                        "cpu"
                        if name == "system.cpu.utilization" and state == "idle"
                        else "memory"
                        if name == "system.memory.utilization" and state == "used"
                        else None
                    )
                    if key:
                        value = float(point.get("asDouble", point.get("asInt", -1)))
                        if not 0 <= value <= 1:
                            raise ValueError("utilization must be a finite fraction in [0,1]")
                        groups[str(point.get("timeUnixNano", 0))][key].append(value)
        for timestamp, values in groups.items():
            if not values.get("cpu") or len(values.get("memory", [])) != 1:
                unsupported += 1
                continue
            cpu = 100 * (1 - sum(values["cpu"]) / len(values["cpu"]))
            memory = 100 * values["memory"][0]
            raw = {"timestamp": timestamp, "cpu_percent": cpu, "memory_percent": memory}
            event = base(resource.get("resource", {}), timestamp, raw, "host")
            event.update(cpu_percent=cpu, memory_percent=memory)
            result.append(validate_event(event))
    return result, unsupported


def normalize(payload: dict[str, Any], signal: str) -> tuple[list[dict[str, Any]], int]:
    """Return schema-v1 events plus unmapped metric groups, failing on malformed data."""
    if not isinstance(payload, dict):
        raise ValueError("OTLP payload must be an object")
    if signal == "traces" and "resourceSpans" in payload:
        return traces(payload), 0
    if signal == "logs" and "resourceLogs" in payload:
        return logs(payload), 0
    if signal == "metrics" and "resourceMetrics" in payload:
        return metrics(payload)
    raise ValueError("signal does not match OTLP JSON envelope")
