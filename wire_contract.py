"""Dependency-free schema-v1 wire contract shared by API and stream workers.

Timestamps are integer UTC Unix milliseconds. Correlation IDs remain optional for
legacy schema-v1 producers; new request/span producers should send trace_id.
"""
import hashlib
import json
import math
import time

MAX_FUTURE_MS = 60_000
MAX_ID_LENGTH = 256


def now_ms():
    return int(time.time() * 1000)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def number(value):
    try:
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    except (OverflowError, ValueError):
        return False


def _finite_json_numbers(value):
    if isinstance(value, dict):
        for child in value.values():
            _finite_json_numbers(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _finite_json_numbers(child)
    elif isinstance(value, (int, float)) and not isinstance(value, bool) and not number(value):
        raise ValueError("event must contain finite JSON numbers")


def validate_event(event):
    if not isinstance(event, dict) or type(event.get("schema_version")) is not int or event["schema_version"] != 1:
        raise ValueError("expected schema_version=1 object")
    if "__kafka_origin" in event:
        raise ValueError("reserved source metadata field")
    for name in ("event_id", "service", "instance_id"):
        if not isinstance(event.get(name), str) or not event[name] or len(event[name]) > MAX_ID_LENGTH:
            raise ValueError(f"invalid {name}")
    timestamp = event.get("event_time")
    if type(timestamp) is not int or not 0 < timestamp <= now_ms() + MAX_FUTURE_MS:
        raise ValueError("event_time must be positive integer epoch milliseconds within 60 seconds of now")
    if not isinstance(event.get("trace_id", ""), str) or len(event.get("trace_id", "")) > MAX_ID_LENGTH:
        raise ValueError("invalid trace_id")
    if "status_code" in event and (type(event["status_code"]) is not int or not 100 <= event["status_code"] <= 599):
        raise ValueError("invalid optional status_code")
    if "latency_ms" in event and (not number(event["latency_ms"]) or event["latency_ms"] < 0):
        raise ValueError("invalid optional latency_ms")
    for key in ("span_id", "parent_span_id", "error_code"):
        if key in event and event[key] is not None and (not isinstance(event[key], str) or len(event[key]) > MAX_ID_LENGTH):
            raise ValueError(f"invalid optional {key}")
    if "downstream" in event and (not isinstance(event["downstream"], list)
                                   or not all(isinstance(v, str) and len(v) <= MAX_ID_LENGTH for v in event["downstream"])):
        raise ValueError("invalid downstream services")
    kind = event.get("event_type")
    if kind in ("request", "span"):
        if not number(event.get("latency_ms")) or event["latency_ms"] < 0:
            raise ValueError("invalid latency_ms")
        if type(event.get("status_code")) is not int or not 100 <= event["status_code"] <= 599:
            raise ValueError("invalid status_code")
    elif kind == "log":
        if not isinstance(event.get("message"), str) or not isinstance(event.get("level"), str):
            raise ValueError("invalid log")
    elif kind == "host":
        for key in ("cpu_percent", "memory_percent"):
            if not number(event.get(key)) or not 0 <= event[key] <= 100:
                raise ValueError(f"invalid {key}")
    else:
        raise ValueError("unsupported event_type")
    try:
        _finite_json_numbers(event)
        canonical(event).encode("utf-8")
    except (TypeError, OverflowError, ValueError, RecursionError, UnicodeError) as error:
        raise ValueError("event must contain finite JSON values and valid Unicode") from error
    return event


def validate_metric(event):
    validate_event(event)
    if event["event_type"] not in ("request", "host"):
        raise ValueError("event_type must be request or host")
    return event
