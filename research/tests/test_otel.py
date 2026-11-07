"""Semantic and durability checks for the OTLP interoperability boundary."""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from research.otel.normalize import normalize
from scripts.research.otel_bridge import process_record


def attr(key: str, value: str) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def envelope() -> dict:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        attr("service.name", "payment"),
                        attr("service.instance.id", "pod-a"),
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "a" * 32,
                                "spanId": "b" * 16,
                                "parentSpanId": "c" * 16,
                                "startTimeUnixNano": "1700000000000000000",
                                "endTimeUnixNano": "1700000000123456789",
                                "kind": 2,
                                "attributes": [attr("rpc.grpc.status_code", "14")],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def test_grpc_projection_preserves_actual_semantics_and_server_requests() -> None:
    events, omitted = normalize(envelope(), "traces")
    assert omitted == 0
    assert [e["event_type"] for e in events] == ["span", "request"]
    assert events[0]["latency_ms"] == 123.456789
    assert events[0]["event_time"] == 1700000000123
    assert events[0]["status_code"] == 500
    assert events[0]["status_projection"] == "grpc_projection"
    assert events[0]["otel_attributes"]["rpc.grpc.status_code"] == "14"
    assert events[0]["event_id"] != events[1]["event_id"]
    assert normalize(envelope(), "traces")[0] == events


def test_client_spans_do_not_double_count_requests() -> None:
    raw = envelope()
    raw["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["kind"] = 3
    assert len(normalize(raw, "traces")[0]) == 1


@pytest.mark.parametrize(
    "mutation", ["missing_identity", "negative_duration", "future_timestamp", "nan_duration"]
)
def test_invalid_trace_fails_closed(mutation: str) -> None:
    raw = envelope()
    resource = raw["resourceSpans"][0]
    span = resource["scopeSpans"][0]["spans"][0]
    if mutation == "missing_identity":
        resource["resource"]["attributes"] = []
    elif mutation == "negative_duration":
        span["startTimeUnixNano"] = "1800000000000000000"
    elif mutation == "future_timestamp":
        span["endTimeUnixNano"] = "9999999999999999999"
    else:
        span["endTimeUnixNano"] = "NaN"
    with pytest.raises(ValueError):
        normalize(raw, "traces")


def test_paired_cpu_memory_only_never_fill_missing_metric_with_zero() -> None:
    resource = envelope()["resourceSpans"][0]["resource"]

    def metric(name: str, state: str, value: float) -> dict:
        return {
            "name": name,
            "gauge": {
                "dataPoints": [
                    {
                        "timeUnixNano": "1700000000000000000",
                        "asDouble": value,
                        "attributes": [attr("state", state)],
                    }
                ]
            },
        }

    cpu = metric("system.cpu.utilization", "idle", 0.25)
    memory = metric("system.memory.utilization", "used", 0.8)
    raw = {"resourceMetrics": [{"resource": resource, "scopeMetrics": [{"metrics": [cpu]}]}]}
    assert normalize(raw, "metrics") == ([], 1)
    raw["resourceMetrics"][0]["scopeMetrics"][0]["metrics"].append(memory)
    events, omitted = normalize(raw, "metrics")
    assert omitted == 0
    assert events[0]["cpu_percent"] == 75
    assert events[0]["memory_percent"] == 80
    invalid = copy.deepcopy(raw)
    invalid["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]["dataPoints"][0][
        "asDouble"
    ] = float("nan")
    with pytest.raises(ValueError):
        normalize(invalid, "metrics")


def test_log_evidence_keeps_message_and_ids() -> None:
    raw = {
        "resourceLogs": [
            {
                "resource": envelope()["resourceSpans"][0]["resource"],
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1700000000000000000",
                                "traceId": "abc",
                                "spanId": "def",
                                "severityText": "ERROR",
                                "body": {"stringValue": "dependency failed"},
                            }
                        ]
                    }
                ],
            }
        ]
    }
    event = normalize(raw, "logs")[0][0]
    assert event["trace_id"] == "abc"
    assert event["message"] == "dependency failed"
    assert event["level"] == "ERROR"


def record(raw: bytes) -> SimpleNamespace:
    return SimpleNamespace(value=raw, topic="otel.traces", partition=1, offset=42)


def test_acknowledgement_failure_propagates_and_cannot_allow_commit() -> None:
    producer = Mock()
    producer.send.return_value.get.side_effect = TimeoutError("unacknowledged")
    with pytest.raises(TimeoutError):
        process_record(record(json.dumps(envelope()).encode()), producer)


def test_invalid_otlp_is_durably_quarantined_with_original_offset() -> None:
    producer = Mock()
    assert process_record(record(b"not json"), producer) == (0, 0, True)
    args, kwargs = producer.send.call_args
    assert args[0] == "otel.deadletter"
    assert kwargs["value"]["origin"]["offset"] == 42
    assert len(kwargs["value"]["raw_sha256"]) == 64
    producer.send.return_value.get.assert_called_once_with(timeout=30)


def test_recursion_poison_goes_to_acknowledged_dlq() -> None:
    producer = Mock()
    raw = b"[" * 2000 + b"0" + b"]" * 2000
    assert process_record(record(raw), producer) == (0, 0, True)
    assert producer.send.call_args[0][0] == "otel.deadletter"
    assert producer.send.call_args[1]["value"]["reason"].startswith("RecursionError")
    producer.send.return_value.get.assert_called_once_with(timeout=30)
    producer.send.return_value.get.side_effect = TimeoutError("DLQ unavailable")
    with pytest.raises(TimeoutError):
        process_record(record(raw), producer)
