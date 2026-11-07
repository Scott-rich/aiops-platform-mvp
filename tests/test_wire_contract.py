import base64
import copy
import hashlib
import json
import time
import unittest
from unittest.mock import patch

from app.contracts import validate_event
from flink.rules import validate_metric
from flink.stream_support import MAX_DLQ_BYTES, ORIGIN_FIELD, bounded_deadletter_json, deadletter, event_fingerprint, parse_metric_envelope
from wire_contract import MAX_FUTURE_MS, number
from flink.aggregate import WindowAccumulator, evaluate_accumulator


def metric(**changes):
    return {"schema_version": 1, "event_id": "event-one", "event_time": int(time.time() * 1000),
            "event_type": "request", "service": "payment", "instance_id": "one", "trace_id": "trace-one",
            "latency_ms": 900, "status_code": 503, **changes}


def envelope(event=None, raw=None):
    return json.dumps({"_aiops_kafka_envelope": 1, "raw": json.dumps(event) if raw is None else raw,
                       "origin": {"topic": "telemetry.metrics", "partition": 2, "offset": 18, "timestamp": 1000}})


class WireContractTests(unittest.TestCase):
    def test_metric_paths_use_identical_contract_and_do_not_mutate_input(self):
        event = metric()
        before = copy.deepcopy(event)
        self.assertIs(validate_event(event), event)
        self.assertIs(validate_metric(event), event)
        self.assertEqual(event, before)
        host = metric(event_type="host", cpu_percent=92, memory_percent=90)
        self.assertIs(validate_metric(host), host)

    def test_bad_input_matrix_rejected_consistently(self):
        mutations = [{"latency_ms": 10 ** 1000}, {"latency_ms": float("nan")}, {"latency_ms": float("inf")},
                     {"latency_ms": True}, {"latency_ms": -1}, {"status_code": True}, {"status_code": 600},
                     {"event_time": 10 ** 1000}, {"event_time": 1.5}, {"event_time": True},
                     {"event_time": int(time.time() * 1000) + MAX_FUTURE_MS + 60_000},
                     {"service": "x" * 257}, {"schema_version": True}, {"trace_id": None},
                     {"downstream": [42]}, {"span_id": {}}, {"extra": float("inf")}, {"extra": [True, {"huge": 10 ** 1000}]},
                     {ORIGIN_FIELD: {"offset": 1}}, {"service": "\ud800"}, {"extra": {"bad": "\udfff"}},
                     {"event_type": "host", "cpu_percent": 10 ** 1000, "memory_percent": 20}]
        for changes in mutations:
            for validator in (validate_event, validate_metric):
                with self.subTest(changes=tuple(changes), validator=validator.__name__):
                    with self.assertRaises(ValueError):
                        validator(metric(**changes))
        self.assertFalse(number(10 ** 1000))

    def test_v1_optional_trace_id_compatibility_and_non_metrics(self):
        event = metric()
        del event["trace_id"]
        self.assertIs(validate_event(event), event)
        self.assertIs(validate_metric(event), event)
        event = metric(event_type="log", level="INFO", message="ok")
        self.assertIs(validate_event(event), event)
        with self.assertRaisesRegex(ValueError, "request or host"):
            validate_metric(event)

    def test_parse_preserves_coordinates_and_rejects_reserved_field(self):
        event = metric()
        parsed = parse_metric_envelope(envelope(event))
        self.assertEqual(parsed[ORIGIN_FIELD]["offset"], 18)
        self.assertEqual({k: v for k, v in parsed.items() if k != ORIGIN_FIELD}, event)
        with self.assertRaisesRegex(ValueError, "reserved"):
            parse_metric_envelope(envelope(metric(**{ORIGIN_FIELD: {}})))
        with self.assertRaises(ValueError):
            parse_metric_envelope(json.dumps(event))

    def test_invalid_raw_and_conflicts_are_auditable(self):
        raw = envelope(raw="{invalid-json")
        with self.assertRaises(ValueError):
            parse_metric_envelope(raw)
        dlq = json.loads(deadletter("invalid_json", value=raw))
        self.assertEqual(dlq["raw"], "{invalid-json")
        self.assertEqual(dlq["origin"]["partition"], 2)
        event = parse_metric_envelope(envelope(metric()))
        other_origin = {**event, ORIGIN_FIELD: {"offset": 19}}
        self.assertEqual(event_fingerprint(event), event_fingerprint(other_origin))
        self.assertNotEqual(event_fingerprint(event), event_fingerprint({**event, "service": "order"}))
        conflict = json.loads(deadletter("conflicting_duplicate_event_id", event=event,
                                         previous={"fingerprint": "abc", "origin": {"offset": 17}}))
        self.assertEqual(conflict["origin"]["offset"], 18)
        self.assertEqual(conflict["first_seen"]["origin"]["offset"], 17)
        self.assertNotIn(ORIGIN_FIELD, conflict["event"])

    def test_deadletter_caps_escaped_raw_and_multibyte_prefixes(self):
        for raw in ('{' + '"' * 600_000, '测' * 330_000):
            serialized = deadletter("invalid_json", value=envelope(raw=raw))
            self.assertLessEqual(len(serialized.encode("utf-8")), MAX_DLQ_BYTES)
            audit = json.loads(serialized)
            self.assertTrue(audit["truncated"])
            self.assertGreater(audit["original_serialized_bytes"], MAX_DLQ_BYTES)
            self.assertEqual(audit["raw_byte_length"], len(raw.encode("utf-8")))
            self.assertEqual(audit["raw_sha256"], hashlib.sha256(raw.encode("utf-8")).hexdigest())
            self.assertLessEqual(len(audit["raw_prefix"].encode("utf-8")), 4096)
            self.assertTrue(raw.startswith(audit["raw_prefix"]))
            self.assertEqual(audit["origin"]["offset"], 18)

    def test_deadletter_caps_expanded_invalid_utf8_and_retains_raw_hash(self):
        raw = bytes([255]) * 600_001
        wrapped = json.dumps({"_aiops_kafka_envelope": 1, "raw": None, "encoding_error": "invalid_utf8",
                              "raw_base64": base64.b64encode(raw).decode(), "origin": {"topic": "telemetry.metrics", "partition": 1, "offset": 7}})
        serialized = deadletter("invalid_utf8", value=wrapped)
        self.assertLessEqual(len(serialized.encode("utf-8")), MAX_DLQ_BYTES)
        audit = json.loads(serialized)
        self.assertTrue(audit["truncated"])
        self.assertEqual(audit["raw_byte_length"], 600_001)
        self.assertEqual(audit["raw_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(audit["encoding_error"], "invalid_utf8")
        self.assertEqual(audit["origin"]["offset"], 7)
        self.assertNotIn("raw_base64", audit)
        self.assertLessEqual(len(audit["raw_base64_prefix"]), 4096)

    def test_deadletter_final_cap_covers_large_late_event_and_arbitrary_fields(self):
        event = metric(extra="large-" * 150_000, **{ORIGIN_FIELD: {"topic": "telemetry.metrics", "partition": 2, "offset": 19}})
        serialized = deadletter("event_time_older_than_watermark", event=event,
                                previous={"fingerprint": "a" * 64, "extra": "huge" * 200_000})
        self.assertLessEqual(len(serialized.encode("utf-8")), MAX_DLQ_BYTES)
        audit = json.loads(serialized)
        self.assertTrue(audit["truncated"])
        self.assertEqual(audit["origin"]["offset"], 19)
        self.assertEqual(audit["event_summary"]["event_id"], event["event_id"])
        self.assertGreater(audit["event_byte_length"], MAX_DLQ_BYTES)
        self.assertEqual(len(audit["event_sha256"]), 64)
        generic = bounded_deadletter_json({"schema_version": 1, "event_type": "deadletter", "source": "x" * 900_000,
                                          "reason": "y" * 900_000, "unknown": "z" * 900_000,
                                          "origin": {"topic": "t" * 900_000, "offset": 22}})
        self.assertLessEqual(len(generic.encode("utf-8")), MAX_DLQ_BYTES)
        self.assertTrue(json.loads(generic)["truncated"])

    def test_window_guard_quarantines_conflict_after_upstream_ttl(self):
        original = metric()
        acc = WindowAccumulator().add(original)
        for i in range(20):
            acc.add_or_quarantine({**original, "latency_ms": 1200 + i,
                                   ORIGIN_FIELD: {"partition": 0, "offset": 30 + i}})
        self.assertEqual(acc.request_count, 1)
        self.assertEqual(acc.seen[original["event_id"]][1], 900)
        self.assertEqual(acc.quarantine_count, 20)
        self.assertEqual(len(acc.quarantine_samples), 10)
        self.assertEqual(acc.quarantine_samples[0]["origin"]["offset"], 30)
        with self.assertRaisesRegex(ValueError, "conflicting duplicate"):
            acc.add({**original, "latency_ms": 1400})
        for i in range(1, 20):
            acc.add(metric(event_id=f"more-{i}"))
        alerts = evaluate_accumulator(acc, "payment", 0, 30_000)
        self.assertTrue(alerts)
        self.assertTrue(all(a["evidence"]["quarantined_conflict_count"] == 20 for a in alerts))


if __name__ == "__main__":
    unittest.main()
