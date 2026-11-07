"""Real Java/PyFlink connector construction tests; no Kafka services are started.

Opt in with AIOPS_SOURCE_BRIDGE_JAR and AIOPS_KAFKA_CONNECTOR_JAR pointing to
compiled artifacts, and run in the optional PyFlink 1.20 environment.
"""
import importlib.util
import json
import os
from pathlib import Path
import time
import unittest

from wire_contract import validate_metric


@unittest.skipUnless(importlib.util.find_spec("pyflink") is not None, "optional PyFlink 1.20 runtime not installed")
class SourceBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        paths = [os.environ.get(key) for key in ("AIOPS_SOURCE_BRIDGE_JAR", "AIOPS_KAFKA_CONNECTOR_JAR")]
        if not all(paths) or not all(Path(path).is_file() for path in paths):
            raise unittest.SkipTest("compiled source bridge / connector paths not configured")
        from pyflink.java_gateway import get_gateway
        from pyflink.util.java_utils import add_jars_to_context_class_loader
        add_jars_to_context_class_loader([Path(path).resolve().as_uri() for path in paths])
        cls.bridge = get_gateway().jvm.aiops.SourceEnvelope

    def test_java_timestamp_guard_matches_python_metric_contract(self):
        base = {"schema_version": 1, "event_id": "guard-probe", "event_time": int(time.time() * 1000),
                "event_type": "request", "service": "payment", "instance_id": "one", "latency_ms": 5, "status_code": 200}
        cases = [{}, {"trace_id": ""}, {"service": "😀" * 256}, {"extra": [True, None, {"finite": 10 ** 100}]},
                 {"event_type": "host", "cpu_percent": 92, "memory_percent": 20},
                 {"event_time": 1.5}, {"event_time": base["event_time"] + 120_000}, {"schema_version": True},
                 {"schema_version": 2 ** 64 + 1}, {"schema_version": 1.0}, {"status_code": True},
                 {"latency_ms": 10 ** 1000}, {"extra": {"too_large": 10 ** 1000}}, {"extra": float("inf")},
                 {"service": "😀" * 257}, {"trace_id": None}, {"__kafka_origin": {}},
                 {"downstream": [1]}, {"service": "\ud800"}, {"extra": {"bad": "\udfff"}}, {"event_type": "log", "level": "INFO", "message": "ok"}]
        for changes in cases:
            candidate = {**base, **changes}
            try:
                validate_metric(candidate)
                accepted = True
            except ValueError:
                accepted = False
            with self.subTest(keys=tuple(changes)):
                timestamp = self.bridge.guardedTimestamp(json.dumps(candidate))
                self.assertEqual(timestamp == candidate["event_time"], accepted)
                if not accepted:
                    self.assertEqual(timestamp, -(2 ** 63))
        self.assertEqual(self.bridge.guardedTimestamp("{invalid"), -(2 ** 63))
        self.assertEqual(self.bridge.guardedTimestamp("null"), -(2 ** 63))

    def test_invalid_utf8_keeps_original_bytes_and_legitimate_replacement_character(self):
        import base64
        from flink.stream_support import deadletter, parse_metric_envelope
        # An actual invalid byte, rather than a literal escaped backslash string.
        raw = b'{"service":"bad-' + bytes([255]) + b'"}'
        output = self.bridge.envelopeForRecord("telemetry.metrics", 2, 18, 1000, bytearray(raw))
        envelope = json.loads(output)
        self.assertEqual(envelope["encoding_error"], "invalid_utf8")
        self.assertEqual(base64.b64decode(envelope["raw_base64"]), raw)
        self.assertEqual(envelope["origin"]["offset"], 18)
        self.assertEqual(envelope["event_time"], -(2 ** 63))
        with self.assertRaisesRegex(ValueError, "invalid_utf8"):
            parse_metric_envelope(output)
        audit = json.loads(deadletter("invalid_utf8", value=output))
        self.assertEqual(audit["raw_base64"], envelope["raw_base64"])
        valid = {"schema_version": 1, "event_id": "replacement-char", "event_time": int(time.time() * 1000),
                 "event_type": "request", "service": "valid-�", "instance_id": "one", "latency_ms": 5, "status_code": 200}
        valid_output = self.bridge.envelopeForRecord("telemetry.metrics", 0, 19, 1000,
                                                     bytearray(json.dumps(valid, ensure_ascii=False).encode("utf-8")))
        self.assertNotIn("encoding_error", json.loads(valid_output))
        self.assertEqual(parse_metric_envelope(valid_output)["service"], "valid-�")

    def test_source_schema_and_partition_watermark_construct_with_real_api(self):
        from pyflink.common import WatermarkStrategy
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
        builder = (KafkaSource.builder().set_bootstrap_servers("127.0.0.1:19092")
                   .set_topics("telemetry.metrics").set_group_id("source-schema-construction-test")
                   .set_starting_offsets(KafkaOffsetsInitializer.earliest()))
        builder._j_builder.setDeserializer(self.bridge())
        source = builder.build()
        strategy = WatermarkStrategy(self.bridge.watermarks(5000, 10000))
        self.assertIsNotNone(source.get_java_function())
        self.assertIsNotNone(strategy._j_watermark_strategy)


if __name__ == "__main__":
    unittest.main()
