"""Operator unit tests. Run with the optional matching PyFlink environment.

These tests exercise Python callbacks; they do not start Kafka/Flink services.
"""
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

HAVE_PYFLINK = importlib.util.find_spec("pyflink") is not None
if HAVE_PYFLINK:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "flink"))
    from detector import AuditedDeduplicate, INVALID, ParseMetric, integer_setting


class MapState:
    def __init__(self):
        self.values = {}
    def get(self, key):
        return self.values.get(key)
    def put(self, key, value):
        self.values[key] = value
    def items(self):
        return self.values.items()
    def remove(self, key):
        self.values.pop(key, None)


class ValueState:
    def __init__(self):
        self.current = None
    def value(self):
        return self.current
    def update(self, value):
        self.current = value
    def clear(self):
        self.current = None


class Runtime:
    def __init__(self):
        self.maps = MapState()
        self.states = {}
    def get_map_state(self, descriptor):
        return self.maps
    def get_state(self, descriptor):
        return self.states.setdefault(descriptor.name, ValueState())


class Context:
    def __init__(self):
        self.now = 1000
        self.timers = []
    def timer_service(self):
        return self
    def current_processing_time(self):
        return self.now
    def register_processing_time_timer(self, timestamp):
        self.timers.append(timestamp)


def event(**changes):
    return {"schema_version": 1, "event_id": "one", "event_type": "request", "service": "payment",
            "instance_id": "one", "event_time": int(time.time() * 1000), "latency_ms": 900,
            "status_code": 503, "__kafka_origin": {"topic": "telemetry.metrics", "partition": 0, "offset": 1}, **changes}


@unittest.skipUnless(HAVE_PYFLINK, "optional PyFlink 1.20 runtime not installed")
class StreamOperatorTests(unittest.TestCase):
    def test_duplicate_conflict_and_bounded_capacity(self):
        operator = AuditedDeduplicate(30_000, 1)
        runtime, context = Runtime(), Context()
        operator.open(runtime)
        first = event()
        self.assertEqual(list(operator.process_element(first, context)), [first])
        self.assertEqual(list(operator.process_element({**first, "__kafka_origin": {"offset": 2}}, context)), [])
        tag, conflict = list(operator.process_element({**first, "latency_ms": 4000}, context))[0]
        self.assertEqual(tag.tag_id, INVALID.tag_id)
        self.assertEqual(json.loads(conflict)["reason"], "conflicting_duplicate_event_id")
        self.assertEqual(json.loads(conflict)["first_seen"]["origin"]["offset"], 1)
        _, rejected = list(operator.process_element(event(event_id="two"), context))[0]
        self.assertEqual(json.loads(rejected)["reason"], "deduplication_bucket_capacity_exceeded")
        self.assertEqual(len(runtime.maps.values), 1)

    def test_cleanup_reclaims_capacity_and_clears_timer(self):
        operator = AuditedDeduplicate(100, 1)
        runtime, context = Runtime(), Context()
        operator.open(runtime)
        list(operator.process_element(event(), context))
        self.assertEqual(context.timers, [1100])
        context.now = 1100
        list(operator.on_timer(1100, context))
        self.assertEqual(runtime.maps.values, {})
        self.assertIsNone(operator.timer.value())
        accepted = event(event_id="two")
        self.assertEqual(list(operator.process_element(accepted, context)), [accepted])

    def test_parser_poison_values_all_reach_dlq_with_coordinates(self):
        parser = ParseMetric()
        for raw in ("{invalid", "null", json.dumps({k: v for k, v in event(latency_ms=10**1000).items() if k != "__kafka_origin"})):
            envelope = json.dumps({"_aiops_kafka_envelope": 1, "raw": raw,
                                   "origin": {"topic": "telemetry.metrics", "partition": 2, "offset": 77}})
            tag, output = list(parser.process_element(envelope, None))[0]
            self.assertEqual(tag.tag_id, INVALID.tag_id)
            self.assertEqual(json.loads(output)["origin"]["offset"], 77)

    def test_configuration_rejects_non_numeric_and_out_of_range(self):
        for value in ("abc", "-1", "0", "999"):
            with patch.dict(os.environ, {"AIOPS_TEST_INTEGER": value}):
                with self.assertRaises(ValueError):
                    integer_setting("AIOPS_TEST_INTEGER", 1, maximum=100)
        with patch.dict(os.environ, {"AIOPS_TEST_INTEGER": "10"}):
            self.assertEqual(integer_setting("AIOPS_TEST_INTEGER", 1), 10)


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(HAVE_PYFLINK, "optional PyFlink 1.20 runtime not installed")
class AdaptiveOperatorTests(unittest.TestCase):
    def test_keyed_window_global_state_reused(self):
        from detector import DetectAggregateWindow
        from aggregate import WindowAccumulator
        from adaptive import DetectionConfig
        from types import SimpleNamespace
        config=DetectionConfig(mode='dynamic',min_windows=3)
        operator=DetectAggregateWindow(config)
        runtime=Runtime();operator.open(runtime)
        state=ValueState()
        class Global:
            def get_state(self,descriptor):return state
        for i in range(4):
            start=1704067200000+i*30000
            context=SimpleNamespace(window=lambda:SimpleNamespace(start=start,end=start+30000),global_state=Global)
            acc=WindowAccumulator()
            for j in range(30):acc.add(event(event_id=f'{i}-{j}',latency_ms=500 if i==3 else 100,status_code=200))
            output=[json.loads(v) for v in operator.process('payment',context,[acc])]
            if i<3:self.assertEqual(output,[])
            else:
                self.assertEqual(output[0]['rule_id'],'high_latency')
                self.assertEqual(output[0]['evidence']['baseline']['baseline_count'],3)
        self.assertEqual(state.value()['last_end'],start+30000)
