import unittest
from app.fast_detection import FastErrorDetector


def metric(i, status=503, service="payment", instance="one", time_ms=None):
    return {"event_type": "request", "event_id": f"{service}-{instance}-{i}", "event_time": time_ms or 100000+i*100,
            "service": service, "instance_id": instance, "trace_id": str(i), "latency_ms": 5, "status_code": status}


class FastDetectorTests(unittest.TestCase):
    def test_five_errors_emit_once_and_replay_dedups(self):
        detector = FastErrorDetector()
        output = []
        for i in range(10):
            output += detector.observe(metric(i))
            output += detector.observe(metric(i))
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]["rule_id"], "fast_errors")
        self.assertEqual(output[0]["evidence"]["consecutive_errors"], 5)

    def test_healthy_resets_and_instances_do_not_mix(self):
        detector = FastErrorDetector()
        for i in range(4):
            self.assertEqual(detector.observe(metric(i)), [])
        self.assertEqual(detector.observe(metric(4, status=200)), [])
        self.assertEqual(detector.observe(metric(5)), [])
        self.assertEqual(detector.observe(metric(6, instance="two")), [])

    def test_stale_gap_breaks_sequence(self):
        detector = FastErrorDetector()
        for i in range(4):
            detector.observe(metric(i))
        self.assertEqual(detector.observe(metric(5, time_ms=120000)), [])
