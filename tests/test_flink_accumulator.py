import copy
import math
import pickle
import random
import unittest

from flink.aggregate import BIN_COUNT, WindowAccumulator, evaluate_accumulator
from flink.rules import evaluate_window


START, END = 1_760_000_000_000, 1_760_000_030_000


def request(i, latency=100, status=200, service="payment"):
    return {"schema_version": 1, "event_id": f"request-{i}", "event_type": "request",
            "event_time": START + i, "service": service, "instance_id": "one", "trace_id": str(i),
            "latency_ms": latency, "status_code": status}


def accumulated(events):
    acc = WindowAccumulator()
    for event in events:
        acc.add(event)
    return acc


def rules_for(acc):
    return {alert["rule_id"] for alert in evaluate_accumulator(acc, "payment", START, END)}


class IncrementalWindowTests(unittest.TestCase):
    def test_p95_threshold_boundaries_match_exact_nearest_rank(self):
        for latency in (0, 799.999, 800, 800.001, 825, 60_000, 60_001):
            for high_samples in (1, 2):
                events = [request(i, latency if i < high_samples else 50) for i in range(20)]
                with self.subTest(latency=latency, high_samples=high_samples):
                    exact = {alert["rule_id"] for alert in evaluate_window(events, "payment", START, END)}
                    self.assertEqual(rules_for(accumulated(events)), exact)

    def test_regular_bucket_upper_bound_precision_and_random_threshold_equivalence(self):
        rng = random.Random(109)
        for size in (20, 21, 40, 199, 400):
            events = [request(i, rng.uniform(0, 60_000), 503 if rng.random() < .23 else 200)
                      for i in range(size)]
            acc = accumulated(events)
            exact_p95 = sorted(event["latency_ms"] for event in events)[math.ceil(size * .95) - 1]
            upper, overflow = acc.p95()
            self.assertFalse(overflow)
            self.assertGreaterEqual(upper, exact_p95)
            self.assertLess(upper - exact_p95, 25)
            self.assertEqual(rules_for(acc), {a["rule_id"] for a in evaluate_window(events, "payment", START, END)})
            self.assertEqual(len(acc.latency_counts), BIN_COUNT)

    def test_overflow_report_does_not_claim_25ms_precision(self):
        events = [request(i, 70_000 + i * 100) for i in range(20)]
        alert = evaluate_accumulator(accumulated(events), "payment", START, END)[0]
        evidence = alert["evidence"]
        self.assertTrue(evidence["p95_overflow"])
        self.assertIsNone(evidence["p95_precision_ms"])
        self.assertEqual(evidence["p95_overflow_lower_bound_ms"], 60_000)
        self.assertEqual(evidence["p95_latency_ms"], 71_900)
        self.assertEqual(evidence["p95_method"], "histogram_upper_bound")

    def test_overlapping_merge_is_idempotent_and_order_independent(self):
        events = [request(i, (i % 7) * 275, 503 if i % 3 == 0 else 200) for i in range(60)]
        whole = accumulated(events)
        left = accumulated(events[:40])
        right = accumulated(events[20:])
        left.merge(right)
        reverse = accumulated(events[20:]).merge(accumulated(events[:40]))
        self.assertEqual(left.request_count, 60)
        self.assertEqual(left.latency_counts, whole.latency_counts)
        self.assertEqual(left.error_count, whole.error_count)
        self.assertEqual(reverse.latency_counts, left.latency_counts)
        self.assertEqual(left.p95(), whole.p95())
        before = pickle.dumps(left)
        left.merge(left)
        left.merge(WindowAccumulator())
        self.assertEqual(pickle.dumps(left), before)
        tree = accumulated(events[:30]).merge(accumulated(events[15:45]).merge(accumulated(events[30:])))
        self.assertEqual(tree.seen, whole.seen)
        self.assertEqual(tree.latency_counts, whole.latency_counts)

    def test_conflicting_duplicate_ids_rejected_without_partial_merge(self):
        acc = accumulated([request(0, 800)])
        with self.assertRaisesRegex(ValueError, "conflicting duplicate"):
            acc.add(request(0, 800.001))
        incoming = accumulated([request(1), request(0, 800, 503)])
        before = copy.deepcopy(acc)
        with self.assertRaisesRegex(ValueError, "conflicting duplicate"):
            acc.merge(incoming)
        self.assertEqual(acc, before)
        with self.assertRaisesRegex(ValueError, "different services"):
            acc.merge(accumulated([request(2, service="order")]))

    def test_error_rate_and_sample_floor_are_exact(self):
        boundary = [request(i, status=503 if i < 4 else 200) for i in range(20)]
        self.assertEqual(rules_for(accumulated(boundary)), set())
        self.assertEqual(rules_for(accumulated(boundary + [request(20, status=503)])), {"high_error_rate"})
        self.assertEqual(rules_for(accumulated([request(i, 1800, 503) for i in range(19)])), set())
        self.assertEqual(WindowAccumulator().p95(), (0.0, False))

    def test_host_aggregation_and_merge_preserve_dedup(self):
        hosts = [{"event_id": f"host-{i}", "event_type": "host", "service": "payment",
                  "cpu_percent": 95, "memory_percent": 91} for i in range(4)]
        acc = accumulated(hosts[:3]).merge(accumulated(hosts[1:]))
        self.assertEqual(acc.host_count, 4)
        self.assertEqual(rules_for(acc), {"high_cpu", "high_memory"})
        self.assertAlmostEqual(acc.cpu_sum, 380)
        self.assertAlmostEqual(acc.memory_sum, 364)

    def test_compact_state_pickle_round_trip_and_alert_identity(self):
        events = [request(i, 1000, 503) for i in range(1000)]
        acc = accumulated(events)
        restored = pickle.loads(pickle.dumps(acc))
        self.assertEqual(restored, acc)
        self.assertLess(len(pickle.dumps(acc)), len(pickle.dumps(events)))
        exact_ids = {a["alert_id"] for a in evaluate_window(events, "payment", START, END)}
        compact_alerts = evaluate_accumulator(restored, "payment", START, END)
        self.assertEqual({a["alert_id"] for a in compact_alerts}, exact_ids)
        self.assertTrue(all(a["rule_version"] == "window-rules-v2" for a in compact_alerts))


if __name__ == "__main__":
    unittest.main()
