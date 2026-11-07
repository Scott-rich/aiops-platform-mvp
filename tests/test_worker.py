import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from app.contracts import now_ms
from app.engine import Pipeline
from app.storage import Store
from app.worker import check_health, decode_record, run_consumer, write_health


def event():
    return {"schema_version": 1, "event_id": "one", "event_time": now_ms(), "event_type": "request",
            "service": "payment", "instance_id": "pay-1", "trace_id": "t", "status_code": 200, "latency_ms": 2}


def record(payload, topic="telemetry.metrics", offset=3):
    return SimpleNamespace(value=json.dumps(payload).encode(), topic=topic, partition=1, offset=offset, timestamp=now_ms())


class WorkerDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name)
        self.pipeline = Pipeline(self.store)
        self.addCleanup(self.pipeline.close)
        self.stop = threading.Event()
        self.path = Path(self.tmp.name)/"worker-health.json"

    def one_poll(self, rows):
        consumer = Mock()
        consumer.poll.return_value = {"partition": rows}
        def committed(**_):
            self.assertGreater(sum(self.store.stats().values()), 0)
            self.stop.set()
        consumer.commit.side_effect = committed
        return consumer

    def test_commits_after_durable_write_and_keeps_origin(self):
        consumer = self.one_poll([record(event())])
        run_consumer(consumer, self.pipeline, self.stop, self.path)
        consumer.commit.assert_called_once()
        with self.store.db() as db:
            origin = json.loads(db.execute("SELECT source FROM events").fetchone()[0])
        self.assertEqual((origin["partition"], origin["offset"]), (1, 3))
        self.assertTrue(check_health(self.path))

    def test_invalid_input_is_durable_before_offset_commit(self):
        consumer = self.one_poll([record(["not", "object"])])
        run_consumer(consumer, self.pipeline, self.stop, self.path)
        self.assertEqual(self.store.stats()["events"], 0)
        self.assertEqual(self.store.stats()["deadletters"], 1)
        consumer.commit.assert_called_once()

    def test_detector_dlq_keeps_original_and_dlq_coordinates(self):
        body = {"reason": "invalid", "source": {"topic": "telemetry.metrics", "offset": 42}, "raw": "bad"}
        consumer = self.one_poll([record(body, "ops.deadletter", 12)])
        run_consumer(consumer, self.pipeline, self.stop, self.path)
        with self.store.db() as db:
            row = db.execute("SELECT raw,source FROM deadletters").fetchone()
        self.assertEqual(json.loads(row[0])["source"]["offset"], 42)
        self.assertEqual(json.loads(row[1])["offset"], 12)

    def test_database_failure_never_commits_or_polls_next_batch(self):
        consumer = self.one_poll([record(event())])
        self.pipeline.ingest_batch = Mock(side_effect=OSError("database unavailable"))
        with self.assertRaises(OSError):
            run_consumer(consumer, self.pipeline, self.stop, self.path)
        consumer.commit.assert_not_called()
        consumer.poll.assert_called_once()

    def test_commit_failure_replay_is_idempotent(self):
        consumer = self.one_poll([record(event())])
        consumer.commit.side_effect = RuntimeError("broker unavailable")
        with self.assertRaises(RuntimeError):
            run_consumer(consumer, self.pipeline, self.stop, self.path)
        consumer.poll.assert_called_once()
        retry = self.one_poll([record(event())])
        # Identical value, including original event_time, is what Kafka actually replays.
        retry.poll.return_value = consumer.poll.return_value
        run_consumer(retry, self.pipeline, self.stop, self.path)
        self.assertEqual(self.store.stats()["events"], 1)

    def test_poll_failure_budget_is_bounded(self):
        consumer = Mock()
        consumer.poll.side_effect = ConnectionError("offline")
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.return_value = False
        with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
            run_consumer(consumer, self.pipeline, stop, self.path, max_failures=2)
        self.assertEqual(consumer.poll.call_count, 2)
        consumer.commit.assert_not_called()
        self.assertFalse(check_health(self.path))

    def test_health_rejects_stale_failed_or_corrupt_state(self):
        write_health(self.path, {"status": "running"})
        self.assertTrue(check_health(self.path))
        self.path.write_text(json.dumps({"status": "running", "updated_at": now_ms()-31_000}))
        self.assertFalse(check_health(self.path))
        write_health(self.path, {"status": "failed"})
        self.assertFalse(check_health(self.path))
        self.path.write_text("invalid")
        self.assertFalse(check_health(self.path))
        self.path.write_text("[]")
        self.assertFalse(check_health(self.path))

    def test_tombstone_huge_body_and_wrong_topic_rejected(self):
        for topic, body in [("telemetry.metrics", None), ("telemetry.metrics", b"x"*1_000_001),
                            ("telemetry.logs", json.dumps(event()).encode())]:
            with self.subTest(topic=topic, size=len(body) if body else 0), self.assertRaises(ValueError):
                decode_record(topic, body)

    def test_deeply_nested_json_is_quarantined_and_committed(self):
        malformed = record(event())
        malformed.value = b"["*2000 + b"0" + b"]"*2000
        consumer = self.one_poll([malformed])
        run_consumer(consumer, self.pipeline, self.stop, self.path)
        self.assertEqual(self.store.stats()["deadletters"], 1)
        consumer.commit.assert_called_once()

    def test_invalid_alert_and_dlq_numbers_are_quarantined_before_persistence(self):
        alert = {"schema_version": 1, "alert_id": "bad", "service": "payment", "rule_id": "high_error_rate",
                 "window_start": 1, "window_end": 2, "evidence": {"bad": float("nan")}}
        dlq = {"reason": "invalid", "raw": float("nan")}
        surrogate = {"reason": "invalid", "raw": "\ud800"}
        consumer = self.one_poll([record(alert, "ops.alerts"), record(dlq, "ops.deadletter"),
                                  record(surrogate, "ops.deadletter")])
        run_consumer(consumer, self.pipeline, self.stop, self.path)
        self.assertEqual(self.store.stats()["alerts"], 0)
        self.assertEqual(self.store.stats()["deadletters"], 3)
        consumer.commit.assert_called_once()
