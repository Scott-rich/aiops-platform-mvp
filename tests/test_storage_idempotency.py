import json
import tempfile
import unittest
from unittest.mock import patch

from app.contracts import canonical, now_ms
from app.engine import Pipeline
from app.storage import Store


class StorageIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name)
        self.base = now_ms() - 10_000

    def event(self, event_id="request-0", index=0, status=503):
        return {"schema_version": 1, "event_id": event_id, "event_time": self.base + index * 100,
                "event_type": "request", "service": "payment", "instance_id": "payment-1",
                "trace_id": "trace-" + event_id, "status_code": status, "latency_ms": 20}

    def persisted(self, event_id):
        with self.store.db() as db:
            row = db.execute("SELECT payload,source FROM events WHERE event_id=?", (event_id,)).fetchone()
        return json.loads(row[0]), json.loads(row[1])

    def deadletters(self):
        with self.store.db() as db:
            return [dict(row) for row in db.execute("SELECT * FROM deadletters ORDER BY id")]

    def pipeline(self):
        pipeline = Pipeline(self.store)
        self.addCleanup(pipeline.close)
        return pipeline

    def test_identical_canonical_payload_is_ignored_even_with_new_source(self):
        event = self.event()
        first_source = {"transport": "kafka", "partition": 0, "offset": 1}
        first = (event, first_source)
        self.assertEqual(self.store.ingest_many([first, first]), [first])
        reordered = dict(reversed(list(event.items())))
        self.assertEqual(self.store.ingest_many([(reordered, {"transport": "kafka", "offset": 2})]), [])
        self.assertEqual(self.persisted(event["event_id"]), first)
        self.assertEqual(self.deadletters(), [])
        self.assertEqual(self.store.stats()["events"], 1)

    def test_same_batch_conflict_keeps_first_and_records_full_raw_and_source(self):
        event = self.event()
        conflict = dict(event, status_code=200, message="conflicting payload")
        source = {"transport": "kafka", "topic": "telemetry.metrics", "partition": 0, "offset": 2}
        first = (event, {"transport": "test", "offset": 1})
        with self.assertLogs("app.storage", level="WARNING"):
            accepted = self.store.ingest_many([first, (conflict, source)])
        self.assertEqual(accepted, [first])
        self.assertEqual(self.persisted(event["event_id"]), first)
        letters = self.deadletters()
        self.assertEqual(len(letters), 1)
        self.assertIn("event_id_payload_conflict", letters[0]["reason"])
        self.assertEqual(letters[0]["raw"], canonical(conflict))
        self.assertEqual(json.loads(letters[0]["source"]), source)

    def test_cross_batch_conflict_survives_reopen_and_does_not_replace_event(self):
        first = (self.event(), {"transport": "original"})
        self.store.ingest_many([first])
        self.store = Store(self.tmp.name)
        conflict = dict(first[0], latency_ms=900, message="x" * 100_001)
        with self.assertLogs("app.storage", level="WARNING"):
            self.assertEqual(self.store.ingest_many([(conflict, None)]), [])
        self.assertEqual(self.persisted(first[0]["event_id"]), first)
        letter = self.deadletters()[0]
        self.assertEqual(json.loads(letter["raw"]), conflict)
        self.assertEqual(json.loads(letter["source"]), {"transport": "local"})

    def test_invalid_batch_preserves_atomic_validation_for_events_and_conflicts(self):
        original = self.event("original")
        self.store.ingest_many([(original, None)])
        conflict = dict(original, latency_ms=900)
        fresh = self.event("fresh", 1)
        invalid = dict(self.event("invalid", 2), status_code="503")
        with self.assertRaises(ValueError):
            self.store.ingest_many([(fresh, None), (conflict, None), (invalid, None)])
        self.assertEqual(self.store.stats()["events"], 1)
        self.assertEqual(self.deadletters(), [])
        self.assertEqual(self.persisted("original")[0], original)

    def test_restart_preserves_partial_burst_without_counting_replays(self):
        # Durable partial state counts once; replaying four IDs cannot fire it.
        entries = [(self.event("replay-" + str(i), i), {"transport": "test"}) for i in range(4)]
        first = self.pipeline()
        self.assertEqual(first.ingest_batch(entries), [])
        first.close()
        restarted = self.pipeline()
        with patch.object(restarted.fast, "observe", wraps=restarted.fast.observe) as observe:
            self.assertEqual(restarted.ingest_batch(entries), [])
            observe.assert_not_called()
        self.assertEqual(len(restarted.ingest_batch([(self.event("new", 4), None)])), 1)
        self.assertEqual(self.store.stats()["alerts"], 1)
        self.assertEqual(self.store.stats()["events"], 5)

    def test_conflicting_replay_cannot_drive_fast_detector(self):
        originals = [(self.event("id-" + str(i), i, status=200), None) for i in range(5)]
        self.store.ingest_many(originals)
        pipeline = self.pipeline()
        conflicts = [(dict(event, status_code=503), {"transport": "conflict"}) for event, _ in originals]
        with self.assertLogs("app.storage", level="WARNING"):
            with patch.object(pipeline.fast, "observe", wraps=pipeline.fast.observe) as observe:
                self.assertEqual(pipeline.ingest_batch(conflicts), [])
                observe.assert_not_called()
        self.assertEqual(self.store.stats()["alerts"], 0)
        self.assertEqual(self.store.stats()["events"], 5)
        self.assertEqual(len(self.deadletters()), 5)


if __name__ == "__main__":
    unittest.main()
