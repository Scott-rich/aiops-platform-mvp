from concurrent.futures import Future
import os
import tempfile
import time
import unittest
from unittest.mock import patch
from app.contracts import now_ms
from app.engine import Pipeline
from app.storage import Store


class EngineSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = patch.dict(os.environ, {"LLM_BASE_URL": "", "LLM_MODEL": "", "REPORT_ENRICH_MIN_MS": "0", "REPORT_ENRICH_DEADLINE_MS": "0"})
        self.config.start()
        self.addCleanup(self.config.stop)
        self.store = Store(self.tmp.name)
        self.pipeline = Pipeline(self.store)
        self.addCleanup(self.pipeline.close)
        self.base = now_ms()-20_000

    def add_fault(self, service):
        entries = []
        for i in range(20):
            common = {"schema_version": 1, "event_time": self.base+i*100, "service": service,
                      "instance_id": service+"-instance", "trace_id": f"{service}-{i}", "span_id": f"span-{service}-{i}"}
            for kind in ("request", "span", "log"):
                event = {**common, "event_id": f"{service}-{i}-{kind}", "event_type": kind}
                if kind in ("request", "span"):
                    event.update(status_code=503, latency_ms=10)
                if kind == "span":
                    event.update(parent_span_id=None, downstream=[])
                if kind == "log":
                    event.update(level="ERROR", message="dependency unavailable")
                entries.append((event, {"transport": "test"}))
        self.pipeline.ingest_batch(entries)
        return next(i for i in self.pipeline.incidents.list() if i["candidate_service"] == service)

    def test_all_three_incidents_receive_enrichment_without_starvation(self):
        incidents = [self.add_fault(s) for s in ("leaf-a", "leaf-b", "leaf-c")]
        for step in range(12):
            self.pipeline.tick(now=now_ms()+step*1000)
            time.sleep(0.05)
        if self.pipeline.future:
            self.pipeline.future.result(timeout=5)
        for incident in incidents:
            stages = {r["stage"] for r in self.pipeline.reports.list_versions(incident["incident_id"])}
            self.assertTrue({"preliminary", "enriched"} <= stages, (incident["incident_id"], stages))

    def test_pending_model_job_does_not_block_a_new_initial_report(self):
        self.add_fault("leaf-a")
        self.pipeline.future = Future()  # Represents an in-flight slow enrichment.
        other = self.add_fault("leaf-b")
        self.pipeline.tick()
        report = self.pipeline.reports.latest(other["incident_id"])
        self.assertIsNotNone(report)
        self.assertEqual(report["stage"], "preliminary")
        self.assertEqual(report["audit"]["analysis_mode"], "heuristic")
        self.pipeline.future = None

    def test_batch_validation_is_atomic(self):
        valid = {"schema_version": 1, "event_id": "good", "event_time": now_ms(), "event_type": "log",
                 "service": "one", "instance_id": "one", "trace_id": "trace", "level": "INFO", "message": "ok"}
        invalid = dict(valid, event_id="bad", downstream={"not": "a-list"})
        with self.assertRaises(ValueError):
            self.store.ingest_many([(valid, None), (invalid, None)])
        self.assertEqual(self.store.stats()["events"], 0)
