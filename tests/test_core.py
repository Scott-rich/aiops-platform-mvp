import copy
import json
import os
import tempfile
import unittest
from unittest.mock import patch
from app.contracts import digest, now_ms, validate_event
from app.diagnosis import diagnose, validate_analysis
from app.storage import Store
from app.worker import process_record
from flink.rules import evaluate_window, validate_metric
from scripts.demo import fixture
from tests.model_support import analysis as model_analysis


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.events, self.start, self.end = fixture()
        self.requests = [e for e in self.events if e["event_type"] == "request" and e["service"] == "gateway"]

    def test_anomaly_threshold_and_event_dedup(self):
        alerts = evaluate_window(self.requests*2, "gateway", self.start, self.end)
        self.assertEqual({a["rule_id"] for a in alerts}, {"high_latency", "high_error_rate"})
        self.assertEqual(alerts[0]["evidence"]["sample_count"], 40)
        again = evaluate_window(self.requests, "gateway", self.start, self.end)
        self.assertEqual([a["alert_id"] for a in alerts], [a["alert_id"] for a in again])

    def test_low_sample_and_healthy_do_not_alert(self):
        self.assertEqual(evaluate_window(self.requests[:19], "gateway", self.start, self.end), [])
        healthy = [dict(e, latency_ms=50, status_code=200) for e in self.requests]
        self.assertEqual(evaluate_window(healthy, "gateway", self.start, self.end), [])

    def test_exact_error_rate_boundary(self):
        requests = [dict(e, latency_ms=100, status_code=503 if i < 4 else 200) for i, e in enumerate(self.requests[:20])]
        self.assertEqual(evaluate_window(requests, "gateway", self.start, self.end), [])

    def test_future_and_nan_rejected(self):
        with self.assertRaises(ValueError):
            validate_metric(dict(self.requests[0], event_time=now_ms()+3_600_000))
        with self.assertRaises(ValueError):
            validate_event(dict(self.requests[0], latency_ms=float("nan")))
        with self.assertRaises(ValueError):
            validate_event(dict(self.requests[0], event_time=10**500))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name)
        self.events, self.start, self.end = fixture()
        for event in self.events:
            self.store.ingest(event, {"transport": "test"})
        self.alert = evaluate_window(self.events, "gateway", self.start, self.end)[0]
        self.store.alert(self.alert)

    def test_trace_join_and_unrelated_exclusion(self):
        unrelated = dict(self.events[0], event_id="unrelated", service="other", trace_id="unrelated")
        self.store.ingest(unrelated)
        snapshot = self.store.evidence(self.alert)
        self.assertEqual({r["event"]["service"] for r in snapshot["records"]}, {"gateway", "order", "payment", "inventory"})
        self.assertNotIn("unrelated", {r["event"]["event_id"] for r in snapshot["records"]})

    @patch.dict(os.environ, {"LLM_BASE_URL": "", "LLM_MODEL": ""})
    def test_report_idempotent_immutable_and_cited(self):
        first = diagnose(self.store, self.alert["alert_id"])
        self.assertEqual(first["audit"]["analysis_mode"], "heuristic")
        self.assertEqual(first["analysis"]["hypotheses"][0]["service"], "payment")
        self.assertEqual(first["audit"]["snapshot_sha256"], digest(first["snapshot"]))
        ids = {r["event"]["event_id"] for r in first["snapshot"]["records"]}
        self.assertTrue(set(first["analysis"]["hypotheses"][0]["supporting_evidence_ids"]) <= ids)
        self.store.ingest(dict(self.events[0], event_id="late-arrival"))
        second = diagnose(self.store, self.alert["alert_id"])
        self.assertEqual(first, second)
        changed = copy.deepcopy(first["snapshot"])
        changed["records"][0]["event"]["service"] = "tampered"
        self.assertNotEqual(digest(changed), first["audit"]["snapshot_sha256"])

    def test_ingestion_and_alert_replay_dedup(self):
        self.store.ingest(self.events[0])
        self.store.alert(self.alert)
        stats = self.store.stats()
        self.assertEqual(stats["events"], len(self.events))
        self.assertEqual(stats["alerts"], 1)

    @patch.dict(os.environ, {"LLM_BASE_URL": "http://mock.invalid/v1", "LLM_MODEL": "mock"})
    def test_unknown_model_citations_fallback(self):
        candidate = model_analysis(ref="invented-id")
        with patch("app.diagnosis.call_model", return_value=(candidate, {"requested_model": "mock"})):
            report = diagnose(self.store, self.alert["alert_id"])
        self.assertEqual(report["audit"]["analysis_mode"], "heuristic")
        self.assertEqual(report["audit"]["model_failure"], "ValueError")

    @patch.dict(os.environ, {"LLM_BASE_URL": "http://mock.invalid/v1", "LLM_MODEL": "mock"})
    def test_valid_model_references_accepted(self):
        def answer(payload):
            ref = next(e["event_id"] for e in payload["records"] if e["service"] == "payment")
            return model_analysis(ref=ref), {"requested_model":"mock"}
        with patch("app.diagnosis.call_model", side_effect=answer):
            report = diagnose(self.store, self.alert["alert_id"])
        self.assertEqual(report["audit"]["analysis_mode"], "llm-assisted")
        self.assertEqual(report["audit"]["prompt_version"], "evidence-first-v2.4-grounded")
        metadata = report['audit']['model_metadata']
        self.assertEqual(metadata['model_status'], 'grounded_observations')
        self.assertEqual(metadata['internal_cause_status'], 'unknown')
        self.assertEqual(metadata['normalized_analysis_sha256'], digest(report['analysis']))
        self.assertEqual(report['analysis']['hypotheses'][0]['confidence'], 'low')

    def test_topic_contract_mismatch_rejected(self):
        request = next(e for e in self.events if e["event_type"] == "request")
        with self.assertRaises(ValueError):
            process_record(self.store, "telemetry.logs", json.dumps(request).encode(), {"transport": "test"})

    def test_poison_log_and_tombstone_rejected(self):
        log = next(e for e in self.events if e["event_type"] == "log")
        with self.assertRaises(ValueError):
            self.store.ingest(dict(log, status_code="oops"))
        with self.assertRaises(ValueError):
            process_record(self.store, "telemetry.metrics", None, {"transport": "test"})


if __name__ == "__main__":
    unittest.main()
