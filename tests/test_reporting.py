import copy
import os
import tempfile
import unittest
from unittest.mock import patch

from app.contracts import digest
from app.diagnosis import evidence_observations, prompt_records
from app.reporting import ReportManager
from app.storage import Store
from flink.rules import evaluate_window
from scripts.demo import fixture
from tests.model_support import analysis as model_analysis


class ProgressiveReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = patch.dict(os.environ, {"LLM_BASE_URL": "", "LLM_MODEL": ""})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.store = Store(self.tmp.name)
        self.events, self.start, self.end = fixture()
        self.store.ingest_many([(event, {"transport": "test"}) for event in self.events])
        self.alert = next(a for a in evaluate_window(self.events, "gateway", self.start, self.end) if a["rule_id"] == "high_error_rate")
        self.store.alert(self.alert)
        self.incident = {"incident_id": "test-incident", "status": "open", "services": ["gateway", "order", "payment"],
                         "candidate_service": "payment", "opened_at": self.end+5000,
                         "first_alert_id": self.alert["alert_id"], "alert_ids": [self.alert["alert_id"]],
                         "first_event_time": self.start, "last_event_time": self.end,
                         "latest_alert_end": self.end}
        self.manager = ReportManager(self.store)
        self.now = self.end+5000

    def test_initial_is_immediate_and_never_calls_model(self):
        with patch.dict(os.environ, {"LLM_BASE_URL": "http://unused", "LLM_MODEL": "mock"}), patch("app.diagnosis.call_model") as call:
            report = self.manager.ensure_initial(self.incident, self.now)
        call.assert_not_called()
        self.assertEqual(report["created_at"], self.now)
        self.assertEqual((report["stage"], report["version"]), ("preliminary", 1))
        self.assertEqual(report["analysis"]["hypotheses"][0]["service"], "payment")
        facts = report["observations"]["services"]
        self.assertEqual(next(f["error_count"] for f in facts if f["service"] == "inventory"), 0)
        self.assertTrue(report["missing_evidence"])

    def test_enrichment_and_resolution_are_immutable_versions(self):
        first = self.manager.ensure_initial(self.incident, self.now)
        original = copy.deepcopy(first)
        self.assertIsNone(self.manager.refresh(self.incident, self.now+1000))
        second = self.manager.refresh(self.incident, self.now+3000)
        self.assertEqual((second["stage"], second["version"]), ("enriched", 2))
        self.assertIsNone(self.manager.refresh(self.incident, self.now+4000))
        resolved = dict(self.incident, status="resolved", resolved_at=self.now+20_000)
        final = self.manager.refresh(resolved, self.now+20_000)
        self.assertEqual((final["stage"], final["version"]), ("resolved", 3))
        self.assertEqual(self.manager.get(first["report_id"]), original)
        self.assertIsNone(self.manager.refresh(resolved, self.now+21_000))
        self.assertEqual([r["version"] for r in self.manager.list_versions("test-incident")], [1, 2, 3])
        for report in (first, second, final):
            self.assertEqual(digest(report["snapshot"]), report["audit"]["snapshot_sha256"])

    def test_one_model_attempt_per_incident_even_when_forced(self):
        def answer(payload):
            ref = next(e["event_id"] for e in payload["records"] if e["service"] == "payment")
            return model_analysis(ref=ref), {"requested_model":"mock"}
        self.manager.ensure_initial(self.incident, self.now)
        with patch.dict(os.environ, {"LLM_BASE_URL": "http://unused", "LLM_MODEL": "mock"}), patch("app.diagnosis.call_model", side_effect=answer) as call:
            second = self.manager.refresh(self.incident, self.now+3000)
            self.manager.refresh(self.incident, self.now+4000, force=True)
            self.manager.refresh(dict(self.incident, status="resolved"), self.now+20_000)
        self.assertEqual(call.call_count, 1)
        self.assertEqual(second["audit"]["analysis_mode"], "llm-assisted")

    def test_missing_spans_are_visible_and_enrich_waits_for_evidence(self):
        other_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(other_tmp.cleanup)
        store = Store(other_tmp.name)
        gateway_only = [e for e in self.events if e["service"] == "gateway"]
        store.ingest_many([(event, None) for event in gateway_only])
        store.alert(self.alert)
        manager = ReportManager(store)
        first = manager.ensure_initial(self.incident, self.now)
        self.assertGreater(first["observations"]["trace_coverage"]["missing_downstream_span_count"], 0)
        self.assertIsNone(manager.refresh(self.incident, self.now+3000))
        store.ingest_many([(event, None) for event in self.events if event["service"] != "gateway"])
        enriched = manager.refresh(self.incident, self.now+4000)
        self.assertEqual(enriched["stage"], "enriched")
        self.assertGreater(enriched["observations"]["trace_coverage"]["complete_anomalous_trace_count"], 0)
        self.assertEqual(manager.get(first["report_id"]), first)

    def test_context_contains_whole_traces_and_healthy_sibling(self):
        snapshot = self.store.evidence(self.alert)
        selected = prompt_records(snapshot, 48)
        self.assertLessEqual(len(selected), 48)
        by_trace = {}
        for event in selected:
            by_trace.setdefault(event["trace_id"], []).append(event)
        all_records = [r["event"] for r in snapshot["records"]]
        for trace_id, rows in by_trace.items():
            self.assertEqual({e["event_id"] for e in rows}, {e["event_id"] for e in all_records if e["trace_id"] == trace_id})
        self.assertTrue(any(e["service"] == "inventory" and e.get("status_code") == 200 for e in selected))
        self.assertTrue(any(e["service"] == "payment" and e.get("status_code") == 503 for e in selected))


if __name__ == "__main__":
    unittest.main()
