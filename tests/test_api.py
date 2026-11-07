import os
import tempfile
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.api import app
from app.diagnosis import diagnose
from app.storage import Store
from flink.rules import evaluate_window
from scripts.demo import fixture


class APITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.env = patch.dict(os.environ, {"DATA_DIR": cls.tmp.name, "LLM_BASE_URL": "", "LLM_MODEL": ""})
        cls.env.start()
        store = Store()
        events, start, end = fixture()
        for event in events:
            store.ingest(event)
        alert = evaluate_window(events, "gateway", start, end)[0]
        store.alert(alert)
        cls.report = diagnose(store, alert["alert_id"])
        from app.incidents import IncidentManager
        from app.reporting import ReportManager
        cls.incident = IncidentManager(store).ingest_alert(alert)
        cls.version = ReportManager(store).ensure_initial(cls.incident)
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.env.stop()
        cls.tmp.cleanup()

    def test_report_list_is_summary_and_detail_has_evidence(self):
        listed = self.client.get("/reports").json()
        self.assertEqual(listed[0]["report_id"], self.report["report_id"])
        self.assertNotIn("prompt", listed[0]["audit"])
        detailed = self.client.get("/reports/"+self.report["report_id"]).json()
        self.assertTrue(detailed["snapshot"]["records"])
        self.assertIn("prompt", detailed["audit"])

    def test_markdown_integrity_and_not_found(self):
        prefix = "/reports/"+self.report["report_id"]
        self.assertIn("诊断报告", self.client.get(prefix+"/markdown").text)
        self.assertTrue(self.client.get(prefix+"/verify").json()["snapshot_sha256_valid"])
        self.assertEqual(self.client.get("/reports/missing").status_code, 404)

    def test_fault_target_and_body_are_bounded(self):
        self.assertEqual(self.client.post("/faults/unknown", json={"kind": "none"}).status_code, 404)
        self.assertEqual(self.client.post("/faults/payment", json={"kind": "latency", "duration_seconds": 601}).status_code, 422)

    def test_operational_dashboard_is_served(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("智能运维", response.text)

    def test_incident_versions_and_integrity_api(self):
        iid = self.incident["incident_id"]
        incidents = self.client.get("/incidents").json()
        self.assertEqual(incidents[0]["incident_id"], iid)
        self.assertEqual(self.client.get("/incidents/"+iid).json()["alert_count"], 1)
        versions = self.client.get(f"/incidents/{iid}/reports").json()
        self.assertEqual(versions[0]["stage"], "preliminary")
        rid = versions[0]["report_id"]
        self.assertTrue(self.client.get(f"/report-versions/{rid}/verify").json()["snapshot_sha256_valid"])
        self.assertIn("证据", self.client.get(f"/report-versions/{rid}/markdown").text)
        self.assertEqual(self.client.get("/incidents/missing").status_code, 404)
        self.assertEqual(self.client.get("/report-versions/missing").status_code, 404)
        notifications = self.client.get("/notifications", params={"incident_id": iid}).json()
        self.assertEqual([n["kind"] for n in notifications], ["opened"])
