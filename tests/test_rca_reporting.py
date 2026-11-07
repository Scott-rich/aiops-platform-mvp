"""Actual SQLite/API integration on explicitly synthetic fault evidence."""
import copy
import json
import os
import tempfile
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.api import app
from app.contracts import canonical, digest
from app.diagnosis import diagnose, markdown
from app.incidents import IncidentManager
from app.rca_integrity import reference_check, verify_attachment
from app.reporting import ReportManager
from app.storage import Store
from flink.rules import evaluate_window
from scripts.demo import fixture


class RCAReportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        env = patch.dict(os.environ, {'DATA_DIR': temporary.name, 'LLM_BASE_URL': '', 'LLM_MODEL': ''})
        env.start(); self.addCleanup(env.stop)
        self.store = Store(); events, start, end = fixture()
        self.store.ingest_many([(event, {'transport': 'synthetic-test'}) for event in events])
        alert = next(a for a in evaluate_window(events, 'gateway', start, end) if a['rule_id'] == 'high_error_rate')
        self.store.alert(alert)
        self.legacy = diagnose(self.store, alert['alert_id'])
        self.incident = IncidentManager(self.store).ingest_alert(alert)
        self.manager = ReportManager(self.store)
        self.report = self.manager.ensure_initial(self.incident)
        self.client = TestClient(app); self.addCleanup(self.client.close)

    def test_legacy_and_progressive_reports_bind_rca_to_snapshot(self):
        for value in (self.legacy, self.report):
            with self.subTest(report=value['report_id']):
                self.assertTrue(all(verify_attachment(value).values()))
                self.assertEqual(value['rca']['candidates'][0]['service'], 'payment')
                self.assertEqual(value['analysis']['hypotheses'][0]['service'], 'payment')
                self.assertIn('gateway', value['rca']['affected_services'])
                self.assertIn('服务依赖与证据根因分析', markdown(value))

    def test_new_routes_and_integrity_checks(self):
        for prefix, value in (('/reports/', self.legacy), ('/report-versions/', self.report)):
            result = self.client.get(prefix + value['report_id'] + '/rca')
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()['rca'], value['rca'])
            verified = self.client.get(prefix + value['report_id'] + '/verify').json()
            self.assertTrue(verified['snapshot_sha256_valid'])
            self.assertTrue(verified['rca_sha256_valid'])
            self.assertTrue(verified['rca_evidence_references_valid'])
        graph = self.client.get('/incidents/' + self.incident['incident_id'] + '/topology')
        self.assertEqual(graph.status_code, 200)
        self.assertEqual(graph.json()['graph'], self.report['rca']['graph'])
        self.assertEqual(self.client.get('/report-versions/missing/rca').status_code, 404)
        self.assertEqual(self.client.get('/incidents/missing/topology').status_code, 404)

    def test_tampered_candidates_and_evidence_set_fail_separately(self):
        changed = copy.deepcopy(self.report)
        changed['rca']['candidates'][0]['supporting_evidence_ids'].append('not-in-snapshot')
        result = verify_attachment(changed)
        self.assertFalse(result['rca_sha256_valid'])
        self.assertFalse(result['rca_evidence_references_valid'])
        self.assertTrue(result['rca_evidence_set_valid'])
        changed['rca']['evidence_set_id'] = 'another-evidence-set'
        self.assertFalse(verify_attachment(changed)['rca_evidence_set_valid'])

    def test_old_report_stays_readable_without_synthesized_rca(self):
        old = copy.deepcopy(self.report); old.pop('rca')
        old['audit'].pop('rca_sha256'); old['audit'].pop('rca_version')
        with self.store.db() as db:
            db.execute('UPDATE report_versions SET payload=? WHERE report_id=?', (canonical(old), old['report_id']))
        self.assertEqual(self.manager.get(old['report_id']), old)
        self.assertEqual(self.client.get('/report-versions/' + old['report_id'] + '/rca').status_code, 404)
        self.assertIsNone(verify_attachment(old)['rca_sha256_valid'])
        self.assertIn('诊断报告', markdown(old))

    def test_existing_ids_do_not_make_a_fabricated_call_edge_valid(self):
        changed = copy.deepcopy(self.report)
        edge = next(edge for edge in changed['rca']['graph']['edges'] if edge['basis'] == 'parent_span')
        edge['callee'] = edge['caller']
        check = reference_check(changed['rca'], changed['snapshot'])
        self.assertFalse(check['valid'])
        self.assertIn('edge_relationship_not_supported', check['errors'])

    def test_malformed_attachments_fail_closed_in_api(self):
        for broken in (None, [], {**self.report['rca'], 'graph': []},
                       {**self.report['rca'], 'graph': {'edges': ['bad']}}):
            value = copy.deepcopy(self.report); value['rca'] = broken
            with self.subTest(broken=type(broken).__name__):
                self.assertFalse(verify_attachment(value)['rca_evidence_references_valid'])
                with self.store.db() as db:
                    db.execute('UPDATE report_versions SET payload=? WHERE report_id=?',
                               (canonical(value), value['report_id']))
                response = self.client.get('/report-versions/' + value['report_id'] + '/verify')
                self.assertEqual(response.status_code, 200)
                self.assertFalse(response.json()['rca_evidence_references_valid'])
                response = self.client.get('/incidents/' + self.incident['incident_id'] + '/topology')
                self.assertEqual(response.status_code, 409)

    def test_empty_trace_ids_cannot_establish_call_relationship(self):
        changed = copy.deepcopy(self.report)
        for row in changed['snapshot']['records']:
            row['event']['trace_id'] = ''
        for edge in changed['rca']['graph']['edges']:
            edge['trace_ids'] = []
            for pair in edge.get('observed_pairs', []):
                pair['trace_id'] = ''
        self.assertFalse(reference_check(changed['rca'], changed['snapshot'])['valid'])

    def test_residual_observation_references_must_exist(self):
        for reference in ({'parent_event_id': 'missing-parent'}, {'child_event_ids': ['missing-child']}):
            changed = copy.deepcopy(self.report)
            changed['rca']['candidates'][0]['latency_residual_observations'] = [reference]
            with self.subTest(reference=reference):
                self.assertFalse(reference_check(changed['rca'], changed['snapshot'])['valid'])

    def test_new_versions_and_database_reopen_preserve_old_rca(self):
        before = canonical(self.report)
        resolved = self.manager.refresh({**self.incident, 'status': 'resolved'}, force=True)
        self.assertIsNotNone(resolved)
        self.assertTrue(all(verify_attachment(resolved).values()))
        reopened = ReportManager(Store(self.store.root))
        self.assertEqual(canonical(reopened.get(self.report['report_id'])), before)
        self.assertEqual(reopened.get(resolved['report_id']), resolved)

    def test_unknown_is_a_valid_integrity_result_not_a_fake_candidate(self):
        from scripts.verify_optimized import report_integrity
        value = copy.deepcopy(self.report)
        value['analysis']['diagnosis_status'] = 'unknown'
        value['analysis']['hypotheses'] = []
        api_check = {'snapshot_sha256_valid': True, **verify_attachment(value)}
        self.assertTrue(all(report_integrity(value, api_check).values()))
        api_check['rca_evidence_references_valid'] = False
        self.assertFalse(report_integrity(value, api_check)['api_rca_checks_valid'])
