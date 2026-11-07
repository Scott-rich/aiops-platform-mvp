"""Deterministic model-boundary tests. HTTP doubles are not real inference."""
from copy import deepcopy
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from app.analysis_schema import validate_analysis
from app.contracts import canonical, digest
from app.diagnosis import model_diagnosis
from app.llm_config import LLMConfig, ModelError
from app.model_grounding import (ground_analysis, selected_analysis, selection_payload,
                                 selection_schema, eligible_support, VERIFY, REMEDIATE)
from app.model_budget import ModelBudget
from app.storage import Store
from tests.model_support import analysis, records, payload, http_stub


def selection(keys=None, unknown=False):
    return {'selection_schema_version': 1, 'diagnosis_status': 'unknown' if unknown else 'candidates',
            'candidate_evidence_keys': [] if unknown else (keys or ['e1']),
            'verification_actions': ['inspect_dependency_health', 'inspect_recent_changes'],
            'remediation_actions': ['verify_then_follow_runbook']}


class GroundingTests(unittest.TestCase):
    def test_healthy_http_and_info_cannot_support_candidate(self):
        rows = [{**e, 'status_code': 200, 'latency_ms': 10, 'level': 'INFO'} for e in records()]
        candidate = analysis();candidate['hypotheses'][0]['confidence'] = 'high'
        with self.assertRaisesRegex(ValueError, 'candidate_has_no_observed_anomaly'):
            validate_analysis(candidate, rows)

    def test_unobserved_internal_cause_and_high_confidence_never_presented_as_fact(self):
        candidate = analysis()
        candidate['hypotheses'][0].update(cause='数据库连接池已经耗尽', confidence='high',
                                           confidence_basis='503证明连接池已经耗尽')
        original = deepcopy(candidate)
        normalized, audit = ground_analysis(candidate, records())
        self.assertEqual(candidate, original)
        self.assertEqual(audit['original_model_analysis'], original)
        self.assertEqual(normalized['hypotheses'][0]['confidence'], 'low')
        self.assertIn('内部根因无法确定', normalized['hypotheses'][0]['cause'])
        self.assertNotIn('已经耗尽', canonical(normalized))
        self.assertIn('HTTP 503', normalized['hypotheses'][0]['cause'])
        self.assertEqual(audit['normalized_analysis_sha256'], digest(normalized))

    def test_unknown_prose_also_cannot_smuggle_unproven_mechanism(self):
        candidate = analysis(unknown=True);candidate['summary'] = '已证明数据库耗尽，立即删库'
        normalized, audit = ground_analysis(candidate, records())
        self.assertEqual(normalized['diagnosis_status'], 'unknown')
        self.assertNotIn('删库', canonical(normalized))
        self.assertEqual(audit['original_model_analysis']['summary'], candidate['summary'])

    def test_reference_errors_are_rejected_instead_of_silently_repaired(self):
        for candidate, code in ((analysis(ref='invented'), 'unknown_evidence_reference'),
                                (analysis(service='inventory'), 'unobserved_involved_service')):
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, code):
                ground_analysis(candidate, records())

    def test_dynamic_platform_alert_can_ground_sub800_latency(self):
        rows = [{**records()[1], 'status_code': 200, 'latency_ms': 300}]
        alert = {'service': 'payment', 'rule_id': 'high_latency',
                 'window_start': rows[0]['event_time']-1, 'window_end': rows[0]['event_time']+1,
                 'evidence': {'detection_mode': 'dynamic', 'threshold': 150}}
        with self.assertRaisesRegex(ValueError, 'candidate_has_no_observed_anomaly'):
            validate_analysis(analysis(), rows)
        self.assertEqual(validate_analysis(analysis(), rows, alert=alert)['diagnosis_status'], 'candidates')
        normalized, _ = ground_analysis(analysis(), rows, alert)
        self.assertIn('300 > 150', normalized['hypotheses'][0]['cause'])

    def test_resource_support_requires_multiple_valid_samples(self):
        row = {'service': 'payment', 'event_id': 'host1', 'event_type': 'host', 'cpu_percent': 99}
        self.assertFalse(eligible_support(row, [row]))
        self.assertTrue(eligible_support(row, [{**row, 'event_id': str(i)} for i in range(3)]))
        self.assertFalse(eligible_support(row, [row, {**row, 'cpu_percent': 1}, {**row, 'cpu_percent': 1}]))


class SelectionTests(unittest.TestCase):
    def test_schema_jointly_constrains_status_and_evidence_not_independent_enums(self):
        schema = selection_schema(payload())
        candidates, unknown = schema['oneOf']
        for branch in (candidates, unknown):
            self.assertFalse(branch['additionalProperties'])
            self.assertEqual(set(branch['required']), set(branch['properties']))
        self.assertEqual(candidates['properties']['diagnosis_status']['enum'], ['candidates'])
        self.assertEqual(candidates['properties']['candidate_evidence_keys']['minItems'], 1)
        self.assertEqual(unknown['properties']['diagnosis_status']['enum'], ['unknown'])
        self.assertEqual(unknown['properties']['candidate_evidence_keys']['maxItems'], 0)

    def test_prior_real_model_unknown_with_payment_keys_still_rejected(self):
        value = selection(['e1', 'e2']);value['diagnosis_status'] = 'unknown'
        with self.assertRaisesRegex(ValueError, 'selection_status_mismatch'):
            selected_analysis(value, payload())

    def test_model_selected_key_maps_to_real_service_and_action(self):
        normalized, audit = selected_analysis(selection(['e2']), payload())
        h = normalized['hypotheses'][0]
        self.assertEqual((h['service'], h['supporting_evidence_ids']), ('payment', ['span-p']))
        self.assertEqual(h['verification_steps'], [VERIFY['inspect_dependency_health'], VERIFY['inspect_recent_changes']])
        self.assertEqual(h['remediation_steps'], [REMEDIATE['verify_then_follow_runbook']])
        self.assertEqual(audit['selected_evidence_mapping'][0]['event_id'], 'span-p')
        validate_analysis(normalized, records())

    def test_model_order_is_preserved_without_rule_ranking(self):
        p = payload();p['records'].append({**p['records'][0], 'event_id': 'gw', 'service': 'gateway'})
        normalized, _ = selected_analysis(selection(['e4', 'e1']), p)
        self.assertEqual([h['service'] for h in normalized['hypotheses']], ['gateway', 'payment'])

    def test_invalid_keys_status_duplicate_and_actions_rejected(self):
        candidates = [({**selection(), 'candidate_evidence_keys': ['invented']}, 'unknown_evidence_reference'),
                      ({**selection(), 'candidate_evidence_keys': ['e1', 'e1']}, 'duplicate_evidence_selection'),
                      ({**selection(), 'diagnosis_status': 'unknown'}, 'selection_status_mismatch'),
                      ({**selection(), 'verification_actions': ['invented']}, 'unknown_model_action'),
                      ({**selection(), 'remediation_actions': []}, 'selection_schema_invalid')]
        for value, code in candidates:
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, code):
                selected_analysis(value, payload())

    def test_healthy_selection_rejected_even_without_provider_schema(self):
        p = payload();p['records'] = [{**e, 'status_code': 200, 'latency_ms': 10, 'level': 'INFO'} for e in p['records']]
        schema = selection_schema(p)
        self.assertEqual(schema['properties']['candidate_evidence_keys']['maxItems'], 0)
        self.assertEqual(schema['properties']['diagnosis_status']['enum'], ['unknown'])
        with self.assertRaisesRegex(ValueError, 'candidate_has_no_observed_anomaly'):
            selected_analysis(selection(), p)
        normalized, _ = selected_analysis(selection(unknown=True), p)
        self.assertEqual(normalized['diagnosis_status'], 'unknown')

    def test_context_preserves_records_trace_times_logs_and_hides_rule_candidate(self):
        p = payload();p.update(incident={'candidate_service': 'secret-rule-choice'}, cases=[{'case': {'case_id': 'history'}}],
                              related_alerts=[{'service': 'payment', 'rule_id': 'high_error_rate'}])
        compact = selection_payload(p)
        self.assertEqual(len(compact['records']), 3)
        self.assertEqual(compact['records'][1]['parent_span_id'], p['records'][1]['parent_span_id'])
        self.assertEqual(compact['records'][2]['message'], p['records'][2]['message'])
        self.assertEqual(compact['historical_cases'], p['cases'])
        self.assertEqual(compact['related_alerts'], p['related_alerts'])
        self.assertNotIn('secret-rule-choice', canonical(compact))

    def test_protocol_configuration_validates_before_any_http(self):
        self.assertEqual(LLMConfig.from_env({'LLM_OUTPUT_PROTOCOL': 'evidence-selection-v1'}).output_protocol, 'evidence-selection-v1')
        with self.assertRaises(ModelError):LLMConfig.from_env({'LLM_OUTPUT_PROTOCOL': 'bad'})


class GroundedPipelineTests(unittest.TestCase):
    def test_compact_protocol_real_http_double_and_persisted_reuse(self):
        value = selection(['e2'])
        with tempfile.TemporaryDirectory() as directory, http_stub([{'analysis': value}]) as (base, seen), patch.dict(os.environ, {
                'LLM_BASE_URL': base, 'LLM_MODEL': 'explicit-test-double', 'LLM_API_KEY': '',
                'LLM_OUTPUT_PROTOCOL': 'evidence-selection-v1', 'LLM_RESPONSE_FORMAT': 'json_schema',
                'LLM_REQUESTS_PER_MINUTE': '1000', 'LLM_MAX_ATTEMPTS': '1', 'LLM_MAX_CALLS_PER_INCIDENT': '1'}):
            store = Store(directory); p = payload()
            first = model_diagnosis(store, 'incident:compact-test', p, p['records'])
            second = model_diagnosis(Store(directory), 'incident:compact-test', p, p['records'])
            self.assertIsNone(first['failure'])
            self.assertEqual(first['metadata']['model_status'], 'grounded_observations')
            self.assertEqual(first['metadata']['original_model_output'], value)
            self.assertEqual(json.loads(first['metadata']['raw_output']), value)
            self.assertEqual(first['analysis'], second['analysis'])
            self.assertEqual(len(seen), 1)
            self.assertEqual(second['budget']['calls_reserved'], 1)
            wire = json.loads(seen[0]['body']['messages'][1]['content'])
            self.assertEqual(wire['protocol'], 'evidence-selection-v1')
            self.assertEqual(seen[0]['body']['response_format']['json_schema']['name'], 'aiops_evidence_selection_v1')

    def test_legacy_model_output_is_normalized_not_labelled_validated(self):
        value = analysis();value['hypotheses'][0].update(confidence='high', cause='已确认数据库故障')
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'LLM_BASE_URL': 'http://unused',
                'LLM_MODEL': 'explicit-double', 'LLM_OUTPUT_PROTOCOL': 'analysis-v2'}), \
                patch('app.diagnosis.call_model', return_value=(value, {})):
            p = payload(); result = model_diagnosis(Store(directory), 'incident:legacy-test', p, p['records'])
        self.assertIsNone(result['failure'])
        self.assertEqual(result['metadata']['model_status'], 'grounded_observations')
        self.assertEqual(result['analysis']['hypotheses'][0]['confidence'], 'low')
        self.assertEqual(result['metadata']['original_model_output'], value)

    def test_historical_report_unchanged_new_version_applies_gate(self):
        from app.reporting import ReportManager
        from flink.rules import evaluate_window
        from scripts.demo import fixture
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'LLM_BASE_URL': '', 'LLM_MODEL': ''}):
            store = Store(directory); events, start, end = fixture()
            store.ingest_many([(e, {'transport': 'test'}) for e in events])
            alert = next(a for a in evaluate_window(events, 'gateway', start, end) if a['rule_id'] == 'high_error_rate')
            store.alert(alert)
            incident = {'incident_id': 'history-test', 'status': 'open', 'services': ['gateway', 'order', 'payment'],
                        'first_alert_id': alert['alert_id'], 'alert_ids': [alert['alert_id']], 'first_event_time': start,
                        'last_event_time': end, 'latest_alert_end': end, 'opened_at': end+5000}
            manager = ReportManager(store); old = manager.ensure_initial(incident, end+5000)
            own = next(e['event_id'] for e in events if e['service'] == 'payment' and e.get('status_code', 0) >= 500)
            old['analysis'] = analysis(ref=own);old['analysis']['hypotheses'][0].update(confidence='high', cause='旧模型无依据的数据库故障')
            old['audit'].update(analysis_mode='llm-assisted', model_metadata={'model_status': 'validated'})
            with store.db() as db:
                db.execute('UPDATE report_versions SET payload=? WHERE report_id=?', (canonical(old), old['report_id']))
            new = manager.refresh(incident, end+10000, force=True)
            self.assertEqual(manager.get(old['report_id']), old)
            self.assertGreater(new['version'], old['version'])
            self.assertEqual(new['analysis']['hypotheses'][0]['confidence'], 'low')
            self.assertEqual(new['audit']['model_metadata']['original_model_analysis'], old['analysis'])
            self.assertEqual(new['audit']['model_metadata']['reused_from_report_id'], old['report_id'])

