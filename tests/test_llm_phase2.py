import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from app.analysis_schema import validate_analysis
from app.contracts import canonical,now_ms
from app.diagnostic_context import build_context,prompt_records
from app.diagnosis import diagnose,evidence_observations,heuristic
from app.llm_client import complete,model_scope
from app.llm_config import LLMConfig,ModelError
from app.model_budget import ModelBudget
from app.storage import Store
from tests.model_support import analysis,http_stub,payload,records


class SchemaV2Tests(unittest.TestCase):
    def test_valid_candidates_and_unknown_are_accepted(self):
        self.assertEqual(validate_analysis(analysis(),records())['diagnosis_status'],'candidates')
        self.assertEqual(validate_analysis(analysis(unknown=True),records())['hypotheses'],[])

    def test_schema_and_evidence_failures(self):
        bad=[]
        v=analysis();del v['hypotheses'][0]['confidence_basis'];bad.append(v)
        v=analysis();v['hypotheses'][0]['supporting_evidence_ids']=['invented'];bad.append(v)
        v=analysis(service='unobserved');bad.append(v)
        v=analysis();v['hypotheses'][0]['against_evidence_ids']=['span-p'];bad.append(v)
        v=analysis();v['hypotheses'][0]['confidence']=.9;bad.append(v)
        v=analysis();v['hypotheses'][0]['verification_steps']=[];bad.append(v)
        v=analysis();v['diagnosis_status']='unknown';bad.append(v)
        v=analysis();v['involved_services']=['not-observed'];bad.append(v)
        v=analysis();v['unexpected']='extra';bad.append(v)
        for value in bad:
            with self.subTest(value=value),self.assertRaises(ValueError):validate_analysis(value,records())

    def test_candidate_must_have_own_service_support(self):
        rows=records()+[{**records()[0],'event_id':'healthy-inventory','service':'inventory'}]
        v=analysis(ref='healthy-inventory')
        with self.assertRaisesRegex(ValueError,'own_service'):validate_analysis(v,rows)


class ContextTests(unittest.TestCase):
    def snapshot(self,rows):return {'records':[{'event':e,'source':{'transport':'test'}} for e in rows],'coverage':{'truncated':False}}
    def context(self,rows):
        snap=self.snapshot(rows);observations,missing=evidence_observations(snap)
        alert={'alert_id':'alert','service':'payment','rule_id':'high_latency','window_start':now_ms()-2000,'window_end':now_ms()}
        return build_context(alert,snap,[],observations,missing)

    def test_oversized_anomalous_trace_is_not_dropped_for_healthy_baseline(self):
        bad=[{**records()[1],'event_id':f'bad-{i}','span_id':f's-{i}','trace_id':'large'} for i in range(49)]
        healthy={**records()[1],'event_id':'healthy','trace_id':'healthy','span_id':'healthy','status_code':200,'latency_ms':1}
        selected=prompt_records(self.snapshot(bad+[healthy]),48)
        self.assertTrue(any(e['event_id'].startswith('bad-') for e in selected))
        self.assertTrue(any(e['event_id']=='healthy' for e in selected))
        value,_=self.context(bad+[healthy])
        self.assertIn('large',value['selection']['partial_trace_ids'])

    def test_context_contains_categories_timeline_sources_and_set_identity(self):
        value,selected=self.context(records())
        self.assertEqual(set(value['evidence_index']),{'metrics','logs','spans'})
        self.assertTrue(value['event_timeline']);self.assertTrue(value['trace_links']);self.assertTrue(value['record_sources'])
        self.assertTrue(value['evidence_set']['id'].startswith('evidence-'))
        self.assertEqual(len(selected),3)

    def test_byte_and_field_limits_are_explicit_and_keep_usable_evidence(self):
        rows=records();rows[2]['message']='x'*20000;rows[2]['secret_extension']='never send'
        with patch.dict(os.environ,{'LLM_MAX_CONTEXT_RECORDS':'8','LLM_MAX_CONTEXT_BYTES':'8000'}):value,selected=self.context(rows)
        self.assertLessEqual(len(canonical(value).encode()),8000);self.assertTrue(selected)
        flag=next(f for f in value['selection']['record_projections'] if f['event_id']=='log-p')
        self.assertIn('message',flag['truncated_fields']);self.assertEqual(flag['omitted_extension_field_count'],1)
        self.assertNotIn('secret_extension',canonical(value))

    def test_known_credential_is_redacted_before_message_prefix(self):
        rows=records();secret='fake-test-credential';rows[2]['message']='x'*1990+secret+'tail'
        with patch.dict(os.environ,{'LLM_API_KEY':secret}):value,_=self.context(rows)
        self.assertNotIn(secret,canonical(value));self.assertNotIn('fake-test-',canonical(value))

    def test_rule_candidate_does_not_mix_independent_leaf_service_refs(self):
        rows=[records()[1],{**records()[1],'event_id':'inventory-bad','service':'inventory',
                           'trace_id':'independent-inventory-trace','span_id':'inventory-span','latency_ms':1002}]
        value=heuristic({'service':'gateway','rule_id':'high_latency'},self.snapshot(rows))
        target=value['hypotheses'][0]
        by_id={e['event_id']:e for e in rows}
        self.assertTrue(all(by_id[r]['service']==target['service'] for r in target['supporting_evidence_ids']))


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(self.tmp.name);self.budget=ModelBudget(self.store);self.cfg=LLMConfig(model='test',base_url='http://test')

    def claim(self,name='incident:one',cfg=None):return self.budget.claim(name,cfg or self.cfg,{'evidence_set_id':'test'})

    def test_schema_is_additive_and_backed_up_before_creation(self):
        backups=list((Path(self.tmp.name)/'backups').glob('*.sqlite3'));self.assertEqual(len(backups),1)
        with sqlite3.connect(backups[0]) as db:
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master")}
        self.assertIn('events',tables);self.assertNotIn('model_jobs',tables)
        ModelBudget(self.store);self.assertEqual(len(list((Path(self.tmp.name)/'backups').glob('*.sqlite3'))),1)

    def test_concurrent_reservations_cannot_exceed_persistent_budget(self):
        session=self.claim()
        def reserve(_):
            try:return session.reserve()
            except ModelError:return None
        with ThreadPoolExecutor(max_workers=5) as pool:results=list(pool.map(reserve,range(5)))
        self.assertEqual(sorted(x for x in results if x),[1,2])
        self.assertEqual(ModelBudget(Store(self.tmp.name)).audit('incident:one')['calls_reserved'],2)

    def test_rate_limit_shared_across_incidents(self):
        cfg=LLMConfig(model='test',base_url='http://test',requests_per_minute=1)
        self.claim('incident:one',cfg).reserve()
        with self.assertRaisesRegex(ModelError,'local_rate_limited'):self.claim('incident:two',cfg).reserve()
        self.assertEqual(self.budget.audit('incident:two')['calls_reserved'],0)

    def test_unused_expired_claim_can_recover_once(self):
        old=self.claim()
        with self.store.db() as db:db.execute("UPDATE model_jobs SET lease_until=0 WHERE scope_id='incident:one'")
        new=self.claim();self.assertIsNotNone(new)
        with self.assertRaisesRegex(ModelError,'claim_lost'):old.reserve()
        self.assertEqual(new.reserve(),1)

    def test_inflight_expired_claim_never_spends_more_calls(self):
        self.claim().reserve()
        with self.store.db() as db:db.execute("UPDATE model_jobs SET lease_until=0 WHERE scope_id='incident:one'")
        self.assertIsNone(self.claim())
        audit=self.budget.audit('incident:one');self.assertEqual(audit['status'],'indeterminate');self.assertEqual(audit['calls_reserved'],1)

    def test_completed_results_cache_without_second_claim(self):
        s=self.claim();s.complete(analysis(),{'requested_model':'test'})
        self.assertIsNone(self.claim());self.assertEqual(self.budget.cached('incident:one')['analysis'],analysis())

    def test_legacy_claim_is_honored(self):
        with self.store.db() as db:db.execute('INSERT INTO incident_model_attempts VALUES(?,?,?,?)',('legacy',now_ms(),'claimed','{}'))
        self.assertIsNone(self.budget.claim('incident:legacy',self.cfg,{},incident_id='legacy'))


class HTTPTransportTests(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{'LLM_MODEL':'test','LLM_API_KEY':'test-only-credential','LLM_MAX_ATTEMPTS':'2',
          'LLM_MAX_CALLS_PER_INCIDENT':'2','LLM_REQUESTS_PER_MINUTE':'1000','LLM_RETRY_BACKOFF_SECONDS':'0',
          'LLM_TIMEOUT_SECONDS':'1','LLM_TOTAL_TIMEOUT_SECONDS':'3','LLM_MAX_CONTEXT_BYTES':'64000','LLM_MAX_RESPONSE_BYTES':'1000000',
          'LLM_INPUT_PRICE_PER_MILLION':'1','LLM_OUTPUT_PRICE_PER_MILLION':'2','LLM_RESPONSE_FORMAT':'none'})
        self.env.start();self.addCleanup(self.env.stop)

    def call(self,base):
        with patch.dict(os.environ,{'LLM_BASE_URL':base}):return complete(payload(),'Return JSON diagnosis. Test double, not real model.')

    def test_success_real_http_and_usage_cost_audit(self):
        with http_stub([{}]) as (base,seen):result,meta=self.call(base)
        self.assertEqual(validate_analysis(result,records())['diagnosis_status'],'candidates')
        self.assertEqual(len(seen),1);self.assertEqual(seen[0]['path'],'/v1/chat/completions')
        self.assertEqual(meta['usage']['total_tokens'],200)
        self.assertAlmostEqual(meta['estimated_cost']['amount'],.00028)
        self.assertNotIn('test-only-credential',canonical(meta))

    def test_429_and_503_retry_bounded_then_success(self):
        for status in (429,503):
            with self.subTest(status=status),http_stub([{'status':status,'headers':{'Retry-After':'0'}},{}]) as (base,seen):
                _,meta=self.call(base)
                self.assertEqual(len(seen),2);self.assertEqual(len(meta['attempts']),2)

    def test_auth_error_no_retry_and_server_error_budget(self):
        for status,count in ((401,1),(503,2)):
            with self.subTest(status=status),http_stub([{'status':status}]) as (base,seen),self.assertRaises(ModelError) as caught:self.call(base)
            self.assertEqual(len(seen),count);self.assertEqual(len(caught.exception.metadata['attempts']),count)

    def test_request_timeout_falls_back_without_retry(self):
        with http_stub([{'delay':.25}]) as (base,seen),patch.dict(os.environ,{'LLM_TIMEOUT_SECONDS':'.06'}),self.assertRaisesRegex(ModelError,'request_timeout'):
            self.call(base)
        self.assertEqual(len(seen),1)

    def test_absolute_timeout_stops_trickling_response(self):
        started=time.monotonic()
        with http_stub([{'body':'{"a":"'+'x'*50+'"}','drip':.02}]) as (base,seen), \
             patch.dict(os.environ,{'LLM_TIMEOUT_SECONDS':'.2','LLM_TOTAL_TIMEOUT_SECONDS':'.15'}),self.assertRaisesRegex(ModelError,'total_timeout'):
            self.call(base)
        self.assertLess(time.monotonic()-started,1)
        self.assertEqual(len(seen),1)

    def test_invalid_json_oversize_truncated_nan_and_duplicate_keys(self):
        specs=[({'body':'not JSON'},'invalid_model_json'),({'body':'x'*300},'response_too_large'),
               ({'finish_reason':'length'},'output_truncated'),({'body':'{"choices":NaN}'},'invalid_model_json'),
               ({'body':'{"choices":[],"choices":[]}'},'invalid_model_json')]
        for spec,code in specs:
            with self.subTest(code=code),http_stub([spec]) as (base,seen),patch.dict(os.environ,{'LLM_MAX_RESPONSE_BYTES':'256' if code=='response_too_large' else '1000000'}),self.assertRaisesRegex(ModelError,code):self.call(base)
            self.assertEqual(len(seen),1)

    def test_persistent_budget_is_used_by_actual_http_retries(self):
        with tempfile.TemporaryDirectory() as data,http_stub([{'status':429},{}]) as (base,seen),patch.dict(os.environ,{'LLM_BASE_URL':base}):
            budget=ModelBudget(Store(data));session=budget.claim('incident:wire',LLMConfig.from_env(),{})
            with model_scope(session):value,meta=self.call(base)
            session.complete(value,meta)
            audit=budget.audit('incident:wire')
            self.assertEqual(audit['calls_reserved'],2);self.assertEqual(len(audit['attempts']),2)
            self.assertEqual(audit['status'],'succeeded')

    def test_rejected_truncated_output_remains_redacted_in_audit(self):
        with http_stub([{'finish_reason':'length'}]) as (base,seen),patch.dict(os.environ,{
             'LLM_BASE_URL':base,'LLM_MODEL':'stub','LLM_API_KEY':'http-test-double','LLM_REQUESTS_PER_MINUTE':'1000'}):
            with self.assertRaises(ModelError) as caught:complete(payload(),'Return JSON.')
        self.assertEqual(caught.exception.code,'output_truncated')
        self.assertEqual(caught.exception.metadata['finish_reason'],'length')
        self.assertTrue(caught.exception.metadata['raw_output'])
        self.assertNotIn('http-test-double',canonical(caught.exception.metadata))


class DiagnosisFallbackTests(unittest.TestCase):
    def store(self,data):
        store=Store(data);rows=records();store.ingest_many([(e,{'transport':'synthetic-test'}) for e in rows])
        alert={'schema_version':1,'alert_id':'test-alert','service':'payment','rule_id':'high_error_rate',
               'window_start':rows[0]['event_time']-1000,'window_end':rows[0]['event_time']+1000,'evidence':{}}
        store.alert(alert);return store

    def test_unknown_service_or_reference_falls_back_and_is_audited(self):
        for value in (analysis(ref='fabricated'),analysis(service='unobserved')):
            with tempfile.TemporaryDirectory() as data,http_stub([{'analysis':value}]) as (base,seen), \
                 patch.dict(os.environ,{'LLM_BASE_URL':base,'LLM_MODEL':'stub','LLM_API_KEY':'','LLM_REQUESTS_PER_MINUTE':'1000'}):
                store=self.store(data);report=diagnose(store,'test-alert')
                self.assertEqual(report['audit']['analysis_mode'],'heuristic')
                self.assertEqual(report['audit']['model_failure'],'ValueError')
                self.assertEqual(report['audit']['model_budget']['calls_reserved'],1)
                self.assertEqual(report['audit']['model_budget']['status'],'failed')
                self.assertEqual(diagnose(store,'test-alert'),report);self.assertEqual(len(seen),1)

    def test_unknown_is_valid_and_no_configuration_uses_rules(self):
        with tempfile.TemporaryDirectory() as data,http_stub([{'analysis':analysis(unknown=True)}]) as (base,seen), \
             patch.dict(os.environ,{'LLM_BASE_URL':base,'LLM_MODEL':'stub','LLM_API_KEY':'','LLM_REQUESTS_PER_MINUTE':'1000'}):
            report=diagnose(self.store(data),'test-alert')
            self.assertEqual(report['audit']['analysis_mode'],'llm-assisted');self.assertEqual(report['analysis']['diagnosis_status'],'unknown')
        with tempfile.TemporaryDirectory() as data,patch.dict(os.environ,{'LLM_BASE_URL':'','LLM_MODEL':''}):
            report=diagnose(self.store(data),'test-alert')
            self.assertEqual(report['audit']['analysis_mode'],'heuristic');self.assertIsNone(report['audit']['model_budget'])

    def test_invalid_settings_do_not_break_rule_report(self):
        with tempfile.TemporaryDirectory() as data,patch.dict(os.environ,{'LLM_BASE_URL':'http://unused','LLM_MODEL':'test','LLM_TIMEOUT_SECONDS':'NaN'}):
            report=diagnose(self.store(data),'test-alert')
            self.assertEqual(report['audit']['analysis_mode'],'heuristic');self.assertEqual(report['audit']['model_failure_code'],'invalid_configuration')

    def test_unavailable_context_keeps_rule_report_without_http_call(self):
        with tempfile.TemporaryDirectory() as data,patch('app.diagnosis._build_context',side_effect=ModelError('context_too_large')), \
             patch('app.diagnosis.call_model') as model:
            report=diagnose(self.store(data),'test-alert')
            self.assertEqual(report['audit']['analysis_mode'],'heuristic')
            self.assertEqual(report['audit']['context_selection']['error_code'],'context_too_large')
            model.assert_not_called()


class ConfigurationAndCasesTests(unittest.TestCase):
    def test_all_llm_settings_are_documented_and_passed_to_compose(self):
        from app.llm_config import ENV_KEYS
        root=Path(__file__).resolve().parents[1]
        example=(root/'.env.example').read_text();compose=(root/'docker-compose.yml').read_text()
        for name in ENV_KEYS:
            with self.subTest(name=name):
                self.assertIn(name+'=',example);self.assertIn(name+': ${'+name,compose)

    def test_invalid_urls_limits_and_headers_are_configuration_failures(self):
        for changes in ({'LLM_BASE_URL':'http://user:password@host/v1'}, {'LLM_BASE_URL':'http://[invalid'},
                        {'LLM_BASE_URL':'http://host:bad/v1'}, {'LLM_BASE_URL':'https://host/v1?api_key=secret'},
                        {'LLM_API_KEY':'bad\r\nheader'}, {'LLM_MAX_CALLS_PER_INCIDENT':'6'},
                        {'LLM_TOTAL_TIMEOUT_SECONDS':'inf'}, {'LLM_TIMEOUT_SECONDS':'601'}):
            with self.subTest(fields=list(changes)),patch.dict(os.environ,changes),self.assertRaises(ModelError):LLMConfig.from_env()
        self.assertNotIn('test-secret',repr(LLMConfig(api_key='test-secret')))
        with patch.dict(os.environ,{'LLM_TIMEOUT_SECONDS':'500','LLM_TOTAL_TIMEOUT_SECONDS':'530'}):
            self.assertEqual(LLMConfig.from_env().timeout_s,500)

    def test_invalid_cases_skip_and_valid_cases_have_frozen_hash(self):
        from app.retrieval import retrieve
        from app.contracts import digest
        with tempfile.TemporaryDirectory() as data:
            p=Path(data)
            valid={'case_id':'real-case','version':1,'services':['payment'],'rule_ids':['high_error_rate'],'keywords':[],'root_cause':'historical example only'}
            (p/'ok.json').write_text(json.dumps(valid));(p/'bad.json').write_text('{invalid')
            (p/'bad-version.json').write_text(json.dumps(dict(valid,version=True)))
            result=retrieve({'service':'payment','rule_id':'high_error_rate'},{'records':[]},case_dir=p)
        self.assertEqual(len(result),1);self.assertEqual(result[0]['case_sha256'],digest(valid))

    def test_json_schema_mode_constrains_only_real_services_and_references(self):
        from app.llm_client import constrained_schema
        schema=constrained_schema(payload())
        props=schema['$defs']['Hypothesis']['properties']
        self.assertEqual(props['service']['enum'],['payment'])
        self.assertEqual(set(props['supporting_evidence_ids']['items']['enum']),{'metric-p','span-p','log-p'})

    def test_partial_usage_keeps_total_cost_unknown(self):
        with http_stub([{'status':429},{}]) as (base,seen),patch.dict(os.environ,{
            'LLM_BASE_URL':base,'LLM_MODEL':'stub','LLM_API_KEY':'','LLM_RETRY_BACKOFF_SECONDS':'0',
            'LLM_REQUESTS_PER_MINUTE':'1000','LLM_INPUT_PRICE_PER_MILLION':'1','LLM_OUTPUT_PRICE_PER_MILLION':'2'}):
            _,meta=complete(payload(),'Return JSON.')
        self.assertIsNone(meta['estimated_cost']);self.assertEqual(meta['attempts_without_usage'],1)
        self.assertAlmostEqual(meta['estimated_cost_for_reported_usage_only']['amount'],.00028)

    def test_unsupported_content_encoding_is_bounded_failure(self):
        with http_stub([{'headers':{'Content-Encoding':'gzip'}}]) as (base,seen),patch.dict(os.environ,{
             'LLM_BASE_URL':base,'LLM_MODEL':'stub','LLM_API_KEY':'','LLM_REQUESTS_PER_MINUTE':'1000'}), \
             self.assertRaisesRegex(ModelError,'unsupported_response_encoding'):complete(payload(),'Return JSON.')
