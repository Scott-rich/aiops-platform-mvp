import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from scripts import acceptance
from scripts.acceptance import classify,run_step,retained_results_match,source_manifest,read_result
from scripts.acceptance_contracts import STEP_NAMES,checks_status


def unit_result():
    return {'status':'passed','tests_run':1,'passed':1,'failures':0,'errors':0,'skipped':0}


def rca_result():
    metric={'numerator':1,'denominator':1,'value':1.0}
    return {'schema_version':1,'cases':[{'execution_status':'ok','references_valid':True}],
            'archived_replays':[], 'metrics':{k:dict(metric) for k in (
                'top1_any_labelled_root_accuracy','top3_any_labelled_root_accuracy',
                'execution_success_rate','evidence_reference_validity')}}

class AcceptanceRunnerTests(unittest.TestCase):
    def test_failed_or_blocked_not_reported_success(self):
        self.assertEqual(classify(0,{'status':'blocked'}),'blocked')
        self.assertEqual(classify(2,{'status':'passed'}),'blocked')
        self.assertEqual(classify(1,{'status':'passed'}),'failed')
        self.assertEqual(classify(0,{'status':'passed','checks':[{'passed':False}]}),'failed')
        self.assertEqual(classify(0,{'status':'running'}),'failed')
        self.assertEqual(classify(0,{'status':'passed'}),'passed')

    def test_empty_unknown_malformed_and_contradictory_results_fail(self):
        for value in ({},{'status':'unknown'},{'status':[]},[],None,
                      {'status':'passed','failures':1},{'status':'passed','errors':-1},
                      {'status':'passed','errors':True},{'status':'passed','counts':{'failed':1}},
                      {'status':'passed','cleanup':{'errors':['child remained']}},
                      {'status':'passed','checks':[{'name':'x','passed':'true'}]},
                      {'status':'passed','checks':[]}):
            with self.subTest(value=value):self.assertEqual(classify(0,value),'failed')

    def test_each_known_step_requires_its_own_evidence(self):
        for name in STEP_NAMES:
            with self.subTest(name=name):
                self.assertEqual(classify(0,{'status':'passed'},name),'failed')
        self.assertEqual(classify(0,{'status':'passed'},'unregistered-step'),'failed')

    def test_unit_counts_must_be_consistent(self):
        self.assertEqual(classify(0,unit_result(),'unit'),'passed')
        for changes in ({'tests_run':0},{'passed':0},{'skipped':1},{'passed':True},{'errors':1}):
            self.assertEqual(classify(0,{**unit_result(),**changes},'unit'),'failed')

    def test_rca_without_status_is_adapted_and_errors_are_not_hidden(self):
        value=rca_result()
        self.assertEqual(classify(0,value,'rca'),'passed')
        # A completed evaluation does not imply an invented accuracy SLO.
        value['metrics']['top1_any_labelled_root_accuracy'].update(numerator=0,value=0.0)
        self.assertEqual(classify(0,value,'rca'),'passed')
        value['cases'][0]['references_valid']=False
        self.assertEqual(classify(0,value,'rca'),'failed')
        value=rca_result();value['metrics']['execution_success_rate']['denominator']=0
        self.assertEqual(classify(0,value,'rca'),'failed')

    def test_benchmark_distinguishes_invalid_control_from_failed_check(self):
        check={'name':'healthy_control','passed':False,'status':'blocked'}
        value={'status':'partial','components':{'http':{'status':'blocked','tiers':[{}],'checks':[check]}}}
        self.assertEqual(classify(2,value,'benchmark-http'),'blocked')
        check.pop('status')
        self.assertEqual(classify(2,value,'benchmark-http'),'failed')
        check['passed']=True;value['status']='passed';value['components']['http']['status']='passed'
        self.assertEqual(classify(0,value,'benchmark-http'),'passed')
        self.assertEqual(classify(0,value,'benchmark-kafka'),'failed')

    def test_fault_observed_http_status_and_repeated_checks_remain_compatible(self):
        self.assertEqual(checks_status([{'name':'observed_503','passed':True,'status':503},
                                        {'name':'observed_503','passed':True,'status':503}]),'passed')
        self.assertEqual(checks_status([{'name':'observed_503','passed':False,'status':503}]),'failed')

    def test_missing_result_is_failure_and_existing_output_is_not_assumed(self):
        with tempfile.TemporaryDirectory() as d:
            result=run_step('missing',[sys.executable,'-c','print("no result")'],Path(d),os.environ.copy(),5)
        self.assertEqual(result['status'],'failed')

    def test_terminal_file_is_preferred_and_later_tamper_fails(self):
        import hashlib
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);terminal=root/'unit'/'completed.json'
            code="import json,sys;from pathlib import Path;p=Path(sys.argv[1]);p.mkdir();(p/'result.json').write_text(json.dumps({'status':'running'}));(p/'completed.json').write_text(sys.argv[2])"
            result=run_step('unit',[sys.executable,'-c',code,str(root/'unit'),json.dumps(unit_result())],root,os.environ.copy(),5)
            self.assertEqual(result['status'],'passed');self.assertEqual(result['result_file'],'unit/completed.json')
            self.assertTrue(retained_results_match([result],root))
            terminal.write_text(json.dumps({'status':'failed'}))
            self.assertFalse(retained_results_match([result],root))

    def test_real_child_empty_json_is_not_success(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            code="import sys;from pathlib import Path;p=Path(sys.argv[1]);p.mkdir();(p/'result.json').write_text('{}')"
            result=run_step('unit',[sys.executable,'-c',code,str(root/'unit')],root,os.environ.copy(),5)
            self.assertEqual(result['status'],'failed')

    def test_benchmark_summary_requires_verified_detailed_checks(self):
        code="import sys,json,hashlib;from pathlib import Path;p=Path(sys.argv[1]);p.mkdir();b=sys.argv[2].encode();(p/'result.json').write_bytes(b);(p/'completed.json').write_text(json.dumps({'status':'passed','result_sha256':hashlib.sha256(b).hexdigest()}))"
        value={'status':'passed','components':{'http':{'status':'passed','tiers':[{}],
               'checks':[{'name':'persisted','passed':True}]}}}
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            result=run_step('benchmark-http',[sys.executable,'-c',code,str(root/'benchmark-http'),json.dumps(value)],root,os.environ.copy(),5)
            self.assertEqual(result['status'],'passed')
            self.assertEqual(len(result['result_files']),2)
            self.assertTrue(retained_results_match([result],root))
            (root/'benchmark-http'/'result.json').write_text('{}')
            self.assertFalse(retained_results_match([result],root))
        value['components']['http']['checks'][0]['passed']=False
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            result=run_step('benchmark-http',[sys.executable,'-c',code,str(root/'benchmark-http'),json.dumps(value)],root,os.environ.copy(),5)
            self.assertEqual(result['status'],'failed')

    def test_result_parser_rejects_duplicate_keys_and_nonfinite_numbers(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'result.json'
            for text in ('{"status":"failed","status":"passed"}', '{"value":NaN}', '[]'):
                path.write_text(text)
                with self.assertRaises(ValueError):read_result(path)

    def test_manifest_detects_case_benchmark_build_and_configuration_changes(self):
        names=('benchmarks/runtime.py','cases/example.json','simulator/Dockerfile','flink/Dockerfile',
               'requirements.txt','requirements-lock.txt','.env.example','.github/workflows/tests.yml',
               'app/static/index.html','tests/fixture.json','infra/topics.yml','docker-compose.override.yml')
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for name in names:
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('before')
            (root/'.env').write_text('PRIVATE=never-in-manifest')
            cache=root/'app'/'__pycache__';cache.mkdir();(cache/'file.pyc').write_bytes(b'cache')
            with patch.object(acceptance,'ROOT',root):
                before=source_manifest()
                self.assertEqual(set(before),set(names))
                for name in names:
                    with self.subTest(name=name):
                        (root/name).write_text('after');after=source_manifest()
                        self.assertNotEqual(after[name],before[name]);(root/name).write_text('before')

    def test_docker_lock_covers_both_declared_dependency_sets(self):
        root=Path(__file__).resolve().parents[1]
        def pins(path):
            return {line.strip() for line in path.read_text().splitlines() if line.strip() and not line.startswith('#')}
        locked=pins(root/'requirements-lock.txt')
        self.assertFalse(pins(root/'requirements.txt')-locked)
        self.assertFalse(pins(root/'simulator/requirements.txt')-locked)

    def test_existing_step_output_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'done').mkdir()
            with self.assertRaises(FileExistsError):run_step('done',[sys.executable,'-c','pass'],root,os.environ.copy(),5)

    def test_timeout_stops_real_child(self):
        with tempfile.TemporaryDirectory() as d:
            result=run_step('slow',[sys.executable,'-c','import time;time.sleep(10)'],Path(d),os.environ.copy(),.1)
        self.assertEqual(result['status'],'failed');self.assertTrue(result['timeout']);self.assertFalse(result['forced_stop'])

if __name__=='__main__':unittest.main()
