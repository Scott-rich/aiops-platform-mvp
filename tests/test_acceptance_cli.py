"""Run the real acceptance CLI against explicit subprocess test doubles.

Only orchestration/argument handling is under test. No fake reports from these
disposable fixtures are presented as Kafka, model or performance validation.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from scripts.acceptance_contracts import REQUIRED_CHECKS


STUB = '''import argparse,json,os,subprocess,sys,hashlib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--model-python');a,_=p.parse_known_args()
a.output.mkdir(parents=True,exist_ok=False)
v=json.loads((Path(__file__).parent.parent/'fixture-results.json').read_text())[a.output.name]
if a.output.name=='unit' and os.getenv('FIXTURE_CORRUPT_UNIT')=='1':v={}
if a.output.name=='model':
    child=subprocess.run([a.model_python,'-c','import acceptance_venv_marker,sys,json;print(json.dumps({"prefix":sys.prefix,"marker":acceptance_venv_marker.VALUE}))'],capture_output=True,text=True,timeout=10)
    if child.returncode:
        v={'status':'failed','error_type':'VirtualenvLost','child_returncode':child.returncode}
    else:v['interpreter_probe']=json.loads(child.stdout)
if a.output.name=='benchmark-http' and os.getenv('FIXTURE_BLOCKED_CONTROL')=='1':
    v['status']='partial';v['components']['http']['status']='blocked';v['components']['http']['checks']=[{'name':'healthy_control','passed':False,'status':'blocked'}]
blob=json.dumps(v).encode();(a.output/'result.json').write_bytes(blob)
if a.output.name=='benchmark-http':(a.output/'completed.json').write_text(json.dumps({'status':v['status'],'result_sha256':hashlib.sha256(blob).hexdigest()}))
sys.exit(1 if v.get('status')=='failed' else 2 if v.get('status')=='partial' else 0)
'''


def fixture_project(root):
    source=Path(__file__).resolve().parents[1]
    scripts=root/'scripts';scripts.mkdir();(scripts/'__init__.py').write_text('')
    for name in ('acceptance.py','acceptance_contracts.py'):
        shutil.copy2(source/'scripts'/name,scripts/name)
    modules={'unit':'test_suite','runtime-http':'verify_runtime_http','launcher':'verify_launcher',
             'evidence':'verify_evidence','detection':'evaluate_detection','rca':'evaluate_rca',
             'model':'verify_llm','benchmark-http':'benchmark_platform'}
    values={name:{'status':'passed','checks':[{'name':check,'passed':True} for check in sorted(REQUIRED_CHECKS[name])]}
            for name in ('runtime-http','launcher','evidence','model')}
    values['evidence']['child_process_stopped']=True
    values['model']['cleanup']={'model_stopped':True}
    values['unit']={'status':'passed','tests_run':1,'passed':1,'failures':0,'errors':0,'skipped':0}
    metric={'tp':1,'fp':0,'tn':0,'fn':0,'labelled':1,'accuracy':1.0,'precision':1.0,'recall':1.0}
    values['detection']={'status':'passed','execution_kind':'offline_labelled_replay','window_count':1,
                         'metrics':{'fixed':metric,'dynamic':metric}}
    values['rca']={'schema_version':1,'cases':[{'execution_status':'ok','references_valid':True}],
                   'metrics':{key:{'numerator':1,'denominator':1,'value':1.0} for key in (
                       'top1_any_labelled_root_accuracy','top3_any_labelled_root_accuracy',
                       'execution_success_rate','evidence_reference_validity')}}
    values['benchmark-http']={'status':'passed','components':{'http':{'status':'passed','tiers':[{}],
                              'checks':[{'name':'persisted','passed':True}]}}}
    (root/'fixture-results.json').write_text(json.dumps(values))
    for module in modules.values():(scripts/(module+'.py')).write_text(STUB)


class AcceptanceCLIRegression(unittest.TestCase):
    def run_cli(self, root, *args, extra_env=None):
        env={**os.environ,**(extra_env or {})}
        env.pop('PYTHONPATH',None)
        return subprocess.run([sys.executable,'-m','scripts.acceptance','--output',str(root/'output'),*args],
                              cwd=root,env=env,capture_output=True,text=True,timeout=30)

    def test_real_cli_preserves_virtualenv_symlink_and_full_gate_blocks(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);fixture_project(root)
            venv=root/'model-venv'
            subprocess.run([sys.executable,'-m','venv','--without-pip','--symlinks',str(venv)],check=True,capture_output=True,timeout=15)
            python=venv/'bin/python'
            self.assertTrue(python.is_symlink())
            site=venv/'lib'/f'python{sys.version_info.major}.{sys.version_info.minor}'/'site-packages'
            (site/'acceptance_venv_marker.py').write_text('VALUE="installed-only-in-this-venv"\n')
            result=self.run_cli(root,'--real-model','--model-python','model-venv/bin/python','--require-full')
            self.assertEqual(result.returncode,2,result.stdout+result.stderr)
            completed=json.loads((root/'output/completed.json').read_text())
            self.assertEqual(completed['gates']['model'],'passed')
            self.assertEqual(completed['gates']['docker'],'blocked_not_executed')
            self.assertFalse(completed['full_acceptance'])
            model_step=next(s for s in completed['steps'] if s['name']=='model')
            index=model_step['command'].index('--model-python')
            self.assertEqual(model_step['command'][index+1],str(python))
            model=json.loads((root/'output/model/result.json').read_text())
            self.assertEqual(model['interpreter_probe']['prefix'],str(venv))
            self.assertEqual(model['interpreter_probe']['marker'],'installed-only-in-this-venv')
            for gate in ('reliability','faults','http_benchmark','kafka_benchmark','flink_benchmark'):
                self.assertEqual(completed['gates'][gate],'blocked_not_requested')

    def test_real_cli_empty_successful_child_fails_overall(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);fixture_project(root)
            result=self.run_cli(root,extra_env={'FIXTURE_CORRUPT_UNIT':'1'})
            self.assertEqual(result.returncode,1,result.stdout+result.stderr)
            value=json.loads((root/'output/completed.json').read_text())
            self.assertEqual(value['gates']['local'],'failed')
            self.assertEqual(value['steps'][0]['status'],'failed')

    def test_real_cli_blocked_benchmark_is_not_passed_or_false_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);fixture_project(root)
            result=self.run_cli(root,'--benchmark','--require-full',extra_env={'FIXTURE_BLOCKED_CONTROL':'1'})
            self.assertEqual(result.returncode,2,result.stdout+result.stderr)
            value=json.loads((root/'output/completed.json').read_text())
            self.assertEqual(value['gates']['local'],'passed')
            self.assertEqual(value['gates']['http_benchmark'],'blocked')
            self.assertFalse(value['full_acceptance'])
            self.assertTrue(value['result_files_unchanged'])

    def test_cli_requires_runtime_and_explicit_resource_pressure_scope(self):
        for flag in ('--reliability','--kafka-benchmark','--flink-benchmark','--allow-resource-pressure'):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as folder:
                root=Path(folder);fixture_project(root)
                result=self.run_cli(root,flag)
                self.assertEqual(result.returncode,2)
                self.assertIn('require',result.stderr)
                self.assertFalse((root/'output').exists())


if __name__=='__main__':unittest.main()
