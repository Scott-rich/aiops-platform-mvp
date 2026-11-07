"""Independent real-model integration test on an archived, hash-verified fault report.

No response is synthesized. An optional GGUF server performs actual CPU inference;
external services use only environment configuration. Missing configuration is a
blocked result (exit 2), never a skipped-success or substituted rule diagnosis.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import traceback
import urllib.request
from app.analysis_schema import validate_analysis
from app.contracts import canonical,digest,now_ms
from app.llm_config import LLMConfig,redact
from app.model_budget import ModelBudget
from app.reporting import ReportManager
from app.storage import Store
from scripts.bootstrap_local_model import MODELS

ROOT=Path(__file__).resolve().parents[1]


def save_json(path,value):
    safe=redact(value)
    path.write_text(json.dumps(safe,ensure_ascii=False,indent=2)+'\n')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-report',type=Path,default=ROOT/'artifacts/native-attempt-04/actual-http-report.json')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--local-model-file',type=Path)
    parser.add_argument('--model-python',type=Path,default=Path('/tmp/aiops-model-runtime/venv/bin/python'))
    parser.add_argument('--model-name',default='qwen2.5-1.5b-instruct-q4_k_m')
    parser.add_argument('--local-context-records',type=int,default=12)
    parser.add_argument('--local-context-bytes',type=int,default=16000)
    parser.add_argument('--local-max-tokens',type=int,default=512)
    parser.add_argument('--local-output-protocol',choices=['analysis-v2','evidence-selection-v1'],default='evidence-selection-v1')
    parser.add_argument('--expected-service',help='Independent test label; never passed to the model. Requires this model-selected candidate at rank 1.')
    parser.add_argument('--expect-unknown',action='store_true',help='Independent healthy/insufficient-evidence label; never passed to the model.')
    parser.add_argument('--local-model-context-size',type=int,default=8192)
    parser.add_argument('--local-model-threads',type=int,default=4)
    args=parser.parse_args()
    if args.expected_service and args.expect_unknown:parser.error('candidate and unknown expectations are mutually exclusive')
    if not 1<=args.local_context_records<=48 or not 4096<=args.local_context_bytes<=64000 or not 64<=args.local_max_tokens<=4096:
        parser.error('local inference input/output safety bounds exceeded')
    if not 2048<=args.local_model_context_size<=16384:parser.error('local model context size out of bounds')
    if not 1<=args.local_model_threads<=8:parser.error('local CPU threads must be between 1 and 8')
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    result={'execution_kind':'real_local_cpu_model' if args.local_model_file else 'configured_model_service',
            'started_at_utc':datetime.now(timezone.utc).isoformat(),'status':'running','checks':[],
            'limitations':['Evidence is replayed from the explicitly recorded prior real HTTP/Kafka run, not newly collected production data.',
                           'A successful structured result is not a measured RCA accuracy score or a calibrated probability.']}
    started=time.monotonic();child=log=None
    result['source_sha256']={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
        ('app/analysis_schema.py','app/model_grounding.py','app/diagnostic_context.py','app/llm_config.py','app/llm_client.py',
         'app/model_budget.py','app/diagnosis.py','app/reporting.py','app/storage.py','app/api.py',
         'requirements.txt','scripts/bootstrap_local_model.py','scripts/local_llm_server.py','scripts/verify_llm.py')}
    def check(name,passed):
        result['checks'].append({'name':name,'passed':bool(passed)})
        if not passed:raise RuntimeError(name)
    try:
        if args.local_model_file:
            model=args.local_model_file.resolve()
            if not model.is_file() or not args.model_python.is_file():raise FileNotFoundError('optional local model/runtime missing')
            with model.open('rb') as model_stream:model_sha=hashlib.file_digest(model_stream,'sha256').hexdigest()
            known=next((s for s in MODELS.values() if s['name'].removesuffix('.gguf')==args.model_name),None)
            if known and model_sha!=known['sha256']:raise ValueError('pinned model must match verified official SHA256')
            result['local_model']={'model':args.model_name,'file_sha256':model_sha,'bytes':model.stat().st_size,
                                   'threads':args.local_model_threads,'context_size':args.local_model_context_size,'quantization':'Q4_K_M','gpu_layers':0}
            with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            log=(output/'model-server.log').open('w')
            env={k:v for k,v in os.environ.items() if not k.startswith('LLM_') and not k.endswith('API_KEY')}
            child=subprocess.Popen([str(args.model_python),'-m','scripts.local_llm_server','--model-file',str(model),
                   '--model-name',args.model_name,'--port',str(port),'--context-size',str(args.local_model_context_size),
                   '--threads',str(args.local_model_threads)],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
            endpoint=f'http://127.0.0.1:{port}'
            deadline=time.monotonic()+90
            while time.monotonic()<deadline:
                if child.poll() is not None:raise RuntimeError('real model server failed to start; inspect log')
                try:
                    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(endpoint+'/health',timeout=2) as response:
                        health=json.load(response)
                    if health.get('ready'):break
                except (OSError,ValueError):pass
                time.sleep(.25)
            else:raise TimeoutError('real model server startup timeout')
            result['model_health']=health
            check('actual_cpu_inference_server_loaded_verified_weights',health.get('execution_kind')=='real_gguf_cpu_inference' and health.get('model_sha256')==model_sha)
            os.environ.update(LLM_BASE_URL=endpoint+'/v1',LLM_MODEL=args.model_name,LLM_API_KEY='',
                LLM_RESPONSE_FORMAT='json_schema',LLM_MAX_CONTEXT_RECORDS=str(args.local_context_records),LLM_MAX_CONTEXT_BYTES=str(args.local_context_bytes),
                LLM_OUTPUT_PROTOCOL=args.local_output_protocol,
                LLM_MAX_TOKENS=str(args.local_max_tokens),LLM_TIMEOUT_SECONDS='500',LLM_TOTAL_TIMEOUT_SECONDS='530',
                LLM_MAX_ATTEMPTS='1',LLM_MAX_CALLS_PER_INCIDENT='1',LLM_REQUESTS_PER_MINUTE='5')
        config=LLMConfig.from_env()
        if not config.configured:
            result.update(status='blocked',blocker='LLM_BASE_URL / LLM_MODEL are not configured; no real request sent')
            return_code=2
        else:
            result['configuration']={k:getattr(config,k) for k in ('model','timeout_s','total_timeout_s','max_attempts',
                 'max_calls_per_incident','max_context_records','max_context_bytes','max_tokens','response_format','output_protocol')}
            raw=args.evidence_report.read_bytes()
            if len(raw)>20_000_000:raise ValueError('evidence report too large for this isolated test')
            source=json.loads(raw);snapshot=source['snapshot']
            check('source_evidence_snapshot_hash_valid',digest(snapshot)==source['audit']['snapshot_sha256'])
            result['evidence_source']={'path':str(args.evidence_report),'file_sha256':hashlib.sha256(raw).hexdigest(),
                'snapshot_sha256':source['audit']['snapshot_sha256'],'records':len(snapshot['records']),
                'provenance_transports':sorted({r.get('source',{}).get('transport','unknown') for r in snapshot['records']})}
            store=Store(output/'data')
            store.ingest_many([(r['event'],r.get('source')) for r in snapshot['records']])
            alert=source['alert'];store.alert(alert,source['audit'].get('alert_source'))
            incident=dict(source.get('incident',{}))
            incident.update(incident_id=source.get('incident_id','model-verification'),status='open',
                 first_alert_id=alert['alert_id'],alert_ids=[alert['alert_id']])
            incident.setdefault('services',sorted({r['event']['service'] for r in snapshot['records']}))
            incident.setdefault('first_event_time',alert['window_start']);incident.setdefault('last_event_time',alert['window_end'])
            incident.setdefault('latest_alert_end',alert['window_end']);incident.setdefault('opened_at',now_ms())
            manager=ReportManager(store)
            preliminary=manager.ensure_initial(incident)
            save_json(output/'preliminary-report.json',preliminary)
            check('preliminary_report_is_rule_only',preliminary['audit']['analysis_mode']=='heuristic')
            call_started=time.monotonic();report=manager.refresh(incident,force=True)
            result['diagnosis_seconds']=round(time.monotonic()-call_started,3)
            if report is None:raise RuntimeError('no enriched diagnostic report generated')
            save_json(output/'model-enriched-report.json',report)
            result['model_failure']=report['audit'].get('model_failure')
            result['model_failure_code']=report['audit'].get('model_failure_code')
            check('actual_model_output_passes_schema_and_evidence_checks',report['audit']['analysis_mode']=='llm-assisted')
            validate_analysis(report['analysis'],report['audit']['prompt']['payload']['records'],alert=alert)
            grounding=report['audit']['model_metadata']
            check('model_observation_grounding_and_low_confidence_ceiling',
                  grounding.get('model_status')=='grounded_observations' and grounding.get('internal_cause_status')=='unknown'
                  and all(h['confidence']=='low' for h in report['analysis']['hypotheses']))
            check('model_raw_output_and_normalization_are_audited',bool(grounding.get('raw_output'))
                  and grounding.get('normalized_analysis_sha256')==digest(report['analysis'])
                  and grounding.get('narrative_origin','').startswith('platform_fact_projection'))
            result['independent_expectation']={'expected_service':args.expected_service,'expected_unknown':args.expect_unknown}
            if args.expected_service:
                check('independently_labelled_root_ranked_first_by_model',
                      [h['service'] for h in report['analysis']['hypotheses']][:1]==[args.expected_service])
            if args.expect_unknown:
                check('healthy_or_insufficient_evidence_model_abstains',report['analysis']['diagnosis_status']=='unknown')
            check('result_snapshot_hash_valid',digest(report['snapshot'])==report['audit']['snapshot_sha256'])
            scope='incident:'+incident['incident_id'];audit=ModelBudget(store).audit(scope)
            check('real_http_attempt_recorded_in_persistent_budget',audit['calls_reserved']>=1 and audit['status']=='succeeded')
            original_calls=audit['calls_reserved']
            manager.refresh(incident,force=True)
            reopened=ModelBudget(Store(output/'data')).audit(scope)
            check('repeat_report_and_database_reopen_do_not_repeat_model_call',reopened['calls_reserved']==original_calls)
            check('reopened_report_preserves_result',ReportManager(Store(output/'data')).get(report['report_id'])==report)
            result.update(status='passed',diagnosis_status=report['analysis']['diagnosis_status'],
                 candidate_services=[h['service'] for h in report['analysis']['hypotheses']],
                 evidence_set_id=report['audit']['evidence_set_id'],budget=reopened,
                 model_metadata=report['audit']['model_metadata'])
            return_code=0
    except Exception as exc:
        result.update(status='failed',error={'type':type(exc).__name__,'message':redact(str(exc)),
                                           'traceback':redact(traceback.format_exc())})
        return_code=1
    finally:
        cleanup={'model_started':child is not None,'model_stopped':child is None,'forced_kill':False}
        if child is not None:
            try:
                child.terminate()
                try:child.wait(timeout=15)
                except subprocess.TimeoutExpired:cleanup['forced_kill']=True;child.kill();child.wait(timeout=5)
                cleanup.update(model_stopped=True,exit_code=child.returncode)
            except Exception as exc:
                cleanup['error_type']=type(exc).__name__;result['status']='failed';return_code=1
        if log:log.close()
        result.update(cleanup=cleanup,elapsed_seconds=round(time.monotonic()-started,3),
             finished_at_utc=datetime.now(timezone.utc).isoformat(),
             check_counts={'passed':sum(c['passed'] for c in result['checks']),
                           'failed':sum(not c['passed'] for c in result['checks'])})
        save_json(output/'result.json',result)
        print(json.dumps({'status':result['status'],'checks':result['check_counts'],'result':str(output/'result.json')},ensure_ascii=False))
    raise SystemExit(return_code)


if __name__=='__main__':main()
