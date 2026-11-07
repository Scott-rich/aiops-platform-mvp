"""Repeatable acceptance orchestrator. Exit 0=local gate passed, 1=failure, 2=full gate blocked."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
from .acceptance_contracts import classify

ROOT=Path(__file__).resolve().parents[1]


def source_manifest():
    # Include code, cases, test fixtures, assets, CI and every nested Dockerfile.
    # Runtime artifacts/data/venvs and credentials are intentionally not sources.
    directories=('app','scripts','simulator','flink','tests','infra','benchmarks','cases','examples','.github')
    ignored={'__pycache__','.pytest_cache','.mypy_cache','.ruff_cache','.venv'}
    files=[p for d in directories for p in (ROOT/d).rglob('*')
           if p.is_file() and not set(p.relative_to(ROOT).parts)&ignored
           and p.suffix not in {'.pyc','.pyo','.sqlite3','.sqlite3-wal','.sqlite3-shm','.db','.jar','.gguf','.zip','.pem','.key','.p12','.pfx'}
           and (not p.name.startswith('.env') or p.name=='.env.example')]
    names={'Dockerfile','.dockerignore','.gitignore','.env.example','pyproject.toml','pytest.ini',
           'setup.cfg','setup.py','tox.ini','Makefile','uv.lock','poetry.lock'}
    files += [p for p in ROOT.iterdir() if p.is_file() and (p.name in names or p.suffix=='.py'
              or p.name.startswith('requirements') and p.suffix=='.txt'
              or p.name.startswith(('docker-compose','compose.')) and p.suffix in {'.yml','.yaml'})]
    return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}


def read_result(path):
    def pairs(values):
        result={}
        for key,value in values:
            if key in result:raise ValueError('duplicate result key')
            result[key]=value
        return result
    def invalid_constant(_):raise ValueError('non-finite result number')
    value=json.loads(path.read_text(),object_pairs_hook=pairs,parse_constant=invalid_constant)
    if not isinstance(value,dict):raise ValueError('result must be an object')
    return value


def model_command(args, output):
    command=[sys.executable,'-m','scripts.verify_llm','--output',str(output)]
    evidence=args.model_evidence
    if evidence is None and args.native_runtime:
        evidence=output.parent/'native'/'actual-http-report.json'
    if evidence:command+=['--evidence-report',str(evidence.resolve())]
    if args.local_model_file:command+=['--local-model-file',str(args.local_model_file.resolve())]
    if args.model_python:
        # Executing a venv symlink is meaningful: resolve() would lose pyvenv.cfg.
        command+=['--model-python',os.path.abspath(os.path.expanduser(str(args.model_python)))]
    return command


def run_step(name, command, output, env, timeout=600):
    if (output/name).exists():raise FileExistsError("step output already exists")
    started=time.monotonic();code=None;timed_out=False;forced=False
    with (output/(name+'.log')).open('w') as log:
        child=subprocess.Popen(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:code=child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out=True;os.killpg(child.pid,signal.SIGINT)
            try:code=child.wait(timeout=60)
            except subprocess.TimeoutExpired:forced=True;os.killpg(child.pid,signal.SIGKILL);code=child.wait(timeout=5)
        except BaseException:
            os.killpg(child.pid,signal.SIGINT)
            try:child.wait(timeout=60)
            except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
            raise
    path=output/name/'completed.json'
    if not path.exists():path=output/name/'result.json'
    retained=[];validation_error=None
    try:
        result=read_result(path)
        retained.append({'path':str(path.relative_to(output)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        # Benchmark completed.json is a hash-bound summary, not the checks.
        if 'result_sha256' in result:
            detail=path.parent/'result.json'
            if detail==path or hashlib.sha256(detail.read_bytes()).hexdigest()!=result['result_sha256']:
                raise ValueError('terminal result hash mismatch')
            detailed=read_result(detail)
            if detailed.get('status')!=result.get('status'):raise ValueError('terminal result status mismatch')
            retained.append({'path':str(detail.relative_to(output)),'sha256':result['result_sha256']})
            result=detailed
        status=classify(code,result,name)
        if status=='failed':validation_error='step_result_contract_or_exit_failed'
    except (OSError,ValueError,TypeError):result={};status='failed';validation_error='invalid_or_missing_result'
    if timed_out:status='failed'
    return {'name':name,'status':status,'command':command,'exit_code':code,'elapsed_seconds':time.monotonic()-started,
            'timeout':timed_out,'forced_stop':forced,'result_file':str(path.relative_to(output)) if path.exists() else None,
            'result_sha256':hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None,
            'result_files':retained,'result_contract':name,'validation_error':validation_error,
            'counts':{k:result[k] for k in ('tests_run','passed','failures','errors','skipped','summary') if k in result}}


def retained_results_match(steps, output):
    for step in steps:
        entries=step.get('result_files') or [{'path':step.get('result_file'),'sha256':step.get('result_sha256')}]
        for entry in entries:
            if not entry['path']:return False
            path=output/entry['path']
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=entry['sha256']:return False
    return True


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--require-full',action='store_true')
    p.add_argument('--native-runtime',type=Path);p.add_argument('--real-model',action='store_true')
    p.add_argument('--model-evidence',type=Path);p.add_argument('--local-model-file',type=Path)
    p.add_argument('--model-python',type=Path);p.add_argument('--benchmark',action='store_true')
    p.add_argument('--reliability',action='store_true',help='Run isolated Kafka/DB/worker recovery scenarios')
    p.add_argument('--faults',action='store_true',help='Run all local fault scenarios; resource pressure remains opt-in')
    p.add_argument('--allow-resource-pressure',action='store_true',help='Allow bounded test subprocess CPU/memory scenarios')
    p.add_argument('--kafka-benchmark',action='store_true',help='Measure Kafka to Python; does not measure Flink')
    p.add_argument('--flink-benchmark',action='store_true',help='Measure the real Kafka/Flink pipeline at three rates')
    p.add_argument('--legacy-source',type=Path)
    args=p.parse_args()
    if (args.reliability or args.kafka_benchmark or args.flink_benchmark) and not args.native_runtime:
        p.error('--reliability/--kafka-benchmark/--flink-benchmark require --native-runtime')
    if args.allow_resource_pressure and not args.faults:p.error('--allow-resource-pressure requires --faults')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    result={'started_at_utc':datetime.now(timezone.utc).isoformat(),'environment':{'python':sys.version,'platform':platform.platform()},
            'source_sha256':source_manifest(),'source_manifest_scope':'repository source/configuration/cases/fixtures; excludes runtime data, credentials and external runtimes',
            'steps':[], 'gates':{},'status':'running'}
    def save():
        path=out/'result.json';tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(result,indent=2)+'\n');tmp.replace(path)
    # Runs do not use provider credentials unless explicitly requested in the dedicated step.
    env={**os.environ,'LLM_BASE_URL':'','LLM_MODEL':'','LLM_API_KEY':'','API_AUTH_MODE':'local',
        'API_READ_TOKEN':'','API_CONTROL_TOKEN':'','TELEMETRY_INGEST_TOKEN':'', 'DETECTION_MODE':'fixed',
        'DATA_DIR':str(out/'unused-data')}
    jobs=[('unit',['scripts.test_suite'],600),('runtime-http',['scripts.verify_runtime_http'],90),
          ('launcher',['scripts.verify_launcher'],120),
          ('evidence',['scripts.verify_evidence',*(['--legacy-source',str(args.legacy_source.resolve())] if args.legacy_source else [])],180),('detection',['scripts.evaluate_detection'],60),('rca',['scripts.evaluate_rca'],60)]
    code=1;save()
    try:
        for name,parts,timeout in jobs:
            result['steps'].append(run_step(name,[sys.executable,'-m',*parts,'--output',str(out/name)],out,env,timeout));save()
        local_states={s['status'] for s in result['steps']}
        result['gates']['local']='failed' if 'failed' in local_states else 'blocked' if 'blocked' in local_states else 'passed'
        for gate in ('native','model','reliability','faults','http_benchmark','kafka_benchmark','flink_benchmark'):
            result['gates'][gate]='blocked_not_requested'
        result['gates']['docker']='blocked_not_executed'
        def execute(name,command,timeout,gate=None,step_env=env):
            step=run_step(name,command,out,step_env,timeout)
            result['steps'].append(step);result['gates'][gate or name]=step['status'];save()
        if args.benchmark:
            execute('benchmark-http',[sys.executable,'-m','scripts.benchmark_platform','--component','http',
                '--business-rates','2,5','--ingest-rates','20,60','--duration','2','--repeats','1','--output',str(out/'benchmark-http')],180,'http_benchmark')
        meta=None
        if args.native_runtime:
            meta=json.loads((args.native_runtime/'runtime.json').read_text())
            execute('native',[meta['python'],'-m','scripts.native_streaming','--runtime-dir',str(args.native_runtime.resolve()),'--output',str(out/'native')],600)
        if args.reliability:
            execute('reliability',[meta['python'],'-m','scripts.verify_reliability','--kafka-home',meta['kafka_home'],'--output',str(out/'reliability')],600)
        if args.faults:
            command=[meta['python'] if meta else sys.executable,'-m','scripts.verify_faults','--scenario','all','--output',str(out/'faults')]
            if meta:command+=['--kafka-home',meta['kafka_home']]
            if args.allow_resource_pressure:command+=['--allow-resource-pressure']
            execute('faults',command,300)
        if args.kafka_benchmark:
            execute('benchmark-kafka',[meta['python'],'-m','scripts.benchmark_platform','--component','kafka',
                '--kafka-home',meta['kafka_home'],'--kafka-rates','50,150,300','--duration','3','--repeats','1','--output',str(out/'benchmark-kafka')],300,'kafka_benchmark')
        if args.flink_benchmark:
            execute('flink-benchmark',[meta['python'],'-m','scripts.benchmark_flink','--runtime-dir',str(args.native_runtime.resolve()),
                '--rates','20,60,120','--duration','3','--repeats','1','--output',str(out/'flink-benchmark')],360,'flink_benchmark')
        if args.real_model:
            execute('model',model_command(args,out/'model'),900,step_env=os.environ.copy())
        result['full_acceptance']=all(v=='passed' for v in result['gates'].values())
        result['status']='failed' if 'failed' in result['gates'].values() else 'passed' if result['full_acceptance'] else 'partial'
        code=1 if result['status']=='failed' else 2 if args.require_full and not result['full_acceptance'] else 0
    except BaseException as exc:
        result.update(status='failed',error_type=type(exc).__name__,full_acceptance=False);code=1
    finally:
        result['result_files_unchanged']=retained_results_match(result['steps'],out)
        if not result['result_files_unchanged']:result.update(status='failed',full_acceptance=False);code=1
        result['source_changed_during_run']=source_manifest()!=result['source_sha256']
        if result['source_changed_during_run']:result.update(status='failed',full_acceptance=False);code=1
        result.update(exit_code=code,ended_at_utc=datetime.now(timezone.utc).isoformat())
        save()
        with (out/'completed.json').open('x') as terminal:
            json.dump(result,terminal,indent=2);terminal.flush();os.fsync(terminal.fileno())
    print(json.dumps({'status':result['status'],'gates':result['gates'],'exit_code':code},indent=2));return code

if __name__=='__main__':sys.exit(main())
