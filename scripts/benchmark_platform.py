"""Bounded isolated phase-6 benchmark. Never targets external business services."""
import argparse
import asyncio
from datetime import datetime,timezone
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from benchmarks.http import run_http
from benchmarks.kafka import run_kafka
from benchmarks.model import run_model
from benchmarks.report import write_report
from benchmarks.resources import environment
from scripts.local_platform import simulator_environment

ROOT=Path(__file__).resolve().parents[1]
VERSION='phase6-v1'


def rates(value):
    try:parsed=[float(v) for v in value.split(',')]
    except ValueError:raise argparse.ArgumentTypeError('rates must be comma-separated numbers')
    if not parsed or any(not math.isfinite(v) or v<1 or v>1000 for v in parsed) or len(parsed)>5 or len(set(parsed))!=len(parsed):
        raise argparse.ArgumentTypeError('1..5 unique rates, each finite and in [1,1000]')
    return parsed


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True,help='New directory; never overwrite old results')
    p.add_argument('--component',choices=('all','http','kafka','evaluation','model'),default='all')
    p.add_argument('--business-rates',type=rates,default=rates('2,5,10'))
    p.add_argument('--ingest-rates',type=rates,default=rates('20,60,120'))
    p.add_argument('--kafka-rates',type=rates,default=rates('50,150,300'))
    p.add_argument('--duration',type=float,default=4,help='Seconds per rate tier, excluding startup/drain')
    p.add_argument('--repeats',type=int,default=2);p.add_argument('--concurrency',type=int,default=16)
    p.add_argument('--consumer-delay-ms',type=int,default=0,help='Bounded delay per Kafka poll batch for backlog experiments')
    p.add_argument('--kafka-home',type=Path);p.add_argument('--real-model',action='store_true')
    p.add_argument('--model-evidence',type=Path)
    return p


def validate(args):
    if not math.isfinite(args.duration) or not 1<=args.duration<=60:raise ValueError('duration must be finite in [1,60]')
    if not 1<=args.repeats<=5 or not 1<=args.concurrency<=64 or not 0<=args.consumer_delay_ms<=2000:
        raise ValueError('repeats [1,5], concurrency [1,64], consumer delay [0,2000] ms')
    all_rates=[]
    if args.component in ('all','http'):all_rates+=args.business_rates+args.ingest_rates
    if args.component in ('all','kafka'):all_rates+=args.kafka_rates
    if sum(math.floor(r*args.duration) for r in all_rates)*args.repeats>20000:
        raise ValueError('maximum 20000 offered inputs per run; use several bounded runs')
    if args.component in ('all','http') and sum(args.business_rates)*args.duration*args.repeats>3000:
        raise ValueError('maximum 3000 business requests per run')


def source_hash():
    files=[]
    for directory in ('app','flink','simulator','faultlab','benchmarks','scripts','tests'):
        files+=list((ROOT/directory).glob('**/*.py'))
    files += [ROOT/'wire_contract.py',ROOT/'requirements.txt']
    hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files)) if p.exists()}
    return hashlib.sha256(json.dumps(hashes,sort_keys=True).encode()).hexdigest(),hashes


def evaluate(output):
    output.mkdir(parents=True,exist_ok=False);results={}
    env={k:v for k,v in simulator_environment(os.environ).items() if not k.startswith('DETECTION_')}
    for name,module in [('detection','scripts.evaluate_detection'),('rca','scripts.evaluate_rca')]:
        with (output/(name+'.log')).open('w') as log:
            child=subprocess.run([sys.executable,'-m',module,'--output',str(output/name)],cwd=ROOT,env=env,
                                 stdout=log,stderr=subprocess.STDOUT,timeout=90)
        detail=json.loads((output/name/'result.json').read_text())
        results[name]={'status':'passed' if child.returncode==0 else 'failed','result_file':name+'/result.json',
                       'metrics':detail['metrics']}
    return {'status':'passed' if all(r['status']=='passed' for r in results.values()) else 'failed',
            'source_kind':'synthetic_labelled_replay','results':results}


def main(argv=None):
    p=parser();args=p.parse_args(argv)
    try:validate(args)
    except ValueError as exc:p.error(str(exc))
    output=args.output.resolve()
    try:output.mkdir(parents=True,exist_ok=False)
    except FileExistsError:p.error('output already exists; choose a new directory')
    started=time.monotonic();config={k:v for k,v in vars(args).items() if k not in ('output','kafka_home','model_evidence')}
    if args.component in ('all','kafka') and args.kafka_home:
        config['kafka_runtime']={'core_jars':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((args.kafka_home/'libs').glob('kafka_2.*.jar'))}}
        try:
            java=subprocess.run(['java','-version'],capture_output=True,text=True,timeout=3)
            config['kafka_runtime']['java_version']=(java.stderr or java.stdout).strip()
        except (OSError,subprocess.TimeoutExpired):config['kafka_runtime']['java_version']=None
    if args.model_evidence:config['model_evidence_sha256']=hashlib.sha256(args.model_evidence.read_bytes()).hexdigest()
    sha,files=source_hash()
    result={'benchmark_version':VERSION,'started_at_utc':datetime.now(timezone.utc).isoformat(),
        'configuration':config,'configuration_id':hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest(),
        'environment':environment(),'source_sha256':sha,'source_files':files,'components':{},'status':'running',
        'unmeasured':['Flink runtime throughput and recovery','Production capacity','Actual provider invoice cost']}
    code=1
    # SIGTERM uses the same finally cleanup paths as Ctrl-C, including owned broker groups.
    def interrupted(*_):raise KeyboardInterrupt()
    old=signal.signal(signal.SIGTERM,interrupted)
    try:
        components=result['components']
        if args.component in ('all','http'):
            components['http']=asyncio.run(run_http(output/'http',args.business_rates,args.ingest_rates,args.duration,args.repeats,args.concurrency))
        if args.component in ('all','kafka'):
            # Benchmark configuration is isolated from user detector/model environment.
            saved={k:v for k,v in os.environ.items() if k.startswith(('LLM_','DETECTION_'))}
            try:
                for key in saved:os.environ.pop(key,None)
                components['kafka']=run_kafka(output/'kafka',args.kafka_home,args.kafka_rates,args.duration,args.repeats,args.consumer_delay_ms)
            finally:os.environ.update(saved)
        if args.component in ('all','evaluation'):components['evaluation']=evaluate(output/'evaluation')
        if args.component in ('all','model'):components['model']=run_model(output/'model',args.real_model,args.model_evidence)
        states={v['status'] for v in components.values()}
        result['status']='failed' if 'failed' in states else 'partial' if states & {'blocked','not_requested'} else 'passed'
        code={'passed':0,'partial':2,'failed':1}[result['status']]
    except BaseException as exc:
        # Paths and exception classes only: avoid accidentally logging a credential-bearing URL.
        result.update(status='interrupted' if isinstance(exc,KeyboardInterrupt) else 'failed',error_type=type(exc).__name__)
        code=130 if isinstance(exc,KeyboardInterrupt) else 1
        for name in ('http','kafka','model'):
            path=output/name/'result.json'
            if name not in result['components'] and path.exists():result['components'][name]=json.loads(path.read_text())
    finally:
        signal.signal(signal.SIGTERM,old)
        result.update(elapsed_seconds=time.monotonic()-started,finished_at_utc=datetime.now(timezone.utc).isoformat(),exit_code=code)
        write_report(output,result)
        (output/'completed.json').write_text(json.dumps({'status':result['status'],'result_sha256':hashlib.sha256((output/'result.json').read_bytes()).hexdigest()})+'\n')
        print(json.dumps({'status':result['status'],'exit_code':code,'output':str(output)},ensure_ascii=False),flush=True)
    return code


if __name__=='__main__':raise SystemExit(main())
