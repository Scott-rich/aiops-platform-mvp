"""Real isolated SIGTERM cleanup and non-destructive benchmark CLI checks."""
import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();root=args.output.resolve();root.mkdir(parents=True,exist_ok=False)
    checks=[];result={'status':'running','checks':checks};code=1
    def check(name,value):
        checks.append({'name':name,'passed':bool(value)})
        if not value:raise AssertionError(name)
    base=[sys.executable,'-m','scripts.benchmark_platform']
    try:
        with (root/'invalid.log').open('w') as log:
            r=subprocess.run(base+['--output',str(root/'invalid'),'--duration','nan'],stdout=log,stderr=subprocess.STDOUT,timeout=10)
        check('non_finite_duration_rejected_before_creation',r.returncode==2 and not (root/'invalid').exists())
        (root/'existing').mkdir();(root/'existing/sentinel').write_text('keep')
        with (root/'existing.log').open('w') as log:
            r=subprocess.run(base+['--output',str(root/'existing')],stdout=log,stderr=subprocess.STDOUT,timeout=10)
        check('existing_output_preserved',r.returncode==2 and (root/'existing/sentinel').read_text()=='keep')
        with (root/'interrupt.log').open('w') as log:
            p=subprocess.Popen(base+['--component','http','--output',str(root/'interrupted'),'--duration','20','--business-rates','2','--ingest-rates','2','--repeats','1'],stdout=log,stderr=subprocess.STDOUT)
            try:
                deadline=time.monotonic()+20;ready=False
                while time.monotonic()<deadline:
                    path=root/'interrupted/http/runtime/logs/gateway.log'
                    if path.exists() and 'Uvicorn running on' in path.read_text():ready=True;break
                    if p.poll() is not None:break
                    time.sleep(.1)
                check('real_http_services_started_before_interrupt',ready)
                time.sleep(.2);p.send_signal(signal.SIGTERM);exit_code=p.wait(timeout=20)
            finally:
                if p.poll() is None:
                    p.terminate()
                    try:p.wait(timeout=20)
                    except subprocess.TimeoutExpired:p.kill();p.wait(timeout=3)
        detail=json.loads((root/'interrupted/result.json').read_text())
        check('sigterm_recorded_as_interrupted',exit_code==130 and detail['status']=='interrupted')
        cleanup=detail['components']['http']['cleanup']
        check('owned_http_processes_stopped',cleanup['all_processes_stopped'] and not cleanup['errors'])
        check('proxy_connections_reaped',cleanup['proxy_tasks_remaining']==0)
        result['status']='passed';code=0
    except Exception as exc:result.update(status='failed',error_type=type(exc).__name__)
    finally:
        result.update(passed=sum(c['passed'] for c in checks),failed=sum(not c['passed'] for c in checks))
        (root/'checks.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
    return code


if __name__=='__main__':raise SystemExit(main())
