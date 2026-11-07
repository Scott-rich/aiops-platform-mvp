"""Isolated real HTTP acceptance for auth, bounded ingestion, readiness and shutdown."""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
import httpx
from app.contracts import now_ms


def graceful_exit(code, content):
    # Uvicorn 0.34 re-raises the captured signal after its completed lifespan.
    messages=[]
    for line in content.splitlines():
        try:
            record=json.loads(line)
            messages.append((record.get('logger'),record.get('message')))
        except (ValueError,AttributeError):pass
    return (code in (0,-signal.SIGTERM)
            and ('app.local_api','local_pipeline_stopped') in messages
            and ('uvicorn.error','Application shutdown complete.') in messages)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    result={'status':'running','execution_kind':'real_http_sqlite_with_synthetic_event','checks':[]}
    def check(name,ok):
        result['checks'].append({'name':name,'passed':bool(ok)})
        if not ok:raise AssertionError(name)
    with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
    tokens={k:secrets.token_urlsafe(32) for k in ('API_READ_TOKEN','API_CONTROL_TOKEN','TELEMETRY_INGEST_TOKEN')}
    env={**os.environ,**tokens,'API_AUTH_MODE':'token','DATA_DIR':str(out/'data'),'EXECUTION_MODE':'local-http-python-rules',
         'LLM_BASE_URL':'','LLM_MODEL':'','LLM_API_KEY':'','DETECTION_MODE':'fixed','LOG_FORMAT':'json','LOG_LEVEL':'INFO'}
    headers=lambda key:{'Authorization':'Bearer '+tokens[key]}
    log=(out/'api.log').open('w');child=None;started=time.monotonic()
    try:
        child=subprocess.Popen([sys.executable,'-m','uvicorn','app.local_api:app', '--no-proxy-headers','--host','127.0.0.1','--port',str(port),
                                '--no-access-log'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        with httpx.Client(base_url=f'http://127.0.0.1:{port}',timeout=5,trust_env=False) as client:
            deadline=time.monotonic()+20
            while True:
                try:
                    ready=client.get('/ready',headers=headers('API_READ_TOKEN'))
                    if ready.status_code==200:break
                except httpx.TransportError:pass
                if time.monotonic()>deadline or child.poll() is not None:raise TimeoutError('startup')
                time.sleep(.1)
            check('live_public',client.get('/live').status_code==200)
            check('anonymous_reports_denied',client.get('/reports').status_code==401)
            check('reader_reports_allowed',client.get('/reports',headers=headers('API_READ_TOKEN')).status_code==200)
            check('reader_fault_denied',client.post('/faults/payment',json={'kind':'none'},headers=headers('API_READ_TOKEN')).status_code==403)
            invalid=client.post('/faults/payment',json={'kind':'untrusted-value'},headers=headers('API_CONTROL_TOKEN'))
            check('control_reaches_validation_without_echo',invalid.status_code==422 and 'untrusted-value' not in invalid.text)
            check('request_id_propagated',invalid.headers.get('x-request-id')==invalid.json().get('request_id'))
            check('cross_origin_write_denied',client.post('/faults/payment',json={'kind':'none'},headers={**headers('API_CONTROL_TOKEN'),'Origin':'http://untrusted.test'}).status_code==403)
            check('ingest_token_cannot_read_reports',client.get('/reports',headers=headers('TELEMETRY_INGEST_TOKEN')).status_code==403)
            event={'schema_version':1,'event_id':'phase10-http-event','event_time':now_ms(),'event_type':'request',
                   'service':'phase10','instance_id':'local','trace_id':'','status_code':200,'latency_ms':10}
            check('authenticated_event_ingestion',client.post('/demo/ingest',json={'topic':'telemetry.metrics','event':event},headers=headers('TELEMETRY_INGEST_TOKEN')).status_code==204)
            check('malformed_json_rejected',client.post('/demo/ingest',content=b'{broken',headers=headers('TELEMETRY_INGEST_TOKEN')).status_code==400)
            check('oversize_body_rejected',client.post('/demo/ingest',content=b'x'*1000001,headers=headers('TELEMETRY_INGEST_TOKEN')).status_code==413)
            check('local_pipeline_ready',client.get('/ready',headers=headers('API_READ_TOKEN')).json()['local_pipeline']['ready'])
        result['status']='passed'
    except Exception as exc:
        result.update(status='failed',error_type=type(exc).__name__)
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=40)
            except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();result['status']='failed'
        log.close()
        content=(out/'api.log').read_text()
        result['exit_code']=child.returncode if child else None
        stopped=graceful_exit(result['exit_code'],content)
        result['checks'].append({'name':'graceful_shutdown','passed':stopped})
        clean=all(v not in content for v in tokens.values())
        result['checks'].append({'name':'generated_tokens_not_logged','passed':clean})
        if not stopped or not clean:result['status']='failed'
        result['elapsed_seconds']=time.monotonic()-started
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2));return 0 if result['status']=='passed' else 1

if __name__=='__main__':sys.exit(main())
