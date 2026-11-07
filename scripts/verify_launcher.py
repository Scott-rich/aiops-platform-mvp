"""Real five-process local launcher acceptance with API and simulator tokens; no external model."""
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


def reserve_ports():
    for _ in range(100):
        socks=[]
        try:
            s=socket.socket();s.bind(('127.0.0.1',0));socks.append(s)
            base=s.getsockname()[1]
            if base>65531:continue
            for port in range(base+1,base+5):
                s=socket.socket();socks.append(s);s.bind(('127.0.0.1',port))
            return base
        except OSError:pass
        finally:
            for s in socks:s.close()
    raise RuntimeError('no port range')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    port=reserve_ports();api=f'http://127.0.0.1:{port}';gateway=f'http://127.0.0.1:{port+1}'
    tokens={key:secrets.token_urlsafe(32) for key in ('API_READ_TOKEN','API_CONTROL_TOKEN','TELEMETRY_INGEST_TOKEN','SIMULATOR_CONTROL_TOKEN')}
    env={**os.environ,**tokens,'API_AUTH_MODE':'token','LLM_BASE_URL':'','LLM_MODEL':'','LLM_API_KEY':'','DETECTION_MODE':'fixed'}
    read={'Authorization':'Bearer '+tokens['API_READ_TOKEN']};control={'Authorization':'Bearer '+tokens['API_CONTROL_TOKEN']}
    result={'status':'running','execution_kind':'real_local_http_simulator_processes','checks':[]};child=None
    def check(name,ok):
        result['checks'].append({'name':name,'passed':bool(ok)})
        if not ok:raise AssertionError(name)
    started=time.monotonic()
    with (out/'launcher.log').open('w') as log:
        try:
            child=subprocess.Popen([sys.executable,'-m','scripts.local_platform','--port',str(port),'--base-port',str(port+1),
                '--data-dir',str(out/'data'),'--no-load'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            with httpx.Client(timeout=5,trust_env=False) as client:
                until=time.monotonic()+25
                while True:
                    if child.poll() is not None:raise RuntimeError('launcher_exit')
                    try:
                        services=client.get(api+'/services',headers=read)
                        if services.status_code==200 and all(v.get('status')=='ok' for v in services.json()['services'].values()):break
                    except httpx.TransportError:pass
                    if time.monotonic()>until:raise TimeoutError('startup')
                    time.sleep(.1)
                check('four_services_healthy',len(services.json()['services'])==4)
                check('unauthorized_fault_denied',client.post(api+'/faults/payment',json={'kind':'error','duration_seconds':3}).status_code==401)
                check('control_forwards_simulator_token',client.post(api+'/faults/payment',json={'kind':'error','duration_seconds':10},headers=control).status_code==200)
                check('real_cascade_5xx',client.get(gateway+'/work').status_code>=500)
                check('fault_cleared',client.post(api+'/faults/payment',json={'kind':'none'},headers=control).status_code==200)
                check('requests_recover',client.get(gateway+'/work').status_code==200)
                until=time.monotonic()+10
                while True:
                    count=client.get(api+'/health',headers=read).json()['storage']['events']
                    if count>=8:break
                    if time.monotonic()>until:raise TimeoutError('authenticated_telemetry')
                    time.sleep(.1)
                check('simulator_events_authenticated_and_persisted',count>=8)
                result['persisted_events']=count
            result['status']='passed'
        except Exception as exc:result.update(status='failed',error_type=type(exc).__name__)
        finally:
            if child and child.poll() is None:
                # Signal launcher only; it must stop its own children in finally.
                child.send_signal(signal.SIGTERM)
                try:child.wait(timeout=45)
                except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();result['status']='failed'
    stopped=child is not None and child.returncode==0
    result['checks'].append({'name':'launcher_stops_successfully','passed':stopped})
    with httpx.Client(timeout=.3,trust_env=False) as client:
        closed=True
        for n in range(port,port+5):
            try:client.get(f'http://127.0.0.1:{n}/live');closed=False
            except httpx.TransportError:pass
    result['checks'].append({'name':'all_five_ports_closed','passed':closed})
    logs=''.join(p.read_text(errors='replace') for p in out.rglob('*.log'))
    clean=all(v not in logs for v in tokens.values())
    result['checks'].append({'name':'generated_tokens_not_logged','passed':clean})
    if not stopped or not closed or not clean:result['status']='failed'
    result['elapsed_seconds']=time.monotonic()-started
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
    return 0 if result['status']=='passed' else 1

if __name__=='__main__':sys.exit(main())
