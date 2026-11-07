"""Real HTTP/SQLite phase8 acceptance with labelled synthetic telemetry.

No external LLM, Kafka, or Docker is claimed by this test. Use a NEW output
directory. All archives, migrations and deletions use invocation-owned copies.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import sqlite3
import subprocess
import sys
import time
import traceback
import tracemalloc
from unittest.mock import patch

import httpx
from app.contracts import canonical, digest, now_ms
from app.storage import Store
from app.reporting import ReportManager
from app.evidence_archive import export_set, verify_archive, restore_events, prune_events
from scripts.demo import fixture
from scripts.verify_storage_upgrade import inventory, digest_file
from flink.rules import evaluate_window


def verify(output, count=23005, legacy=None):
    output = output.resolve(); output.mkdir(parents=True, exist_ok=False)
    result = {'status': 'running', 'execution_kind': 'real_http_sqlite_with_synthetic_telemetry',
              'started_at_utc': datetime.now(timezone.utc).isoformat(), 'environment': {'python': sys.version, 'platform': platform.platform(), 'sqlite': sqlite3.sqlite_version},
              'configuration': {'matching_events': count, 'unrelated_events': 20100, 'page_size': 200, 'llm': 'disabled'}, 'checks': []}
    started = time.monotonic(); process = None; log = None
    def save():
        temporary=output/'result.tmp'
        with temporary.open('w') as stream:
            stream.write(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
            stream.flush();os.fsync(stream.fileno())
        temporary.replace(output/'result.json')
    def check(name, condition, **details):
        result['checks'].append({'name': name, 'passed': bool(condition), **details}); save()
        if not condition: raise AssertionError(name)
    env = {**os.environ, 'DATA_DIR': str(output/'data'), 'LLM_BASE_URL': '', 'LLM_MODEL': '', 'EVIDENCE_RETENTION_DAYS': '30', 'EVIDENCE_OPERATION_TIMEOUT_SECONDS': '30'}
    try:
        with patch.dict(os.environ, env):
            store = Store(); events, start, end = fixture(now_ms()-40*86400000)
            if count < len(events): raise ValueError('count must be at least fixture length')
            store.ingest_many([(e, {'transport':'synthetic-acceptance'}) for e in events])
            for base in range(0, count-len(events), 500):
                store.ingest_many([(dict(events[0], event_id=f'bulk-{n:08}', trace_id='bulk-trace', event_time=start+1000), {'transport':'synthetic-acceptance'}) for n in range(base, min(base+500, count-len(events)))])
            for base in range(0, 20100, 500):
                store.ingest_many([(dict(events[0], event_id=f'unrelated-{n:08}', service='unrelated', trace_id='unrelated', event_time=start-4000), {'transport':'synthetic-acceptance'}) for n in range(base, min(base+500, 20100))])
            # The seeded test dataset represents old receipts, enabling real
            # retention without exposing a bypass of the production age guard.
            with store.db() as db: db.execute('UPDATE events SET ingested_at=event_time')
            alert = next(a for a in evaluate_window(events, 'gateway', start, end) if a['rule_id']=='high_error_rate')
            store.alert(alert)
            incident = {'incident_id':'evidence-acceptance', 'status':'open', 'services':['gateway','order','payment'], 'candidate_service':'payment',
                        'first_alert_id':alert['alert_id'], 'alert_ids':[alert['alert_id']], 'first_event_time':start, 'last_event_time':end, 'latest_alert_end':end}
            manager = ReportManager(store)
            tracemalloc.start(); capture_started = time.monotonic()
            first = manager.ensure_initial(incident, end+5000)
            peak = tracemalloc.get_traced_memory()[1]; tracemalloc.stop()
            result['capture_measurement'] = {'wall_seconds':time.monotonic()-capture_started, 'python_tracemalloc_peak_bytes':peak,
                                             'scope':'full initial report generation; excludes seed ingest; includes tracing overhead; not a capacity benchmark'}
            (output/'first-report.json').write_text(json.dumps(first, ensure_ascii=False, indent=2))
            check('full_set_exceeds_old_limit_and_analysis_is_explicitly_sampled', first['audit']['full_evidence_count']==count and len(first['snapshot']['records'])==1800 and first['audit']['evidence_completeness']=='analysis_sampled')
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0)); port = sock.getsockname()[1]
            log = (output/'api.log').open('w')
            process = subprocess.Popen([sys.executable,'-m','uvicorn','app.api:app', '--no-proxy-headers','--host','127.0.0.1','--port',str(port)], env=env, stdout=log, stderr=subprocess.STDOUT)
            base_url = 'http://127.0.0.1:'+str(port)
            with httpx.Client(base_url=base_url, trust_env=False, timeout=40) as client:
                until = time.monotonic()+15
                while True:
                    try:
                        if client.get('/health').status_code == 200: break
                    except httpx.TransportError: pass
                    if time.monotonic()>until or process.poll() is not None: raise TimeoutError('HTTP API startup failed')
                    time.sleep(.1)
                sid = first['audit']['full_evidence_set_id']; url = '/evidence-sets/'+sid+'/records'
                stream = hashlib.sha256(); seen = set(); cursor = None; pages = 0; fetched = 0
                reading_started = time.monotonic()
                while True:
                    params = {'page_size':200}
                    if cursor: params['cursor']=cursor
                    response = client.get(url, params=params); response.raise_for_status(); page=response.json(); pages+=1
                    for row in page['records']:
                        original = {k:row[k] for k in ('event','source','ingested_at')}
                        sha = digest(original)
                        if sha!=row['record_sha256'] or row['event']['event_id'] in seen: raise AssertionError('invalid page record')
                        seen.add(row['event']['event_id']); stream.update(bytes.fromhex(sha)); fetched+=1
                    cursor = page['next_cursor']
                    if cursor is None: break
                check('http_full_pagination_and_client_hash', fetched==count and stream.hexdigest()==first['snapshot']['full_evidence']['records_sha256'], pages=pages, rows=fetched, wall_seconds=time.monotonic()-reading_started)
                verification = client.get('/report-versions/'+first['report_id']+'/verify').json()
                (output/'verification.json').write_text(json.dumps(verification, indent=2))
                check('http_report_full_evidence_and_rca_integrity', all(verification.get(k) is True for k in ('snapshot_sha256_valid','full_evidence_valid','rca_sha256_valid','rca_evidence_references_valid')))
                event=events[0]
                filtered=client.get(url,params={'service':event['service'],'event_type':event['event_type'],'event_id':event['event_id'],'trace_id':event['trace_id'],'start':event['event_time'],'end':event['event_time']}).json()
                check('http_combined_filters', filtered['filtered_count']==1 and filtered['records'][0]['event']==event)
                check('http_invalid_cursor_rejected', client.get(url,params={'cursor':'!!!'}).status_code==422)
                second = manager.refresh(incident,end+8000)
                store.ingest(dict(events[0],event_id='late-evidence-real-http'))
                third = manager.refresh(incident,end+12000)
                check('late_event_creates_new_immutable_version', third['version']==3 and manager.get(first['report_id'])==first)
                diff=client.get('/report-versions/'+second['report_id']+'/compare/'+third['report_id']).json()
                (output/'version-comparison.json').write_text(json.dumps(diff,indent=2))
                check('http_version_comparison_counts_full_sets', diff['evidence_changes']['added']['count']==1 and diff['evidence_changes']['added']['event_ids']==['late-evidence-real-http'])
                history=[]; cursor=None
                while True:
                    params={'page_size':1}
                    if cursor: params['cursor']=cursor
                    page=client.get('/incidents/'+incident['incident_id']+'/report-history',params=params).json()
                    history.extend(page['reports']);cursor=page['next_cursor']
                    if cursor is None:break
                check('http_history_pagination', [r['version'] for r in history]==[1,2,3])
                page=client.get('/incidents/'+incident['incident_id']+'/evidence').json()
                check('incident_evidence_pins_latest_report',page['report_id']==third['report_id'])
                export = export_set(store, sid, output/'evidence.jsonl.gz')
                check('streaming_archive_verified', verify_archive(export['path'])['count']==count)
                restored=Store(output/'restored'); restore=restore_events(restored,export['path'])
                check('archive_roundtrip_preserves_content_and_provenance',restore['inserted']==count and restored.evidence(alert)['full_evidence']['set_id']==sid)
                # Remove the now-reported test alert's retention protection via
                # its normal legacy report path, leaving frozen versions intact.
                from app.diagnosis import diagnose
                diagnose(store,alert['alert_id'])
                dry=prune_events(store,end+10000,output/'archives',size=200)
                check('retention_defaults_to_dry_run',not dry['applied'] and dry['deleted']==0)
                pruned=prune_events(store,end+10000,output/'archives',size=200,apply=True)
                (output/'retention.json').write_text(json.dumps(pruned,indent=2))
                check('bounded_cleanup_has_backup_and_archive',pruned['deleted']==200 and Path(pruned['backup']).is_file() and verify_archive(pruned['archive']['path'])['count']==200)
                check('reports_and_frozen_evidence_survive_cleanup',client.get('/report-versions/'+first['report_id']).json()==first and client.get('/evidence-sets/'+sid+'/verify').json()['valid'])
                check('cleanup_archive_restores_deleted_rows',restore_events(store,pruned['archive']['path'])['inserted']==200)
                # Real process restart; report equality and full-set hash remain.
                process.terminate();process.wait(timeout=10)
                process=subprocess.Popen([sys.executable,'-m','uvicorn','app.api:app', '--no-proxy-headers','--host','127.0.0.1','--port',str(port)],env=env,stdout=log,stderr=subprocess.STDOUT)
                until=time.monotonic()+15
                while True:
                    try:
                        response=client.get('/report-versions/'+first['report_id'])
                        if response.status_code==200:break
                    except httpx.TransportError:pass
                    if time.monotonic()>until:raise TimeoutError('API restart failed')
                    time.sleep(.1)
                check('historical_report_and_hash_survive_api_restart',response.json()==first and client.get('/evidence-sets/'+sid+'/verify').json()['valid'])
            if legacy:
                legacy=legacy.resolve();original=digest_file(legacy); target=output/'legacy-data';target.mkdir()
                with closing(sqlite3.connect(legacy.as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(target/'aiops.sqlite3')) as dest: source.backup(dest)
                before=inventory(target/'aiops.sqlite3'); upgraded=Store(target); after=inventory(upgraded.path)
                check('legacy_database_payloads_preserved',all(after['tables'].get(k)==v for k,v in before['tables'].items() if k!='platform_migrations'))
                check('legacy_database_source_bytes_unchanged',digest_file(legacy)==original)
                check('legacy_upgrade_integrity_and_backup',after['integrity']==['ok'] and bool(list((target/'backups').glob('before-phase8-*.sqlite3'))))
                result['legacy_upgrade']={'source_sha256':original,'before':before,'after':after}
            else:
                result['legacy_upgrade']={'status':'not_run','reason':'--legacy-source not supplied'}
            result['status']='passed'
    except Exception as exc:
        result.update(status='failed',error={'type':type(exc).__name__,'message':str(exc),'traceback':traceback.format_exc()})
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        if log:log.close()
        if tracemalloc.is_tracing():tracemalloc.stop()
        result.update(ended_at_utc=datetime.now(timezone.utc).isoformat(),wall_seconds=time.monotonic()-started,child_process_stopped=process is None or process.poll() is not None)
        save()
        # One immutable terminal record, distinct from frequently updated progress.
        with (output/'completed.json').open('x') as terminal:
            json.dump(result,terminal,ensure_ascii=False,indent=2)
            terminal.flush();os.fsync(terminal.fileno())
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--count',type=int,default=23005)
    parser.add_argument('--legacy-source',type=Path)
    args=parser.parse_args();result=verify(args.output,args.count,args.legacy_source)
    print(json.dumps({k:result[k] for k in ('status','checks')},ensure_ascii=False))
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
