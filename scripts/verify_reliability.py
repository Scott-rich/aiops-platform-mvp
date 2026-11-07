"""Phase 7 acceptance: owned native Kafka, real worker/API, bounded outages.

No external services are targeted. Generated input is synthetic telemetry transported
through real Kafka. The LLM endpoint is an explicitly labelled timeout stub.
Every output directory is new; failures and cleanup are saved in result.json.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import platform
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from unittest.mock import patch

import httpx
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.structs import OffsetAndMetadata

from app.contracts import canonical, digest, now_ms
from app.engine import Pipeline
from app.model_budget import ModelBudget
from app.reporting import ReportManager
from app.storage import Store
from app.worker import TOPICS
from benchmarks.runtime import KafkaBroker
from scripts.local_platform import simulator_environment
from scripts.verify_faults import free_port

ROOT = Path(__file__).resolve().parents[1]


def utc():
    return datetime.now(timezone.utc).isoformat()


def metric(identity, at=None, status=200, service='payment'):
    return {'schema_version': 1, 'event_id': identity, 'event_time': now_ms() if at is None else at,
            'event_type': 'request', 'service': service, 'instance_id': service+'-1',
            'trace_id': '', 'status_code': status, 'latency_ms': 10}


def until(label, predicate, timeout=30):
    end = time.monotonic()+timeout
    last = None
    while time.monotonic() < end:
        try:
            value = predicate()
            if value: return value
        except (OSError, sqlite3.Error, httpx.HTTPError) as exc:
            last = type(exc).__name__
        time.sleep(.1)
    raise TimeoutError(f'{label} exceeded {timeout}s; last_error={last}')


def stop(process):
    if process and process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try: process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=3)


class RestartableBroker(KafkaBroker):
    def restart(self):
        # Reuse the same formatted log directory; never reformat existing data.
        environment = simulator_environment(os.environ)
        environment.update(KAFKA_HEAP_OPTS='-Xms128m -Xmx384m', LOG_DIR=str(self.output/'logs'))
        self.log = (self.output/'broker.log').open('a')
        self.process = subprocess.Popen([str(self.home/'bin/kafka-server-start.sh'),
            str(self.output/'kafka.properties')], env=environment, stdout=self.log,
            stderr=subprocess.STDOUT, start_new_session=True)
        def ready():
            if self.process.poll() is not None: raise RuntimeError('broker exited during restart')
            try:
                self.admin = KafkaAdminClient(bootstrap_servers=self.bootstrap,
                    request_timeout_ms=2000, api_version_auto_timeout_ms=500)
                return True
            except Exception: return False
        until('broker restart', ready, 30)


class Acceptance:
    def __init__(self, output, kafka_home):
        self.output = output.resolve(); self.output.mkdir(parents=True, exist_ok=False)
        (self.output/'logs').mkdir()
        self.result = {'schema_version': 1, 'execution_kind': 'real_native_kafka_worker_http_sqlite',
            'started_at_utc': utc(), 'status': 'running', 'scenarios': [],
            'environment': {'python': sys.version, 'platform': platform.platform(),
                            'cpu_count_visible': os.cpu_count()},
            'configuration': {'broker_count': 1, 'partitions': 1, 'storage_attempts': 3,
                'database_timeout_seconds': .15, 'broker_probe_timeout_seconds': 15,
                'telemetry': 'generated test events; real Kafka transport',
                'llm': 'controlled loopback timeout stub; no model inference'},
            'limitations': ['Single broker/local SQLite; no high availability claim.',
                'Unacknowledged producer messages have no disk outbox and may be lost.',
                'Database outage is an access lock, not power loss or damaged hardware.',
                'Process exit tests do not establish filesystem/power-loss durability.',
                'Flink checkpoint recovery is executed separately by native_streaming.']}
        files = ['app/storage.py', 'app/engine.py', 'app/worker.py', 'app/incidents.py',
                 'app/local_api.py', 'scripts/verify_reliability.py']
        self.result['source_sha256'] = {f: hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in files}
        self.broker = RestartableBroker(kafka_home, self.output/'broker')
        self.children = []; self.handles = []; self.worker = self.api = None
        self.producer = self.observer = None
        self.store = Store(self.output/'data')
        self.group = 'phase7-'+uuid.uuid4().hex
        self.tp = TopicPartition('telemetry.metrics', 0)
        self.port = free_port(); self.api_url = f'http://127.0.0.1:{self.port}'
        self.client = httpx.Client(trust_env=False, timeout=3)
        self.current = None; self.save()

    def save(self):
        tmp = self.output/'result.tmp'
        tmp.write_text(json.dumps(self.result, ensure_ascii=False, indent=2)+'\n')
        tmp.replace(self.output/'result.json')

    @contextmanager
    def scenario(self, name):
        self.current = {'name': name, 'started_at_utc': utc(), 'status': 'running', 'checks': []}
        self.result['scenarios'].append(self.current); started = time.monotonic()
        try:
            yield self.current
            self.current['status'] = 'passed'
        except BaseException as exc:
            self.current.update(status='failed', error={'type': type(exc).__name__, 'message': str(exc)})
            raise
        finally:
            self.current.update(ended_at_utc=utc(), duration_seconds=round(time.monotonic()-started, 6))
            print(json.dumps(self.current, ensure_ascii=False), flush=True); self.save()

    def check(self, name, condition, **details):
        self.current['checks'].append({'name': name, 'passed': bool(condition), **details})
        self.save()
        if not condition: raise AssertionError(name+': '+canonical(details))

    def spawn(self, name, command):
        env = simulator_environment(os.environ)
        env.update(DATA_DIR=str(self.store.root), KAFKA_BOOTSTRAP_SERVERS=self.broker.bootstrap,
            WORKER_GROUP_ID=self.group, WORKER_CONNECT_ATTEMPTS='3', WORKER_POLL_FAILURES='2',
            WORKER_STORAGE_ATTEMPTS='3', DB_TIMEOUT_SECONDS='.15', DETECTION_MODE='fixed',
            EXECUTION_MODE='kafka-worker', REPORT_ENRICH_MIN_MS='0', REPORT_ENRICH_DEADLINE_MS='0',
            LLM_BASE_URL='', LLM_MODEL='', LLM_API_KEY='')
        log = (self.output/'logs'/f'{name}.log').open('a'); self.handles.append(log)
        child = subprocess.Popen([sys.executable, *command], cwd=ROOT, env=env,
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.children.append(child)
        return child

    def start_worker(self):
        started_at = now_ms()
        self.worker = self.spawn('worker', ['-m', 'app.worker'])
        until('worker ready', lambda: self.worker.poll() is None and
              self.health().get('status') == 'running' and
              self.health().get('updated_at', 0) >= started_at, 35)

    def start_api(self):
        self.api = self.spawn('api', ['-m', 'uvicorn', 'app.api:app', '--no-proxy-headers', '--host', '127.0.0.1',
                                     '--port', str(self.port), '--no-access-log'])
        until('api ready', lambda: self.client.get(self.api_url+'/health').status_code == 200)

    def health(self):
        path = self.store.root/'worker-health.json'
        try: return json.loads(path.read_text())
        except (OSError, ValueError): return {}

    def read_json(self, path, params=None):
        # The deliberately short DB timeout may return 503 during concurrent
        # writes. Record it, retry at most twice, and never parse it as an incident.
        for attempt in range(3):
            response = self.client.get(self.api_url+path, params=params)
            if response.status_code == 503:
                self.current.setdefault('transient_http_503', []).append(
                    {'path': path, 'attempt': attempt+1, 'observed_at_utc': utc()})
                self.save()
                if attempt < 2:
                    time.sleep(.1*2**attempt)
                    continue
            response.raise_for_status()
            return response.json()

    def producer_client(self):
        return KafkaProducer(bootstrap_servers=self.broker.bootstrap, acks='all', retries=0,
            max_block_ms=2000, request_timeout_ms=2000, delivery_timeout_ms=3000, api_version_auto_timeout_ms=1000)

    def send(self, event):
        raw = event if isinstance(event, bytes) else canonical(event).encode()
        return self.producer.send(self.tp.topic, value=raw, partition=0).get(timeout=5)

    def committed(self): return self.observer.committed(self.tp, timeout_ms=5000) or 0

    def drained(self):
        end = self.observer.end_offsets([self.tp])[self.tp]
        until('consumer catch-up', lambda: self.committed() >= end, 40)
        return end

    def run(self):
        self.broker.start()
        self.broker.admin.create_topics([NewTopic(t, 1, 1) for t in TOPICS])
        self.producer = self.producer_client()
        self.observer = KafkaConsumer(bootstrap_servers=self.broker.bootstrap, group_id=self.group,
            enable_auto_commit=False, request_timeout_ms=15000, session_timeout_ms=10000,
            api_version_auto_timeout_ms=2000)
        self.observer.assign([self.tp])
        self.start_worker(); self.start_api()
        with self.scenario('worker_restart_partial_burst'):
            base = now_ms()
            entries = [metric(f'fault-{i}', base+i*50, 503) for i in range(5)]
            for event in entries[:4]: self.send(event)
            self.drained(); before = self.store.stats()
            self.check('four_errors_no_alert', before['events'] == 4 and before['alerts'] == 0, stats=before)
            stop(self.worker); start = time.monotonic(); self.start_worker()
            self.send(entries[4]); self.drained()
            until('fifth error alert', lambda: self.store.stats()['alerts'] == 1)
            self.current['restart_to_detection_seconds'] = time.monotonic()-start
            incident = Pipeline(self.store)
            try:
                self.iid = incident.incidents.list()[0]['incident_id']
            finally: incident.close()
            self.check('state_restored_fifth_error_fired_once', self.store.stats()['alerts'] == 1)

        with self.scenario('backlog_duplicate_malformed_and_offset_replay'):
            stop(self.worker)
            for i in range(150): self.send(metric(f'backlog-{i}', service='backlog'))
            for event in entries: self.send(event)
            self.send(b'{invalid json')
            end = self.observer.end_offsets([self.tp])[self.tp]
            lag = end-self.committed(); self.check('backlog_observed', lag == 156, lag=lag)
            start = time.monotonic(); self.start_worker(); self.drained()
            self.current['catch_up_seconds'] = time.monotonic()-start
            before = self.store.stats()
            self.check('duplicates_deduplicated_and_malformed_quarantined',
                       before['events'] == 155 and before['alerts'] == 1 and before['deadletters'] == 1, stats=before)
            stop(self.worker)
            self.observer.commit({self.tp: OffsetAndMetadata(0, '', -1)}, timeout_ms=5000)
            self.start_worker(); self.drained()
            after = self.store.stats()
            keys = ('events', 'alerts', 'incidents', 'deadletters', 'incident_notifications')
            self.check('full_log_replay_is_idempotent', all(before[k] == after[k] for k in keys),
                       before={k: before[k] for k in keys}, after={k: after[k] for k in keys})

        with self.scenario('database_access_outage_and_http_503'):
            committed = self.committed()
            start = time.monotonic()
            with self.store.access_lock():
                self.send(metric('during-db-outage', service='database-probe'))
                response = self.client.get(self.api_url+'/health')
                self.check('api_returns_retryable_503', response.status_code == 503, status=response.status_code)
                until('worker exits after bounded storage retries', lambda: self.worker.poll() is not None, 10)
                health = self.health()
                self.check('failed_poll_did_not_advance_offsets', self.committed() == committed,
                           committed_before=committed, committed_during=self.committed(), health=health)
            self.current['outage_seconds'] = time.monotonic()-start
            start = time.monotonic(); self.start_worker(); self.drained()
            self.current['release_to_catch_up_seconds'] = time.monotonic()-start
            self.check('database_recovers_and_event_survives', self.store.stats()['events'] == 156)
            self.check('http_health_recovers', self.client.get(self.api_url+'/health').status_code == 200)

        with self.scenario('broker_outage_reconnect_and_acknowledged_data'):
            start = time.monotonic(); self.broker.close()
            try:
                self.send(metric('unacknowledged-outage-message'))
            except Exception as exc:
                failed = type(exc).__name__
            else: failed = None
            self.check('producer_failure_is_explicit', failed is not None, error_type=failed)
            until('worker notices broker failure', lambda: self.health().get('status') in {'degraded', 'failed'}, 40)
            self.current['outage_to_detection_seconds'] = time.monotonic()-start
            # This scenario measures native worker failure/reconnect. Only this
            # test-owned supervisor restarts it if its finite budget was exhausted.
            self.producer.close(timeout=2); self.producer = None
            self.observer.close(); self.observer = None
            restored = time.monotonic(); self.broker.restart()
            self.producer = self.producer_client()
            self.observer = KafkaConsumer(bootstrap_servers=self.broker.bootstrap, group_id=self.group,
                enable_auto_commit=False, request_timeout_ms=15000, session_timeout_ms=10000)
            self.observer.assign([self.tp])
            until('worker reconnect or exit', lambda: self.worker.poll() is not None or self.health().get('status') == 'running', 40)
            restarts = [0]
            if self.worker.poll() is not None:
                self.start_worker(); restarts[0] += 1
            self.send(metric('after-broker-recovery', service='broker-probe'))
            end = self.observer.end_offsets([self.tp])[self.tp]
            def recovered():
                # Rejoining can invalidate a commit after the first health probe.
                # Mirror restart-on-failure supervision until offsets catch up,
                # with a strict two-restart budget for this owned test worker.
                if self.worker.poll() is not None:
                    if restarts[0] >= 2: raise RuntimeError('supervisor restart budget exhausted')
                    self.start_worker(); restarts[0] += 1
                return self.committed() >= end
            until('supervised broker recovery catch-up', recovered, 40)
            self.current.update(recovery_seconds=time.monotonic()-restored,
                                supervisor_restarted_worker=bool(restarts[0]), worker_restarts=restarts[0])
            with self.store.db() as db:
                prior = db.execute("SELECT COUNT(*) FROM events WHERE event_id LIKE 'backlog-%'").fetchone()[0]
                unack = db.execute("SELECT COUNT(*) FROM events WHERE event_id='unacknowledged-outage-message'").fetchone()[0]
                self.check('confirmed_messages_preserved', prior == 150, count=prior)
                self.check('failed_message_not_claimed_delivered', unack == 0, count=unack)

        with self.scenario('platform_restart_history_and_report_hashes'):
            reports = until('report versions', lambda: self.read_json(f'/incidents/{self.iid}/reports'))
            rid = reports[0]['report_id']
            original = self.read_json('/report-versions/'+rid)
            stop(self.worker); stop(self.api)
            start = time.monotonic(); self.start_worker(); self.start_api()
            restored = self.read_json('/report-versions/'+rid)
            verified = self.read_json('/report-versions/'+rid+'/verify')
            self.current['restart_to_history_read_seconds'] = time.monotonic()-start
            self.check('immutable_report_and_incident_survive', original == restored and restored['incident_id'] == self.iid)
            self.check('snapshot_and_rca_hashes_valid', all(verified[k] for k in (
                'snapshot_sha256_valid', 'rca_sha256_valid', 'rca_evidence_references_valid', 'rca_evidence_set_valid')),
                verification=verified)
            (self.output/'historical-report.json').write_text(json.dumps(restored, ensure_ascii=False, indent=2))

        with self.scenario('real_time_recovery_confirmation'):
            # No clock override: continuously send actual wall-clock telemetry,
            # observe healthy sampling window plus the configured 10-second hold.
            began = time.monotonic(); observed = set(); resolved = False; count = 0
            while time.monotonic()-began < 35:
                for _ in range(2):
                    self.send(metric(f'recovery-{count}')); count += 1
                value = self.read_json('/incidents/'+self.iid)
                observed.add(value['status'])
                if value['status'] == 'resolved': resolved = True; break
                time.sleep(.4)
            self.current.update(confirmation_seconds=time.monotonic()-began, observed_states=sorted(observed), samples_sent=count)
            self.check('healthy_hold_observed_before_resolve', resolved and 'recovering' in observed)
            notes = self.read_json('/notifications', params={'incident_id': self.iid})
            self.check('one_local_recovery_notification', sum(n['kind'] == 'resolved' for n in notes) == 1)
            self.drained()

        self.crash_transactions()
        self.model_timeout()
        with self.scenario('final_database_integrity'):
            with self.store.db() as db:
                result = [r[0] for r in db.execute('PRAGMA integrity_check')]
            self.check('integrity_check_ok', result == ['ok'], result=result)

    def crash_transactions(self):
        with self.scenario('process_exit_transaction_boundaries'):
            data = self.output/'crash-data'
            # Both children deliberately skip graceful interpreter cleanup.
            code = '''import os,sys,json
from app.storage import Store
s=Store(sys.argv[1]); event=json.loads(sys.argv[2])
if sys.argv[3]=='uncommitted':
    with s.atomic():
        s.ingest(event); os._exit(17)
s.ingest(event); os._exit(18)
'''
            for mode in ('uncommitted', 'committed'):
                done = subprocess.run([sys.executable, '-c', code, str(data), canonical(metric(mode)), mode],
                                      cwd=ROOT, capture_output=True, timeout=10)
                self.check(mode+'_exit_observed', done.returncode == (17 if mode == 'uncommitted' else 18), returncode=done.returncode)
            reopened = Store(data)
            with reopened.db() as db:
                ids = [r[0] for r in db.execute('SELECT event_id FROM events')]
                integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
            self.check('rollback_uncommitted_preserve_committed', ids == ['committed'] and integrity == 'ok', ids=ids, integrity=integrity)

    def model_timeout(self):
        with self.scenario('llm_timeout_stub_fallback_and_persistent_call_cap'):
            received = []
            class Slow(BaseHTTPRequestHandler):
                def do_POST(handler):
                    handler.rfile.read(min(int(handler.headers.get('Content-Length', '0')), 1_000_000))
                    received.append(time.monotonic())
                    time.sleep(.4)
                    try:
                        handler.send_response(503); handler.end_headers(); handler.wfile.write(b'controlled timeout stub')
                    except (BrokenPipeError, ConnectionResetError): pass
                def log_message(*_): pass
            server = ThreadingHTTPServer(('127.0.0.1', 0), Slow)
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            try:
                env = {'LLM_BASE_URL': f'http://127.0.0.1:{server.server_port}/v1', 'LLM_MODEL': 'timeout-stub',
                       'LLM_API_KEY': '', 'LLM_TIMEOUT_SECONDS': '.08', 'LLM_TOTAL_TIMEOUT_SECONDS': '.3',
                       'LLM_MAX_CALLS_PER_INCIDENT': '1', 'LLM_MAX_ATTEMPTS': '1', 'LLM_TRUST_ENV': 'false'}
                with patch.dict(os.environ, env):
                    store = Store(self.output/'model-timeout-data'); pipeline = Pipeline(store)
                    try:
                        base = now_ms()
                        incidents = pipeline.ingest_batch([(metric(f'model-{i}', base+i, 503), None) for i in range(5)])
                        incident = incidents[0]; iid = incident['incident_id']
                        pipeline.reports.ensure_initial(incident)
                        start = time.monotonic(); report = pipeline.reports.refresh(incident, force=True)
                        self.current['diagnosis_seconds'] = time.monotonic()-start
                        self.check('timeout_returns_evidence_based_rule_report',
                            report['audit']['analysis_mode'] == 'heuristic' and
                            report['audit']['model_failure_code'] in {'request_timeout', 'total_timeout'} and
                            len(report['snapshot']['records']) == 5,
                            failure_code=report['audit']['model_failure_code'], evidence_count=len(report['snapshot']['records']))
                    finally: pipeline.close()
                    reopened = Store(store.root)
                    ReportManager(reopened).refresh(incident, force=True)
                    audit = ModelBudget(reopened).audit('incident:'+iid)
                    self.check('restart_does_not_repeat_timed_out_call', len(received) == 1 and audit['calls_reserved'] == 1,
                               observed_http_requests=len(received), budget=audit)
                    self.check('fallback_snapshot_hash_valid', digest(report['snapshot']) == report['audit']['snapshot_sha256'])
                    (self.output/'timeout-fallback-report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
            finally:
                server.shutdown(); server.server_close(); thread.join(timeout=2)

    def close(self):
        errors = []
        for child in reversed(self.children):
            try: stop(child)
            except Exception as exc: errors.append('process:'+type(exc).__name__)
        for label, client in (('producer', self.producer), ('observer', self.observer), ('http', self.client)):
            if client:
                try:
                    if label == 'producer': client.close(timeout=2)
                    else: client.close()
                except Exception as exc: errors.append(label+':'+type(exc).__name__)
        try: self.broker.close()
        except Exception as exc: errors.append('broker:'+type(exc).__name__)
        for handle in self.handles: handle.close()
        self.result['cleanup'] = {'errors': errors, 'all_children_stopped': all(c.poll() is not None for c in self.children),
                                  **self.broker.cleanup}
        if errors or not self.result['cleanup']['all_children_stopped']: self.result['status'] = 'failed'
        self.result['ended_at_utc'] = utc(); self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka-home', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    run = Acceptance(args.output, args.kafka_home)
    try:
        run.run(); run.result['status'] = 'passed'
    except BaseException as exc:
        run.result.update(status='failed', error={'type': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()})
        traceback.print_exc()
    finally: run.close()
    print(str(run.output/'result.json'), flush=True)
    return 0 if run.result['status'] == 'passed' else 1


if __name__ == '__main__': raise SystemExit(main())
