"""Phase 7: transaction, durable detector, bounded retry and recovery invariants."""
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from app.contracts import canonical, now_ms
from app.engine import Pipeline
from app.incidents import IncidentManager
from app.storage import Store
from app.worker import check_health, persist_batch, run_consumer, storage_operation
from tests.test_worker import record


def request(i, *, service='payment', instance='pay-1', status=503, at=None):
    return {'schema_version': 1, 'event_id': str(i), 'event_time': now_ms() if at is None else at,
            'event_type': 'request', 'service': service, 'instance_id': instance,
            'trace_id': '', 'status_code': status, 'latency_ms': 12}


class DurableStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name)
        self.base = now_ms() - 5000
        config = patch.dict(os.environ, {'LLM_BASE_URL': '', 'LLM_MODEL': ''})
        config.start(); self.addCleanup(config.stop)

    def pipeline(self, store=None):
        pipeline = Pipeline(store or self.store)
        self.addCleanup(pipeline.close)
        return pipeline

    def entries(self, start, end, **kw):
        return [(request(i, at=self.base+i*10, **kw), None) for i in range(start, end)]

    def test_failure_after_incident_write_rolls_back_whole_poll(self):
        pipeline = self.pipeline()
        original = pipeline.incidents.ingest_alert
        def interrupted(*args):
            original(*args)
            raise sqlite3.OperationalError('database is locked')
        with patch.object(pipeline.incidents, 'ingest_alert', side_effect=interrupted):
            with self.assertRaises(sqlite3.OperationalError):
                pipeline.ingest_batch(self.entries(0, 5))
        stats = self.store.stats()
        self.assertEqual([stats[k] for k in ('events', 'alerts', 'incidents', 'incident_notifications')], [0]*4)
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pipeline_fast_state').fetchone()[0], 0)
        self.assertEqual(len(pipeline.ingest_batch(self.entries(0, 5))), 1)
        self.assertEqual(pipeline.ingest_batch(self.entries(0, 5)), [])
        self.assertEqual(self.store.stats()['alerts'], 1)

    def test_two_instances_share_partial_state_and_fired_flag(self):
        first, second = self.pipeline(), self.pipeline(Store(self.temp.name))
        self.assertEqual(first.ingest_batch(self.entries(0, 3)), [])
        self.assertEqual(len(second.ingest_batch(self.entries(3, 5))), 1)
        self.assertEqual(first.ingest_batch(self.entries(5, 8)), [])
        saved = self.store.list_alerts()[0]
        self.assertEqual(saved['evidence']['event_ids'], [str(i) for i in range(5)])
        self.assertEqual(self.store.stats()['alerts'], 1)

    def test_healthy_observation_resets_persisted_burst(self):
        self.pipeline().ingest_batch(self.entries(0, 5))
        second = self.pipeline(Store(self.temp.name))
        second.ingest_batch(self.entries(5, 6, status=200))
        self.assertEqual(len(second.ingest_batch(self.entries(6, 11))), 1)
        self.assertEqual(self.store.stats()['alerts'], 2)

    def test_late_event_after_reopen_does_not_regress_state(self):
        self.pipeline().ingest_batch(self.entries(0, 3))
        late = request('late', at=self.base-100, status=200)
        other = self.pipeline(Store(self.temp.name))
        other.ingest_batch([(late, None)])
        self.assertEqual(len(other.ingest_batch(self.entries(3, 5))), 1)

    def test_state_key_limit_is_bounded_and_logged(self):
        with patch.dict(os.environ, {'FAST_STATE_MAX_KEYS': '2'}):
            pipeline = self.pipeline()
        with self.assertLogs('app.engine', level='WARNING'):
            for i in range(3):
                pipeline.ingest_batch([(request(i, service=f's{i}'), None)])
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pipeline_fast_state').fetchone()[0], 2)

    def test_kafka_deadletter_replay_is_durable_and_new_offset_is_distinct(self):
        pipeline = self.pipeline()
        malformed = record(['bad'], offset=7)
        persist_batch(pipeline, {'p': [malformed]})
        persist_batch(self.pipeline(Store(self.temp.name)), {'p': [malformed]})
        self.assertEqual(self.store.stats()['deadletters'], 1)
        persist_batch(pipeline, {'p': [record(['bad'], offset=8)]})
        self.assertEqual(self.store.stats()['deadletters'], 2)

    def test_legacy_schema_gets_restorable_backup_before_additive_migration(self):
        self.store.ingest(request('before'))
        with self.store.db() as db:
            for table in ('pipeline_fast_state', 'platform_migrations', 'deadletter_keys'):
                db.execute('DROP TABLE '+table)
        reopened = Store(self.temp.name)
        with reopened.db() as db:
            backup = db.execute("SELECT backup_path FROM platform_migrations WHERE version='phase7-v1'").fetchone()[0]
        with sqlite3.connect(backup) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 1)
            self.assertEqual(db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
            names = {r[0] for r in db.execute('SELECT name FROM sqlite_master')}
            self.assertNotIn('pipeline_fast_state', names)
        self.assertEqual(reopened.stats()['events'], 1)
        self.assertEqual(len(list((Path(self.temp.name)/'backups').glob('before-phase7-*.sqlite3'))), 1)
        self.assertEqual(len(list((Path(self.temp.name)/'backups').glob('before-phase8-*.sqlite3'))), 1)
        Store(self.temp.name)
        self.assertEqual(len(list((Path(self.temp.name)/'backups').glob('*.sqlite3'))), 2)

    def test_nested_transaction_rollback(self):
        with self.assertRaises(RuntimeError):
            with self.store.atomic():
                self.store.ingest(request('one'))
                with self.store.atomic():
                    self.store.ingest(request('two'))
                raise RuntimeError('interrupt')
        self.assertEqual(self.store.stats()['events'], 0)

    def test_backup_publication_failure_aborts_migration(self):
        self.store.ingest(request('old-event'))
        with self.store.db() as db:
            for table in ('pipeline_fast_state', 'platform_migrations', 'deadletter_keys'):
                db.execute('DROP TABLE '+table)
        with patch.object(Path, 'replace', side_effect=OSError('backup disk unavailable')):
            with self.assertRaises(OSError): Store(self.temp.name)
        with self.store.db() as db:
            names = {r[0] for r in db.execute('SELECT name FROM sqlite_master')}
            self.assertNotIn('pipeline_fast_state', names)
            self.assertEqual(db.execute('SELECT event_id FROM events').fetchone()[0], 'old-event')
        self.assertEqual(list((Path(self.temp.name)/'backups').glob('*.partial')), [])

    def test_local_api_database_failure_has_503_handler(self):
        from fastapi.testclient import TestClient
        from app.local_api import app
        with patch.dict(os.environ, {'DATA_DIR': self.temp.name, 'DB_TIMEOUT_SECONDS': '.05'}):
            # No lifespan: /health still opens the real Store in the API thread.
            client = TestClient(app, raise_server_exceptions=False)
            self.addCleanup(client.close)
            with self.store.access_lock():
                response = client.get('/health')
            self.assertEqual(response.status_code, 503)
            self.assertEqual(client.get('/health').status_code, 200)

    def test_invalid_timeout_configuration_rejected(self):
        for value in ('nan', 'inf', '0', '31'):
            with self.subTest(value=value), patch.dict(os.environ, {'DB_TIMEOUT_SECONDS': value}):
                with self.assertRaises(ValueError): Store(self.temp.name)


class BoundedRetryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'health.json'
        self.stop = Mock(); self.stop.is_set.return_value = False; self.stop.wait.return_value = False
        self.counters = {'storage_failures': 0}

    def test_transient_database_lock_recovers_with_two_delays(self):
        operation = Mock(side_effect=[sqlite3.OperationalError('database is locked')]*2+[42])
        self.assertEqual(storage_operation(operation, self.stop, self.path, self.counters, 3), 42)
        self.assertEqual(operation.call_count, 3)
        self.assertEqual([c.args[0] for c in self.stop.wait.call_args_list], [.2, .4])

    def test_database_failure_never_commits_next_poll(self):
        pipeline, consumer = Mock(), Mock()
        consumer.poll.return_value = {'p': [record(request('one'))]}
        pipeline.ingest_batch.side_effect = sqlite3.OperationalError('database is locked')
        with self.assertRaises(sqlite3.OperationalError):
            run_consumer(consumer, pipeline, self.stop, self.path, storage_attempts=3)
        self.assertEqual(pipeline.ingest_batch.call_count, 3)
        consumer.commit.assert_not_called(); consumer.poll.assert_called_once()
        self.assertFalse(check_health(self.path))

    def test_schema_error_is_not_retried(self):
        operation = Mock(side_effect=sqlite3.OperationalError('no such table: events'))
        with self.assertRaises(sqlite3.OperationalError):
            storage_operation(operation, self.stop, self.path, self.counters, 3)
        operation.assert_called_once(); self.stop.wait.assert_not_called()

    def test_corruption_is_not_retried(self):
        operation = Mock(side_effect=sqlite3.DatabaseError('database disk image is malformed'))
        with self.assertRaises(sqlite3.DatabaseError):
            storage_operation(operation, self.stop, self.path, self.counters, 3)
        operation.assert_called_once()

    def test_stop_interrupts_retry_backoff(self):
        self.stop.wait.return_value = True
        operation = Mock(side_effect=sqlite3.OperationalError('database is locked'))
        with self.assertRaisesRegex(RuntimeError, 'stopped'):
            storage_operation(operation, self.stop, self.path, self.counters, 3)
        operation.assert_called_once()

    def test_idle_broker_disconnect_has_bounded_offset_probes(self):
        consumer, pipeline = Mock(), Mock()
        consumer.poll.return_value = {}
        consumer.end_offsets.side_effect = ConnectionError('broker offline')
        with self.assertRaisesRegex(RuntimeError, 'budget exhausted'):
            run_consumer(consumer, pipeline, self.stop, self.path, max_failures=2)
        self.assertEqual(consumer.end_offsets.call_count, 2)
        consumer.commit.assert_not_called(); pipeline.tick.assert_not_called()
        self.assertEqual(json.loads(self.path.read_text())['probe_failures'], 2)
        self.assertFalse(check_health(self.path))


class ConservativeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name); self.manager = IncidentManager(self.store)
        self.base = now_ms()-600_000
        self.store.ingest(request('fault', at=self.base, instance='pay-broken'))
        alert = {'schema_version': 1, 'alert_id': 'fault-alert', 'service': 'payment',
                 'rule_id': 'high_error_rate', 'window_start': self.base-1,
                 'window_end': self.base+1, 'evidence': {}}
        self.incident = self.manager.ingest_alert(alert, now=self.base+2)
        self.iid = self.incident['incident_id']

    def healthy(self, start, end, instance='pay-broken'):
        self.store.ingest_many([(request(f'{instance}-{i}', at=self.base+i*1000,
                                      instance=instance, status=200), None)
                                for i in range(start, end+1)])

    def status(self): return self.manager.get(self.iid)['status']

    def test_healthy_other_instance_cannot_resolve_missing_root(self):
        self.healthy(10, 50, 'pay-healthy')
        self.manager.tick(now=self.base+30_000); self.manager.tick(now=self.base+40_000)
        self.assertEqual(self.status(), 'open')

    def test_missing_or_future_samples_cannot_resolve(self):
        self.healthy(70, 90)
        self.manager.tick(now=self.base+30_000); self.manager.tick(now=self.base+40_000)
        self.assertEqual(self.status(), 'open')

    def test_restart_gap_restarts_hold_and_resolves_once_after_fresh_hold(self):
        self.healthy(10, 90)
        self.manager.tick(now=self.base+30_000)
        self.assertEqual(self.status(), 'recovering')
        self.manager = IncidentManager(Store(self.temp.name))
        self.manager.tick(now=self.base+60_000)
        current = self.manager.get(self.iid)
        self.assertEqual(current['status'], 'recovering')
        self.assertEqual(current['recovery_started_at'], self.base+60_000)
        self.manager.tick(now=self.base+70_000); self.manager.tick(now=self.base+80_000)
        self.assertEqual(self.status(), 'resolved')
        self.assertEqual([n['kind'] for n in self.manager.notifications(self.iid)], ['opened', 'resolved'])

    def test_new_bad_sample_resets_hold(self):
        self.healthy(10, 60)
        self.manager.tick(now=self.base+30_000)
        self.store.ingest(request('relapse', at=self.base+31_000, instance='pay-broken'))
        self.manager.tick(now=self.base+32_000)
        self.assertEqual(self.status(), 'open')

    def test_unknown_rule_requires_manual_confirmation(self):
        self.healthy(10, 60)
        with self.store.db() as db:
            incident = self.manager.get(self.iid)
            incident['family'] = 'rule:unknown'
            db.execute('UPDATE incidents SET payload=? WHERE incident_id=?', (canonical(incident), self.iid))
        self.manager.tick(now=self.base+30_000); self.manager.tick(now=self.base+40_000)
        self.assertEqual(self.status(), 'open')

    def test_recovery_read_is_bounded_and_truncation_blocks_resolution(self):
        # Use real SQLite pages: the former fetchall mock could not validate
        # keyset pagination or enforce the total cross-page record budget.
        self.store.ingest_many([(request(i, at=self.base+30_000, instance='pay-broken', status=200), None)
                                for i in range(50_001)])
        manager = IncidentManager(self.store, context_page_size=1000, context_byte_limit=64*1024*1024)
        with self.store.db() as db:
            ready, details, _ = manager._recovery(db, self.incident, self.base+30_000)
        self.assertFalse(ready)
        service = details['services']['payment']
        self.assertTrue(service['truncated'])
        self.assertEqual(service['metric_coverage']['records'], 50_000)
        self.assertEqual(service['metric_coverage']['truncation_reason'], 'record-limit')
        self.assertGreater(service['metric_coverage']['pages'], 1)



if __name__ == '__main__': unittest.main()
