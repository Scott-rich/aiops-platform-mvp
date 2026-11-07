import copy
import gzip
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from app.api import app
from app.contracts import canonical, digest, now_ms
from app.storage import Store
from app.reporting import ReportManager
from app.evidence import EvidenceRepository, evidence_audit
from app.evidence_api import verify_report_evidence
from app.evidence_archive import export_set, verify_archive, restore_events, prune_events, retention_plan
from flink.rules import evaluate_window
from scripts.demo import fixture


class EvidencePhase8Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        env = patch.dict(os.environ, {'DATA_DIR': str(self.root/'data'), 'LLM_BASE_URL': '', 'LLM_MODEL': '', 'EVIDENCE_RETENTION_DAYS': '30'})
        env.start(); self.addCleanup(env.stop)
        self.store = Store(); self.repo = EvidenceRepository(self.store)
        self.events, self.start, self.end = fixture()
        self.store.ingest_many([(e, {'transport': 'test'}) for e in self.events])
        self.alert = evaluate_window(self.events, 'gateway', self.start, self.end)[0]
        self.store.alert(self.alert)
        self.manager = ReportManager(self.store)
        self.incident = {'incident_id': 'phase8-incident', 'status': 'open', 'services': ['gateway', 'order', 'payment'],
                         'candidate_service': 'payment', 'first_alert_id': self.alert['alert_id'], 'alert_ids': [self.alert['alert_id']],
                         'first_event_time': self.start, 'last_event_time': self.end, 'latest_alert_end': self.end}
        self.client = TestClient(app); self.addCleanup(self.client.close)

    def capture(self, limit=1800):
        snapshot = self.store.evidence(self.alert, limit)
        return snapshot, snapshot['full_evidence']['set_id']

    def test_complete_pagination_has_no_duplicate_or_omission(self):
        snapshot, sid = self.capture(7)
        self.assertTrue(snapshot['coverage']['truncated'])
        ids = []; cursor = None
        while True:
            page = self.repo.page(sid, 17, cursor)
            ids += [r['event']['event_id'] for r in page['records']]
            cursor = page['next_cursor']
            if cursor is None: break
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), {e['event_id'] for e in self.events})
        self.assertTrue(self.repo.verify(sid)['valid'])

    def test_related_events_beyond_old_20000_scan_cap_are_saved(self):
        seed = self.events[0]
        for start in range(0, 20100, 500):
            self.store.ingest_many([(dict(seed, event_id=f'distractor-{n}', service='unrelated', trace_id='isolated', event_time=self.start-4000), None) for n in range(start, min(start+500, 20100))])
        snapshot, sid = self.capture()
        self.assertEqual(snapshot['full_evidence']['total_count'], len(self.events))
        self.assertEqual(snapshot['coverage']['range_candidate_count'], 20100+len(self.events))
        self.assertTrue(self.repo.verify(sid)['valid'])

    def test_matching_rows_over_analysis_limit_saved_completely(self):
        self.store.ingest_many([(dict(self.events[0], event_id=f'extra-{n}'), None) for n in range(2000)])
        snapshot, sid = self.capture()
        self.assertEqual(len(snapshot['records']), 1800)
        self.assertEqual(snapshot['full_evidence']['total_count'], 2000+len(self.events))
        self.assertTrue(self.repo.verify(sid)['valid'])

    def test_duplicate_ingest_reuses_set_and_late_arrival_freezes_new_set(self):
        old, sid = self.capture()
        self.store.ingest_many([(e, None) for e in self.events])
        self.assertEqual(self.capture()[1], sid)
        self.store.ingest(dict(self.events[0], event_id='late-arrival'))
        new, newer = self.capture()
        self.assertNotEqual(sid, newer)
        self.assertEqual(self.repo.metadata(sid), old['full_evidence'])
        self.assertEqual(new['full_evidence']['total_count'], old['full_evidence']['total_count']+1)

    def test_combined_filters_use_inclusive_time_boundaries(self):
        _, sid = self.capture(); event = self.events[0]
        page = self.repo.page(sid, service=event['service'], event_type=event['event_type'],
                              event_id=event['event_id'], trace_id=event['trace_id'], start=event['event_time'], end=event['event_time'])
        self.assertEqual(page['filtered_count'], 1)
        self.assertEqual(page['records'][0]['event'], event)

    def test_cursor_cannot_cross_set_or_filter(self):
        _, sid = self.capture(); cursor = self.repo.page(sid, 1)['next_cursor']
        with self.assertRaises(ValueError): self.repo.page(sid, 1, cursor, service='payment')
        self.store.ingest(dict(self.events[0], event_id='late')); _, newer = self.capture()
        with self.assertRaises(ValueError): self.repo.page(newer, 1, cursor)

    def test_api_invalid_filters_and_cursors_rejected(self):
        _, sid = self.capture(); url = '/evidence-sets/'+sid+'/records'
        for params in ({'cursor':'!!!'}, {'cursor':'e30='}, {'page_size':501}, {'start':10,'end':9}, {'event_type':'sql'}, {'start':-1}, {'end':2**64}):
            with self.subTest(params=params): self.assertEqual(self.client.get(url, params=params).status_code, 422)
        self.assertEqual(self.client.get('/evidence-sets/missing/records').status_code, 404)

    def test_empty_set_and_empty_filtered_page(self):
        _, sid = self.capture()
        self.assertEqual(self.repo.page(sid, service='missing')['records'], [])
        snapshot = self.repo.capture(dict(self.alert, service='missing'))
        self.assertEqual(snapshot['full_evidence']['total_count'], 0)
        self.assertTrue(self.repo.verify(snapshot['full_evidence']['set_id'])['valid'])

    def test_hash_detects_modified_payload(self):
        _, sid = self.capture()
        with self.store.db() as db:
            row = db.execute('SELECT record FROM evidence_records WHERE set_id=? AND ordinal=1', (sid,)).fetchone()
            record = json.loads(row[0]); record['source']['tampered'] = True
            db.execute('UPDATE evidence_records SET record=? WHERE set_id=? AND ordinal=1', (canonical(record), sid))
        self.assertFalse(self.repo.verify(sid)['valid'])

    def test_hash_detects_deleted_row(self):
        _, sid = self.capture()
        with self.store.db() as db: db.execute('DELETE FROM evidence_records WHERE set_id=? AND ordinal=2', (sid,))
        self.assertFalse(self.repo.verify(sid)['valid'])

    def test_hash_detects_tampered_filter_columns(self):
        _, sid = self.capture()
        with self.store.db() as db: db.execute("UPDATE evidence_records SET service='invented' WHERE set_id=? AND ordinal=1", (sid,))
        self.assertFalse(self.repo.verify(sid)['valid'])

    def test_capture_failure_rolls_back_partial_set(self):
        with patch('app.evidence.set_identity', side_effect=ValueError('injected')), self.assertRaises(ValueError): self.capture()
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM evidence_records').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM evidence_sets').fetchone()[0], 0)

    def test_invalid_operation_timeout_rejected(self):
        with patch.dict(os.environ, {'EVIDENCE_OPERATION_TIMEOUT_SECONDS':'nan'}), self.assertRaises(ValueError): self.capture()

    def test_deadline_aborts_capture_without_partial_evidence(self):
        from types import SimpleNamespace
        from itertools import chain, repeat
        from unittest.mock import Mock
        clock = SimpleNamespace(monotonic=Mock(side_effect=chain([0], repeat(2))))
        with patch.dict(os.environ, {'EVIDENCE_OPERATION_TIMEOUT_SECONDS':'1'}), patch('app.evidence.time', clock), self.assertRaises(sqlite3.OperationalError):
            self.capture()
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM evidence_records').fetchone()[0], 0)

    def test_additive_migration_backed_up_before_changes(self):
        with self.store.db() as db:
            db.execute('DROP TABLE evidence_records'); db.execute('DROP TABLE evidence_sets')
            db.execute("DELETE FROM platform_migrations WHERE version='phase8-v1'")
        Store(self.store.root)
        backups = list((self.store.root/'backups').glob('before-phase8-*.sqlite3'))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], len(self.events))
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='evidence_sets'").fetchone())
        Store(self.store.root)
        self.assertEqual(len(list((self.store.root/'backups').glob('before-phase8-*.sqlite3'))), 1)

    def test_migration_backup_failure_leaves_schema_unmodified(self):
        with self.store.db() as db:
            db.execute('DROP TABLE evidence_records'); db.execute('DROP TABLE evidence_sets')
            db.execute("DELETE FROM platform_migrations WHERE version='phase8-v1'")
        with patch('app.evidence.online_backup', side_effect=OSError('no space')), self.assertRaises(OSError): Store(self.store.root)
        with self.store.db() as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='evidence_sets'").fetchone())

    def test_report_tracks_citations_and_full_set(self):
        report = self.manager.ensure_initial(self.incident, self.end+5000)
        self.assertEqual(report['audit']['evidence_completeness'], 'stored_query_complete')
        self.assertTrue(report['audit']['referenced_evidence_ids'])
        self.assertNotEqual(report['audit']['evidence_set_id'], report['audit']['full_evidence_set_id'])
        self.assertTrue(verify_report_evidence(self.store, report)['full_evidence_valid'])

    def test_invented_report_reference_rejected(self):
        report = self.manager.ensure_initial(self.incident)
        report['analysis']['hypotheses'][0]['supporting_evidence_ids'].append('invented')
        with self.assertRaises(ValueError): evidence_audit(report)
        self.assertFalse(verify_report_evidence(self.store, report)['full_evidence_valid'])

    def test_late_evidence_creates_version_without_trace_coverage_improvement(self):
        first = self.manager.ensure_initial(self.incident, self.end+5000)
        second = self.manager.refresh(self.incident, self.end+8000)
        self.store.ingest(dict(self.events[0], event_id='late-evidence'))
        third = self.manager.refresh(self.incident, self.end+12000)
        self.assertEqual(third['version'], 3)
        self.assertEqual(self.manager.get(first['report_id']), first)
        diff = self.manager.compare(second['report_id'], third['report_id'])
        self.assertEqual(diff['evidence_changes']['added']['event_ids'], ['late-evidence'])
        self.assertEqual(diff['evidence_changes']['removed']['count'], 0)

    def test_history_pagination_and_cross_incident_compare(self):
        first = self.manager.ensure_initial(self.incident, self.end+5000)
        second = self.manager.refresh(self.incident, self.end+8000)
        page = self.manager.history(self.incident['incident_id'], 1)
        self.assertEqual(page['reports'][0]['report_id'], first['report_id'])
        self.assertEqual(self.manager.history(self.incident['incident_id'], 1, page['next_cursor'])['reports'][0]['report_id'], second['report_id'])
        other = self.manager.ensure_initial(dict(self.incident, incident_id='other'))
        with self.assertRaises(ValueError): self.manager.compare(first['report_id'], other['report_id'])

    def test_legacy_report_remains_readable_with_unknown_completeness(self):
        report = self.manager.ensure_initial(self.incident)
        report['snapshot'].pop('full_evidence')
        for key in evidence_audit(report): report['audit'].pop(key, None)
        with self.store.db() as db: db.execute('UPDATE report_versions SET payload=? WHERE report_id=?', (canonical(report), report['report_id']))
        url = '/report-versions/'+report['report_id']
        page = self.client.get(url+'/evidence', params={'page_size':1}).json()
        self.assertIsNone(page['stored_complete']); self.assertIsNone(page['total_count'])
        self.assertEqual(len(page['records']), 1)
        self.assertIsNone(self.client.get(url+'/verify').json()['full_evidence_valid'])
        self.assertFalse(self.manager.compare(report['report_id'], report['report_id'])['full_comparison_available'])

    def test_archive_roundtrip_and_idempotent_restore(self):
        _, sid = self.capture(); path = self.root/'archive.jsonl.gz'
        result = export_set(self.store, sid, path)
        self.assertEqual(result['count'], len(self.events))
        target = Store(self.root/'restore')
        self.assertEqual(restore_events(target, path)['inserted'], len(self.events))
        self.assertEqual(restore_events(target, path)['inserted'], 0)
        self.assertEqual(EvidenceRepository(target).capture(self.alert)['full_evidence']['set_id'], sid)

    def test_archive_never_overwrites_existing_file(self):
        _, sid = self.capture(); path = self.root/'existing.gz'; path.write_bytes(b'keep')
        with self.assertRaises(FileExistsError): export_set(self.store, sid, path)
        self.assertEqual(path.read_bytes(), b'keep')

    def test_bad_archive_rolls_back_restore(self):
        _, sid = self.capture(); path = self.root/'archive.gz'; export_set(self.store, sid, path)
        with gzip.open(path, 'rt') as f: lines = f.readlines()
        with gzip.open(path, 'wt') as f: f.writelines(lines[:-1])
        target = Store(self.root/'restore')
        with self.assertRaises(ValueError): restore_events(target, path)
        self.assertEqual(target.stats()['events'], 0)

    def test_archive_header_tampering_is_rejected(self):
        _, sid = self.capture(); path = self.root/'archive.gz'; export_set(self.store, sid, path)
        with gzip.open(path, 'rt') as f: lines = f.readlines()
        header = json.loads(lines[0]); header['metadata']['query']['service'] = 'tampered'
        lines[0] = canonical(header)+'\n'
        with gzip.open(path, 'wt') as f: f.writelines(lines)
        with self.assertRaises(ValueError): verify_archive(path)

    def test_restore_conflict_preserves_existing_event(self):
        _, sid = self.capture(); path = self.root/'archive.gz'; export_set(self.store, sid, path)
        target = Store(self.root/'restore'); target.ingest(dict(self.events[0], service='conflict'))
        with self.assertRaises(ValueError): restore_events(target, path)
        self.assertEqual(target.stats()['events'], 1)

    def prepare_retention(self):
        # Fixture-only data: make receipt times as old as event times.
        with self.store.db() as db: db.execute('UPDATE events SET ingested_at=event_time')
        from app.diagnosis import diagnose
        report = diagnose(self.store, self.alert['alert_id'])
        now = self.end + 40*86400000; before = self.end + 10000
        return report, now, before

    def test_retention_dry_run_and_one_bounded_batch(self):
        report, now, before = self.prepare_retention()
        plan = prune_events(self.store, before, self.root/'archives', size=17, now=now)
        self.assertEqual(plan['deleted'], 0)
        result = prune_events(self.store, before, self.root/'archives', size=17, apply=True, now=now)
        self.assertEqual(result['deleted'], 17)
        self.assertTrue(Path(result['backup']).is_file())
        self.assertTrue(verify_archive(result['archive']['path'])['valid'])
        self.assertEqual(self.store.get_report(report['report_id']), report)
        self.assertTrue(self.repo.verify(report['audit']['full_evidence_set_id'])['valid'])
        self.assertEqual(restore_events(self.store, result['archive']['path'])['inserted'], 17)

    def test_archive_failure_does_not_delete_raw_events(self):
        _, now, before = self.prepare_retention()
        with patch('app.evidence_archive.write_archive', side_effect=OSError('no space')), self.assertRaises(OSError):
            prune_events(self.store, before, self.root/'archives', apply=True, now=now)
        self.assertEqual(self.store.stats()['events'], len(self.events))

    def test_backup_failure_does_not_delete_raw_events(self):
        _, now, before = self.prepare_retention()
        with patch('app.evidence_archive.online_backup', side_effect=OSError('no space')), self.assertRaises(OSError):
            prune_events(self.store, before, self.root/'archives', apply=True, now=now)
        self.assertEqual(self.store.stats()['events'], len(self.events))

    def test_active_incident_and_unreported_alert_protect_raw_data(self):
        with self.store.db() as db: db.execute('UPDATE events SET ingested_at=event_time')
        now = self.end+40*86400000; before = self.end+10000
        self.assertEqual(retention_plan(self.store, before, now=now)['eligible_count'], 0)
        self.prepare_retention()
        from app.incidents import IncidentManager
        IncidentManager(self.store).ingest_alert(self.alert)
        self.assertEqual(retention_plan(self.store, before, now=now)['eligible_count'], 0)

    def test_fresh_receipts_and_retention_minimum_are_protected(self):
        from app.diagnosis import diagnose
        diagnose(self.store, self.alert['alert_id'])
        self.assertEqual(retention_plan(self.store, self.end+10000, now=self.end+40*86400000)['eligible_count'], 0)
        with self.assertRaises(ValueError): retention_plan(self.store, now_ms())
        with patch.dict(os.environ, {'EVIDENCE_RETENTION_DAYS':'0'}), self.assertRaises(ValueError): retention_plan(self.store, 0)


if __name__ == '__main__':
    unittest.main()
