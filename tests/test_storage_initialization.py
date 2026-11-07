"""Regression tests for a backup preceding every Store initialization mutation."""
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from app.storage import Store
from scripts.verify_storage_upgrade import inventory


EVENT_DDL = '''CREATE TABLE events (
    event_id TEXT PRIMARY KEY, event_time INTEGER NOT NULL,
    service TEXT NOT NULL, trace_id TEXT NOT NULL,
    event_type TEXT NOT NULL, payload TEXT NOT NULL,
    source TEXT NOT NULL, ingested_at INTEGER NOT NULL)'''


def schema(path):
    with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True) as db:
        return list(db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))


class PreInitializationBackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'aiops.sqlite3'

    def partial_legacy(self):
        with sqlite3.connect(self.path) as db:
            db.execute(EVENT_DDL)
            db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',
                ('old', 1, 'payment', 'trace-1', 'request', '{"status_code":503}', '{}', 2))
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')

    def backups(self, label=''):
        return sorted((self.root/'backups').glob(label+'*.sqlite3'))

    def test_partial_legacy_backup_precedes_base_ddl_and_wal_change(self):
        self.partial_legacy()
        before, before_schema = inventory(self.path), schema(self.path)
        store = Store(self.root)
        backups = self.backups('before-phase7-')
        self.assertEqual(len(backups), 1)
        self.assertEqual(inventory(backups[0]), before)
        self.assertEqual(schema(backups[0]), before_schema)
        with sqlite3.connect(backups[0]) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')
        with store.db() as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            self.assertEqual(db.execute("SELECT backup_path FROM platform_migrations WHERE version='phase7-v1'").fetchone()[0], str(backups[0]))
            self.assertEqual(db.execute('SELECT payload FROM events').fetchone()[0], '{"status_code":503}')
        count = len(self.backups())
        Store(self.root)
        self.assertEqual(len(self.backups()), count)

    def test_backup_failure_prevents_base_ddl_and_journal_change(self):
        self.partial_legacy()
        before, original = schema(self.path), self.path.read_bytes()
        with patch('app.evidence.online_backup', side_effect=OSError('backup unavailable')):
            with self.assertRaises(OSError):
                Store(self.root)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(schema(self.path), before)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')

    def test_base_index_repair_is_backed_up_without_redundant_backups(self):
        store = Store(self.root)
        with store.db() as db:
            db.execute('DROP INDEX trace_idx')
        before = schema(self.path)
        Store(self.root)
        backups = self.backups('before-initialize-')
        self.assertEqual(len(backups), 1)
        self.assertEqual(schema(backups[0]), before)
        for _ in range(4):
            Store(self.root)
        self.assertEqual(len(self.backups()), 1)

    def test_journal_change_alone_is_backed_up(self):
        Store(self.root)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode=DELETE').fetchone()[0], 'delete')
        before = inventory(self.path)
        Store(self.root)
        backups = self.backups('before-initialize-')
        self.assertEqual(len(backups), 1)
        self.assertEqual(inventory(backups[0]), before)
        with sqlite3.connect(backups[0]) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')

    def test_phase8_marker_does_not_hide_missing_index_or_base_table(self):
        store = Store(self.root)
        with store.db() as db:
            db.execute('DROP INDEX evidence_filter_idx')
            db.execute('DROP TABLE reports')
        before = schema(self.path)
        Store(self.root)
        backups = self.backups('before-phase8-')
        self.assertEqual(len(backups), 1)
        self.assertEqual(schema(backups[0]), before)
        with store.db() as db:
            self.assertIsNotNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='evidence_filter_idx'").fetchone())
            self.assertIsNotNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='reports'").fetchone())
            self.assertEqual(db.execute("SELECT backup_path FROM platform_migrations WHERE version='phase8-v1'").fetchone()[0], str(backups[0]))
        Store(self.root)
        self.assertEqual(len(self.backups()), 1)

    def test_missing_phase7_migration_row_is_audited(self):
        store = Store(self.root)
        with store.db() as db:
            db.execute("DELETE FROM platform_migrations WHERE version='phase7-v1'")
        Store(self.root)
        backups = self.backups('before-phase7-')
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM platform_migrations WHERE version='phase7-v1'").fetchone())
        with store.db() as db:
            self.assertIsNotNone(db.execute("SELECT 1 FROM platform_migrations WHERE version='phase7-v1'").fetchone())

    def test_online_backup_contains_committed_wal_rows(self):
        # Keep a separate connection alive so the committed row remains in WAL.
        with sqlite3.connect(self.path) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute(EVENT_DDL)
            writer.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',
                ('wal-row', 1, 'payment', '', 'request', '{}', '{}', 2))
            writer.commit()
            self.assertTrue(self.path.with_name('aiops.sqlite3-wal').exists())
            Store(self.root)
            backup = self.backups('before-phase7-')[0]
            with sqlite3.connect(backup) as db:
                self.assertEqual(db.execute('SELECT event_id FROM events').fetchone()[0], 'wal-row')

    def test_current_database_reopens_without_backup(self):
        for _ in range(5):
            Store(self.root)
        self.assertEqual(self.backups(), [])

    def test_corruption_is_not_overwritten_or_marked_as_backed_up(self):
        original = b'not-a-sqlite-database'
        self.path.write_bytes(original)
        with self.assertRaises(sqlite3.DatabaseError):
            Store(self.root)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(self.backups(), [])

    def test_backup_publication_failure_preserves_legacy_source(self):
        self.partial_legacy()
        before = self.path.read_bytes()
        with patch.object(Path, 'replace', side_effect=OSError('publication failed')):
            with self.assertRaises(OSError):
                Store(self.root)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.backups(), [])
        self.assertEqual(list((self.root/'backups').glob('*.partial')), [])

    def test_optional_schema_is_backed_up_and_reopening_is_noop(self):
        store = Store(self.root)
        before = schema(self.path)
        script = 'CREATE TABLE IF NOT EXISTS optional_probe (id TEXT PRIMARY KEY); CREATE INDEX IF NOT EXISTS optional_idx ON optional_probe(id);'
        required = {'optional_probe', 'optional_idx'}
        backup = store.ensure_schema(script, required, 'optional-v1')
        self.assertEqual(schema(backup), before)
        self.assertIsNone(Store(self.root).ensure_schema(script, required, 'optional-v1'))
        self.assertEqual(len(self.backups('before-optional-v1-')), 1)
        with store.db() as db:
            self.assertEqual(db.execute("SELECT backup_path FROM platform_migrations WHERE version='schema:optional-v1'").fetchone()[0], str(backup))

    def test_optional_schema_backup_failure_leaves_missing_tables_missing(self):
        store = Store(self.root)
        before = schema(self.path)
        with patch('app.evidence.online_backup', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                store.ensure_schema('CREATE TABLE optional_probe (id TEXT)', {'optional_probe'}, 'optional-v1')
        self.assertEqual(schema(self.path), before)

    def test_optional_schema_failure_rolls_back_all_ddl(self):
        store = Store(self.root)
        before = schema(self.path)
        with self.assertRaises(sqlite3.Error):
            store.ensure_schema('CREATE TABLE optional_probe (id TEXT); CREATE INDEX invalid_idx ON missing_table(id);',
                                {'optional_probe', 'invalid_idx'}, 'optional-v1')
        self.assertEqual(schema(self.path), before)
        self.assertEqual(len(self.backups('before-optional-v1-')), 1)

    def test_optional_schema_missing_required_object_rolls_back(self):
        store = Store(self.root)
        before = schema(self.path)
        with self.assertRaises(sqlite3.OperationalError):
            store.ensure_schema('CREATE TABLE optional_probe (id TEXT);', {'optional_probe', 'uncreated'}, 'optional-v1')
        self.assertEqual(schema(self.path), before)

    def test_optional_schema_refuses_to_commit_caller_transaction(self):
        store = Store(self.root)
        with self.assertRaises(sqlite3.OperationalError):
            with store.atomic() as db:
                db.execute('CREATE TABLE transactional_probe(id TEXT)')
                store.ensure_schema('CREATE TABLE optional_probe (id TEXT)', {'optional_probe'}, 'optional-v1')
        self.assertFalse(any(row[1] in {'transactional_probe', 'optional_probe'} for row in schema(self.path)))
        self.assertEqual(self.backups(), [])

    def test_optional_schema_rejects_unsafe_identifiers_and_labels(self):
        store = Store(self.root)
        for required, label in [({'name; DROP TABLE events'}, 'safe'), ({'name'}, '../unsafe'),
                                ({}, 'safe'), ({'name'}, ''), ({'name': 'table'}, 'safe')]:
            with self.subTest(required=required, label=label), self.assertRaises(ValueError):
                store.ensure_schema('CREATE TABLE name(id TEXT)', required, label)
        self.assertEqual(self.backups(), [])

    def test_optional_schema_concurrent_initializers_create_one_backup(self):
        stores = [Store(self.root) for _ in range(4)]
        start = threading.Barrier(len(stores))
        errors, created = [], []
        def initialize(store):
            try:
                start.wait(timeout=3)
                created.append(store.ensure_schema('CREATE TABLE IF NOT EXISTS optional_probe(id TEXT)',
                                                   {'optional_probe'}, 'optional-v1'))
            except Exception as exc:
                errors.append(type(exc).__name__)
        threads = [threading.Thread(target=initialize, args=(store,)) for store in stores]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(sum(path is not None for path in created), 1)
        self.assertEqual(len(self.backups('before-optional-v1-')), 1)


if __name__ == '__main__':
    unittest.main()
