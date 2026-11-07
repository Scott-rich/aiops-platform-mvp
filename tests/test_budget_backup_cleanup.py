"""Failure-path checks for the additive model-schema backup."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app.model_budget import ModelBudget
from app.storage import Store


class TrackedConnection(sqlite3.Connection):
    closed_explicitly = False
    fail_backup = False
    fail_close = False

    def backup(self, target, **kwargs):
        if self.fail_backup:
            raise sqlite3.OperationalError("injected backup failure")
        return super().backup(target, **kwargs)

    def close(self):
        self.closed_explicitly = True
        super().close()
        if self.fail_close:
            raise sqlite3.OperationalError("injected destination close failure")


class BudgetBackupCleanupTests(unittest.TestCase):
    def exercise_failure(self, failure):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(directory)
            with store.db() as db:
                db.execute("CREATE TABLE retained_history(id INTEGER PRIMARY KEY, payload TEXT)")
                db.execute("INSERT INTO retained_history VALUES(1, 'keep original record')")
            connect = sqlite3.connect
            handles = []

            def tracked_connect(path, *args, **kwargs):
                destination = Path(path).suffix == ".partial"
                if destination and failure == "connect":
                    raise sqlite3.OperationalError("injected destination open failure")
                kwargs["factory"] = TrackedConnection
                handle = connect(path, *args, **kwargs)
                handle.fail_backup = not destination and failure == "backup"
                handle.fail_close = destination and failure == "close"
                handles.append(handle)
                return handle

            with patch("app.model_budget.sqlite3.connect", side_effect=tracked_connect):
                with self.assertRaises(sqlite3.OperationalError):
                    ModelBudget(store)

            self.assertTrue(handles)
            self.assertTrue(all(handle.closed_explicitly for handle in handles))
            self.assertFalse(list((Path(directory) / "backups").glob("*.partial")))
            self.assertFalse(list((Path(directory) / "backups").glob("*.sqlite3")))
            with store.db() as db:
                self.assertEqual(db.execute("SELECT payload FROM retained_history").fetchone()[0],
                                 "keep original record")
                self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='model_jobs'").fetchone())
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_destination_open_failure_closes_source_and_preserves_history(self):
        self.exercise_failure("connect")

    def test_backup_failure_closes_both_connections_and_removes_partial(self):
        self.exercise_failure("backup")

    def test_destination_close_failure_still_closes_source(self):
        self.exercise_failure("close")


if __name__ == "__main__":
    unittest.main()
