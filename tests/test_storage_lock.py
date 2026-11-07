import json
import multiprocessing
import os
from pathlib import Path
import queue
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.api import app
from app.storage import Store


def write_rows(root,number,start,results):
    try:
        start.wait(10)
        for index in range(40):
            store=Store(root)
            with store.db() as db:
                db.execute('CREATE TABLE IF NOT EXISTS concurrency_probe(k TEXT PRIMARY KEY,v TEXT)')
                db.execute('INSERT INTO concurrency_probe VALUES(?,?)',(f'{number}-{index}','x'*5000))
        results.put(None)
    except Exception as error:results.put(type(error).__name__)


class StorageCoordinationTests(unittest.TestCase):
    def test_nested_connections_to_same_store_path_remain_usable(self):
        with tempfile.TemporaryDirectory() as directory:
            first=Store(directory);second=Store(Path(directory).resolve())
            with first.db() as a:
                with second.db() as b:self.assertEqual(b.execute('SELECT 1').fetchone()[0],1)
                self.assertEqual(a.execute('SELECT 2').fetchone()[0],2)

    def test_busy_database_access_has_bounded_failure_and_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            first=Store(directory);second=Store(directory);entered=threading.Event();release=threading.Event();errors=[]
            def hold():
                try:
                    with first.db() as db:
                        db.execute('CREATE TABLE lock_probe(k INTEGER)');entered.set();release.wait(3)
                except Exception as error:errors.append(type(error).__name__);entered.set()
            thread=threading.Thread(target=hold);thread.start()
            try:
                self.assertTrue(entered.wait(3));second._access_lock.timeout=.05
                with self.assertRaisesRegex(sqlite3.OperationalError,'access lock unavailable'):
                    with second.db():pass
            finally:
                second._access_lock.timeout=30;release.set();thread.join(3)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            with second.db() as db:self.assertIsNotNone(db.execute('SELECT name FROM sqlite_master WHERE name="lock_probe"').fetchone())

    def test_multiple_processes_keep_all_committed_rows_and_integrity(self):
        with tempfile.TemporaryDirectory() as directory:
            context=multiprocessing.get_context('spawn');start=context.Event();results=context.Queue()
            processes=[context.Process(target=write_rows,args=(directory,i,start,results)) for i in range(4)]
            try:
                for process in processes:process.start()
                start.set()
                for process in processes:process.join(20)
                self.assertTrue(all(not p.is_alive() and p.exitcode==0 for p in processes))
                self.assertEqual([results.get(timeout=3) for _ in processes],[None]*4)
                with Store(directory).db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM concurrency_probe').fetchone()[0],160)
                    self.assertEqual([r[0] for r in db.execute('PRAGMA integrity_check')],['ok'])
            finally:
                for process in processes:
                    if process.is_alive():process.kill();process.join(3)
                results.close();results.join_thread()

    def test_corrupted_database_returns_503_and_preserves_file(self):
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,{'DATA_DIR':directory,'EXECUTION_MODE':'kafka-worker'}):
            path=Path(directory)/'aiops.sqlite3';raw=b'explicit corrupt database fixture';path.write_bytes(raw)
            with self.assertRaises(sqlite3.DatabaseError):
                with TestClient(app):pass
            self.assertEqual(path.read_bytes(),raw)
            # Requests on an already-started API still translate database errors.
            client=TestClient(app);self.addCleanup(client.close)
            response=client.get('/health')
            self.assertEqual(response.status_code,503)
            self.assertEqual(response.json()['detail'],'storage_unavailable')
            self.assertNotIn('fixture',response.text);self.assertEqual(path.read_bytes(),raw)
