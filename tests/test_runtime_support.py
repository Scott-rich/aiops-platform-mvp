import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.runtime_support import require_supported_runtime, UnsupportedPythonRuntime, SUPPORTED_PYTHON
from app.storage import Store
from scripts.local_platform import main as launch_local
from scripts.preflight import inspect_config


class RuntimeSupportTests(unittest.TestCase):
    def test_python_311_and_312_policy_supports_patch_versions(self):
        for version in ((3, 11), (3, 11, 0), (3, 11, 17), (3, 12), (3, 12, 0), (3, 12, 10)):
            with self.subTest(version=version):
                self.assertEqual(require_supported_runtime(version), version[:2])

    def test_other_interpreters_are_explicitly_unsupported(self):
        for version in ((3, 10), (3, 13), (4, 0)):
            with self.subTest(version=version):
                with self.assertRaisesRegex(UnsupportedPythonRuntime, 'requires Python 3.11'):
                    require_supported_runtime(version)

    def test_detector_is_patchable_without_changing_global_python_state(self):
        with patch('app.runtime_support.python_version', return_value=(3, 13)):
            with self.assertRaisesRegex(UnsupportedPythonRuntime, 'detected Python 3.13'):
                require_supported_runtime()

    def test_supported_interpreter_creates_real_healthy_store(self):
        self.assertIn(require_supported_runtime(), SUPPORTED_PYTHON)
        with tempfile.TemporaryDirectory() as parent:
            store = Store(Path(parent) / 'supported')
            with store.db() as db:
                self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                self.assertIsNotNone(db.execute(
                    "SELECT name FROM sqlite_master WHERE name='events'").fetchone())

    def test_unsupported_store_does_not_touch_new_data_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            data = Path(parent) / 'must-not-exist'
            with patch('app.runtime_support.python_version', return_value=(3, 10)), \
                    patch('app.storage.sqlite3.connect') as connect, \
                    patch('app.storage.FileLock') as lock, \
                    patch('pathlib.Path.mkdir') as mkdir:
                with self.assertRaises(UnsupportedPythonRuntime):
                    Store(data)
                mkdir.assert_not_called()
                lock.assert_not_called()
                connect.assert_not_called()
            self.assertFalse(data.exists())
            self.assertEqual(list(Path(parent).iterdir()), [])

    def test_unsupported_store_preserves_existing_files(self):
        with tempfile.TemporaryDirectory() as parent:
            data = Path(parent)
            path = data / 'aiops.sqlite3'
            original = b'existing data must not be opened or migrated'
            path.write_bytes(original)
            before = path.stat()
            with patch('app.runtime_support.python_version', return_value=(3, 13)), \
                    patch('app.storage.sqlite3.connect') as connect, \
                    patch('app.storage.FileLock') as lock:
                with self.assertRaises(UnsupportedPythonRuntime):
                    Store(data)
                connect.assert_not_called()
                lock.assert_not_called()
            after = path.stat()
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual((before.st_size, before.st_mtime_ns), (after.st_size, after.st_mtime_ns))
            self.assertEqual(list(data.iterdir()), [path])

    def test_preflight_reports_supported_runtime(self):
        for version in ((3, 11), (3, 12)):
            with self.subTest(version=version), patch('app.runtime_support.python_version', return_value=version):
                result = inspect_config({}, '/nonexistent-config')
            check = next(item for item in result['checks'] if item['check'] == 'python_runtime')
            self.assertEqual(check['status'], 'pass')
            self.assertEqual(check['detail'], 'supported_python_' + '.'.join(map(str, version)))
            self.assertTrue(result['ok'])

    def test_preflight_marks_unsupported_runtime_as_error(self):
        with patch('app.runtime_support.python_version', return_value=(3, 10)):
            result = inspect_config({}, '/nonexistent-config')
        self.assertFalse(result['ok'])
        check = next(item for item in result['checks'] if item['check'] == 'python_runtime')
        self.assertEqual(check['status'], 'error')
        self.assertIn('requires Python 3.11 or 3.12', check['detail'])
        self.assertIn('detected Python 3.10', check['detail'])

    def test_launcher_rejects_before_mkdir_port_bind_or_child_start(self):
        with tempfile.TemporaryDirectory() as parent:
            data = Path(parent) / 'must-not-exist'
            stderr = io.StringIO()
            with patch('sys.argv', ['local_platform', '--data-dir', str(data)]), \
                    patch('app.runtime_support.python_version', return_value=(3, 13)), \
                    patch('scripts.local_platform.socket.socket') as socket, \
                    patch('scripts.local_platform.subprocess.Popen') as popen, \
                    patch('pathlib.Path.mkdir') as mkdir, contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    launch_local()
                self.assertEqual(raised.exception.code, 2)
                mkdir.assert_not_called()
                socket.assert_not_called()
                popen.assert_not_called()
            self.assertFalse(data.exists())
            self.assertIn('requires Python 3.11', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
