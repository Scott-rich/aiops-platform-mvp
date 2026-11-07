"""Failure-response handling for the real acceptance client, without daemons."""
from contextlib import ExitStack
import tempfile
import unittest
from unittest.mock import Mock, patch

import httpx
from scripts.verify_reliability import Acceptance
from scripts.verify_storage_upgrade import verify
from app.storage import Store


class ReliabilityClientTests(unittest.TestCase):
    def client(self, statuses):
        run = Acceptance.__new__(Acceptance)
        run.api_url = 'http://127.0.0.1:12345'
        run.current = {}; run.save = Mock()
        run.client = httpx.Client(transport=httpx.MockTransport(
            lambda req: httpx.Response(statuses.pop(0), request=req, json={'status': 'open'})))
        self.addCleanup(run.client.close)
        return run

    def test_transient_503_recorded_before_valid_incident(self):
        run = self.client([503, 200])
        with patch('scripts.verify_reliability.time.sleep'):
            self.assertEqual(run.read_json('/incidents/one'), {'status': 'open'})
        self.assertEqual(len(run.current['transient_http_503']), 1)

    def test_persistent_503_fails_after_three_attempts(self):
        run = self.client([503, 503, 503])
        with patch('scripts.verify_reliability.time.sleep'), self.assertRaises(httpx.HTTPStatusError):
            run.read_json('/incidents/one')
        self.assertEqual(len(run.current['transient_http_503']), 3)

    def test_nonretryable_http_error_not_parsed_as_incident(self):
        run = self.client([404])
        with self.assertRaises(httpx.HTTPStatusError): run.read_json('/incidents/unknown')
        self.assertEqual(run.current, {})

    def test_upgrade_verifier_preserves_old_database_and_rejects_output_overwrite(self):
        from pathlib import Path
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); store = Store(root/'old')
            with store.db() as db:
                for table in ('pipeline_fast_state', 'deadletter_keys', 'platform_migrations'):
                    db.execute('DROP TABLE '+table)
            output = root/'verification'
            result = verify(store.path, output)
            self.assertEqual(result['status'], 'passed', result.get('error'))
            before = (output/'result.json').read_bytes()
            with self.assertRaises(FileExistsError): verify(store.path, output)
            self.assertEqual((output/'result.json').read_bytes(), before)


if __name__ == '__main__': unittest.main()
