import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.local_platform import simulator_environment, platform_environment
from scripts.preflight import inspect_config


class EngineeringChecks(unittest.TestCase):
    def test_secret_names_filtered_but_control_token_preserved(self):
        incoming = {'EXAMPLE_API_KEY': 'secret', 'CUSTOM_SECRET_VALUE': 'secret',
                    'AWS_SESSION_TOKEN': 'secret', 'DB_PASSWORD': 'secret',
                    'SIMULATOR_CONTROL_TOKEN': 'keep', 'SERVICE_NAME': 'order', 'PATH': '/bin'}
        self.assertEqual(simulator_environment(incoming), {'SIMULATOR_CONTROL_TOKEN': 'keep',
                         'SERVICE_NAME': 'order', 'PATH': '/bin'})

    def test_preflight_valid_fallback_and_no_secrets_in_output(self):
        result = inspect_config({'LLM_API_KEY': 'not-to-be-printed'}, '/nonexistent-config')
        self.assertTrue(result['ok'])
        self.assertNotIn('not-to-be-printed', json.dumps(result))
        self.assertTrue(any(c['detail'] == 'rules_fallback' for c in result['checks']))

    def test_invalid_values_fail_without_echoing_input(self):
        result = inspect_config({'DETECTION_MODE': 'secret-invalid', 'DB_TIMEOUT_SECONDS': 'nan',
                                 'LLM_BASE_URL': 'http://user:password@host/v1'}, '/nonexistent-config')
        self.assertFalse(result['ok'])
        self.assertNotIn('password', json.dumps(result))
        self.assertNotIn('secret-invalid', json.dumps(result))
        self.assertEqual(sum(c['status'] == 'error' for c in result['checks']), 3)

    def test_partial_model_configuration_and_file_permissions_warn(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'config'
            path.write_text('not read')
            path.chmod(0o644)
            result = inspect_config({'LLM_BASE_URL': 'http://localhost/v1'}, path)
            self.assertTrue(any(c['check'] == 'llm_pair' for c in result['checks']))
            self.assertTrue(any(c['check'] == 'config_permissions' and c['status'] == 'warning' for c in result['checks']))

    def test_local_file_config_and_environment_precedence(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'config'
            path.write_text('DB_TIMEOUT_SECONDS=2\nEVIDENCE_RETENTION_DAYS=9\n')
            result = platform_environment({'DB_TIMEOUT_SECONDS': '4'}, path)
            self.assertEqual(result['DB_TIMEOUT_SECONDS'], '4')
            self.assertEqual(result['EVIDENCE_RETENTION_DAYS'], '9')

    def test_missing_dependency_is_error(self):
        with patch('scripts.preflight.importlib.metadata.version', side_effect=__import__('importlib').metadata.PackageNotFoundError):
            result = inspect_config({}, '/nonexistent-config')
        self.assertFalse(result['ok'])
        self.assertEqual(sum(c['status'] == 'error' for c in result['checks']), 6)

if __name__ == '__main__':
    unittest.main()
