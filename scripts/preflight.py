"""Read-only, local configuration checks; does not start services or connect to providers."""
import argparse
import importlib.metadata
import json
import math
import os
from pathlib import Path
import sys
from app.runtime_config import RuntimeConfig
from app.runtime_support import require_supported_runtime, UnsupportedPythonRuntime

from app.llm_config import LLMConfig, ModelError
from flink.adaptive import DetectionConfig
from scripts.local_platform import platform_environment

EXPECTED = {'fastapi': '0.115.12', 'httpx': '0.28.1', 'kafka-python': '2.2.12',
            'uvicorn': '0.34.2', 'pydantic': '2.14.1', 'filelock': '4.2.4'}


def inspect_config(env, config_path):
    """Return codes and field names only; never expose values or mutate storage."""
    checks = []
    def add(name, status, detail):
        checks.append({'check': name, 'status': status, 'detail': detail})
    try:
        version = require_supported_runtime()
        add('python_runtime', 'pass', 'supported_python_' + '.'.join(map(str, version)))
    except UnsupportedPythonRuntime as exc:
        add('python_runtime', 'error', str(exc))
    for package, expected in EXPECTED.items():
        try:
            actual = importlib.metadata.version(package)
            add('dependency:' + package, 'pass' if actual == expected else 'error',
                'version_matches' if actual == expected else 'version_mismatch')
        except importlib.metadata.PackageNotFoundError:
            add('dependency:' + package, 'error', 'not_installed')
    try:
        DetectionConfig.from_env(env)
        add('detection', 'pass', 'validated')
    except (ValueError, TypeError):
        add('detection', 'error', 'invalid_configuration')
    try:
        model = LLMConfig.from_env(env)
        add('llm', 'pass' if model.configured else 'warning',
            'configured_not_contacted' if model.configured else 'rules_fallback')
        if bool(model.base_url) != bool(model.model):
            add('llm_pair', 'warning', 'base_url_and_model_both_required')
    except ModelError as exc:
        add('llm', 'error', 'invalid_configuration:' + exc.metadata.get('configuration_field', 'unknown'))
    try:
        runtime = RuntimeConfig.from_env(env)
        add('runtime', 'pass', 'validated')
        add('access_boundary', 'warning' if runtime.auth_mode=='local' else 'pass',
            'local_trusted_environment_only_no_user_authentication' if runtime.auth_mode=='local' else 'token_roles_configured_tls_and_network_isolation_still_required')
    except (ValueError, TypeError):
        add('runtime', 'error', 'invalid_configuration')
    if not env.get('SIMULATOR_CONTROL_TOKEN'):
        add('simulator_token', 'warning', 'empty_token_simulator_controls_unprotected')
    path = Path(config_path)
    if path.is_file() and os.name == 'posix' and path.stat().st_mode & 0o077:
        add('config_permissions', 'warning', 'config_readable_by_group_or_others')
    else:
        add('config_permissions', 'pass', 'no_exposed_config_file_detected')
    return {'ok': not any(c['status'] == 'error' for c in checks), 'checks': checks,
            'scope': 'static_local_checks_only_no_network_database_or_docker_validation'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', default='.env', help='local launcher-compatible config file')
    args = parser.parse_args()
    try:
        result = inspect_config(platform_environment(os.environ, Path(args.env_file)), args.env_file)
    except (OSError, UnicodeError):
        result = {'ok': False, 'checks': [{'check': 'config_file', 'status': 'error',
                                            'detail': 'cannot_read_config'}]}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result['ok'] else 1

if __name__ == '__main__':
    sys.exit(main())
