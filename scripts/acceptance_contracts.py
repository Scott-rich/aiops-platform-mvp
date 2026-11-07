"""Fail-closed result contracts for each acceptance subprocess.

Evaluation completion is distinct from an accuracy/SLO target. RCA intentionally
has no top-level status; its cases and execution/reference metrics are validated.
"""
import math


CHECK_STEPS = {'runtime-http', 'launcher', 'evidence', 'native', 'model', 'flink-benchmark'}
STEP_NAMES = CHECK_STEPS | {'unit', 'detection', 'rca', 'reliability', 'faults',
                           'benchmark-http', 'benchmark-kafka'}
REQUIRED_CHECKS = {
    'runtime-http': {'anonymous_reports_denied', 'authenticated_event_ingestion',
                     'graceful_shutdown', 'generated_tokens_not_logged'},
    'launcher': {'real_cascade_5xx', 'requests_recover', 'all_five_ports_closed'},
    'evidence': {'http_full_pagination_and_client_hash', 'streaming_archive_verified',
                 'historical_report_and_hash_survive_api_restart'},
    'native': {'real_broker_and_flink_running', 'checkpoint_window_state_and_offsets_restored',
               'http_telemetry_through_real_flink', 'healthy_window_not_alerted_at_end'},
    'model': {'actual_model_output_passes_schema_and_evidence_checks',
              'real_http_attempt_recorded_in_persistent_budget',
              'repeat_report_and_database_reopen_do_not_repeat_model_call',
              'reopened_report_preserves_result'},
}
SCENARIOS = {
    'reliability': {'worker_restart_partial_burst', 'backlog_duplicate_malformed_and_offset_replay',
                    'database_access_outage_and_http_503', 'broker_outage_reconnect_and_acknowledged_data',
                    'platform_restart_history_and_report_hashes', 'real_time_recovery_confirmation',
                    'process_exit_transaction_boundaries', 'llm_timeout_stub_fallback_and_persistent_call_cap',
                    'final_database_integrity'},
    'faults': {'http_5xx', 'latency', 'timeout', 'unavailable', 'network_delay',
               'cpu', 'memory', 'cascade', 'messages', 'backlog'},
}


def nonnegative_integer(value):
    return type(value) is int and value >= 0


def probability(value, nullable=False):
    return (value is None and nullable) or (type(value) in (int, float)
            and math.isfinite(value) and 0 <= value <= 1)


def checks_status(checks, required=()):
    if not isinstance(checks, list) or not checks:
        return 'failed'
    names = set(); blocked = False
    for check in checks:
        if not isinstance(check, dict) or not isinstance(check.get('name'), str) or not check['name']:
            return 'failed'
        # Repeated observations may share a check name (e.g. six 502 calls).
        names.add(check['name'])
        if check.get('passed') is False and check.get('status') == 'blocked':
            blocked = True
        elif check.get('passed') is not True:
            return 'failed'
    if not set(required) <= names:
        return 'failed'
    return 'blocked' if blocked else 'passed'


def contradictory_failures(result):
    if result.get('error') or result.get('error_type'):
        return True
    for container in (result, result.get('counts', {}), result.get('check_counts', {}),
                      result.get('cleanup', {})):
        if not isinstance(container, dict):
            return True
        for key in ('failed', 'failures', 'errors'):
            if key in container:
                value = container[key]
                if isinstance(value, list):
                    if value: return True
                elif not nonnegative_integer(value) or value:
                    return True
    return False


def contract_status(step, result):
    if step == 'unit':
        keys = ('tests_run', 'passed', 'failures', 'errors', 'skipped')
        if not all(nonnegative_integer(result.get(k)) for k in keys): return 'failed'
        valid = result['tests_run'] > 0 and result['passed'] > 0
        valid &= result['tests_run'] == sum(result[k] for k in keys[1:])
        return 'passed' if valid else 'failed'
    if step in CHECK_STEPS:
        state = checks_status(result.get('checks'), REQUIRED_CHECKS.get(step, ()))
        if step == 'flink-benchmark' and (result.get('flink_engine') is not True or
                result.get('execution_kind') != 'real_native_kafka_flink_benchmark' or not result.get('tiers')):
            return 'failed'
        cleanup_key = {'native': 'all_processes_stopped', 'model': 'model_stopped',
                       'flink-benchmark': 'all_processes_stopped'}.get(step)
        if cleanup_key and result.get('cleanup', {}).get(cleanup_key) is not True: return 'failed'
        if step == 'evidence' and result.get('child_process_stopped') is not True: return 'failed'
        return state
    if step == 'detection':
        if result.get('execution_kind') != 'offline_labelled_replay': return 'failed'
        if not nonnegative_integer(result.get('window_count')) or result['window_count'] == 0: return 'failed'
        for mode in ('fixed', 'dynamic'):
            metrics = result.get('metrics', {}).get(mode, {})
            if not all(nonnegative_integer(metrics.get(k)) for k in ('tp', 'fp', 'tn', 'fn', 'labelled')): return 'failed'
            if not metrics['labelled'] or sum(metrics[k] for k in ('tp', 'fp', 'tn', 'fn')) != metrics['labelled']: return 'failed'
            if not probability(metrics.get('accuracy')): return 'failed'
            if not all(probability(metrics.get(k), nullable=True) for k in ('precision', 'recall')): return 'failed'
        return 'passed'
    if step == 'rca':
        cases = result.get('cases'); archived = result.get('archived_replays', [])
        if result.get('schema_version') != 1 or not isinstance(cases, list) or not cases or not isinstance(archived, list): return 'failed'
        if any(not isinstance(row, dict) or row.get('execution_status') != 'ok' or row.get('references_valid') is not True for row in cases + archived): return 'failed'
        metrics = result.get('metrics', {})
        for key in ('top1_any_labelled_root_accuracy', 'top3_any_labelled_root_accuracy',
                    'execution_success_rate', 'evidence_reference_validity'):
            value = metrics.get(key, {})
            if not all(nonnegative_integer(value.get(k)) for k in ('numerator', 'denominator')): return 'failed'
            n, d = value['numerator'], value['denominator']
            if n > d or not probability(value.get('value'), nullable=(d == 0)): return 'failed'
            if d and not math.isclose(value['value'], n/d): return 'failed'
            if key in {'execution_success_rate', 'evidence_reference_validity'} and (n != len(cases) or d != len(cases)): return 'failed'
        return 'passed'
    if step in SCENARIOS:
        rows = result.get('scenarios')
        if not isinstance(rows, list) or not rows: return 'failed'
        names = [r.get('name') for r in rows if isinstance(r, dict)]
        if len(names) != len(rows) or len(set(names)) != len(names) or not SCENARIOS[step] <= set(names): return 'failed'
        if any(r.get('status') != 'passed' or checks_status(r.get('checks')) != 'passed' for r in rows): return 'failed'
        cleanup_key = 'all_children_stopped' if step == 'reliability' else 'all_processes_stopped'
        return 'passed' if result.get('cleanup', {}).get(cleanup_key) is True else 'failed'
    if step in {'benchmark-http', 'benchmark-kafka'}:
        expected = step.removeprefix('benchmark-')
        components = result.get('components')
        if not isinstance(components, dict) or set(components) != {expected}: return 'failed'
        component = components[expected]
        if not isinstance(component, dict) or contradictory_failures(component): return 'failed'
        if component.get('status') not in {'passed', 'blocked'} or not component.get('tiers'): return 'failed'
        checked = checks_status(component.get('checks'))
        return 'failed' if checked == 'failed' else 'blocked' if component['status'] == 'blocked' or checked == 'blocked' else 'passed'
    return 'failed'


def classify(returncode, result, step=None):
    """A successful exit is necessary, never sufficient for a known step."""
    if not isinstance(result, dict) or not result or returncode not in (0, 2): return 'failed'
    if contradictory_failures(result): return 'failed'
    status = result.get('status')
    if not isinstance(status, str) and not (step == 'rca' and status is None): return 'failed'
    if status in {'failed', 'interrupted', 'running'}: return 'failed'
    if status not in {'passed', 'blocked', 'partial', 'not_requested'} and not (step == 'rca' and status is None): return 'failed'
    if status in {'blocked', 'partial', 'not_requested'}:
        # Do not mask an actual failed check behind a broad "partial" status.
        if result.get('checks') and checks_status(result['checks']) == 'failed': return 'failed'
        if isinstance(result.get('components'), dict):
            for component in result['components'].values():
                if not isinstance(component, dict) or contradictory_failures(component) or component.get('status') == 'failed': return 'failed'
                if component.get('checks') and checks_status(component['checks']) == 'failed': return 'failed'
        if isinstance(result.get('scenarios'), list):
            for scenario in result['scenarios']:
                if not isinstance(scenario, dict) or contradictory_failures(scenario) or scenario.get('status') == 'failed': return 'failed'
                if scenario.get('checks') and checks_status(scenario['checks']) == 'failed': return 'failed'
        return 'blocked'
    if step is None:
        checked = checks_status(result['checks']) if 'checks' in result else 'passed'
    elif step not in STEP_NAMES:
        return 'failed'
    else:
        try: checked = contract_status(step, result)
        except (TypeError, ValueError, KeyError, AttributeError): return 'failed'
    if checked == 'failed': return 'failed'
    return 'blocked' if checked == 'blocked' or returncode == 2 else 'passed'
