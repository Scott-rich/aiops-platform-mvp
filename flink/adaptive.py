"""Dependency-free causal, bounded median/MAD detector for closed windows.

No label, injection metadata or future window is used for fitting. Scores are
threshold decisions, not probabilities. Call once per service in end-time order.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import logging
import math
import os
from statistics import median
import time
from wire_contract import number

LOG = logging.getLogger('aiops.detection')
VERSION = 'adaptive-mad-v1'
METRICS = {
    'p95_latency_ms': ('high_latency', 'request_count', 'latency_margin_ms'),
    'error_rate': ('high_error_rate', 'request_count', 'error_margin'),
    'avg_cpu_percent': ('high_cpu', 'host_count', 'cpu_margin'),
    'avg_memory_percent': ('high_memory', 'host_count', 'memory_margin'),
}


@dataclass(frozen=True)
class DetectionConfig:
    mode: str = 'fixed'
    min_requests: int = 20
    min_hosts: int = 3
    min_windows: int = 12
    history_windows: int = 120
    bucket_hours: int = 6
    max_age_ms: int = 7 * 86400_000
    sigma: float = 3.0
    relative_margin: float = 0.2
    latency_margin_ms: float = 50.0
    error_margin: float = 0.05
    cpu_margin: float = 5.0
    memory_margin: float = 5.0
    max_services: int = 256

    def __post_init__(self):
        if self.mode not in ('fixed', 'dynamic'):
            raise ValueError('DETECTION_MODE must be fixed or dynamic')
        limits = {'min_requests': (2, 100000), 'min_hosts': (2, 100000),
                  'min_windows': (3, 1000), 'history_windows': (3, 1000),
                  'bucket_hours': (1, 24), 'max_age_ms': (60000, 90*86400_000),
                  'max_services': (1, 4096)}
        for field, (low, high) in limits.items():
            value = getattr(self, field)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f'DETECTION_{field.upper()} must be an integer in {low}..{high}')
        if self.history_windows < self.min_windows or 24 % self.bucket_hours:
            raise ValueError('history must cover min_windows; bucket_hours must divide 24')
        for field, high in [('sigma', 20), ('relative_margin', 10), ('latency_margin_ms', 60000),
                            ('error_margin', 1), ('cpu_margin', 100), ('memory_margin', 100)]:
            value = getattr(self, field)
            if not number(value) or not 0 < value <= high:
                raise ValueError(f'DETECTION_{field.upper()} must be finite and in (0,{high}]')

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ
        defaults = asdict(cls())
        values = {}
        for field, default in defaults.items():
            key = 'DETECTION_' + field.upper()
            if key in env:
                try:
                    values[field] = type(default)(env[key])
                except (ValueError, TypeError) as error:
                    raise ValueError(f'invalid {key}') from error
        return cls(**values)

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]


ENV_KEYS = {'DETECTION_' + key.upper() for key in asdict(DetectionConfig())}


def fresh_state(config):
    return {'version': VERSION, 'config_id': config.fingerprint, 'last_end': 0, 'buckets': {}}


def evaluate_adaptive(stats, service, start, end, config, state):
    """Mutate bounded state; return all metric decisions plus only actual alerts.

    P95 must use the same histogram as Flink. Overflow is an abstention for
    latency only. Each metric learns independently, from non-overlapping normal
    windows (or unlabelled warmup). A rejected high window never raises its baseline.
    """
    if config.mode != 'dynamic':
        raise ValueError('adaptive evaluator requires dynamic mode')
    if type(start) is not int or type(end) is not int or start < 0 or end <= start:
        raise ValueError('invalid window interval')
    if not isinstance(service, str) or not service or len(service) > 256:
        raise ValueError('invalid service')
    if state.get('version') != VERSION or state.get('config_id') != config.fingerprint:
        raise ValueError('incompatible detection state; back up and explicitly reset it')
    if end <= state['last_end']:
        LOG.warning('detection_out_of_order service=%s window_end=%s', service, end)
        return {'alerts': [], 'decisions': {key: {'status': 'out_of_order'} for key in METRICS}}
    state['last_end'] = end
    # Bound stale history even when the corresponding metric disappears.
    cutoff = end - config.max_age_ms
    for key in list(state['buckets']):
        row = state['buckets'][key]
        row['samples'] = [pair for pair in row['samples'] if pair[0] >= cutoff]
        if not row['samples']:
            del state['buckets'][key]
    bucket = ((end - 1) // 3600_000 % 24) // config.bucket_hours
    result = {'alerts': [], 'decisions': {}}
    for metric, (rule, count_key, margin_key) in METRICS.items():
        value, count = stats.get(metric), stats.get(count_key, 0)
        minimum = config.min_requests if count_key == 'request_count' else config.min_hosts
        detail = {'metric': metric, 'value': value, 'sample_count': count,
                  'minimum_samples': minimum, 'time_bucket_utc': bucket,
                  'baseline_count': 0, 'learned': False}
        result['decisions'][metric] = detail
        if value is None:
            detail['status'] = 'missing_metric'
            continue
        if (not number(value) or value < 0 or
                value > (1e12 if metric == 'p95_latency_ms' else 1 if metric == 'error_rate' else 100) or
                type(count) is not int or count < 0):
            detail.update(status='invalid_metric', value=None, sample_count=None)
            LOG.warning('detection_invalid_summary service=%s metric=%s', service, metric)
            continue
        if count < minimum:
            detail['status'] = 'insufficient_samples'
            continue
        if metric == 'p95_latency_ms' and stats.get('p95_overflow'):
            detail['status'] = 'precision_limited'
            continue
        key = f'{bucket}:{metric}'
        baseline = state['buckets'].setdefault(key, {'samples': [], 'last_train_end': 0})
        samples = baseline['samples']
        detail['baseline_count'] = len(samples)
        detail['baseline_first_end'] = samples[0][0] if samples else None
        detail['baseline_last_end'] = samples[-1][0] if samples else None
        if len(samples) < config.min_windows:
            detail['status'] = 'warming_up'
        else:
            center = median(pair[1] for pair in samples)
            mad = median(abs(pair[1] - center) for pair in samples)
            threshold = max(center + config.sigma * 1.4826 * mad,
                            center * (1 + config.relative_margin), center + getattr(config, margin_key))
            # Histograms use inclusive 25ms upper bounds. Round the latency
            # threshold upwards to a bucket boundary to avoid rounding-only alerts.
            if metric == 'p95_latency_ms':
                threshold = math.ceil(threshold / 25) * 25
            detail.update(median=center, mad=mad, threshold=threshold,
                          status='anomaly' if value > threshold else 'normal')
        if detail['status'] == 'anomaly':
            identity = f'{VERSION}|{config.fingerprint}|{service}|{rule}|{start}|{end}'
            evidence = {k: v for k, v in stats.items() if k not in ('request_count', 'host_count')}
            evidence.update(sample_count=count, threshold=detail['threshold'],
                            detection_mode='dynamic', baseline=dict(detail), config_id=config.fingerprint)
            result['alerts'].append({'schema_version': 1, 'event_type': 'alert',
                'alert_id': hashlib.sha256(identity.encode()).hexdigest()[:24],
                'service': service, 'rule_id': rule, 'rule_version': VERSION,
                'severity': 'warning', 'window_start': start, 'window_end': end,
                'detected_at': int(time.time()*1000), 'evidence': evidence})
            LOG.info('adaptive_anomaly service=%s metric=%s end=%s threshold=%s samples=%s',
                     service, metric, end, detail['threshold'], len(samples))
        elif start >= baseline['last_train_end']:
            samples.append([end, value])
            del samples[:-config.history_windows]
            baseline['last_train_end'] = end
            detail['learned'] = True
    return result
