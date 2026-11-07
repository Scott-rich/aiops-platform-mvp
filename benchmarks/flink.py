"""Bounded workload accounting for the real Flink benchmark (no runtime imports)."""
import hashlib
import math
from flink.aggregate import RULE_VERSION as AGGREGATE_RULE_VERSION

WINDOW_MS = 30000
SLIDE_MS = 10000


def lag_caught_up(value):
    """An unavailable/failed sample is never evidence of zero lag."""
    return type(value) is int and value == 0


def measured_window_alert(value, services):
    """Use the deployed aggregate contract, not the legacy exact-P95 v1 rule."""
    return (value.get('service') in services
            and value.get('rule_version') == AGGREGATE_RULE_VERSION
            and value.get('rule_id') in {'high_error_rate','high_latency'})


def validate_workload(rates, duration, repeats):
    if not rates or len(rates) > 8 or any(not math.isfinite(x) or not 1 <= x <= 1000 for x in rates):
        raise ValueError('one to eight finite rates in 1..1000 required')
    if not math.isfinite(duration) or not 1 <= duration <= 600:
        raise ValueError('duration must be finite in 1..600 seconds')
    if type(repeats) is not int or not 1 <= repeats <= 5:
        raise ValueError('repeats must be in 1..5')
    if min(math.floor(rate*duration) for rate in rates) < 20:
        raise ValueError('every tier must offer at least 20 unique events for the existing detector')
    planned = sum(math.floor(rate*duration) for rate in rates)*repeats
    if planned > 300000:
        raise ValueError('at most 300000 planned unique events per isolated run')
    return planned


def containing_windows(timestamp):
    """The three [start,end) sliding windows containing an event timestamp."""
    newest_start = timestamp//SLIDE_MS*SLIDE_MS
    return [(newest_start-offset, newest_start-offset+WINDOW_MS)
            for offset in range(0, WINDOW_MS, SLIDE_MS)]


def alert_id(service, rule, start, end):
    return hashlib.sha256(f'{service}|{rule}|{start}|{end}'.encode()).hexdigest()[:24]


class WindowLedger:
    """Only IDs and per-window summaries; no unbounded raw telemetry retention."""
    def __init__(self, service, max_events):
        self.service = service
        self.max_events = max_events
        self.ids = set()
        self.windows = {}

    def acknowledge(self, identity, event_time, sent_at_ms):
        if identity in self.ids:
            return False
        if len(self.ids) >= self.max_events:
            raise ValueError('ledger event bound exceeded')
        self.ids.add(identity)
        for start, end in containing_windows(event_time):
            item = self.windows.setdefault((start, end), {'sample_count': 0,
                'first_input_sent_at_ms': sent_at_ms, 'last_input_sent_at_ms': sent_at_ms})
            item['sample_count'] += 1
            item['first_input_sent_at_ms'] = min(item['first_input_sent_at_ms'], sent_at_ms)
            item['last_input_sent_at_ms'] = max(item['last_input_sent_at_ms'], sent_at_ms)
        return True

    def expected(self):
        rows = {}
        for (start, end), value in self.windows.items():
            if value['sample_count'] < 20:
                continue
            for rule in ('high_error_rate', 'high_latency'):
                identity = alert_id(self.service, rule, start, end)
                rows[identity] = {'alert_id': identity, 'service': self.service,
                    'rule_id': rule, 'window_start': start, 'window_end': end, **value}
        return rows


def compare_window(expected, actual):
    return (all(actual.get(k) == expected[k] for k in
                ('alert_id', 'service', 'rule_id', 'window_start', 'window_end'))
            and actual.get('evidence', {}).get('sample_count') == expected['sample_count'])
