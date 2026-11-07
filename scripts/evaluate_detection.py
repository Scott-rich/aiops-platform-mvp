"""Reproducible fixed/dynamic comparison on labelled windows, not a load test.

Labels are evaluation-only and never enter the detector. Abstention counts as
no alert in all-labelled confusion matrices, and is also reported separately.
"""
import argparse
from dataclasses import asdict, replace
from datetime import datetime, timezone
import csv
import hashlib
import json
from pathlib import Path
import platform
import random
import sys
import time
from flink.adaptive import DetectionConfig, METRICS, evaluate_adaptive, fresh_state
from flink.aggregate import WindowAccumulator, evaluate_accumulator, summarize_accumulator
from wire_contract import canonical, validate_metric


def synthetic_windows(seed=42, windows=100):
    rng = random.Random(seed)
    base = 1704067200000  # 2024-01-01 UTC; deliberately historical synthetic data
    families = ('fast-api', 'slow-batch', 'error-burst', 'host-pressure', 'low-traffic', 'drift', 'cold-fault')
    for i in range(windows):
        for name in families:
            # The second segment warms independently and has a different normal level.
            segment = int(i >= windows//2)
            slot = i if not segment else i-windows//2
            start = base + segment*6*3600_000 + slot*30000
            fault = (24 <= slot < 30) or (slot < 5 and name == 'cold-fault')
            slow = name == 'slow-batch'
            latency = (1100 if slow else 120) + segment*(200 if slow else 100) + rng.uniform(-12, 12)
            labels = {metric: False for metric in METRICS}
            error_rate, cpu, memory = 0, 40+segment*10, 45+segment*8
            count = 5 if name == 'low-traffic' else 40
            if name == 'drift':
                latency += slot*7
            if fault:
                if name in ('fast-api', 'slow-batch', 'low-traffic', 'drift', 'cold-fault'):
                    latency += 350 if not slow else 700
                    labels['p95_latency_ms'] = True
                elif name == 'error-burst':
                    error_rate = .15
                    labels['error_rate'] = True
                elif name == 'host-pressure':
                    cpu, memory = 85, 88
                    labels['avg_cpu_percent'] = labels['avg_memory_percent'] = True
            events = []
            for j in range(count):
                events.append({'schema_version': 1, 'event_id': f'{name}-{i}-r{j}', 'service': name,
                    'instance_id': name+'-1', 'event_type': 'request', 'event_time': start+500+j*100,
                    'latency_ms': max(0, latency+rng.uniform(-5,5)), 'status_code': 503 if j < count*error_rate else 200})
            # Missing host measurements are labelled unknown, never zero-imputed.
            if i % 17:
                for j in range(3):
                    events.append({'schema_version': 1, 'event_id': f'{name}-{i}-h{j}', 'service': name,
                        'instance_id': name+'-1', 'event_type': 'host', 'event_time': start+1000+j*100,
                        'cpu_percent': cpu+rng.uniform(-1,1), 'memory_percent': memory+rng.uniform(-1,1)})
            else:
                labels['avg_cpu_percent'] = labels['avg_memory_percent'] = None
            # Same immutable duplicates and arrival reordering for both algorithms.
            events += events[:2]
            rng.shuffle(events)
            yield {'service': name, 'start': start, 'end': start+30000, 'events': events,
                   'labels': labels, 'dataset_kind': 'synthetic-labelled', 'scenario': name}


def read_windows(path):
    with Path(path).open() as source:
        while line := source.readline(8_000_001):
            if len(line) > 8_000_000:
                raise ValueError('input window exceeds 8MB limit')
            row = json.loads(line)
            if type(row['start']) is not int or type(row['end']) is not int or row['end'] <= row['start']:
                raise ValueError('invalid labelled window')
            if set(row['labels']) != set(METRICS) or any(v is not None and type(v) is not bool for v in row['labels'].values()):
                raise ValueError('labels must map all metrics to bool or null')
            for event in row['events']:
                validate_metric(event)
                if event['service'] != row['service'] or not row['start'] <= event['event_time'] < row['end']:
                    raise ValueError('event does not belong to labelled window')
            yield row


def confusion(rows):
    matrix = {'tp': 0, 'fp': 0, 'tn': 0, 'fn': 0}
    abstained = 0
    for row in rows:
        if row['label'] is None:
            continue
        matrix['tp' if row['alert'] and row['label'] else 'fp' if row['alert'] else 'fn' if row['label'] else 'tn'] += 1
        abstained += row['status'] not in ('normal', 'anomaly')
    tp, fp, tn, fn = [matrix[k] for k in ('tp', 'fp', 'tn', 'fn')]
    def ratio(a,b):
        return a/b if b else None
    return {**matrix, 'labelled': tp+fp+tn+fn, 'abstained': abstained,
        'accuracy': ratio(tp+tn, tp+fp+tn+fn), 'precision': ratio(tp,tp+fp),
        'recall': ratio(tp,tp+fn), 'false_positive_rate': ratio(fp,fp+tn),
        'false_negative_rate': ratio(fn,tp+fn), 'decision_coverage': ratio(tp+fp+tn+fn-abstained,tp+fp+tn+fn)}


def evaluate(windows, config=None):
    config = replace(config or DetectionConfig(), mode='dynamic')
    states, records, latencies, episodes, active = {}, [], {'fixed': [], 'dynamic': []}, [], {}
    hasher = hashlib.sha256()
    count = 0
    last_end = {}
    for window in windows:
        service, start, end = window['service'], window['start'], window['end']
        if end <= last_end.get(service, 0):
            raise ValueError('evaluation windows must increase per service')
        last_end[service] = end
        hasher.update((canonical(window)+'\n').encode())
        count += 1
        acc = WindowAccumulator()
        for event in window['events']:
            acc.add(event)
        stats = summarize_accumulator(acc)
        before = time.perf_counter_ns()
        fixed = evaluate_accumulator(acc, service, start, end)
        latencies['fixed'].append((time.perf_counter_ns()-before)/1e6)
        before = time.perf_counter_ns()
        dynamic = evaluate_adaptive(stats, service, start, end, config, states.setdefault(service, fresh_state(config)))
        latencies['dynamic'].append((time.perf_counter_ns()-before)/1e6)
        fixed_rules = {a['rule_id'] for a in fixed}
        for metric, (rule, count_key, _) in METRICS.items():
            label = window['labels'][metric]
            for mode in ('fixed', 'dynamic'):
                decision = dynamic['decisions'][metric] if mode == 'dynamic' else {
                    'status': 'missing_metric' if metric not in stats else
                    'insufficient_samples' if stats[count_key] < (20 if count_key == 'request_count' else 3)
                    else 'anomaly' if rule in fixed_rules else 'normal'}
                alerted = decision['status'] == 'anomaly'
                row = {'mode': mode, 'service': service, 'metric': metric, 'start': start, 'end': end,
                       'label': label, 'alert': alerted, 'status': decision['status'],
                       'value': stats.get(metric), 'threshold': decision.get('threshold'),
                       'baseline_count': decision.get('baseline_count')}
                records.append(row)
                key = (mode, service, metric)
                if label is True:
                    previous = active.get(key)
                    if previous is None or previous['last_end'] != start:
                        previous = {'mode': mode, 'service': service, 'metric': metric,
                                    'onset_event_time': start, 'first_alert_window_end': None}
                        active[key] = previous
                        episodes.append(previous)
                    previous['last_end'] = end
                    if alerted and previous['first_alert_window_end'] is None:
                        previous['first_alert_window_end'] = end
                else:
                    active.pop(key, None)
    if not count:
        raise ValueError('empty evaluation dataset')
    summary = {}
    for mode in ('fixed', 'dynamic'):
        subset = [row for row in records if row['mode'] == mode]
        relevant = [e for e in episodes if e['mode'] == mode]
        delays = [e['first_alert_window_end']-e['onset_event_time'] for e in relevant if e['first_alert_window_end'] is not None]
        summary[mode] = {**confusion(subset),
            'by_metric': {m: confusion([r for r in subset if r['metric']==m]) for m in METRICS},
            'episodes': len(relevant), 'detected_episodes': len(delays), 'missed_episodes': len(relevant)-len(delays),
            'mean_event_time_detection_delay_ms': sum(delays)/len(delays) if delays else None,
            'max_event_time_detection_delay_ms': max(delays) if delays else None,
            'evaluation_compute_ms': sum(latencies[mode]),
            'by_service': {s: confusion([r for r in subset if r['service']==s]) for s in states}}
    return {'schema_version': 1, 'window_count': count, 'dataset_sha256': hasher.hexdigest(),
            'config': asdict(config), 'metrics': summary, 'episodes': episodes}, records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--input', type=Path, help='JSONL labelled windows; omit for explicit synthetic dataset')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    start = time.perf_counter()
    try:
        source = args.input
        if source is None:
            source = args.output/'dataset.jsonl'
            with source.open('w') as dest:
                for row in synthetic_windows(args.seed):
                    dest.write(canonical(row)+'\n')
        result, rows = evaluate(read_windows(source), DetectionConfig.from_env())
        result.update(status='passed', execution_kind='offline_labelled_replay',
            dataset_kind='user_supplied_labels_unverified' if args.input else 'synthetic-labelled', seed=None if args.input else args.seed,
            environment={'python': sys.version, 'platform': platform.platform()},
            created_at_utc=datetime.now(timezone.utc).isoformat(), elapsed_seconds=time.perf_counter()-start,
            limitations=['Synthetic labels are not production accuracy.',
              'Delay is window-end minus labelled onset in event time, not measured Kafka/Flink wall-clock latency.',
              'All labelled abstentions count as no-alert, including cold-start and insufficient traffic.',
              'FastErrorDetector is independent and excluded from this window-detector comparison.',
              'Dynamic and fixed use identical 25ms histogram features; existing exact Python fixed path remains unchanged.',
              'Compute timings are measured evaluator CPU work only, not platform throughput.'])
        with (args.output/'decisions.csv').open('w', newline='') as dest:
            writer = csv.DictWriter(dest, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
        with (args.output/'metrics.csv').open('w', newline='') as dest:
            keys = ['mode','tp','fp','tn','fn','accuracy','precision','recall','false_positive_rate','false_negative_rate','decision_coverage','mean_event_time_detection_delay_ms','missed_episodes']
            writer = csv.DictWriter(dest, fieldnames=keys, extrasaction='ignore'); writer.writeheader()
            for mode, metrics in result['metrics'].items(): writer.writerow({'mode':mode, **metrics})
    except Exception as error:
        result = {'status':'failed','error_type':type(error).__name__, 'error':str(error)}
        (args.output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        raise
    (args.output/'result.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps({mode:{k:v for k,v in metrics.items() if not isinstance(v,dict)} for mode, metrics in result['metrics'].items()},indent=2))


if __name__ == '__main__':
    main()
