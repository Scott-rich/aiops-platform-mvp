"""Explicit metric definitions shared by live tests and report generation."""
import math


def distribution(values):
    values=list(values)
    valid=sorted(float(v) for v in values if isinstance(v,(int,float)) and not isinstance(v,bool)
                 and math.isfinite(v) and v>=0)
    def percentile(p):
        return valid[max(0,math.ceil(p*len(valid))-1)] if valid else None
    return {'count':len(valid),'missing_or_invalid':len(values)-len(valid),
            'mean':sum(valid)/len(valid) if valid else None,'p50':percentile(.50),
            'p95':percentile(.95),'p99':percentile(.99),'max':valid[-1] if valid else None}


def ratio(numerator,denominator):
    return numerator/denominator if denominator else None


def request_metrics(rows, offered, duration, elapsed):
    if duration<=0 or elapsed<=0 or offered<len(rows):raise ValueError('invalid workload accounting')
    successful=[r for r in rows if r.get('status') is not None and 200<=r['status']<300]
    return {'offered':offered,'started':len(rows),'load_generator_skipped':offered-len(rows),
            'successes':len(successful),'failed':len(rows)-len(successful),
            'success_rate_of_started':ratio(len(successful),len(rows)),
            'success_rate_of_offered':ratio(len(successful),offered),
            'offered_per_s':offered/duration,'completed_per_s_including_drain':len(rows)/elapsed,
            'successful_per_s_including_drain':len(successful)/elapsed,
            'send_window_s':duration,'elapsed_with_drain_s':elapsed,
            'all_attempt_latency_ms':distribution(r.get('elapsed_ms') for r in rows),
            'successful_latency_ms':distribution(r.get('elapsed_ms') for r in successful),
            'scheduler_delay_ms':distribution(r.get('scheduler_delay_ms') for r in rows)}


def detection_delays(alerts,onset_ms,first_error_ms=None):
    # Never invent zero delay for a missed episode or hide a negative clock delta.
    times=[a['detected_at'] for a in alerts if isinstance(a.get('detected_at'),int) and a['detected_at']>=onset_ms]
    first=min(times) if times else None
    return {'detected':first is not None,'fault_started_at_ms':onset_ms,'first_alert_at_ms':first,
            'fault_to_first_alert_ms':first-onset_ms if first is not None else None,
            'first_error_to_alert_ms':first-first_error_ms if first is not None and first_error_ms is not None and first>=first_error_ms else None}


def comparable(a,b):
    return {'same_protocol':bool(a.get('benchmark_version')) and a.get('benchmark_version')==b.get('benchmark_version'),
            'same_configuration':bool(a.get('configuration_id')) and a.get('configuration_id')==b.get('configuration_id'),
            'same_environment':bool(a.get('environment',{}).get('hardware_id')) and a.get('environment',{}).get('hardware_id')==b.get('environment',{}).get('hardware_id'),
            'same_source':bool(a.get('source_sha256')) and a.get('source_sha256')==b.get('source_sha256')}
