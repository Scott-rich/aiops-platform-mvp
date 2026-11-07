"""Real Kafka -> Flink -> Kafka -> SQLite benchmark in owned local processes.

Only the final tail uses explicit watermark-control events. Tail latency is
reported separately from windows observed before that control flush. This is a
bounded research measurement, not a production capacity/SLO or Docker test.
"""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time
import traceback
import uuid

from benchmarks.flink import WindowLedger, compare_window, lag_caught_up, measured_window_alert, validate_workload
from benchmarks.metrics import distribution
from benchmarks.resources import ProcessSampler, environment, summarize
from scripts.native_streaming import NativeAcceptance, http_json, metric

ROOT = Path(__file__).resolve().parents[1]


class FlinkBenchmark(NativeAcceptance):
    def __init__(self, runtime, output, rates, duration, repeats):
        planned = validate_workload(rates, duration, repeats)
        super().__init__(runtime, output)
        self.rates, self.duration, self.repeats = rates, duration, repeats
        self.results.update(execution_kind='real_native_kafka_flink_benchmark',
            benchmark_version='flink-end-to-end-v1', flink_engine=True,
            environment=environment(), tiers=[], window_results=[], lag_samples=[])
        self.results['configuration'].update(rates=rates, duration_seconds=duration,
            repeats=repeats, planned_unique_events=planned, duplicate_every=20,
            maximum_planned_unique_events=300000,
            final_watermark_control=True, natural_detection_delay_scope='windows observed before final control flush')
        self.results['limitations'] += [
            'Window latency is from first/last acknowledged input send to durable window alert; it is not a per-request latency percentile.',
            'Only the final tail is forced closed using watermark-control events; forced-tail latencies are not natural production detection latency.',
            'Kafka lag uses checkpoint-committed source offsets and may overstate in-flight backlog until the next checkpoint.',
            'CPU/RSS cover owned broker, JobManager, TaskManager, worker and API PIDs; separately spawned PyFlink/Java descendants are not included.',
            'Short multi-rate runs do not prove capacity or dedup TTL/capacity safety; use --duration 310 for an explicitly longer run.']
        self.results['limitations']=[value for value in self.results['limitations']
            if not value.startswith('Test input includes explicitly synthetic telemetry; HTTP scenario')]
        self.results['limitations'].append('Synthetic telemetry uses actual Kafka/Flink/worker processes; this benchmark does not execute the separate business-HTTP fault scenario.')
        self.ledgers, self.durable, self.before_flush_ids = {}, {}, set()
        self.resource_samples = []
        self.sampler = None
        self.next_sample = 0
        self.next_lag_sample = 0
        self.input_log = self.resource_log = None
        self.results['source_sha256'].update({str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [Path(__file__), ROOT/'benchmarks/flink.py', ROOT/'benchmarks/resources.py']})

    def observe(self):
        """Nonblocking Kafka collection and bounded SQLite reads, outside send accounting."""
        for records in self.consumer.poll(timeout_ms=0, max_records=2000).values():
            for record in records:
                payload = json.loads(record.value)
                self.results['output_records'].append({'topic': record.topic, 'partition': record.partition,
                    'offset': record.offset, 'received_at_ms': int(time.time()*1000), 'payload': payload})
                if len(self.results['output_records']) > 20000:
                    raise RuntimeError('bounded output accounting exceeded; retain logs and reduce workload')
        if self.sampler is not None and time.monotonic() >= self.next_sample:
            sample = self.sampler.sample()
            self.resource_samples.append(sample)
            self.resource_log.write(json.dumps(sample)+'\n')
            self.next_sample = time.monotonic()+.5
        if time.monotonic() >= self.next_lag_sample:
            self.source_lag('periodic')
            self.next_lag_sample = time.monotonic()+2
        path = self.output/'app-data/aiops.sqlite3'
        try:
            with sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=.1) as db:
                rows = db.execute('SELECT payload,source,received_at FROM alerts').fetchall()
            for payload, source, received in rows:
                value = json.loads(payload)
                if measured_window_alert(value,self.ledgers):
                    self.durable[value['alert_id']] = {'alert': value, 'source': json.loads(source),
                                                      'persisted_at_ms': received}
        except sqlite3.OperationalError:
            # The next bounded drain poll retries. A timeout cannot become passed.
            return

    def input_counts(self):
        if not self.ledgers:
            return {}
        path = self.output/'app-data/aiops.sqlite3'
        try:
            with sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=.1) as db:
                names=list(self.ledgers)
                return dict(db.execute('SELECT service,COUNT(*) FROM events WHERE service IN ('+
                                       ','.join('?' for _ in names)+') GROUP BY service',names))
        except sqlite3.OperationalError:
            return {}

    def source_lag(self, stage):
        from kafka import TopicPartition
        from kafka.errors import KafkaError
        partitions = [TopicPartition('telemetry.metrics', i) for i in range(3)]
        errors=[]
        for attempt in range(1,4):
            row = {'stage': stage, 'observed_at_ms': int(time.time()*1000),
                   'kind': 'checkpoint_committed_offset_lag', 'attempts':attempt,
                   'retries_performed':attempt-1, 'transient_errors':list(errors)}
            try:
                # The admin call rediscovers the group coordinator on each retry.
                ends = self.consumer.end_offsets(partitions)
                committed = self.admin.list_consumer_group_offsets('aiops-flink-detector-v3')
                values = [{'partition': p.partition, 'end_offset': ends[p],
                           'checkpoint_committed_offset': max(0, committed[p].offset) if p in committed else 0}
                          for p in partitions]
                total = sum(max(0, v['end_offset']-v['checkpoint_committed_offset']) for v in values)
                row.update(available=True,lag=total,partitions=values)
                self.results['lag_samples'].append(row)
                return total
            except Exception as error:
                transient=isinstance(error,KafkaError) and error.retriable
                if not transient:
                    row.update(available=False,lag=None,error_type=type(error).__name__,transient=False)
                    self.results['lag_samples'].append(row)
                    raise
                errors.append(type(error).__name__)
                if attempt==3:
                    row.update(available=False,lag=None,error_type=type(error).__name__,
                               transient=True,transient_errors=list(errors))
                    self.results['lag_samples'].append(row)
                    return None
                time.sleep(.1*attempt)

    def produce(self, event, stage_id, duplicate=False):
        before = int(time.time()*1000)
        wire = json.dumps(event, separators=(',', ':')).encode()
        try:
            record = self.producer.send('telemetry.metrics', value=wire,
                partition=int(event['event_id'].rsplit('-', 1)[1]) % 3).get(timeout=15)
            row = {'stage_id': stage_id, 'event_id': event['event_id'], 'event_time': event['event_time'],
                   'sent_at_ms': before, 'acknowledged_at_ms': int(time.time()*1000),
                   'duplicate': duplicate, 'partition': record.partition, 'offset': record.offset,
                   'acknowledged': True}
        except Exception as error:
            row = {'stage_id': stage_id, 'event_id': event['event_id'], 'sent_at_ms': before,
                   'duplicate': duplicate, 'acknowledged': False, 'error_type': type(error).__name__}
        self.input_log.write(json.dumps(row)+'\n')
        return row

    def tier(self, rate, repeat, ordinal):
        stage = f'flink-r{repeat}-{rate:g}-{ordinal}'
        service = stage+'-'+uuid.uuid4().hex[:8]
        offered = math.floor(rate*self.duration)
        ledger = WindowLedger(service, offered)
        self.ledgers[service] = ledger
        result = {'stage_id': stage, 'service': service, 'target_unique_events_per_s': rate,
            'offered': offered, 'started': 0, 'producer_acknowledged_unique': 0,
            'producer_acknowledged_duplicates': 0, 'producer_failed': 0,
            'input_id_prefix': service+'-', 'first_input_id': None, 'last_input_id': None}
        self.results['tiers'].append(result)
        self.source_lag(stage+'-before')
        samples_at = len(self.resource_samples)
        start = time.monotonic()
        delays = []
        for index in range(offered):
            scheduled = start+index/rate
            while time.monotonic() < scheduled:
                self.observe()
                time.sleep(min(.02, max(0, scheduled-time.monotonic())))
            if time.monotonic() >= start+self.duration:
                break
            delays.append(max(0, (time.monotonic()-scheduled)*1000))
            event = metric(f'{service}-{index:08d}', int(time.time()*1000), service, 503, 1001)
            result['started'] += 1
            value = self.produce(event, stage)
            if value['acknowledged']:
                result['producer_acknowledged_unique'] += 1
                ledger.acknowledge(event['event_id'], event['event_time'], value['sent_at_ms'])
                result['first_input_id'] = result['first_input_id'] or event['event_id']
                result['last_input_id'] = event['event_id']
                if (index+1) % 20 == 0:
                    duplicate = self.produce(event, stage, duplicate=True)
                    result['producer_acknowledged_duplicates'] += int(duplicate['acknowledged'])
                    result['producer_failed'] += int(not duplicate['acknowledged'])
            else:
                result['producer_failed'] += 1
            self.observe()
        while time.monotonic() < start+self.duration:
            self.observe();time.sleep(.02)
        send_elapsed = time.monotonic()-start
        self.producer.flush(timeout=15)
        result.update(load_generator_skipped=offered-result['started'], send_window_seconds=send_elapsed,
            actual_unique_send_per_s=result['producer_acknowledged_unique']/send_elapsed,
            scheduler_delay_ms=distribution(delays), resources=summarize(self.resource_samples[samples_at:]),
            expected_eligible_window_alerts=len(ledger.expected()))
        result['checkpoint_lag_after_send'] = self.source_lag(stage+'-after-send')
        self.mark('benchmark_tier_sent', stage_id=stage, acknowledged=result['producer_acknowledged_unique'],
                  duplicates=result['producer_acknowledged_duplicates'], failed=result['producer_failed'],
                  skipped=result['load_generator_skipped'])

    def scenario(self):
        self.input_log = (self.output/'inputs.jsonl').open('w')
        self.resource_log = (self.output/'resources.jsonl').open('w')
        self.sampler = ProcessSampler({name: p.pid for name, p in self.processes})
        self.observe()
        for repeat in range(self.repeats):
            for ordinal, rate in enumerate(self.rates):
                self.tier(rate, repeat, ordinal)
        self.observe()
        self.before_flush_ids = set(self.durable)
        expected = {key: value for ledger in self.ledgers.values() for key, value in ledger.expected().items()}
        self.check('each_tier_has_measurable_detector_windows',
                   bool(expected) and all(ledger.expected() for ledger in self.ledgers.values()))
        self.results['final_flush_started_at_ms'] = int(time.time()*1000)
        watermark = max(value['window_end'] for value in expected.values())+5001 if expected else int(time.time()*1000)+35001
        self.advance_watermark(watermark)
        self.results['final_control_watermark_event_time'] = watermark
        deadline = time.monotonic()+120
        while time.monotonic() < deadline:
            self.observe()
            sink_ids = {row['payload'].get('alert_id') for row in self.results['output_records'] if row['topic']=='ops.alerts'}
            counts=self.input_counts()
            if (set(expected) <= self.durable.keys() and set(expected) <= sink_ids
                    and all(counts.get(name,0)==len(ledger.ids) for name,ledger in self.ledgers.items())):
                break
            time.sleep(.1)
        self.check('expected_windows_reach_flink_sink_and_durable_store', bool(expected)
                   and set(expected) <= self.durable.keys() and set(expected) <= sink_ids,
                   expected=len(expected), durable_expected=len(set(expected) & self.durable.keys()))
        window_rows=[]
        for identity, value in sorted(expected.items()):
            stored=self.durable[identity]
            row={**value, 'persisted_at_ms':stored['persisted_at_ms'],
                 'observed_before_final_flush':identity in self.before_flush_ids,
                 'matches_expected':compare_window(value,stored['alert']),
                 'first_input_to_persist_ms':stored['persisted_at_ms']-value['first_input_sent_at_ms'],
                 'last_input_to_persist_ms':stored['persisted_at_ms']-value['last_input_sent_at_ms']}
            window_rows.append(row)
        self.results['window_results']=window_rows
        self.check('all_window_counts_and_ids_match_unique_inputs', all(row['matches_expected'] for row in window_rows))
        self.check('window_latency_timestamps_are_valid',all(row['first_input_to_persist_ms']>=0
                   and row['last_input_to_persist_ms']>=0 for row in window_rows))
        self.check('duplicates_sent_without_inflating_window_counts',
                   all(t['producer_acknowledged_duplicates']>0 for t in self.results['tiers']))
        self.check('no_unexpected_benchmark_window_alerts', set(self.durable)==set(expected))
        self.check('no_valid_input_quarantined',not any(row['topic']=='ops.deadletter' for row in self.results['output_records']))
        self.check('window_alerts_have_kafka_sink_provenance',all(self.durable[key]['source'].get('topic')=='ops.alerts' for key in expected))
        with sqlite3.connect(f'file:{self.output / "app-data/aiops.sqlite3"}?mode=ro',uri=True,timeout=3) as db:
            for tier in self.results['tiers']:
                tier['unique_inputs_persisted']=db.execute('SELECT COUNT(*) FROM events WHERE service=?',(tier['service'],)).fetchone()[0]
                tier['persisted_unique_window_alerts']=sum(row['service']==tier['service'] for row in window_rows)
                selected=[r for r in window_rows if r['service']==tier['service']]
                tier['window_first_input_to_persist_ms']=distribution(r['first_input_to_persist_ms'] for r in selected)
                tier['window_last_input_to_persist_ms']=distribution(r['last_input_to_persist_ms'] for r in selected)
                tier['natural_window_last_input_to_persist_ms']=distribution(r['last_input_to_persist_ms'] for r in selected if r['observed_before_final_flush'])
                tier['controlled_tail_last_input_to_persist_ms']=distribution(r['last_input_to_persist_ms'] for r in selected if not r['observed_before_final_flush'])
                tier['natural_window_last_input_to_persist_p95_ms']=tier['natural_window_last_input_to_persist_ms']['p95']
                tier['controlled_tail_last_input_to_persist_p95_ms']=tier['controlled_tail_last_input_to_persist_ms']['p95']
                tier['forced_tail_window_alerts']=sum(not r['observed_before_final_flush'] for r in selected)
                tier['first_input_sent_at_ms']=min(r['first_input_sent_at_ms'] for r in selected)
                tier['all_windows_persisted_at_ms']=max(r['persisted_at_ms'] for r in selected)
                tier['elapsed_first_send_to_all_windows_persisted_seconds']=(tier['all_windows_persisted_at_ms']
                    -tier['first_input_sent_at_ms'])/1000
                tier['verified_unique_events_per_s_including_window_completion']=(tier['producer_acknowledged_unique']
                    /tier['elapsed_first_send_to_all_windows_persisted_seconds'])
        completed_seconds=(max(r['persisted_at_ms'] for r in window_rows)-min(r['first_input_sent_at_ms'] for r in window_rows))/1000
        self.results['verified_unique_events_per_s_including_window_completion']=(sum(len(v.ids) for v in self.ledgers.values())/completed_seconds)
        self.results['input_to_last_window_completion_seconds']=completed_seconds
        self.check('unique_inputs_persist_once',all(t['unique_inputs_persisted']==t['producer_acknowledged_unique'] for t in self.results['tiers']))
        self.check('no_producer_failures',all(t['producer_failed']==0 for t in self.results['tiers']))
        self.check('cpu_and_rss_measured',all(t['resources']['status']=='measured' for t in self.results['tiers']))
        deadline=time.monotonic()+40
        lag=self.source_lag('final-drain')
        while not lag_caught_up(lag) and time.monotonic()<deadline:
            self.observe();time.sleep(.5);lag=self.source_lag('final-drain')
        self.results['checkpoint_committed_lag_after']=lag
        self.check('checkpoint_committed_offsets_caught_up',lag_caught_up(lag))
        self.check('flink_job_still_running',http_json(self.rest+f'/jobs/{self.job_id}')['state']=='RUNNING')
        self.results['actual_stage_over_300_seconds']=any(t['send_window_seconds']>300 for t in self.results['tiers'])
        self.results['dedup_expiration_replay_tested']=False
        self.results['status']='passed'

    def close(self):
        for stream in (self.input_log,self.resource_log):
            if stream:stream.close()
        super().close()
        fields=['stage_id','target_unique_events_per_s','offered','started','producer_acknowledged_unique',
                'producer_acknowledged_duplicates','producer_failed','load_generator_skipped','actual_unique_send_per_s',
                'unique_inputs_persisted','expected_eligible_window_alerts','persisted_unique_window_alerts','forced_tail_window_alerts',
                'first_input_sent_at_ms','all_windows_persisted_at_ms',
                'elapsed_first_send_to_all_windows_persisted_seconds','verified_unique_events_per_s_including_window_completion',
                'natural_window_last_input_to_persist_p95_ms','controlled_tail_last_input_to_persist_p95_ms']
        with (self.output/'metrics.csv').open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(self.results['tiers'])
        with (self.output/'completed.json').open('x') as stream:
            json.dump(self.results,stream,ensure_ascii=False,indent=2);stream.flush();os.fsync(stream.fileno())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-dir',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--rates',default='20,60,120')
    parser.add_argument('--duration',type=float,default=5)
    parser.add_argument('--repeats',type=int,default=1)
    args=parser.parse_args()
    try:
        rates=[float(x) for x in args.rates.split(',')];validate_workload(rates,args.duration,args.repeats)
    except ValueError as error:parser.error(str(error))
    os.environ.update(DETECTION_MODE='fixed',API_AUTH_MODE='local',FLINK_WINDOW_MS='30000',
        FLINK_SLIDE_MS='10000',FLINK_OUT_OF_ORDER_MS='5000',FLINK_IDLE_MS='10000',
        FLINK_METRICS_TOPIC='telemetry.metrics',FLINK_CONSUMER_GROUP='aiops-flink-detector-v3',
        FLINK_JOB_NAME='aiops-event-time-detector-v3',FLINK_DEDUP_TTL_MS='300000',
        FLINK_DEDUP_BUCKETS='64',FLINK_DEDUP_BUCKET_CAPACITY='1000')
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    test=FlinkBenchmark(args.runtime_dir.resolve(),args.output.resolve(),rates,args.duration,args.repeats)
    try:
        test.setup();test.scenario()
    except BaseException as error:
        test.results.update(status='failed',error={'type':type(error).__name__,'message':str(error),'traceback':traceback.format_exc()})
    finally:test.close()
    print(json.dumps({'status':test.results['status'],'checks':test.results['check_counts'],'output':str(test.output)}))
    return 0 if test.results['status']=='passed' else 1

if __name__=='__main__':sys.exit(main())
