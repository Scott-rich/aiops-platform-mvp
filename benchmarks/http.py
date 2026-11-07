"""Real HTTP measurements: business requests and event→durable-ingest acknowledgement."""
import asyncio
from collections import Counter
import json
import logging
import math
import os
from pathlib import Path
import time
import uuid

import httpx
from app.storage import Store
from scripts.evaluate_rca import analyze_input, score_rows
from app.rca import THRESHOLDS, analyze
from .metrics import request_metrics, detection_delays, distribution
from .resources import ProcessSampler, summarize
from .runtime import http_lab

log = logging.getLogger(__name__)


async def paced(rate,duration,concurrency,send):
    offered=math.floor(rate*duration)
    start=time.monotonic();pending=set();rows=[]
    async def one(index,scheduled):
        mark=time.monotonic();row={'index':index,'started_at_ms':int(time.time()*1000),
            'scheduler_delay_ms':max(0,(mark-scheduled)*1000),'status':None,'error':None}
        try:row.update(await send(index))
        except Exception as exc:row['error']=type(exc).__name__
        except asyncio.CancelledError:
            row['error']='cancelled_at_drain_deadline';raise
        finally:
            row.update(elapsed_ms=(time.monotonic()-mark)*1000,ended_at_ms=int(time.time()*1000))
            rows.append(row)
    try:
        for i in range(offered):
            scheduled=start+i/rate
            await asyncio.sleep(max(0,scheduled-time.monotonic()))
            if time.monotonic()>=start+duration:break
            if len(pending)<concurrency:
                task=asyncio.create_task(one(i,scheduled));pending.add(task);task.add_done_callback(pending.discard)
        await asyncio.sleep(max(0,start+duration-time.monotonic()))
        if pending:
            await asyncio.wait_for(asyncio.gather(*list(pending)),8)
    finally:
        tasks=list(pending)
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
    rows.sort(key=lambda row:row['index'])
    return rows,request_metrics(rows,offered,duration,time.monotonic()-start)


def evidence_for_traces(store,traces):
    rows=[]
    for start in range(0,len(traces),400):
        chunk=traces[start:start+400]
        if not chunk:continue
        with store.db() as db:
            rows.extend({'event':json.loads(row[0]),'source':json.loads(row[1]),'ingested_at':row[2]}
                for row in db.execute('SELECT payload,source,ingested_at FROM events WHERE trace_id IN ('+','.join('?' for _ in chunk)+')',chunk))
    return sorted(rows,key=lambda r:(r['event']['event_time'],r['event']['event_id']))


def episode_coverage(records, traces, services):
    """Check the known simulator call graph, not merely a total row count."""
    expected = {(trace, service, kind) for trace in traces for service in services
                for kind in ('request', 'span', 'log')}
    actual = Counter((r['event']['trace_id'], r['event']['service'], r['event']['event_type'])
                     for r in records)
    missing = sorted(expected - actual.keys())
    duplicate = sorted(key for key in expected if actual.get(key, 0) > 1)
    unexpected = sorted(actual.keys() - expected)
    by_key = {(r['event']['trace_id'], r['event']['service'], r['event']['event_type']): r['event'] for r in records}
    relationship_errors = []
    for trace in traces:
        for service in services:
            span = by_key.get((trace, service, 'span'))
            if not span:
                continue
            parent_service = 'order' if service in ('inventory','payment') else 'gateway' if service == 'order' else None
            parent = by_key.get((trace, parent_service, 'span')) if parent_service else None
            if span.get('parent_span_id') != (parent.get('span_id') if parent else None):
                relationship_errors.append([trace, service, 'parent_span'])
            for kind in ('request','log'):
                event = by_key.get((trace, service, kind))
                if event and event.get('span_id') != span.get('span_id'):
                    relationship_errors.append([trace, service, kind+'_span'])
    unique_traces = len(set(traces)) == len(traces)
    complete = bool(traces) and unique_traces and not (missing or duplicate or unexpected or relationship_errors)
    return {'selected': len(records), 'expected_records': len(expected),
            'requested_traces': len(traces), 'observed_traces': len({r['event']['trace_id'] for r in records}),
            'unique_requested_traces': unique_traces, 'expected_services': list(services),
            'missing_observations': [list(x) for x in missing],
            'duplicate_observations': [list(x) for x in duplicate],
            'unexpected_observations': [list(x) for x in unexpected[:50]],
            'unexpected_observation_count': len(unexpected),
            'relationship_errors': relationship_errors,
            'stored_complete': complete, 'truncated': False,
            'completeness_scope': 'expected request/span/log triples for each explicitly generated simulator trace; truncated=false only means this query did not truncate'}


async def wait_episode_evidence(store, traces, services, timeout=3.0):
    if not math.isfinite(timeout) or not 0 <= timeout <= 10:
        raise ValueError('evidence wait must be finite and within 0..10 seconds')
    started = time.monotonic()
    while True:
        records = await asyncio.to_thread(evidence_for_traces, store, traces)
        coverage = episode_coverage(records, traces, services)
        elapsed = time.monotonic() - started
        if coverage['stored_complete'] or elapsed >= timeout:
            coverage.update(wait_seconds=elapsed, wait_timed_out=not coverage['stored_complete'])
            if not coverage['stored_complete']:
                log.error('benchmark_episode_evidence_incomplete expected=%s observed=%s',coverage['expected_records'],len(records))
            return records, coverage
        await asyncio.sleep(min(.05, timeout-elapsed))


def timing_headers(response):
    values = {}
    for key in ('handler-downstream-ms', 'telemetry-publish-ms', 'observed-total-ms'):
        try:
            value = float(response.headers.get('x-'+key, 'nan'))
        except (TypeError, ValueError):
            value = float('nan')
        values[key.replace('-', '_')] = value if math.isfinite(value) and value >= 0 else None
    return values


def quality_episode(name, root, responses, records, coverage):
    expected_status = 502 if root else 200
    response_valid = len(responses) == 6 and all(r['status'] == expected_status and r['trace_matches'] for r in responses)
    metrics = [r['event'] for r in records if r['event']['event_type'] in ('request', 'span')]
    invalid_sli = [e['event_id'] for e in metrics
                   if e.get('status_code', 0) >= 400 or e.get('latency_ms', 0) > THRESHOLDS['latency_ms']]
    data_valid = coverage['stored_complete'] and response_valid
    control_status = 'not_a_healthy_control' if root else (
        'invalid_input' if not data_valid else 'degraded' if invalid_sli else 'valid_healthy')
    if not root and control_status != 'valid_healthy':
        log.warning('benchmark_healthy_control_unavailable status=%s unhealthy_records=%s',control_status,len(invalid_sli))
    snapshot = {'records': records, 'coverage': coverage}
    row = {'case_id': name, 'source_kind': 'actual_controlled_http',
           'labels': {'expected_status': 'candidates' if root else 'unknown',
                      'root_services': [root] if root else [], 'healthy': root is None},
           'responses': responses, 'responses_match_expected': response_valid,
           'expected_gateway_status': expected_status, 'input_complete': coverage['stored_complete'],
           'control': {'status': control_status, 'max_latency_ms': max((e.get('latency_ms', 0) for e in metrics), default=None),
                       'latency_limit_ms': THRESHOLDS['latency_ms'], 'unhealthy_evidence_ids': invalid_sli,
                       'definition': 'No injection is the planned label, not proof of healthy observed SLI. A degraded control remains visible and blocks healthy acceptance.'}}
    row.update(analyze_input(analyze, snapshot))
    row['snapshot'] = snapshot
    return row


def quality_checks(quality):
    rows = quality['cases']
    healthy = next(r for r in rows if r['labels']['healthy'])
    control_valid = healthy['control']['status'] == 'valid_healthy'
    data_valid = all(r['input_complete'] and r['responses_match_expected'] for r in rows)
    return [
        {'name': 'quality_episode_http_status_and_trace_identity', 'passed': all(r['responses_match_expected'] for r in rows)},
        {'name': 'quality_episode_complete_trace_evidence', 'passed': all(r['input_complete'] for r in rows)},
        {'name': 'live_5xx_episodes_detected', 'passed': len(quality['detection']) == 2 and all(d['detected'] for d in quality['detection'])},
        {'name': 'live_rca_references_exist', 'passed': all(r['references_valid'] for r in rows)},
        {'name': 'live_rca_labelled_roots_top1', 'passed': quality['rca_metrics']['top1_any_labelled_root_accuracy']['value'] == 1},
        {'name': 'healthy_control_valid', 'passed': control_valid,
         'status': 'passed' if control_valid else 'blocked' if data_valid else 'failed'},
        {'name': 'healthy_episode_unknown', 'passed': control_valid and healthy['predicted_status'] == 'unknown',
         'status': 'blocked' if not control_valid and data_valid else 'passed' if control_valid and healthy['predicted_status'] == 'unknown' else 'failed'}]


def status_for_checks(checks):
    if any(not c['passed'] and c.get('status') != 'blocked' for c in checks):
        return 'failed'
    return 'blocked' if any(not c['passed'] for c in checks) else 'passed'


async def live_quality(lab,store):
    rows=[];detection=[]
    for name,root in [('healthy',None),('payment_5xx','payment'),('inventory_5xx','inventory')]:
        onset=int(time.time()*1000)
        # Healthy requests reset the existing fast detector between independently labelled episodes.
        for _ in range(2):
            response = await lab.client.get(lab.urls['gateway']+'/work')
            response.raise_for_status()
        if root:
            response=await lab.client.post(lab.urls[root]+'/fault',json={'kind':'error','duration_seconds':10},headers={'x-simulator-token':lab.token})
            response.raise_for_status();onset=response.json().get('started_at_ms',int(time.time()*1000))
        traces=[];responses=[]
        try:
            for _ in range(6):
                trace = uuid.uuid4().hex;traces.append(trace)
                response=await lab.client.get(lab.urls['gateway']+'/work', headers={'x-trace-id': trace})
                responses.append({'trace_id': trace, 'status': response.status_code,
                                  'trace_matches': response.headers.get('x-trace-id') == trace,
                                  **timing_headers(response)})
            # Inventory failure short-circuits order before payment is called.
            services = ('gateway','order','inventory') if root == 'inventory' else ('gateway','order','inventory','payment')
            events, coverage = await wait_episode_evidence(store,traces,services)
            rows.append(quality_episode(name,root,responses,events,coverage))
            metrics=[r['event'] for r in events if r['event']['event_type']=='request' and r['event']['service']==root and r['event']['status_code']>=500]
            if root:
                alerts=(await lab.client.get(lab.urls['api']+'/alerts')).json()
                ids={e['event_id'] for e in metrics}
                matching=[a for a in alerts if a['service']==root and a.get('rule_id')=='fast_errors'
                          and set(a.get('evidence',{}).get('event_ids',[])) & ids]
                detection.append({'case_id':name,'lane':'fast-consecutive-5xx','root':root,
                    **detection_delays(matching,onset,min((e['event_time'] for e in metrics),default=None)),
                    'alert_ids':[a['alert_id'] for a in matching]})
        finally:
            if root:
                response=await lab.client.post(lab.urls[root]+'/fault',json={'kind':'none'},headers={'x-simulator-token':lab.token})
                response.raise_for_status()
    valid_labels = [row for row in rows if not row['labels']['healthy'] or row['control']['status'] == 'valid_healthy']
    return {'source_kind':'actual_controlled_http_injections','cases':rows,'rca_metrics':score_rows(rows),
            'validated_control_metrics':score_rows(valid_labels),
            'label_coverage':{'planned_cases':len(rows),'validated_control_cases':len(valid_labels),
                              'degraded_or_invalid_controls':[r['case_id'] for r in rows if r not in valid_labels]},
            'metric_scope':'rca_metrics retains every planned label, including degraded controls; validated_control_metrics excludes only explicitly invalid healthy labels. No execution error or fault case is removed. A missing valid healthy control blocks acceptance.',
            'detection':detection,'fault_to_alert_ms':distribution(r['fault_to_first_alert_ms'] for r in detection),
            'limitations':['Only two injected root services and one healthy case; not a production accuracy estimate.',
                           'Live detection delay measures fast 5xx lane. Window detection delays are reported separately by labelled replay.']}


async def run_http(output,business_rates,ingest_rates,duration,repeats,concurrency):
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    result={'status':'running','kind':'real_loopback_http','tiers':[]}
    try:
        async with http_lab(output/'runtime') as lab:
            store=Store(output/'runtime/data')
            sampler=ProcessSampler({name:p.pid for name,p in lab.children.items()})
            raw=(output/'requests.jsonl').open('w');resource_file=(output/'resources.jsonl').open('w')
            try:
                # Exclude startup and explicit warmup traffic from measured stages.
                for _ in range(2):(await lab.client.get(lab.urls['gateway']+'/work')).raise_for_status()
                for kind,rates in [('business_http',business_rates),('event_ingest_http',ingest_rates)]:
                    for repeat in range(repeats):
                        for rate in rates:
                            stage_id=f'{kind}-r{repeat}-{rate:g}';samples=[];stop=asyncio.Event();sampler.previous.clear()
                            async def sample():
                                while not stop.is_set():
                                    value=sampler.sample();samples.append(value)
                                    resource_file.write(json.dumps({'stage_id':stage_id,**value})+'\n')
                                    try:await asyncio.wait_for(stop.wait(),.2)
                                    except asyncio.TimeoutError:pass
                            task=asyncio.create_task(sample());prefix=uuid.uuid4().hex
                            async def send(index):
                                trace=uuid.uuid5(uuid.NAMESPACE_URL,f'{prefix}/{index}').hex
                                if kind=='business_http':
                                    response=await lab.client.get(lab.urls['gateway']+'/work',headers={'x-trace-id':trace})
                                else:
                                    event={'schema_version':1,'event_type':'request','event_id':f'{prefix}-{index}',
                                        'service':'benchmark-input','instance_id':'benchmark-1','trace_id':trace,
                                        'event_time':int(time.time()*1000),'latency_ms':10,'status_code':200}
                                    response=await lab.client.post(lab.urls['api']+'/demo/ingest',json={'topic':'telemetry.metrics','event':event})
                                return {'status':response.status_code,'trace_id':trace, **timing_headers(response)}
                            try:
                                rows,stats=await paced(rate,duration,concurrency,send)
                            finally:
                                stop.set();await task
                            records=await asyncio.to_thread(evidence_for_traces,store,[r['trace_id'] for r in rows if r.get('trace_id') and r.get('status') is not None and 200<=r['status']<300])
                            delay=[r['ingested_at']-r['event']['event_time'] for r in records]
                            expected=stats['successes']*(12 if kind=='business_http' else 1)
                            tier={'stage_id':stage_id,'kind':kind,'repeat':repeat,'target_per_s':rate,**stats,
                                  'persisted_events':len(records),'expected_success_events':expected,
                                  'all_success_events_present':len(records)==expected,
                                  'observed_events_per_s_including_drain':len(records)/stats['elapsed_with_drain_s'],
                                  'event_to_ingestion_timestamp_ms':distribution(delay),'resources':summarize(samples),
                                  'timing_components':{key:distribution(r.get(key) for r in rows) for key in ('handler_downstream_ms','telemetry_publish_ms','observed_total_ms')},
                                  'latency_scope':'business request including synchronous telemetry' if kind=='business_http' else 'event creation and HTTP send to durable ingest acknowledgement'}
                            for row in rows:raw.write(json.dumps({'stage_id':stage_id,**row})+'\n')
                            result['tiers'].append(tier)
                            print(json.dumps({'http_stage':stage_id,'completed':stats['started'],'failed':stats['failed'],'skipped':stats['load_generator_skipped']}),flush=True)
                result['live_quality']=await live_quality(lab,store)
            finally:raw.close();resource_file.close()
        result['cleanup']=lab.result['cleanup']
        result['checks']=[{'name':t['stage_id']+':successful_trace_events_persisted','passed':t['all_success_events_present']} for t in result['tiers']]
        result['checks'] += [{'name':t['stage_id']+':cpu_and_rss_measured','passed':t['resources']['status']=='measured'} for t in result['tiers']]
        quality=result['live_quality']
        result['checks'] += quality_checks(quality)
        result['status']=status_for_checks(result['checks'])
    except BaseException as exc:
        result.update(status='failed',error_type=type(exc).__name__,error=str(exc))
        raise
    finally:
        if 'lab' in locals():result['cleanup']=lab.result.get('cleanup')
        (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    return result
