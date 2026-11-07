"""Real Kafka rate sweeps; a dedicated consumer thread runs production persistence."""
import json
import math
import os
from pathlib import Path
import threading
import time
import uuid

from .metrics import distribution
from .resources import ProcessSampler, summarize
from .runtime import KafkaBroker


def drain_confirmed(observation,expected_end):
    # Require broker-visible confirmation. A newer worker-local commit flag cannot
    # turn an earlier lag sample into a zero-lag observation.
    return observation['committed_offset']>=expected_end and observation['lag']==0


def run_kafka(output,home,rates,duration,repeats,consumer_delay_ms=0):
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    result={'kind':'real_kafka_python_pipeline','flink_engine':False,'status':'running','tiers':[]}
    if not home:
        result.update(status='blocked',reason='No --kafka-home configured; no Kafka measurements fabricated')
        (output/'result.json').write_text(json.dumps(result,indent=2));return result
    from kafka import KafkaProducer,KafkaConsumer,TopicPartition
    from kafka.admin import NewTopic
    from app.engine import Pipeline
    from app.storage import Store
    from app.worker import persist_batch
    broker=KafkaBroker(home,output/'broker')
    producer=observer=None;worker_thread=None;stop=threading.Event();ready=threading.Event()
    lock=threading.Lock();receipts={};worker_errors=[];progress={'committed':0}
    topic='telemetry.metrics';samples=[];lag_samples=[]
    try:
        broker.start();broker.admin.create_topics([NewTopic(topic,1,1)],timeout_ms=10000)
        tp=TopicPartition(topic,0);group='phase6-'+uuid.uuid4().hex
        def consume():
            consumer=pipeline=None
            try:
                consumer=KafkaConsumer(bootstrap_servers=broker.bootstrap,group_id=group,enable_auto_commit=False,
                    auto_offset_reset='earliest',max_poll_records=50,request_timeout_ms=10000,session_timeout_ms=6000)
                consumer.assign([tp]);consumer.seek(tp,0)
                pipeline=Pipeline(Store(output/'worker-data'));ready.set()
                while not stop.is_set():
                    batches=consumer.poll(timeout_ms=100)
                    if not batches:continue
                    if consumer_delay_ms and stop.wait(consumer_delay_ms/1000):break
                    persist_batch(pipeline,batches)
                    completed_ns=time.monotonic_ns();completed_ms=int(time.time()*1000)
                    with lock:
                        for records in batches.values():
                            for record in records:
                                event=json.loads(record.value)
                                receipts[event['event_id']]={'event_id':event['event_id'],'completed_ns':completed_ns,
                                    'completed_at_ms':completed_ms,'offset':record.offset,'kafka_timestamp_ms':record.timestamp}
                    consumer.commit()
                    with lock:progress['committed']=consumer.position(tp)
            except BaseException as exc:
                worker_errors.append(type(exc).__name__+': '+str(exc));ready.set()
            finally:
                if consumer:consumer.close(autocommit=False)
                if pipeline:pipeline.close()
        worker_thread=threading.Thread(target=consume,name='benchmark-kafka-worker',daemon=False);worker_thread.start()
        if not ready.wait(10) or worker_errors:raise RuntimeError('worker startup: '+str(worker_errors))
        producer=KafkaProducer(bootstrap_servers=broker.bootstrap,acks='all',retries=2,max_block_ms=1000,
            request_timeout_ms=5000,max_in_flight_requests_per_connection=1,buffer_memory=4*1024**2)
        observer=KafkaConsumer(bootstrap_servers=broker.bootstrap,group_id=group,enable_auto_commit=False,
            request_timeout_ms=5000,session_timeout_ms=3000,heartbeat_interval_ms=1000)
        sampler=ProcessSampler({'kafka_broker':broker.process.pid,'driver_and_worker':os.getpid()})
        with (output/'lag-resources.jsonl').open('w') as raw:
            for repeat in range(repeats):
                for rate in rates:
                    stage=f'kafka-r{repeat}-{rate:g}';initial=observer.end_offsets([tp])[tp]
                    total=math.floor(rate*duration);rows=[];samples_stage=[];lags=[]
                    start=time.monotonic();last_sample=0;sampler.previous.clear()
                    def sample():
                        end=observer.end_offsets([tp])[tp]
                        committed=observer.committed(tp) or 0
                        resources=sampler.sample();samples_stage.append(resources)
                        value={'stage_id':stage,'observed_at_ms':int(time.time()*1000),'end_offset':end,
                               'committed_offset':committed,'lag':max(0,end-committed),'resources':resources}
                        lags.append(value);raw.write(json.dumps(value)+'\n');raw.flush()
                    for index in range(total):
                        due=start+index/rate
                        remaining=due-time.monotonic()
                        if remaining>0:time.sleep(remaining)
                        if time.monotonic()>=start+duration:break
                        event_id=f'{stage}-{index}'
                        mark=time.monotonic_ns();created=int(time.time()*1000)
                        event={'schema_version':1,'event_id':event_id,'event_time':created,'event_type':'request',
                            'service':stage,'instance_id':'benchmark-kafka','trace_id':'','latency_ms':10,
                            'status_code':503 if index>=int(total*.8) else 200}
                        row={'event_id':event_id,'stage_id':stage,'emitted_ns':mark,'event_time':created,
                             'scheduler_delay_ms':max(0,(time.monotonic()-due)*1000),'error':None}
                        try:
                            future=producer.send(topic,partition=0,value=json.dumps(event).encode())
                            row['future']=future
                        except Exception as exc:row['error']=type(exc).__name__
                        rows.append(row)
                        if time.monotonic()-last_sample>=.2:
                            sample();last_sample=time.monotonic()
                        if worker_errors:raise RuntimeError(str(worker_errors))
                    time.sleep(max(0,start+duration-time.monotonic()))
                    producer.flush(timeout=10)
                    for row in rows:
                        future=row.pop('future',None)
                        if future:
                            try:future.get(timeout=5)
                            except Exception as exc:row['error']=type(exc).__name__
                    acknowledged=[r for r in rows if r['error'] is None]
                    expected_end=initial+len(acknowledged);drain_started=time.monotonic();deadline=drain_started+20
                    while time.monotonic()<deadline:
                        sample()
                        done=drain_confirmed(lags[-1],expected_end)
                        if done:break
                        if worker_errors:raise RuntimeError(str(worker_errors))
                        time.sleep(.05)
                    else:raise TimeoutError('Kafka drain deadline exceeded; partial results retained')
                    elapsed=time.monotonic()-start
                    with lock:
                        for row in rows:
                            row.update(receipts.get(row['event_id'],{}))
                    completion=[(r['completed_ns']-r['emitted_ns'])/1e6 if 'completed_ns' in r else None for r in rows]
                    # Ingestion timestamps are not used as commit timestamps.
                    tier={'stage_id':stage,'repeat':repeat,'target_events_per_s':rate,'offered':total,
                          'started':len(rows),'producer_acknowledged':len(acknowledged),'producer_failed':len(rows)-len(acknowledged),
                          'load_generator_skipped':total-len(rows),'durably_processed':sum('completed_ns' in r for r in rows),
                          'send_window_s':duration,'elapsed_with_drain_s':elapsed,
                          'durable_events_per_s_including_drain':sum('completed_ns' in r for r in rows)/elapsed,
                          'producer_events_per_s':len(acknowledged)/duration,
                          'event_to_processing_complete_ms':distribution(completion),
                          'scheduler_delay_ms':distribution(r['scheduler_delay_ms'] for r in rows),
                          'lag':distribution(s['lag'] for s in lags),'lag_after':lags[-1]['lag'],
                          'drain_seconds':time.monotonic()-drain_started,'resources':summarize(samples_stage)}
                    result['tiers'].append(tier)
                    with (output/'events.jsonl').open('a') as stream:
                        for row in rows:stream.write(json.dumps(row)+'\n')
                    print(json.dumps({'kafka_stage':stage,'durable':tier['durably_processed'],'lag_peak':tier['lag']['max']}),flush=True)
        store=Store(output/'worker-data')
        result['storage']=store.stats()
        result['detection_delays']=[]
        with store.db() as db:
            for row in db.execute('SELECT payload FROM alerts'):
                alert=json.loads(row[0])
                if alert['rule_id']!='fast_errors':continue
                result['detection_delays'].append({'service':alert['service'],'lane':'fast-5xx',
                    'first_error_to_alert_ms':alert['detected_at']-alert['window_start'],'alert_id':alert['alert_id']})
        result['checks']=[{'name':t['stage_id']+':acknowledged_persisted_and_lag_zero',
                           'passed':t['lag_after']==0 and t['durably_processed']==t['producer_acknowledged'] and t['producer_failed']==0} for t in result['tiers']]
        result['checks'] += [{'name':t['stage_id']+':cpu_and_rss_measured','passed':t['resources']['status']=='measured'} for t in result['tiers']]
        result['status']='passed' if all(c['passed'] for c in result['checks']) else 'failed'
        result['limits']=['Single broker, one partition, Python worker; no Flink runtime or production capacity claim.',
                          'Driver and consumer share a process; resource sampling includes observer overhead.',
                          'Input uses actual current timestamps and explicitly generated test telemetry.']
    except BaseException as exc:
        result.update(status='failed',error_type=type(exc).__name__,error=str(exc))
        if isinstance(exc,KeyboardInterrupt):raise
    finally:
        stop.set();errors=[]
        if worker_thread:
            worker_thread.join(timeout=12)
            if worker_thread.is_alive():errors.append('consumer thread did not stop')
        for name,obj in [('observer',observer),('producer',producer)]:
            if obj:
                try:obj.close(timeout=3) if name=='producer' else obj.close(autocommit=False)
                except Exception as exc:errors.append(name+':'+type(exc).__name__)
        try:broker.close()
        except Exception as exc:errors.append('broker:'+type(exc).__name__)
        result['cleanup']={**broker.cleanup,'consumer_thread_stopped':worker_thread is None or not worker_thread.is_alive(),'errors':errors,'worker_errors':worker_errors}
        if errors or worker_errors:result['status']='failed'
        (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    return result
