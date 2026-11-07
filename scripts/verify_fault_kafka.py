"""Real isolated Kafka pause/resume and event-fault test. No Flink engine is started."""
import argparse
import csv
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid

from scripts.native_streaming import free_port
from scripts.local_platform import simulator_environment
from simulator.scenarios import message_cases


def run(kafka_home, output):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {"schema_version":1,"execution_kind":"real_isolated_kafka_python_worker",
              "flink_engine":False,"llm_calls":0,"started_at_ms":int(time.time()*1000),
              "environment":{"python":sys.version,"platform":platform.platform()},
              "checks":[],"status":"running","cleanup":{}}
    broker = admin = producer = consumer = pipeline = log = None
    def save():
        temp=output/"result.tmp"
        temp.write_text(json.dumps(result,indent=2)+"\n");temp.replace(output/"result.json")
    def check(name, condition, **details):
        result["checks"].append({"name":name,"passed":bool(condition),**details})
        save()
        if not condition:raise AssertionError(name+": "+str(details))
    def cleanup_signal(*_):
        raise KeyboardInterrupt("signal requested cleanup")
    previous={sig:signal.signal(sig,cleanup_signal) for sig in (signal.SIGTERM,signal.SIGINT)}
    try:
        home=Path(kafka_home).resolve() if kafka_home else Path('/nonexistent')
        if not shutil.which("java") or not (home/"bin/kafka-server-start.sh").is_file():
            result.update(status="blocked",reason="Java and a Kafka binary installation required; supply --kafka-home. No mock substituted.")
            return 2
        result["environment"]["java"]=subprocess.check_output(["java","-version"],stderr=subprocess.STDOUT,text=True,timeout=5).strip()
        result["environment"]["kafka_home"]=str(home)
        from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
        from kafka.admin import NewTopic
        from app.engine import Pipeline
        from app.storage import Store
        from app.worker import persist_batch
        from flink.stream_support import parse_metric_envelope
        from flink.aggregate import WindowAccumulator
        port,controller=free_port(),free_port()
        while port==controller:controller=free_port()
        bootstrap=f"127.0.0.1:{port}"
        config=output/"kafka.properties"
        config.write_text('\n'.join([
            "process.roles=broker,controller","node.id=1",
            f"controller.quorum.voters=1@127.0.0.1:{controller}",
            f"listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}",
            f"advertised.listeners=PLAINTEXT://{bootstrap}",
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "controller.listener.names=CONTROLLER","inter.broker.listener.name=PLAINTEXT",
            f"log.dirs={output/'kafka-data'}","num.partitions=1",
            "offsets.topic.replication.factor=1","transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1","group.initial.rebalance.delay.ms=0",
            "auto.create.topics.enable=false","num.network.threads=2","num.io.threads=2",
            "log.retention.hours=1","log.segment.bytes=16777216"])+'\n')
        env=simulator_environment(os.environ)
        env.update(KAFKA_HEAP_OPTS="-Xms128m -Xmx384m",LOG_DIR=str(output/"broker-logs"))
        with (output/"format.log").open('w') as stream:
            subprocess.run([str(home/"bin/kafka-storage.sh"),"format","-t",uuid.uuid4().hex[:22],"-c",str(config)],
                env=env,stdout=stream,stderr=subprocess.STDOUT,check=True,timeout=30)
        log=(output/"broker.log").open('w')
        broker=subprocess.Popen([str(home/"bin/kafka-server-start.sh"),str(config)],env=env,
                                 stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            if broker.poll() is not None:raise RuntimeError("Kafka exited; see broker.log")
            try:
                admin=KafkaAdminClient(bootstrap_servers=bootstrap,request_timeout_ms=3000,api_version_auto_timeout_ms=1000)
                break
            except Exception:
                time.sleep(.2)
        if admin is None:raise TimeoutError("Kafka startup deadline exceeded")
        topic="telemetry.metrics"
        admin.create_topics([NewTopic(topic,1,1)],timeout_ms=10000)
        producer=KafkaProducer(bootstrap_servers=bootstrap,acks="all",retries=2,
            request_timeout_ms=5000,max_block_ms=5000,max_in_flight_requests_per_connection=1)
        consumer=KafkaConsumer(bootstrap_servers=bootstrap,enable_auto_commit=False,
            group_id="phase5-"+uuid.uuid4().hex,request_timeout_ms=10000,session_timeout_ms=6000,
            auto_offset_reset="earliest",max_poll_records=20)
        tp=TopicPartition(topic,0)
        consumer.assign([tp]);consumer.seek(tp,0);consumer.pause(tp)
        result["fault"]={"kind":"consumer_pause","scope":[topic+":0",consumer.config['group_id']],
                         "started_at_ms":int(time.time()*1000)}
        epoch=int(time.time()*1000)
        cases=message_cases(epoch,uuid.uuid4().hex)
        normal=[{**cases['valid'][0],"event_id":f"load-{i}","event_time":epoch,"service":"phase5-backlog"} for i in range(50)]
        values=[json.dumps(e).encode() for e in [*normal,*cases['valid'],*cases['duplicates'],cases['conflict']]]+cases['invalid']
        result["input"]={"records":len(values),"unique_expected":55,"conflicts_expected":1,"invalid_expected":6}
        for value in values:producer.send(topic,partition=0,value=value).get(timeout=8)
        producer.flush(timeout=8)
        paused=consumer.poll(timeout_ms=400)
        end=consumer.end_offsets([tp])[tp]
        lag=end-consumer.position(tp)
        check("paused_consumer_builds_real_lag",not paused and lag==len(values),lag_records=lag,end_offset=end)
        consumer.resume(tp)
        result["fault"]["ended_at_ms"]=int(time.time()*1000)
        result["fault"]["end_reason"]="resumed"
        pipeline=Pipeline(Store(output/"worker-data"))
        counts={"telemetry_received":0,"deadletters_received":0,"alerts_received":0}
        received=[];delays=[];parsed=[];invalid=0
        deadline=time.monotonic()+12
        started=time.monotonic()
        while len(received)<len(values) and time.monotonic()<deadline:
            batches=consumer.poll(timeout_ms=200)
            if not batches:continue
            values_counts=persist_batch(pipeline,batches)
            for key,value in values_counts.items():counts[key]+=value
            for records in batches.values():
                for record in records:
                    received.append(record.offset)
                    delays.append(int(time.time()*1000)-record.timestamp)
                    envelope={"_aiops_kafka_envelope":1,"raw":record.value.decode('utf-8',errors='replace'),
                              "origin":{"topic":record.topic,"partition":record.partition,"offset":record.offset}}
                    try:parsed.append(parse_metric_envelope(json.dumps(envelope)))
                    except (ValueError,TypeError):invalid+=1
            consumer.commit()  # Only after durable production-worker persistence.
        lag_after=consumer.end_offsets([tp])[tp]-consumer.position(tp)
        check("resume_catches_up",len(received)==len(values) and lag_after==0,received=len(received),lag_after=lag_after,
              recovery_seconds=time.monotonic()-started,min_consumption_delay_ms=min(delays,default=None),max_consumption_delay_ms=max(delays,default=None))
        check("worker_unique_and_quarantine",pipeline.store.stats()['events']==55 and pipeline.store.stats()['deadletters']==7,
              stats=pipeline.store.stats(),worker_counts=counts)
        accumulators={}
        for event in parsed:
            accumulator=accumulators.setdefault(event['service'],WindowAccumulator())
            accumulator.add_or_quarantine(event)
        unique=sum(acc.request_count for acc in accumulators.values())
        conflicts=sum(acc.quarantine_count for acc in accumulators.values())
        check("shared_stream_parser_and_dedup",invalid==6 and unique==55 and conflicts==1,
              invalid=invalid,unique=unique,conflicts=conflicts,service_groups=sorted(accumulators))
        check("committed_durable_offset",consumer.committed(tp)==end,committed=consumer.committed(tp),end=end)
        result['status']='passed'
    except BaseException as error:
        result.update(status="failed",error_type=type(error).__name__,error=str(error))
        (output/"error.log").write_text(traceback.format_exc())
    finally:
        errors=[]
        for name,action in [("resume",lambda:consumer.resume(*consumer.paused()) if consumer else None),
            ("consumer",lambda:consumer.close(autocommit=False) if consumer else None),
            ("producer",lambda:producer.close(timeout=3) if producer else None),
            ("pipeline",lambda:pipeline.close() if pipeline else None),("admin",lambda:admin.close() if admin else None)]:
            try:action()
            except Exception as error:errors.append(name+': '+type(error).__name__)
        if broker and broker.poll() is None:
            os.killpg(broker.pid,signal.SIGTERM)
            try:broker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(broker.pid,signal.SIGKILL);broker.wait(timeout=3)
        if log:log.close()
        for sig,handler in previous.items():signal.signal(sig,handler)
        if result.get('fault') and 'ended_at_ms' not in result['fault']:
            result['fault'].update(ended_at_ms=int(time.time()*1000),end_reason='cleanup')
        result['cleanup']={"errors":errors,"broker_stopped":broker is None or broker.poll() is not None}
        if errors:result['status']='failed'
        result['ended_at_ms']=int(time.time()*1000)
        save()
        with (output/'checks.csv').open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=['name','passed']);writer.writeheader()
            writer.writerows({k:item[k] for k in writer.fieldnames} for item in result['checks'])
        print(json.dumps({'status':result['status'],'checks':len(result['checks']),'cleanup':result['cleanup']}),flush=True)
    return 0 if result['status']=='passed' else 2 if result['status']=='blocked' else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka-home',default=os.getenv('KAFKA_HOME'))
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    raise SystemExit(run(args.kafka_home,args.output))


if __name__=='__main__':main()
