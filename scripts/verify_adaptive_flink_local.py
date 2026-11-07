"""Optional real embedded Flink verification; explicitly does NOT run Kafka."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time
import traceback


def run(output):
    output.mkdir(parents=True,exist_ok=False)
    result={'execution_kind':'real_embedded_flink_no_kafka','status':'running','checks':[]}
    started=time.monotonic()
    try:
        from pyflink.common import Types, Time, WatermarkStrategy
        from pyflink.common.watermark_strategy import TimestampAssigner
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.window import SlidingEventTimeWindows
        # Same modules imported by the production submitted job.
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'flink'))
        from detector import RuleAggregate, DetectAggregateWindow
        from adaptive import DetectionConfig, fresh_state, evaluate_adaptive
        from aggregate import WindowAccumulator, summarize_accumulator
        config=DetectionConfig(mode='dynamic',min_windows=3,bucket_hours=24)
        result['configuration']=asdict(config)
        base=1704067200000
        events=[]
        for w in range(6):
            for i in range(30):
                events.append({'event_id':f'w{w}-{i}','service':'probe','event_type':'request',
                    'event_time':base+w*30000+1000+i*10,'latency_ms':500 if w==5 else 100,'status_code':200})
        class EventTimestamp(TimestampAssigner):
            def extract_timestamp(self,value,record_timestamp):return value['event_time']
        env=StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        env.set_python_executable(sys.executable)
        stream=env.from_collection([json.dumps(e) for e in events],type_info=Types.STRING()).map(json.loads,output_type=Types.PICKLED_BYTE_ARRAY()).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(EventTimestamp()))
        stream=(stream.key_by(lambda e:e['service'],key_type=Types.STRING())
            .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
            .aggregate(RuleAggregate(),window_function=DetectAggregateWindow(config),
                       accumulator_type=Types.PICKLED_BYTE_ARRAY(),output_type=Types.STRING()))
        with stream.execute_and_collect() as iterator:
            actual=[json.loads(row) for row in iterator]
        state=fresh_state(config);expected=[]
        for end in range(base+10000,base+180001,10000):
            acc=WindowAccumulator()
            for event in events:
                if end-30000<=event['event_time']<end:acc.add(event)
            if acc.request_count:expected.extend(evaluate_adaptive(summarize_accumulator(acc),'probe',end-30000,end,config,state)['alerts'])
        by_id={a['alert_id']:a for a in expected}
        checks={'three_fault_windows':len(actual)==3,
                'exact_ids':set(by_id)=={a['alert_id'] for a in actual},
                'baseline_state_and_evidence':all(a['evidence']==by_id.get(a['alert_id'],{}).get('evidence') for a in actual)}
        result.update(checks=checks,actual=actual,expected=expected,status='passed' if all(checks.values()) else 'failed')
    except BaseException as error:
        result.update(status='failed',error={'type':type(error).__name__,'message':str(error),'traceback':traceback.format_exc()})
    result['elapsed_seconds']=time.monotonic()-started
    (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
    return result['status']=='passed'


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    raise SystemExit(0 if run(parser.parse_args().output) else 1)
