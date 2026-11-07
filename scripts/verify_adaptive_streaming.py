"""Real Kafka/Flink adaptive warmup, replay and checkpoint recovery verification.

Uses existing isolated native harness. Requires bootstrap_native runtime; no LLM.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time
import traceback
from flink.adaptive import DetectionConfig, evaluate_adaptive, fresh_state
from flink.aggregate import WindowAccumulator, summarize_accumulator
from scripts.native_streaming import NativeAcceptance, http_json, metric, wait_until


class AdaptiveAcceptance(NativeAcceptance):
    def scenario(self):
        config = DetectionConfig.from_env()
        self.results['configuration']['adaptive'] = {**config.__dict__}
        self.results['execution_kind'] = 'real_native_kafka_flink_adaptive'
        base = int(time.time()*1000)//30000*30000-360000
        service = 'adaptive-probe'
        events=[]
        self.advance_watermark(base)
        for window in range(5):
            batch=[metric(f'adaptive-{window}-{i}',base+window*30000+1000+i*10,service,200,100) for i in range(30)]
            events.extend(batch)
            for event in reversed(batch):self.send(event,0)
            self.send(batch[0],0)  # immutable duplicate
        self.advance_watermark(base+150000+5001)
        conflict=self.send({**events[-1],'latency_ms':999},0)
        invalid=self.send(None,1,raw='{invalid')
        def audit_ready():
            self.collect()
            reasons={r['payload'].get('reason','') for r in self.results['output_records'] if r['topic']=='ops.deadletter'}
            return reasons if any('conflicting_duplicate' in r for r in reasons) and any('Expecting' in r for r in reasons) else None
        wait_until('warmup and bad input processed',audit_ready,timeout=60)
        self.check('warmup_no_false_alert',not self.alerts_for(service))
        previous=(self.checkpoint().get('latest',{}).get('completed') or {}).get('id',-1)
        def checkpoint_ready():
            checkpoint=self.checkpoint().get('latest',{}).get('completed') or {}
            offsets=self.admin.list_consumer_group_offsets('aiops-flink-detector-v3')
            acknowledged=any(k.topic=='telemetry.metrics' and k.partition==0 and v.offset>conflict['offset'] for k,v in offsets.items())
            return checkpoint if checkpoint.get('id',-1)>previous and acknowledged else None
        checkpoint=wait_until('adaptive baseline checkpoint',checkpoint_ready,timeout=90)
        self.results['checkpoint_before_failure']=checkpoint
        self.check('checkpoint_completed_with_consumed_training',bool(checkpoint.get('external_path')))
        recovery=time.monotonic()
        self.stop(self.taskmanager)
        self.taskmanager=self.start('taskmanager-restarted',[self.flink/'bin/taskmanager.sh','start-foreground'],self.env)
        def restored():
            value=self.checkpoint().get('latest',{}).get('restored') or {}
            detail=http_json(self.rest+f'/jobs/{self.job_id}')
            return value if value.get('id',-1)>=checkpoint['id'] and detail.get('state')=='RUNNING' and all(v['status']=='RUNNING' for v in detail.get('vertices',[])) else None
        restore=wait_until('adaptive state restore',restored,timeout=120)
        self.results['checkpoint_restored']=restore
        self.results['recovery_seconds']=time.monotonic()-recovery
        self.check('checkpoint_restored',restore['id']>=checkpoint['id'])
        fault=[metric(f'adaptive-fault-{i}',base+180000+1000+i*10,service,200,500) for i in range(30)]
        events.extend(fault)
        for event in reversed(fault):self.send(event,0)
        for event in fault[:5]:self.send(event,0)
        self.advance_watermark(base+210000+5001)
        expected=[]
        state=fresh_state(config)
        for end in range(base+10000,base+210001,10000):
            selected=[event for event in events if end-30000<=event['event_time']<end]
            if not selected:continue
            acc=WindowAccumulator()
            for event in selected:acc.add(event)
            expected.extend(evaluate_adaptive(summarize_accumulator(acc),service,end-30000,end,config,state)['alerts'])
        expected_ids={a['alert_id'] for a in expected}
        def all_alerts():
            actual=self.alerts_for(service)
            return actual if expected_ids <= {a['alert_id'] for a in actual} else None
        actual=wait_until('post-restore adaptive alerts',all_alerts,timeout=90)
        self.results['python_reference']=expected;self.results['flink_comparison']=actual
        self.check('adaptive_python_flink_ids_match',expected_ids=={a['alert_id'] for a in actual} and len(expected_ids)==3)
        self.check('training_survived_restart',all(a['evidence']['baseline']['baseline_count']==5 for a in actual))
        self.check('duplicates_do_not_change_count',all(a['evidence']['sample_count']==30 for a in actual))
        by_id={a['alert_id']:a for a in expected}
        self.check('threshold_and_evidence_match',all(a['evidence']==by_id[a['alert_id']]['evidence'] for a in actual))
        late=self.send(metric('adaptive-too-late',base-60000,service,200,999),0)
        def late_dlq():
            self.collect()
            return any(r['topic']=='ops.deadletter' and r['payload'].get('origin',{}).get('offset')==late['offset']
                       and r['payload'].get('origin',{}).get('partition')==0 and 'watermark' in r['payload'].get('reason','')
                       for r in self.results['output_records'])
        wait_until('late event audit',late_dlq,timeout=40)
        self.check('late_event_quarantined',True)
        self.check('job_still_running',http_json(self.rest+f'/jobs/{self.job_id}')['state']=='RUNNING')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-dir',type=Path,default=Path('/tmp/aiops-native-runtime'))
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    # Isolated acceptance configuration, does not write user's .env.
    os.environ.update(DETECTION_MODE='dynamic',DETECTION_MIN_WINDOWS='3',DETECTION_BUCKET_HOURS='24')
    signal.signal(signal.SIGTERM,lambda *args:(_ for _ in ()).throw(KeyboardInterrupt()))
    test=AdaptiveAcceptance(args.runtime_dir.resolve(),args.output.resolve())
    try:
        test.setup();test.scenario();test.results['status']='passed'
    except BaseException as error:
        test.results['status']='failed'
        test.results['error']={'type':type(error).__name__,'message':str(error),'traceback':traceback.format_exc()}
        print(traceback.format_exc(),file=sys.stderr)
    finally:
        test.close()
    print(test.output/'result.json')
    if test.results['status']!='passed':raise SystemExit(1)


if __name__=='__main__':main()
