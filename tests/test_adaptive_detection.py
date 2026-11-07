import copy
from dataclasses import replace
import json
import math
import pickle
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from flink.adaptive import DetectionConfig, evaluate_adaptive, fresh_state
from flink.aggregate import WindowAccumulator, summarize_accumulator
from app.detection_state import DetectionStateFile
from app.local_detection import LocalWindowDetector
from app.storage import Store
from scripts.evaluate_detection import confusion, evaluate, synthetic_windows
from scripts.local_platform import platform_environment

BASE = 1704067200000


def stats(**changes):
    return dict(request_count=40, host_count=3, p95_latency_ms=100.0,
                error_rate=0, avg_cpu_percent=40, avg_memory_percent=50, **changes) if not changes else {**stats(), **changes}


def request(i, timestamp, latency=100, service='s'):
    return dict(schema_version=1, event_id=f'{service}-{timestamp}-{i}', event_time=timestamp,
                event_type='request', service=service, instance_id='one', status_code=200, latency_ms=latency)


class AdaptiveTests(unittest.TestCase):
    def setUp(self):
        self.config = DetectionConfig(mode='dynamic', min_windows=3, history_windows=5)
        self.state = fresh_state(self.config)

    def apply(self, i, values=None, state=None, config=None):
        start = BASE+i*30000
        return evaluate_adaptive(values or stats(), 's', start, start+30000,
                                 config or self.config, self.state if state is None else state)

    def warm(self):
        for i in range(3):
            result = self.apply(i)
            self.assertEqual(result['alerts'], [])
            self.assertEqual(result['decisions']['p95_latency_ms']['status'], 'warming_up')

    def test_warmup_then_deviation_and_all_metrics(self):
        self.warm()
        result = self.apply(3, stats(p95_latency_ms=350,error_rate=.15,avg_cpu_percent=80,avg_memory_percent=80))
        self.assertEqual({a['rule_id'] for a in result['alerts']}, {'high_latency','high_error_rate','high_cpu','high_memory'})
        for alert in result['alerts']:
            self.assertEqual(alert['evidence']['baseline']['baseline_count'],3)
            self.assertEqual(alert['rule_version'],'adaptive-mad-v1')
        self.assertEqual(self.apply(4)['alerts'], [])

    def test_no_current_window_or_future_leakage_and_no_anomaly_learning(self):
        self.warm()
        first = self.apply(3,stats(p95_latency_ms=500))
        second = self.apply(4,stats(p95_latency_ms=1000))
        self.assertEqual(first['alerts'][0]['evidence']['threshold'],150)
        self.assertEqual(second['alerts'][0]['evidence']['threshold'],150)
        self.assertEqual(second['decisions']['p95_latency_ms']['baseline_count'],3)

    def test_strict_boundary_and_zero_variance_floor(self):
        self.warm()
        self.assertEqual(self.apply(3,stats(p95_latency_ms=150,error_rate=.05))['alerts'],[])
        self.assertTrue(self.apply(4,stats(p95_latency_ms=175,error_rate=.1))['alerts'])

    def test_mad_and_relative_margin(self):
        for i,value in enumerate((1000,1100,1200)):
            self.apply(i,stats(p95_latency_ms=value))
        result = self.apply(3,stats(p95_latency_ms=1500))
        d=result['decisions']['p95_latency_ms']
        self.assertEqual(d['median'],1100)
        self.assertEqual(d['mad'],100)
        self.assertEqual(d['threshold'],1550)
        self.assertEqual(d['status'],'normal')

    def test_insufficient_missing_invalid_and_overflow_do_not_learn(self):
        for values,expected in [(stats(request_count=1),'insufficient_samples'),
                                (stats(p95_latency_ms=None),'missing_metric'),
                                (stats(p95_latency_ms=float('nan')),'invalid_metric'),
                                (stats(p95_latency_ms=-1),'invalid_metric'),
                                (stats(p95_latency_ms=10**1000),'invalid_metric'),
                                (stats(request_count=True),'invalid_metric'),
                                (stats(p95_overflow=True),'precision_limited')]:
            with self.subTest(expected=expected):
                state=fresh_state(self.config)
                for i in range(5):
                    result=self.apply(i,values,state)
                self.assertEqual(result['decisions']['p95_latency_ms']['status'],expected)
                self.assertNotIn('0:p95_latency_ms',state['buckets'])
                self.assertEqual(result['alerts'],[])

    def test_metric_groups_warm_independently(self):
        for i in range(3):self.apply(i,stats(host_count=0,avg_cpu_percent=None,avg_memory_percent=None))
        result=self.apply(3,stats(p95_latency_ms=500,avg_cpu_percent=99))
        self.assertEqual([a['rule_id'] for a in result['alerts']],['high_latency'])
        self.assertEqual(result['decisions']['avg_cpu_percent']['status'],'warming_up')

    def test_time_buckets_and_expiry(self):
        self.warm()
        result=self.apply(720,stats(p95_latency_ms=500))
        self.assertEqual(result['decisions']['p95_latency_ms']['time_bucket_utc'],1)
        self.assertEqual(result['alerts'],[])
        result=self.apply(8*2880,stats(p95_latency_ms=500))
        self.assertEqual(result['decisions']['p95_latency_ms']['status'],'warming_up')
        self.assertEqual(len(self.state['buckets']),4)

    def test_overlap_does_not_inflate_training_samples(self):
        for i in range(10):
            evaluate_adaptive(stats(),'s',BASE+i*10000,BASE+i*10000+30000,self.config,self.state)
        self.assertEqual(len(self.state['buckets']['0:p95_latency_ms']['samples']),4)

    def test_duplicate_and_old_windows_ignored_without_state_mutation(self):
        self.warm(); snapshot=copy.deepcopy(self.state)
        for i in (2,1):
            result=self.apply(i,stats(p95_latency_ms=500))
            self.assertEqual(result['alerts'],[])
            self.assertEqual(result['decisions']['p95_latency_ms']['status'],'out_of_order')
        self.assertEqual(snapshot,self.state)

    def test_bounded_history_and_checkpoint_serialization(self):
        for i in range(20):self.apply(i)
        self.assertEqual(len(self.state['buckets']['0:p95_latency_ms']['samples']),5)
        restored=pickle.loads(pickle.dumps(self.state))
        first=self.apply(20,stats(p95_latency_ms=500))
        second=self.apply(20,stats(p95_latency_ms=500),restored)
        self.assertEqual(first['alerts'][0]['alert_id'],second['alerts'][0]['alert_id'])
        self.assertEqual(self.state,restored)

    def test_config_validation_and_environment(self):
        self.assertEqual(DetectionConfig.from_env({'DETECTION_MODE':'dynamic','DETECTION_MIN_WINDOWS':'3'}).min_windows,3)
        for changes in [{'mode':'oops'},{'min_windows':True},{'min_windows':1},{'history_windows':3},
                        {'bucket_hours':5},{'sigma':math.inf},{'relative_margin':0},{'error_margin':2}]:
            with self.subTest(changes=changes),self.assertRaises(ValueError):DetectionConfig(**changes)
        with self.assertRaisesRegex(ValueError,'DETECTION_MIN_WINDOWS'):
            DetectionConfig.from_env({'DETECTION_MIN_WINDOWS':'not-an-integer'})
        with self.assertRaises(ValueError):self.apply(1,config=replace(self.config,sigma=4))
        with self.assertRaises(ValueError):evaluate_adaptive(stats(),'s',20,10,self.config,self.state)

    def test_dotenv_detection_and_inherited_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'.env';p.write_text('DETECTION_MODE=dynamic\nDETECTION_MIN_WINDOWS=3\nIGNORED=value\n')
            env=platform_environment({'DETECTION_MODE':'fixed'},p)
            self.assertEqual(env['DETECTION_MODE'],'fixed');self.assertEqual(env['DETECTION_MIN_WINDOWS'],'3')
            self.assertNotIn('IGNORED',env)

    def test_service_isolation_and_duplicate_event_features(self):
        other=fresh_state(self.config);self.warm()
        self.assertEqual(self.apply(3,stats(p95_latency_ms=500),other)['alerts'],[])
        events=[request(i,BASE+1,101) for i in range(40)]
        acc=WindowAccumulator()
        for event in events+list(reversed(events)):acc.add(event)
        self.assertEqual(summarize_accumulator(acc)['request_count'],40)
        self.assertEqual(summarize_accumulator(acc)['p95_latency_ms'],125)


class LocalAdaptiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(self.tmp.name)
        self.pipeline=SimpleNamespace(store=self.store,accept_alert=lambda alert,source:self.store.alert(alert,source))
        self.config=DetectionConfig(mode='dynamic',min_windows=3,history_windows=5)
        self.detector=LocalWindowDetector(self.pipeline,BASE,config=self.config)

    def feed(self, window, latency=100):
        start=BASE+window*30000
        self.store.ingest_many([(request(i,start+1000+i,latency),{}) for i in range(30)])
        self.store.ingest(request(999,start+35001,1,service='clock'))
        return self.detector.advance()

    def test_real_sqlite_pipeline_detection_restart_and_idempotency(self):
        for i in range(5):self.feed(i)
        self.detector=LocalWindowDetector(self.pipeline,BASE+999999,config=self.config)
        result=self.feed(5,500)
        self.assertTrue(any(a['rule_id']=='high_latency' for a in result))
        count=self.store.stats()['alerts']
        self.assertEqual(self.detector.advance(),[])
        self.assertEqual(self.store.stats()['alerts'],count)
        self.assertTrue((Path(self.tmp.name)/'detection-state-v1.json').exists())

    def test_pending_alert_replays_after_delivery_failure(self):
        for i in range(5):self.feed(i)
        self.pipeline.accept_alert=lambda *args:(_ for _ in ()).throw(RuntimeError('test delivery failure'))
        with self.assertRaises(RuntimeError):self.feed(5,500)
        state=self.detector.state_file.load(0)
        self.assertTrue(state['pending'])
        ids={a['alert_id'] for a in state['pending']}
        self.pipeline.accept_alert=lambda alert,source:self.store.alert(alert,source)
        new=LocalWindowDetector(self.pipeline,config=self.config)
        result=new.advance()
        self.assertTrue(ids <= {a['alert_id'] for a in result})
        self.assertEqual(new.state_file.load(0)['pending'],[])

    def test_corruption_and_config_change_fail_closed(self):
        self.feed(0)
        path=self.detector.state_file.path
        original=path.read_text()
        with self.assertRaisesRegex(ValueError,'config changed'):
            LocalWindowDetector(self.pipeline,config=replace(self.config,sigma=4))
        body=json.loads(original);body['payload']['next_end']+=1;path.write_text(json.dumps(body))
        with self.assertRaisesRegex(ValueError,'checksum'):
            LocalWindowDetector(self.pipeline,config=self.config)
        self.assertEqual(json.loads(path.read_text()),body)

    def test_failed_atomic_write_keeps_prior_state(self):
        self.feed(0);path=self.detector.state_file.path;original=path.read_bytes()
        state=self.detector.state_file.load(0);state['next_end']+=10000
        with patch('app.detection_state.os.replace',side_effect=OSError('test full disk')):
            with self.assertRaises(OSError):self.detector.state_file.save(state)
        self.assertEqual(path.read_bytes(),original)
        self.assertEqual(list(Path(self.tmp.name).glob('.detection-*.tmp')),[])

    def test_service_capacity_is_bounded(self):
        self.config=replace(self.config,max_services=1)
        self.detector=LocalWindowDetector(self.pipeline,BASE,config=self.config)
        self.feed(0)
        self.assertLessEqual(len(self.detector.state_file.load(0)['services']),1)

    def test_fixed_mode_default_ignores_dynamic_sidecar(self):
        self.feed(0)
        self.detector.state_file.path.write_text('corrupt')
        fixed=LocalWindowDetector(self.pipeline,BASE,config=DetectionConfig())
        self.assertIsNone(fixed.state_file)
        self.assertEqual(fixed.advance(),[])


class EvaluationTests(unittest.TestCase):
    def test_metrics_include_abstentions_as_missed_faults(self):
        rows=[{'label':True,'alert':True,'status':'anomaly'},
              {'label':True,'alert':False,'status':'warming_up'},
              {'label':False,'alert':True,'status':'anomaly'},
              {'label':False,'alert':False,'status':'normal'},
              {'label':None,'alert':False,'status':'missing_metric'}]
        result=confusion(rows)
        self.assertEqual([result[k] for k in ('tp','fp','tn','fn')],[1,1,1,1])
        self.assertEqual(result['recall'],.5);self.assertEqual(result['decision_coverage'],.75)
        self.assertIsNone(confusion([])['precision'])

    def test_same_seed_same_data_metrics_and_label_blindness(self):
        data=list(synthetic_windows(7,40));first,rows=evaluate(data)
        second,other=evaluate(data)
        self.assertEqual(rows,other);self.assertEqual(first['dataset_sha256'],second['dataset_sha256'])
        flipped=copy.deepcopy(data)
        for row in flipped:row['labels']={k:False for k in row['labels']}
        _,blind=evaluate(flipped)
        self.assertEqual([r['alert'] for r in rows],[r['alert'] for r in blind])

    def test_evaluation_has_real_misses_not_perfect_claim(self):
        result,_=evaluate(synthetic_windows())
        for mode in ('fixed','dynamic'):
            self.assertGreater(result['metrics'][mode]['fn'],0)
            self.assertGreater(result['metrics'][mode]['missed_episodes'],0)
        self.assertGreater(result['metrics']['fixed']['fp'],0)

    def test_empty_or_backward_dataset_rejected(self):
        with self.assertRaises(ValueError):evaluate([])
        data=list(synthetic_windows(1,4))
        with self.assertRaises(ValueError):evaluate(data+data[:1])


class AdaptiveIncidentTests(unittest.TestCase):
    def test_sustained_sub800_latency_does_not_false_resolve_and_rca_has_real_evidence(self):
        from app.incidents import IncidentManager
        from app.rca import analyze
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(tmp);manager=IncidentManager(store)
            events=[request(i,BASE+i*1000,300) for i in range(90)]
            store.ingest_many([(event,{}) for event in events])
            config=DetectionConfig(mode='dynamic',min_windows=3,bucket_hours=24)
            state=fresh_state(config)
            for i in range(3):evaluate_adaptive(stats(),'s',BASE-90000+i*30000,BASE-60000+i*30000,config,state)
            alert=evaluate_adaptive(stats(p95_latency_ms=300),'s',BASE,BASE+30000,config,state)['alerts'][0]
            incident=manager.ingest_alert(alert,now=BASE+35000)
            manager.tick(now=BASE+60000);manager.tick(now=BASE+75000)
            self.assertEqual(manager.get(incident['incident_id'])['status'],'open')
            snapshot={'records':[{'event':e} for e in events],'coverage':{'truncated':False}}
            rca=analyze(snapshot,alert)
            self.assertEqual(rca['candidates'][0]['service'],'s')
            self.assertTrue(set(rca['candidates'][0]['supporting_evidence_ids']) <= {e['event_id'] for e in events[:30]})
            for i in range(100,131):store.ingest(request(i,BASE+i*1000,100))
            # Keep observing within the 10-second recovery window. An 11-second
            # monitoring gap must restart the hold under phase7 safety rules.
            manager.tick(now=BASE+115000);manager.tick(now=BASE+120000);manager.tick(now=BASE+126000)
            self.assertEqual(manager.get(incident['incident_id'])['status'],'resolved')

    def test_normal_slow_service_can_recover_at_its_own_baseline(self):
        from app.incidents import _bad
        from app.detection_policy import adaptive_policy
        alert={'service':'s','rule_id':'high_latency','window_start':BASE,'window_end':BASE+30000,
               'evidence':{'detection_mode':'dynamic','threshold':1500}}
        policy=adaptive_policy(alert)
        self.assertFalse(_bad(request(1,BASE,1200),'latency',policy))
        self.assertTrue(_bad(request(1,BASE,1200,service='other'),'latency',policy))
        self.assertTrue(_bad(request(1,BASE,1600),'latency',policy))
