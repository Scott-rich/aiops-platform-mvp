import unittest
from unittest.mock import Mock,patch
from benchmarks.flink import WindowLedger, alert_id, compare_window, containing_windows, lag_caught_up, measured_window_alert, validate_workload
from flink.rules import evaluate_window


class FlinkBenchmarkAccountingTests(unittest.TestCase):
    def test_half_open_window_boundaries(self):
        self.assertEqual(containing_windows(30000),[(30000,60000),(20000,50000),(10000,40000)])
        self.assertIn((0,30000),containing_windows(29999))
        self.assertNotIn((0,30000),containing_windows(30000))

    def test_duplicate_does_not_change_expected_windows(self):
        ledger=WindowLedger('payment',20)
        for index in range(20):
            self.assertTrue(ledger.acknowledge(str(index),30001+index,50000+index))
            self.assertFalse(ledger.acknowledge(str(index),30001+index,90000))
        self.assertEqual(len(ledger.expected()),6)
        self.assertTrue(all(v['sample_count']==20 for v in ledger.expected().values()))
        self.assertTrue(all(v['last_input_sent_at_ms']==50019 for v in ledger.expected().values()))

    def test_bounded_ledger_rejects_new_overflow_not_duplicate(self):
        ledger=WindowLedger('x',1);ledger.acknowledge('a',30001,50000)
        self.assertFalse(ledger.acknowledge('a',30001,50000))
        with self.assertRaises(ValueError):ledger.acknowledge('b',30002,50000)

    def test_sparse_windows_are_not_expected_alerts(self):
        ledger=WindowLedger('x',100)
        for i in range(19):ledger.acknowledge(str(i),30001+i,60000+i)
        self.assertEqual(ledger.expected(),{})

    def test_expected_count_matches_actual_python_window_semantics(self):
        ledger=WindowLedger('x',100)
        events=[]
        for i in range(80):
            event={'event_id':str(i),'event_type':'request','service':'x','event_time':25000+i*500,
                   'status_code':503,'latency_ms':1001}
            events.append(event);ledger.acknowledge(str(i),event['event_time'],event['event_time'])
        expected=ledger.expected();actual={}
        for start,end in ledger.windows:
            actual.update({a['alert_id']:a for a in evaluate_window(events,'x',start,end)})
        self.assertEqual(set(expected),set(actual))
        self.assertTrue(all(compare_window(e,actual[key]) for key,e in expected.items()))
        broken=dict(actual[next(iter(actual))]);broken['evidence']={**broken['evidence'],'sample_count':999}
        self.assertFalse(compare_window(expected[broken['alert_id']],broken))

    def test_resource_safety_and_minimum_detector_workload(self):
        self.assertEqual(validate_workload([20,60,120],3,1),600)
        self.assertEqual(validate_workload([120],310,1),37200)
        for values in [([float('nan')],3,1),([0],3,1),([20],float('inf'),1),([20],1,0),([1],3,1),([1000],600,1)]:
            with self.subTest(values=values),self.assertRaises(ValueError):validate_workload(*values)

    def test_unavailable_is_never_zero_lag(self):
        self.assertTrue(lag_caught_up(0))
        for value in (None,False,0.0,-1,1,'0'):
            self.assertFalse(lag_caught_up(value))

    def test_real_aggregate_alerts_are_measured(self):
        from flink.aggregate import WindowAccumulator,evaluate_accumulator
        from scripts.native_streaming import metric
        acc=WindowAccumulator();ledger=WindowLedger('probe',20)
        for index in range(20):
            event=metric(str(index),30001+index,'probe',503,1001)
            acc.add(event);ledger.acknowledge(str(index),event['event_time'],event['event_time'])
        alerts=evaluate_accumulator(acc,'probe',10000,40000)
        self.assertEqual(len(alerts),2)
        for value in alerts:
            self.assertTrue(measured_window_alert(value,{'probe'}))
            self.assertTrue(compare_window(ledger.expected()[value['alert_id']],value))

    def test_fast_foreign_and_wrong_version_alerts_are_excluded(self):
        from flink.aggregate import RULE_VERSION
        value={'service':'probe','rule_version':RULE_VERSION,'rule_id':'high_error_rate'}
        self.assertTrue(measured_window_alert(value,{'probe'}))
        self.assertFalse(measured_window_alert({**value,'rule_id':'fast_errors'},{'probe'}))
        self.assertFalse(measured_window_alert({**value,'service':'foreign'},{'probe'}))
        self.assertFalse(measured_window_alert({**value,'rule_version':'window-rules-v1'},{'probe'}))

    def lag_probe(self,offsets):
        from kafka import TopicPartition
        from scripts.benchmark_flink import FlinkBenchmark
        test=object.__new__(FlinkBenchmark)
        test.results={'lag_samples':[]}
        test.consumer=Mock()
        test.consumer.end_offsets.return_value={TopicPartition('telemetry.metrics',i):10 for i in range(3)}
        test.admin=Mock()
        test.admin.list_consumer_group_offsets.side_effect=offsets
        return test

    @patch('scripts.benchmark_flink.time.sleep')
    def test_coordinator_startup_retries_then_records_real_lag(self,sleep):
        from kafka import TopicPartition
        from kafka.errors import NotCoordinatorError
        from kafka.structs import OffsetAndMetadata
        offsets={TopicPartition('telemetry.metrics',i):OffsetAndMetadata(10,'',-1) for i in range(3)}
        test=self.lag_probe([NotCoordinatorError(),offsets])
        self.assertEqual(test.source_lag('startup'),0)
        row=test.results['lag_samples'][0]
        self.assertTrue(row['available']);self.assertEqual(row['attempts'],2)
        self.assertEqual(row['transient_errors'],['NotCoordinatorError'])
        self.assertEqual(test.admin.list_consumer_group_offsets.call_count,2)
        sleep.assert_called_once_with(.1)

    @patch('scripts.benchmark_flink.time.sleep')
    def test_exhausted_transient_sample_is_unavailable_not_passed(self,sleep):
        from kafka.errors import NotCoordinatorError
        test=self.lag_probe([NotCoordinatorError(),NotCoordinatorError(),NotCoordinatorError()])
        lag=test.source_lag('startup')
        self.assertIsNone(lag);self.assertFalse(lag_caught_up(lag))
        row=test.results['lag_samples'][0]
        self.assertFalse(row['available']);self.assertEqual(row['attempts'],3)
        self.assertEqual(len(row['transient_errors']),3)
        self.assertEqual(test.admin.list_consumer_group_offsets.call_count,3)

    @patch('scripts.benchmark_flink.time.sleep')
    def test_permanent_error_fails_without_silent_retries(self,sleep):
        from kafka.errors import UnknownError
        test=self.lag_probe([UnknownError()])
        with self.assertRaises(UnknownError):test.source_lag('end')
        self.assertFalse(test.results['lag_samples'][0]['available'])
        self.assertFalse(test.results['lag_samples'][0]['transient'])
        self.assertEqual(test.admin.list_consumer_group_offsets.call_count,1)
        sleep.assert_not_called()

if __name__=='__main__':unittest.main()
