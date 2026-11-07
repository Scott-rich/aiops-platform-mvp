"""Benchmark accounting edge cases; these fixtures are unit tests, not live measurements."""
import argparse
import asyncio
from copy import deepcopy
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import httpx
from benchmarks.http import paced
from benchmarks.kafka import drain_confirmed
from benchmarks.metrics import distribution,request_metrics,detection_delays,comparable
from benchmarks.model import summarize_reports,run_model
from benchmarks.resources import parse_stat,ProcessSampler
from benchmarks.report import compare_runs,write_report
from scripts.benchmark_platform import parser,rates,validate


class MetricTests(unittest.TestCase):
    def test_nearest_rank_percentiles_and_invalids(self):
        result=distribution([10,30,20,40,None,float('nan'),True,-1])
        self.assertEqual((result['p50'],result['p95'],result['mean']),(20,40,25))
        self.assertEqual(result['missing_or_invalid'],4)

    def test_empty_is_not_zero(self):
        self.assertIsNone(distribution([])['p95'])
        self.assertIsNone(distribution([None])['mean'])

    def test_failures_and_generator_skips_stay_in_denominators(self):
        result=request_metrics([{'status':204,'elapsed_ms':10},{'status':503,'elapsed_ms':20},{'status':None,'elapsed_ms':30}],5,1,2)
        self.assertEqual((result['failed'],result['load_generator_skipped']),(2,2))
        self.assertEqual(result['success_rate_of_started'],1/3)
        self.assertEqual(result['success_rate_of_offered'],.2)
        self.assertEqual(result['successful_per_s_including_drain'],.5)
        self.assertEqual(result['all_attempt_latency_ms']['count'],3)

    def test_invalid_accounting_rejected(self):
        with self.assertRaises(ValueError):request_metrics([{}],0,1,1)
        with self.assertRaises(ValueError):request_metrics([],0,0,1)

    def test_missed_and_old_alerts_are_not_zero_delays(self):
        result=detection_delays([{'detected_at':99}],100)
        self.assertFalse(result['detected']);self.assertIsNone(result['fault_to_first_alert_ms'])
        result=detection_delays([{'detected_at':120},{'detected_at':110}],100,105)
        self.assertEqual(result['fault_to_first_alert_ms'],10)
        self.assertEqual(result['first_error_to_alert_ms'],5)

    def test_stale_lag_sample_cannot_claim_drain(self):
        self.assertFalse(drain_confirmed({'committed_offset':1197,'lag':3},1200))
        self.assertFalse(drain_confirmed({'committed_offset':1197,'lag':0},1200))
        self.assertFalse(drain_confirmed({'committed_offset':1200,'lag':1},1200))
        self.assertTrue(drain_confirmed({'committed_offset':1200,'lag':0},1200))

    def test_missing_metadata_cannot_be_compared(self):
        self.assertFalse(all(comparable({},{}).values()))

    def test_comparison_blocks_different_environment_and_config(self):
        a={'benchmark_version':'1','configuration_id':'a','source_sha256':'b','environment':{'hardware_id':'c'}}
        self.assertTrue(compare_runs(a,deepcopy(a))['comparable'])
        b=deepcopy(a);b['configuration_id']='other'
        self.assertFalse(compare_runs(a,b)['comparable']);self.assertEqual(compare_runs(a,b)['changes'],[])
        b=deepcopy(a);b['environment']['hardware_id']='different'
        self.assertFalse(compare_runs(a,b)['comparable'])


class SamplerTests(unittest.TestCase):
    def stat(self,user=100,system=20,start=10,rss=5):
        fields=['0']*50
        fields[0]='S';fields[11]=str(user);fields[12]=str(system);fields[19]=str(start);fields[21]=str(rss)
        return '42 (name with ) space) '+' '.join(fields)

    def test_proc_indices_with_parentheses(self):
        self.assertEqual(parse_stat(self.stat(),100,4096),{'cpu_seconds':1.2,'start_ticks':10,'rss_bytes':20480})

    @unittest.skipUnless(Path('/proc/self/stat').exists(),'Linux proc required')
    def test_real_owned_child_and_self_resolve_in_current_pid_namespace(self):
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(2)'])
        try:
            sampler=ProcessSampler({'self':os.getpid(),'child':child.pid})
            first=sampler.sample();time.sleep(.05);second=sampler.sample()
            self.assertEqual(first['available_count'],2)
            self.assertGreater(second['rss_sum_bytes'],0)
            self.assertIsNotNone(second['cpu_sum_percent_one_core'])
            child.terminate();child.wait(timeout=3)
            self.assertFalse(sampler.sample()['processes'][1]['available'])
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=3)

    def test_cpu_delta_pid_reuse_missing_and_rss(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'42').mkdir();p=root/'42/stat';p.write_text(self.stat())
            sampler=ProcessSampler({'test':42},root);sampler.hz=100;sampler.page_size=4096
            self.assertIsNone(sampler.sample(1)['cpu_sum_percent_one_core'])
            p.write_text(self.stat(user=150));sample=sampler.sample(2)
            self.assertAlmostEqual(sample['cpu_sum_percent_one_core'],50)
            self.assertEqual(sample['rss_sum_bytes'],20480)
            p.write_text(self.stat(user=160,start=20));self.assertIsNone(sampler.sample(3)['cpu_sum_percent_one_core'])
            p.unlink();self.assertIsNone(sampler.sample(4)['rss_sum_bytes'])
            self.assertFalse(sampler.sample(5)['processes'][0]['available'])


class ModelTests(unittest.TestCase):
    def report(self,missing=False,currency='USD'):
        attempt={'ordinal':1,'started_at':10,'input_sha256':'input','outcome':'transport_success','duration_ms':20,
                 'usage':{'prompt_tokens':10,'completion_tokens':5,'total_tokens':15},
                 'estimated_cost':{'amount':.1,'currency':currency}}
        if missing:attempt['usage']=attempt['estimated_cost']=None
        return {'incident_id':'i','audit':{'model_metadata':{'elapsed_ms':30,'input_sha256':'input','attempts':[attempt]}}}

    def test_no_model_attempt_has_no_latency_or_cost(self):
        result=summarize_reports([])
        self.assertEqual(result['actual_attempts'],0);self.assertIsNone(result['estimated_cost_by_currency'])
        self.assertIsNone(result['attempt_latency_ms']['p95'])

    def test_report_reuse_not_double_charged(self):
        report=self.report();reused=deepcopy(report)
        reused['audit']['model_metadata']['reused_from_report_id']='original'
        result=summarize_reports([report,reused])
        self.assertEqual(result['actual_attempts'],1);self.assertEqual(result['reused_reports'],1)
        self.assertEqual(result['estimated_cost_by_currency'],{'USD':.1})
        self.assertEqual(result['transport_successful_attempts'],1)

    def test_retry_missing_usage_prevents_complete_cost_claim(self):
        report=self.report();retry=self.report(True)['audit']['model_metadata']['attempts'][0]
        retry.update(ordinal=2,started_at=11,outcome='request_timeout')
        report['audit']['model_metadata']['attempts'].append(retry)
        result=summarize_reports([report])
        self.assertEqual(result['actual_attempts'],2);self.assertEqual(result['attempts_without_usage'],1)
        self.assertIsNone(result['estimated_cost_by_currency'])
        self.assertEqual(result['known_partial_estimated_cost_by_currency'],{'USD':.1})

    def test_currencies_never_combined(self):
        second=self.report(currency='CNY');second['incident_id']='other'
        result=summarize_reports([self.report(),second])
        self.assertEqual(result['estimated_cost_by_currency'],{'USD':.1,'CNY':.1})

    def test_unconfigured_real_model_is_blocked_without_subprocess(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict('os.environ',{},clear=True),patch('benchmarks.model.subprocess.Popen') as run:
            result=run_model(Path(tmp)/'result',True)
            self.assertEqual(result['status'],'blocked');run.assert_not_called()

    def test_configured_model_needs_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict('os.environ',{'LLM_BASE_URL':'http://127.0.0.1:1/v1','LLM_MODEL':'test'}),patch('benchmarks.model.subprocess.Popen') as run:
            self.assertEqual(run_model(Path(tmp)/'result')['status'],'not_requested');run.assert_not_called()

    def test_invalid_token_usage_not_counted(self):
        report=self.report();report['audit']['model_metadata']['attempts'][0]['usage']['total_tokens']=1
        self.assertEqual(summarize_reports([report])['attempts_without_usage'],1)
        self.assertIsNone(summarize_reports([report])['estimated_cost_by_currency'])


class LoadTests(unittest.IsolatedAsyncioTestCase):
    async def test_pacer_bounds_concurrency_records_skips_and_finishes(self):
        active=0;peak=0
        async def send(i):
            nonlocal active,peak
            active+=1;peak=max(peak,active)
            try:await asyncio.sleep(.04);return {'status':204}
            finally:active-=1
        rows,result=await paced(500,.1,2,send)
        self.assertLessEqual(peak,2);self.assertEqual(active,0)
        self.assertGreater(result['load_generator_skipped'],0)
        self.assertEqual(result['offered'],len(rows)+result['load_generator_skipped'])

    async def test_network_and_unexpected_exceptions_are_counted(self):
        async def send(i):
            if i==0:raise httpx.ConnectError('unreachable')
            raise RuntimeError('worker failure')
        rows,result=await paced(20,.1,2,send)
        self.assertEqual(result['failed'],2)
        self.assertEqual({r['error'] for r in rows},{'ConnectError','RuntimeError'})

    async def test_cancel_reaps_active_tasks(self):
        active=0
        async def send(i):
            nonlocal active
            active+=1
            try:await asyncio.sleep(10)
            finally:active-=1
        task=asyncio.create_task(paced(20,1,2,send));await asyncio.sleep(.03);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(active,0)


class CliTests(unittest.TestCase):
    def test_invalid_rates_are_rejected(self):
        for value in ('nan','inf','0','-2','1001','2,2','x','1,2,3,4,5,6'):
            with self.subTest(value=value),self.assertRaises(argparse.ArgumentTypeError):rates(value)

    def test_workload_caps_and_invalid_duration(self):
        args=parser().parse_args(['--output','unused']);validate(args)
        args.duration=float('nan')
        with self.assertRaises(ValueError):validate(args)
        args.duration=60;args.repeats=5
        with self.assertRaises(ValueError):validate(args)
        args.duration=4;args.repeats=2;args.concurrency=65
        with self.assertRaises(ValueError):validate(args)

    def test_machine_report_retains_missing_values(self):
        result={'status':'partial','started_at_utc':'test','components':{'model':{'status':'blocked'}}}
        with tempfile.TemporaryDirectory() as tmp:
            write_report(tmp,result)
            self.assertEqual(json.loads((Path(tmp)/'result.json').read_text()),result)
            self.assertTrue((Path(tmp)/'metrics.csv').read_text().startswith('component,kind,stage_id'))


if __name__=='__main__':unittest.main()
