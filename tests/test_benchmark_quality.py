"""Negative benchmark controls use synthetic evidence; HTTP timing uses real ASGI handling."""
from contextlib import AsyncExitStack
from copy import deepcopy
import time
import unittest
from unittest.mock import patch

import httpx

from benchmarks.http import (episode_coverage, quality_episode, quality_checks,
                             status_for_checks, timing_headers, wait_episode_evidence)
from scripts.evaluate_rca import score_rows
from simulator.service import Settings, create_app
from simulator.telemetry import Telemetry


SERVICES=('gateway','order','inventory','payment')


def evidence(root=None, slow=False):
    traces=[f'{i+1:032x}' for i in range(6)]
    services=SERVICES[:-1] if root=='inventory' else SERVICES
    rows=[];now=int(time.time()*1000)
    for trace in traces:
        spans={s:(trace[-12:]+f'{i:04x}') for i,s in enumerate(services)}
        for service in services:
            failed=bool(root) and service in (root,'gateway','order')
            status=503 if failed and service==root else 502 if failed else 200
            parent='order' if service in ('payment','inventory') else 'gateway' if service=='order' else None
            latency=859 if slow and service=='gateway' else 20
            children=['order'] if service=='gateway' else list(services[2:]) if service=='order' else []
            for kind in ('request','span','log'):
                event={'schema_version':1,'event_id':f'{trace}-{service}-{kind}',
                       'event_time':now,'service':service,'instance_id':service+'-1',
                       'trace_id':trace,'span_id':spans[service],'event_type':kind,
                       'latency_ms':latency,'status_code':status,
                       'parent_span_id':spans.get(parent),'downstream':children,
                       'level':'ERROR' if failed else 'INFO','message':'observed request',
                       'error_code':'SERVICE_UNAVAILABLE' if root==service else 'UPSTREAM_ERROR' if failed else None}
                rows.append({'event':event,'source':{'transport':'test'},'ingested_at':now})
    responses=[{'trace_id':t,'status':502 if root else 200,'trace_matches':True} for t in traces]
    return rows,traces,services,responses


def measured_quality(slow=False):
    rows=[]
    for name,root in [('healthy',None),('payment_5xx','payment'),('inventory_5xx','inventory')]:
        records,traces,services,responses=evidence(root,slow=slow and root is None)
        rows.append(quality_episode(name,root,responses,records,episode_coverage(records,traces,services)))
    return {'cases':rows,'rca_metrics':score_rows(rows),'detection':[{'detected':True},{'detected':True}]}


class CompletenessTests(unittest.TestCase):
    def test_complete_known_graph_has_72_rows_and_six_traces(self):
        rows,traces,services,_=evidence()
        result=episode_coverage(rows,traces,services)
        self.assertTrue(result['stored_complete'])
        self.assertEqual((result['selected'],result['expected_records'],result['observed_traces']),(72,72,6))

    def test_count_alone_cannot_mask_missing_service_observation(self):
        rows,traces,services,_=evidence()
        rows[-1]=deepcopy(rows[0]);rows[-1]['event']['event_id']='distinct-id-but-duplicate-observation'
        result=episode_coverage(rows,traces,services)
        self.assertEqual(len(rows),72)
        self.assertFalse(result['stored_complete'])
        self.assertTrue(result['missing_observations']);self.assertTrue(result['duplicate_observations'])

    def test_wrong_parent_link_cannot_claim_complete_trace(self):
        rows,traces,services,_=evidence()
        next(r['event'] for r in rows if r['event']['event_type']=='span' and r['event']['service']=='payment')['parent_span_id']='wrong'
        result=episode_coverage(rows,traces,services)
        self.assertFalse(result['stored_complete']);self.assertTrue(result['relationship_errors'])

    def test_missing_trace_is_explicit_not_hidden_by_truncated_false(self):
        rows,traces,services,_=evidence()
        result=episode_coverage(rows[12:],traces,services)
        self.assertFalse(result['stored_complete']);self.assertEqual(result['observed_traces'],5)
        self.assertEqual(len(result['missing_observations']),12)
        self.assertFalse(result['truncated'])

    def test_inventory_failure_expects_three_services_not_unreachable_payment(self):
        rows,traces,services,_=evidence('inventory')
        result=episode_coverage(rows,traces,services)
        self.assertTrue(result['stored_complete']);self.assertEqual(result['expected_records'],54)

    def test_duplicate_requested_trace_is_not_a_complete_six_request_episode(self):
        rows,traces,services,_=evidence();traces[-1]=traces[0]
        self.assertFalse(episode_coverage(rows,traces,services)['stored_complete'])


class HealthyControlTests(unittest.TestCase):
    def test_slow_no_injection_control_is_visible_and_blocks_acceptance(self):
        quality=measured_quality(slow=True);healthy=quality['cases'][0]
        self.assertEqual(healthy['control']['status'],'degraded')
        self.assertEqual(healthy['control']['max_latency_ms'],859)
        self.assertTrue(healthy['labels']['healthy'])  # Original planned denominator remains visible.
        self.assertEqual(quality['rca_metrics']['healthy_false_positive_rate']['denominator'],1)
        checks=quality_checks(quality)
        self.assertEqual(status_for_checks(checks),'blocked')
        self.assertFalse(next(c for c in checks if c['name']=='healthy_episode_unknown')['passed'])

    def test_real_healthy_control_still_requires_unknown_prediction(self):
        quality=measured_quality();healthy=quality['cases'][0]
        self.assertEqual(healthy['control']['status'],'valid_healthy')
        self.assertEqual(status_for_checks(quality_checks(quality)),'passed')
        healthy['predicted_status']='candidates';healthy['candidate_services']=['gateway']
        self.assertEqual(status_for_checks(quality_checks(quality)),'failed')

    def test_missing_evidence_is_failure_not_degraded_success(self):
        quality=measured_quality();quality['cases'][0]['input_complete']=False
        self.assertEqual(status_for_checks(quality_checks(quality)),'failed')

    def test_response_status_and_trace_identity_are_both_required(self):
        rows,traces,services,responses=evidence()
        for key,value in [('status',502),('trace_matches',False)]:
            copied=deepcopy(responses);copied[0][key]=value
            result=quality_episode('healthy',None,copied,rows,episode_coverage(rows,traces,services))
            self.assertFalse(result['responses_match_expected']);self.assertEqual(result['control']['status'],'invalid_input')

    def test_broken_fault_accuracy_is_not_masked_by_blocked_control(self):
        quality=measured_quality(slow=True)
        quality['rca_metrics']['top1_any_labelled_root_accuracy']['value']=0
        self.assertEqual(status_for_checks(quality_checks(quality)),'failed')


class EvidenceWaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_delayed_observation_is_waited_for(self):
        rows,traces,services,_=evidence()
        with patch('benchmarks.http.evidence_for_traces',side_effect=[rows[:-1],rows]) as query:
            received,coverage=await wait_episode_evidence(None,traces,services,timeout=.5)
        self.assertEqual(query.call_count,2);self.assertEqual(len(received),72)
        self.assertTrue(coverage['stored_complete']);self.assertFalse(coverage['wait_timed_out'])

    async def test_timeout_retains_incomplete_observations(self):
        rows,traces,services,_=evidence()
        with patch('benchmarks.http.evidence_for_traces',return_value=rows[:57]),self.assertLogs('benchmarks.http',level='ERROR'):
            received,coverage=await wait_episode_evidence(None,traces,services,timeout=0)
        self.assertEqual(len(received),57);self.assertTrue(coverage['wait_timed_out'])
        self.assertFalse(coverage['stored_complete']);self.assertEqual(len(coverage['missing_observations']),15)


class TelemetryTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_synchronous_publication_is_measured_separately_without_changing_events(self):
        class SlowTelemetry(Telemetry):
            def publish(self,topic,event):
                time.sleep(.02)
                return super().publish(topic,event)
        sink=SlowTelemetry(mode='memory')
        app=create_app(Settings(service_name='payment',instance_id='p-1'),sink)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://payment') as client:
                response=await client.get('/work')
        self.assertEqual(response.status_code,200)
        timing=timing_headers(response)
        self.assertGreaterEqual(timing['telemetry_publish_ms'],50)
        self.assertGreaterEqual(timing['observed_total_ms']+.01,timing['handler_downstream_ms']+timing['telemetry_publish_ms'])
        self.assertEqual(len(sink.events),3)
        metric=next(e for _,e in sink.events if e['event_type']=='request')
        self.assertEqual(metric['latency_ms'],timing['handler_downstream_ms'])


if __name__=='__main__':unittest.main()
