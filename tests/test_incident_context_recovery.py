"""Regression cases found by the independent phase10 acceptance review."""
import tempfile
import unittest

from app.contracts import now_ms
from app.incidents import IncidentManager
from app.storage import Store


class ContextAndRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name)
        self.manager = IncidentManager(self.store)
        self.base = now_ms() - 600_000

    def event(self, service, at, status=200, kind='request', trace='', instance=None, **extra):
        return {'schema_version': 1, 'event_id': f'{service}-{kind}-{at}-{trace}-{instance}',
                'event_time': self.base+at, 'event_type': kind, 'service': service,
                'instance_id': instance or service+'-1', 'trace_id': trace,
                'status_code': status, 'latency_ms': 20, **extra}

    def fault_trace(self, trace='fault', at=20_000):
        rows = []
        for service, parent, downstream, status in (
                ('gateway', None, ['order'], 502), ('order', trace+'-gateway', ['payment'], 502),
                ('payment', trace+'-order', [], 503)):
            rows.append((self.event(service, at, status, trace=trace), None))
            rows.append((self.event(service, at, status, 'span', trace, span_id=trace+'-'+service,
                                    parent_span_id=parent, downstream=downstream), None))
        self.store.ingest_many(rows)

    def alert(self):
        return {'schema_version': 1, 'event_type': 'alert', 'alert_id': 'review-alert',
                'service': 'gateway', 'rule_id': 'high_error_rate', 'window_start': self.base,
                'window_end': self.base+30_000, 'evidence': {'sample_count': 30, 'error_rate': 1}}

    def opened(self, manager=None):
        return (manager or self.manager).ingest_alert(self.alert(), now=self.base+35_000)

    def healthy(self, start=40_000, end=100_000, gateway_instance='gateway-1'):
        self.store.ingest_many([(self.event(service, at, instance=gateway_instance if service == 'gateway'
                                           else service+'-1'), None)
                               for at in range(start, end+1, 1000)
                               for service in ('gateway', 'order', 'payment')])

    def test_unrelated_twenty_thousand_events_cannot_displace_root_evidence(self):
        self.fault_trace()
        noise = []
        for index in range(20_000):
            value = self.event('unrelated', 1000)
            value['event_id'] = 'noise-'+str(index)
            noise.append((value, None))
        self.store.ingest_many(noise)
        incident = self.opened()
        self.assertEqual(incident['root_anchor'], 'payment')
        self.assertTrue(incident['correlation_verified'])
        self.assertEqual(incident['affected_services'], ['gateway', 'order', 'payment'])
        self.assertTrue(incident['correlation_coverage']['complete'])
        self.assertEqual(incident['correlation_coverage']['seeds']['records'], 2)
        self.assertEqual(incident['correlation_coverage']['trace_records']['records'], 6)
        self.assertTrue(incident['origin_evidence_ids'])

    def test_keyset_pages_keep_equal_timestamp_records_and_trace_chunks(self):
        for i in range(66):
            self.fault_trace('trace-'+str(i))
        manager = IncidentManager(self.store, context_page_size=7)
        incident = self.opened(manager)
        coverage = incident['correlation_coverage']
        self.assertEqual(incident['root_anchor'], 'payment')
        self.assertTrue(incident['correlation_verified'])
        self.assertEqual(coverage['seeds']['records'], 132)
        self.assertEqual(coverage['trace_records']['records'], 396)
        self.assertEqual(coverage['selected_trace_count'], 66)
        self.assertGreater(coverage['trace_records']['pages'], 2)
        self.assertEqual(len(incident['origin_evidence_ids']), 66)

    def test_seed_row_limit_is_visible_and_never_certifies_correlation(self):
        self.fault_trace()
        manager = IncidentManager(self.store, context_page_size=1, context_event_limit=1)
        with self.assertLogs('app.incidents', level='WARNING'):
            incident = self.opened(manager)
        self.assertTrue(incident['correlation_truncated'])
        self.assertEqual(incident['root_instance_id'], 'unknown')
        self.assertFalse(incident['correlation_verified'])
        self.assertEqual(incident['correlation_coverage']['seeds']['truncation_reason'], 'record-limit')
        self.healthy()
        manager.tick(now=self.base+60_000)
        manager.tick(now=self.base+70_000)
        self.assertEqual(manager.get(incident['incident_id'])['status'], 'open')

    def test_trace_record_limit_is_visible_even_with_complete_seeds(self):
        self.fault_trace()
        manager = IncidentManager(self.store, context_page_size=1, context_event_limit=2)
        with self.assertLogs('app.incidents', level='WARNING'):
            incident = self.opened(manager)
        coverage = incident['correlation_coverage']
        self.assertFalse(coverage['seeds']['truncated'])
        self.assertTrue(coverage['trace_records']['truncated'])
        self.assertEqual(coverage['trace_records']['records'], 2)
        self.assertFalse(incident['correlation_verified'])

    def test_trace_id_limit_is_visible_and_conservative(self):
        self.fault_trace('first')
        self.fault_trace('second')
        manager = IncidentManager(self.store, context_trace_limit=1)
        with self.assertLogs('app.incidents', level='WARNING'):
            incident = self.opened(manager)
        self.assertTrue(incident['correlation_coverage']['trace_ids_truncated'])
        self.assertEqual(len(incident['trace_ids']), 1)
        self.assertFalse(incident['correlation_verified'])

    def test_byte_limit_is_visible_before_decoding_large_payloads(self):
        value = self.event('gateway', 20_000, 502, text='x'*4000)
        self.store.ingest(value)
        manager = IncidentManager(self.store, context_byte_limit=1024)
        with self.assertLogs('app.incidents', level='WARNING'):
            incident = self.opened(manager)
        coverage = incident['correlation_coverage']['seeds']
        self.assertEqual(coverage['truncation_reason'], 'byte-limit')
        self.assertEqual(coverage['records'], 0)
        self.assertFalse(incident['correlation_verified'])

    def test_exact_record_limit_does_not_claim_truncation(self):
        self.store.ingest_many([(self.event('gateway', at, 503), None) for at in (10_000, 20_000)])
        incident = self.opened(IncidentManager(self.store, context_page_size=1, context_event_limit=2))
        self.assertTrue(incident['correlation_coverage']['complete'])
        self.assertFalse(incident['correlation_truncated'])
        self.assertEqual(incident['root_instance_id'], 'gateway-1')

    def test_fresh_error_span_blocks_recovery_and_restarts_healthy_hold(self):
        self.fault_trace()
        incident = self.opened()
        iid = incident['incident_id']
        self.healthy(gateway_instance='gateway-2')
        self.manager.tick(now=self.base+60_000)
        self.assertEqual(self.manager.get(iid)['status'], 'recovering')
        failed_span = self.event('gateway', 65_000, 502, 'span', 'ongoing', instance='gateway-1',
                                 span_id='ongoing-gateway', parent_span_id=None, downstream=['order'])
        self.store.ingest(failed_span)
        self.manager.tick(now=self.base+70_000)
        current = self.manager.get(iid)
        self.assertEqual(current['status'], 'open')
        self.assertIsNone(current['recovery_started_at'])
        self.assertEqual(current['last_anomaly_at'], self.base+65_000)
        service = current['recovery']['services']['gateway']
        self.assertFalse(service['healthy'])
        self.assertEqual(service['bad_span_samples'], 1)
        self.assertEqual(service['blocking_span_evidence_ids'], [failed_span['event_id']])
        self.manager.tick(now=self.base+80_000)
        self.assertEqual(self.manager.get(iid)['status'], 'recovering')
        self.manager.tick(now=self.base+90_000)
        self.assertEqual(self.manager.get(iid)['status'], 'resolved')
        self.assertEqual([n['kind'] for n in self.manager.notifications(iid)], ['opened', 'resolved'])

    def test_old_future_and_unrelated_error_spans_do_not_block_recovery(self):
        self.fault_trace()
        incident = self.opened()
        self.healthy()
        for service, at, trace in (('gateway', 10_000, 'old'), ('gateway', 100_000, 'future'),
                                   ('unrelated', 65_000, 'unrelated')):
            self.store.ingest(self.event(service, at, 502, 'span', trace, span_id=trace,
                                         parent_span_id=None, downstream=[]))
        self.manager.tick(now=self.base+60_000)
        self.manager.tick(now=self.base+70_000)
        self.assertEqual(self.manager.get(incident['incident_id'])['status'], 'resolved')

    def test_healthy_spans_do_not_replace_required_request_samples(self):
        self.fault_trace()
        incident = self.opened()
        for service in ('gateway', 'order', 'payment'):
            self.store.ingest_many([(self.event(service, at, 200, 'span', str(at), span_id=str(at),
                                                parent_span_id=None, downstream=[]), None)
                                   for at in range(40_000, 80_001, 1000)])
        self.manager.tick(now=self.base+60_000)
        self.manager.tick(now=self.base+70_000)
        self.assertEqual(self.manager.get(incident['incident_id'])['status'], 'open')

    def test_recovery_span_budget_overflow_cannot_claim_health(self):
        self.fault_trace()
        incident = self.opened()
        self.healthy()
        self.store.ingest_many([(self.event('gateway', 65_000, 200, 'span', 'large-'+str(i),
                                           span_id='large-'+str(i), parent_span_id=None,
                                           downstream=[], text='x'*(512*1024)), None)
                               for i in range(20)])
        self.manager.tick(now=self.base+60_000)
        self.manager.tick(now=self.base+70_000)
        current = self.manager.get(incident['incident_id'])
        self.assertEqual(current['status'], 'open')
        self.assertTrue(current['recovery']['services']['gateway']['span_coverage']['truncated'])

    def test_legacy_incident_without_coverage_fields_remains_readable_and_recovers(self):
        self.fault_trace()
        incident = self.opened()
        incident.pop('correlation_coverage')
        incident.pop('correlation_truncated')
        with self.store.db() as db:
            self.manager._save(db, incident)
        self.healthy()
        reopened = IncidentManager(self.store)
        reopened.tick(now=self.base+60_000)
        reopened.tick(now=self.base+70_000)
        self.assertEqual(reopened.get(incident['incident_id'])['status'], 'resolved')

    def test_context_budgets_reject_invalid_configuration(self):
        for name in ('context_page_size', 'context_event_limit', 'context_trace_limit', 'context_byte_limit'):
            for value in (0, -1, True, 1.5, 100_000_000):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    IncidentManager(self.store, **{name: value})


if __name__ == '__main__':
    unittest.main()
