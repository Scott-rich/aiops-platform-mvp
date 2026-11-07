import tempfile
import unittest

from app.contracts import now_ms
from app.incidents import IncidentManager
from app.storage import Store


class IncidentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name)
        self.manager = IncidentManager(self.store)
        self.base = now_ms()-600_000

    def event(self, service, t, status=200, trace="", kind="request", **extra):
        event = {"schema_version": 1, "event_id": f"{service}-{kind}-{t}-{trace}-{len(extra)}",
                 "event_time": self.base+t, "event_type": kind, "service": service,
                 "instance_id": service+"-1", "trace_id": trace,
                 "status_code": status, "latency_ms": 20, **extra}
        self.store.ingest(event)
        return event

    def trace(self, t, trace, failed="payment", failed_instance=None):
        span_ids = {s: trace+"-"+s for s in ("gateway", "order", "inventory", "payment")}
        for service in span_ids:
            downstream = {"gateway": ["order"], "order": ["inventory", "payment"], "inventory": [], "payment": []}[service]
            status = 503 if service == failed else (502 if service in {"gateway", "order"} else 200)
            if failed is None:
                status = 200
            self.event(service, t, status, trace)
            parent = None if service == "gateway" else span_ids["gateway" if service == "order" else "order"]
            extra = {"span_id": span_ids[service], "parent_span_id": parent, "downstream": downstream}
            if service == failed and failed_instance:
                extra["instance_id"] = failed_instance
            self.event(service, t, status, trace, "span", **extra)

    def alert(self, service="gateway", start=0, end=30_000, suffix="", rule="high_error_rate"):
        return {"schema_version": 1, "event_type": "alert", "alert_id": f"{service}-{rule}-{start}-{end}-{suffix}",
                "service": service, "rule_id": rule, "window_start": self.base+start,
                "window_end": self.base+end, "evidence": {"sample_count": 30, "error_rate": 1}}

    def healthy(self, start, end, services=("gateway", "order", "payment")):
        for t in range(start, end+1, 1000):
            for service in services:
                self.event(service, t)

    def test_fifteen_correlated_windows_are_one_durable_incident(self):
        for t in range(10_000, 40_000, 1000):
            self.trace(t, "trace-"+str(t))
        first = None
        for end in (20_000, 30_000, 40_000, 50_000, 60_000):
            for service in ("gateway", "order", "payment"):
                alert = self.alert(service, end-30_000, end)
                incident = self.manager.ingest_alert(alert, now=self.base+end+5000)
                first = first or incident["incident_id"]
                self.assertEqual(first, incident["incident_id"])
        self.assertEqual(1, len(self.manager.list()))
        saved = IncidentManager(self.store).get(first)
        self.assertEqual(15, saved["alert_count"])
        self.assertEqual("payment", saved["candidate_service"])
        self.assertEqual(["gateway", "order", "payment"], saved["services"])
        self.assertEqual(15, self.store.stats()["alerts"])
        self.assertEqual(["opened"], [n["kind"] for n in self.manager.notifications(first)])
        replay = self.manager.ingest_alert(self.alert("gateway", -10_000, 20_000), now=self.base+100_000)
        self.assertEqual(15, replay["alert_count"])

    def test_unrelated_leaf_faults_do_not_merge(self):
        self.trace(10_000, "payment-failure", "payment")
        self.trace(10_000, "inventory-failure", "inventory")
        a = self.manager.ingest_alert(self.alert("payment"), now=self.base+35_000)
        b = self.manager.ingest_alert(self.alert("inventory"), now=self.base+35_000)
        self.assertNotEqual(a["incident_id"], b["incident_id"])
        self.assertEqual({"payment", "inventory"}, {a["root_anchor"], b["root_anchor"]})

    def test_different_instances_do_not_merge(self):
        self.trace(10_000, "instance-a", failed_instance="payment-a")
        a = self.manager.ingest_alert(self.alert("payment", 9000, 11_000), now=self.base+15_000)
        self.trace(20_000, "instance-b", failed_instance="payment-b")
        b = self.manager.ingest_alert(self.alert("payment", 19_000, 21_000), now=self.base+25_000)
        self.assertNotEqual(a["incident_id"], b["incident_id"])

    def test_cross_service_same_origin_needs_a_shared_trace(self):
        self.trace(10_000, "older")
        a = self.manager.ingest_alert(self.alert("gateway", 9000, 11_000), now=self.base+15_000)
        self.trace(20_000, "independent")
        b = self.manager.ingest_alert(self.alert("order", 19_000, 21_000), now=self.base+25_000)
        self.assertNotEqual(a["incident_id"], b["incident_id"])

    def test_resolve_once_then_historic_alert_does_not_reopen(self):
        self.trace(20_000, "fault")
        opened = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        self.healthy(40_000, 80_000)
        self.manager.tick(now=self.base+60_000)
        self.assertEqual("recovering", self.manager.get(opened["incident_id"])["status"])
        self.manager.tick(now=self.base+70_000)
        closed = self.manager.get(opened["incident_id"])
        self.assertEqual("resolved", closed["status"])
        self.manager.tick(now=self.base+80_000)
        historical = self.manager.ingest_alert(self.alert("order", 0, 90_000, "late"), now=self.base+95_000)
        self.assertEqual(opened["incident_id"], historical["incident_id"])
        self.assertEqual("resolved", historical["status"])
        self.assertEqual(2, historical["alert_count"])
        kinds = [n["kind"] for n in IncidentManager(self.store).notifications(opened["incident_id"])]
        self.assertEqual(["opened", "resolved"], kinds)

    def test_genuinely_new_failure_after_resolution_opens_new_incident(self):
        self.trace(20_000, "first")
        first = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        self.healthy(40_000, 80_000)
        self.manager.tick(now=self.base+60_000)
        self.manager.tick(now=self.base+70_000)
        self.trace(110_000, "recurrence")
        second = self.manager.ingest_alert(self.alert("gateway", 100_000, 120_000), now=self.base+125_000)
        self.assertNotEqual(first["incident_id"], second["incident_id"])
        self.assertEqual("open", second["status"])
        self.assertEqual(2, len(self.manager.list()))

    def test_historic_replay_after_raw_evidence_expiry_stays_closed(self):
        self.trace(20_000, "first")
        incident = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        self.healthy(40_000, 80_000)
        self.manager.tick(now=self.base+60_000)
        self.manager.tick(now=self.base+70_000)
        with self.store.db() as db:
            db.execute("DELETE FROM events")
        historical = self.manager.ingest_alert(self.alert("gateway", 5000, 35_000, "late-without-raw"),
                                               now=self.base+100_000)
        self.assertEqual(incident["incident_id"], historical["incident_id"])
        self.assertEqual("resolved", historical["status"])
        self.assertEqual(self.base+20_000, historical["last_anomaly_at"])
        self.assertEqual(2, len(self.manager.notifications()))

    def test_stale_or_missing_metrics_never_resolve(self):
        self.trace(20_000, "fault")
        incident = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        self.healthy(50_000, 60_000)
        self.manager.tick(now=self.base+60_000)
        self.assertEqual("recovering", self.manager.get(incident["incident_id"])["status"])
        self.manager.tick(now=self.base+80_000)
        missing = self.manager.get(incident["incident_id"])
        self.assertEqual("open", missing["status"])
        self.assertIsNone(missing["resolved_at"])
        self.assertIsNone(missing["recovery_started_at"])
        self.assertEqual(1, len(self.manager.notifications()))

    def test_recent_bad_metrics_cancel_recovery_without_waiting_for_alert(self):
        self.trace(20_000, "fault")
        incident = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        self.healthy(50_000, 65_000)
        self.manager.tick(now=self.base+60_000)
        self.event("payment", 61_000, 503, "new-bad")
        self.manager.tick(now=self.base+65_000)
        current = self.manager.get(incident["incident_id"])
        self.assertEqual("open", current["status"])
        self.assertEqual(self.base+61_000, current["last_anomaly_at"])

    def test_low_traffic_and_dense_late_burst_do_not_claim_continuous_recovery(self):
        self.trace(20_000, "fault")
        incident = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        for service in ("gateway", "order", "payment"):
            # Enough samples, but only within the last second, with no prior coverage.
            for t in range(59_000, 60_000, 50):
                self.event(service, t)
        self.manager.tick(now=self.base+60_000)
        self.assertEqual("open", self.manager.get(incident["incident_id"])["status"])
        # Sparse, individually fresh samples are insufficient too.
        for service in ("gateway", "order", "payment"):
            for t in (71_000, 74_000, 77_000, 80_000):
                self.event(service, t)
        self.manager.tick(now=self.base+80_000)
        self.assertEqual("open", self.manager.get(incident["incident_id"])["status"])
        self.assertEqual(1, len(self.manager.notifications()))

    def test_fast_and_regular_errors_share_family_but_latency_stays_separate(self):
        self.trace(20_000, "fault")
        fast = self.manager.ingest_alert(self.alert(rule="fast_errors"), now=self.base+35_000)
        regular = self.manager.ingest_alert(self.alert(), now=self.base+35_000)
        self.assertEqual(fast["incident_id"], regular["incident_id"])
        latency = self.manager.ingest_alert(self.alert(rule="high_latency"), now=self.base+35_000)
        self.assertNotEqual(fast["incident_id"], latency["incident_id"])

    def test_late_leaf_evidence_upgrades_provisional_anchor_without_new_notification(self):
        self.event("gateway", 20_000, 502, "late-leaf")
        self.event("gateway", 20_000, 502, "late-leaf", "span", span_id="late-leaf-gateway",
                   parent_span_id=None, downstream=["order"])
        provisional = self.manager.ingest_alert(self.alert(rule="fast_errors"), now=self.base+35_000)
        self.assertEqual("gateway", provisional["root_anchor"])
        self.assertFalse(provisional["correlation_verified"])
        self.trace(20_000, "late-leaf")
        correlated = self.manager.ingest_alert(self.alert("order"), now=self.base+36_000)
        self.assertEqual(provisional["incident_id"], correlated["incident_id"])
        self.assertEqual("payment", correlated["root_anchor"])
        self.assertTrue(correlated["correlation_verified"])
        self.assertEqual(1, len(self.manager.notifications()))

    def test_resource_recovery_requires_fresh_host_samples(self):
        for t in (10_000, 15_000, 20_000):
            self.event("linux-node", t, kind="host", cpu_percent=96, memory_percent=30)
        incident = self.manager.ingest_alert(self.alert("linux-node", rule="high_cpu"), now=self.base+35_000)
        for t in (50_000, 55_000, 60_000, 65_000, 70_000):
            self.event("linux-node", t, kind="host", cpu_percent=20, memory_percent=30)
        self.manager.tick(now=self.base+60_000)
        self.assertEqual("recovering", self.manager.get(incident["incident_id"])["status"])
        self.manager.tick(now=self.base+70_000)
        self.assertEqual("resolved", self.manager.get(incident["incident_id"])["status"])


if __name__ == "__main__":
    unittest.main()
