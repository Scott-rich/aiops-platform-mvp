"""Small labelled, explicitly synthetic RCA cases; no model inference."""
import copy
import unittest

from app.contracts import digest
from app.rca import RCA_VERSION, analyze


def span(service, identifier, parent=None, *, trace="trace-a", status=502, latency=50, downstream=()):
    return {"schema_version": 1, "event_id": "event-" + identifier, "event_type": "span", "event_time": 1700000000000,
            "service": service, "instance_id": service + "-1", "trace_id": trace, "span_id": identifier,
            "parent_span_id": parent, "downstream": list(downstream), "status_code": status, "latency_ms": latency}


def snapshot(*events, truncated=False):
    return {"records": [{"event": e, "source": {"transport": "explicit-test-fixture"}} for e in events],
            "coverage": {"truncated": truncated}}


def chain():
    return [span("gateway", "g", downstream=["order"]), span("order", "o", "g", downstream=["payment"]),
            span("payment", "p", "o", status=503)]


def model(service, reference):
    return {"analysis_schema_version": 2, "diagnosis_status": "candidates", "summary": "Candidate needs verification",
            "involved_services": [service], "hypotheses": [{"cause": "Observed service anomaly", "service": service,
            "confidence": "low", "confidence_basis": "Observed error only", "supporting_evidence_ids": [reference],
            "against_evidence_ids": [], "uncertainty": ["Internal cause unknown"], "verification_steps": ["Collect metrics"],
            "remediation_steps": ["Verify before acting"]}], "uncertainties": ["No internal metrics"],
            "verification_steps": ["Collect metrics"], "recommendations": ["Review evidence"]}


class RCATests(unittest.TestCase):
    def assert_references_exist(self, result, source):
        known = {r["event"]["event_id"]: r["event"] for r in source["records"]}
        for edge in result["graph"]["edges"]:
            self.assertTrue(set(edge["evidence_ids"]) <= known.keys())
        for candidate in result["candidates"]:
            self.assertTrue(set(candidate["supporting_evidence_ids"] + candidate["against_evidence_ids"]) <= known.keys())
            self.assertTrue(any(known[ref]["service"] == candidate["service"] for ref in candidate["own_anomalous_evidence_ids"]))
            self.assertFalse(set(candidate["supporting_evidence_ids"]) & set(candidate["against_evidence_ids"]))

    def test_cascade_root_is_callee_and_callers_are_affected(self):
        source = snapshot(*chain())
        result = analyze(source)
        self.assertEqual(result["rca_version"], RCA_VERSION)
        self.assertEqual([c["service"] for c in result["candidates"]], ["payment"])
        self.assertEqual(result["direct_anomalous_services"], ["gateway", "order", "payment"])
        self.assertEqual(result["affected_services"], ["gateway", "order"])
        self.assertEqual(result["candidates"][0]["confidence"], "medium")
        self.assertEqual(result["evidence_set_id"], "evidence-" + digest(source))
        self.assert_references_exist(result, source)

    def test_latency_chain_uses_same_trace(self):
        rows = chain()
        for row, latency in zip(rows, (1800, 1500, 1200)):
            row.update(status_code=200, latency_ms=latency)
        result = analyze(snapshot(*rows))
        self.assertEqual([c["service"] for c in result["candidates"]], ["payment"])
        self.assertEqual(result["candidates"][0]["anomaly_channels"], ["latency"])

    def test_parallel_anomalous_leaves_remain_two_candidates(self):
        rows = chain() + [span("inventory", "i", "o", status=503)]
        rows[1]["downstream"].append("inventory")
        result = analyze(snapshot(*rows))
        self.assertEqual({c["service"] for c in result["candidates"]}, {"payment", "inventory"})
        self.assertEqual({c["rank"] for c in result["candidates"]}, {1})

    def test_same_span_id_in_different_traces_is_not_a_parent_edge(self):
        rows = [span("caller", "same", trace="first"), span("callee", "child", "same", trace="second")]
        result = analyze(snapshot(*rows))
        self.assertEqual(result["graph"]["edges"], [])
        self.assertEqual(result["affected_services"], [])
        self.assertEqual({c["service"] for c in result["candidates"]}, {"caller", "callee"})

    def test_service_graph_edges_cannot_splice_propagation_across_traces(self):
        rows = [span("a", "a1", trace="one"), span("b", "b1", "a1", trace="one"),
                span("b", "b2", trace="two"), span("c", "c2", "b2", trace="two")]
        result = analyze(snapshot(*rows))
        candidates = {c["service"]: c for c in result["candidates"]}
        self.assertEqual(set(candidates), {"b", "c"})
        self.assertEqual(candidates["c"]["affected_services"], ["b"])
        self.assertNotIn("event-a1", candidates["c"]["supporting_evidence_ids"])

    def test_declared_only_edge_does_not_invent_a_root(self):
        source = snapshot(span("gateway", "g", status=200, downstream=["missing"]))
        result = analyze(source, alert={"service": "missing", "rule_id": "high_error_rate"})
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["graph"]["edges"][0]["basis"], "declared_only")
        self.assert_references_exist(result, source)

    def test_missing_span_and_truncation_reduce_confidence(self):
        full = analyze(snapshot(*chain()))["candidates"][0]
        rows = chain()
        rows[2]["downstream"] = ["unobserved"]
        weak = analyze(snapshot(*rows, truncated=True))["candidates"][0]
        self.assertEqual(weak["confidence"], "low")
        self.assertLess(weak["score"], full["score"])

    def test_cycle_returns_unknown_without_recursive_loop(self):
        result = analyze(snapshot(span("a", "a", "b"), span("b", "b", "a")))
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["input_quality"]["cycle_spans"], 2)

    def test_healthy_child_does_not_displace_anomalous_parent(self):
        result = analyze(snapshot(span("gateway", "g"), span("order", "o", "g"),
                                  span("inventory", "i", "o", status=200)))
        self.assertEqual([c["service"] for c in result["candidates"]], ["order"])
        self.assertEqual(result["affected_services"], ["gateway"])

    def test_counterevidence_requires_same_service_problem_and_instance(self):
        rows = chain() + [span("inventory", "healthy-other", status=200),
                          span("payment", "healthy-own", status=200, trace="healthy")]
        other_instance = span("payment", "other-instance", status=200, trace="different")
        other_instance["instance_id"] = "payment-2"
        result = analyze(snapshot(*(rows + [other_instance])))
        candidate = result["candidates"][0]
        self.assertEqual(candidate["against_evidence_ids"], ["event-healthy-own"])

    def test_host_metrics_use_existing_sample_and_average_gates(self):
        base = {"event_type": "host", "service": "node", "instance_id": "node-1", "cpu_percent": 96,
                "memory_percent": 10, "event_time": 1700000000000}
        rows = [{**base, "event_id": "host-" + str(i)} for i in range(3)]
        self.assertEqual(analyze(snapshot(*rows[:2]))["status"], "unknown")
        result = analyze(snapshot(*rows))
        self.assertEqual(result["candidates"][0]["anomaly_channels"], ["cpu"])
        self.assertEqual(result["candidates"][0]["confidence"], "low")
        healthy = [{**e, "cpu_percent": 90} for e in rows]
        self.assertEqual(analyze(snapshot(*healthy))["status"], "unknown")

    def test_duplicates_and_order_do_not_change_candidates_or_edges(self):
        rows = chain()
        expected = analyze(snapshot(*rows))
        actual = analyze(snapshot(*(list(reversed(rows)) + [copy.deepcopy(rows[2])])))
        self.assertEqual(expected["candidates"], actual["candidates"])
        self.assertEqual(expected["graph"], actual["graph"])

    def test_conflicting_event_ids_are_excluded_and_invalid_input_is_clear(self):
        row = span("a", "a")
        result = analyze(snapshot(row, {**row, "service": "b"}))
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["input_quality"]["conflicting_event_ids"], 1)
        with self.assertRaisesRegex(ValueError, "rca_snapshot_records_required"):
            analyze({})

    def test_history_and_validated_model_do_not_modify_rule_scores(self):
        source = snapshot(*chain())
        baseline = analyze(source)
        case = {"case_id": "synthetic-case", "services": ["payment"], "title": "Example only"}
        result = analyze(source, historical_cases=[{"case": case, "case_sha256": digest(case)}],
                         model_analysis=model("payment", "event-p"), model_metadata={"model_status": "validated"})
        self.assertEqual(result["candidates"][0]["score"], baseline["candidates"][0]["score"])
        self.assertEqual(len(result["candidates"][0]["historical_case_matches"]), 1)
        self.assertEqual(len(result["candidates"][0]["model_hypotheses"]), 1)

    def test_model_requires_validated_status_and_existing_evidence(self):
        source = snapshot(*chain())
        untrusted = analyze(source, model_analysis=model("payment", "event-p"))
        self.assertEqual(untrusted["source_integration"]["model_status"], "ignored_unvalidated")
        invalid = analyze(source, model_analysis=model("payment", "invented"), model_metadata={"model_status": "validated"})
        self.assertEqual(invalid["source_integration"]["model_status"], "rejected_invalid_structure_or_evidence")
        disagreement = analyze(source, model_analysis=model("gateway", "event-g"), model_metadata={"model_status": "validated"})
        self.assertEqual([c["service"] for c in disagreement["candidates"]], ["payment"])
        self.assertEqual(len(disagreement["source_integration"]["disagreements"]), 2)

    def test_request_without_span_stays_low_confidence(self):
        row = {**span("isolated", "r", status=503), "event_type": "request"}
        result = analyze(snapshot(row))
        self.assertEqual(result["candidates"][0]["confidence"], "low")
        self.assertEqual(result["affected_services"], [])

    def test_anomaly_channels_are_attributed_independently(self):
        rows = [span("gateway", "g", latency=1600), span("order", "o", "g", latency=1500),
                span("payment", "p", "o", status=200, latency=1400)]
        result = analyze(snapshot(*rows))
        candidates = {c["service"]: c for c in result["candidates"]}
        self.assertEqual(set(candidates), {"order", "payment"})
        self.assertEqual(candidates["order"]["anomaly_channels"], ["http_5xx"])
        self.assertEqual(candidates["payment"]["anomaly_channels"], ["latency"])
        self.assertIn("order", result["affected_services"])
        node = next(n for n in result["graph"]["nodes"] if n["service"] == "order")
        self.assertTrue(node["candidate"] and node["affected"])

    def test_latency_residual_retains_parent_as_an_independent_candidate(self):
        rows = [span("gateway", "g", status=200, latency=5100),
                span("order", "o", "g", status=200, latency=5000),
                span("payment", "p", "o", status=200, latency=1000)]
        source = snapshot(*rows)
        result = analyze(source)
        candidates = {c["service"]: c for c in result["candidates"]}
        self.assertEqual(set(candidates), {"order", "payment"})
        self.assertEqual(candidates["order"]["latency_residual_observations"][0]["observed_latency_residual_ms"], 4000)
        self.assert_references_exist(result, source)

    def test_propagation_cannot_change_anomaly_channel_mid_path(self):
        rows = [span("a", "a", status=200, latency=1000),
                span("b", "b", "a", status=503, latency=1000),
                span("c", "c", "b", status=503, latency=20),
                span("d", "d", "c", status=503, latency=1000)]
        result = analyze(snapshot(*rows))
        root = next(c for c in result["candidates"] if c["service"] == "d")
        self.assertNotIn("a", root["affected_services"])
        self.assertNotIn("event-a", root["supporting_evidence_ids"])

    def test_ambiguous_span_identity_is_not_used_for_localization(self):
        first = span("a", "collision")
        second = {**span("b", "collision"), "event_id": "event-distinct"}
        result = analyze(snapshot(first, second))
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["input_quality"]["ambiguous_span_ids"], 1)


if __name__ == "__main__":
    unittest.main()
