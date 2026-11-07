"""Independent labelled cases and evaluation accounting; no live model calls."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from app.contracts import digest, validate_event
from scripts.evaluate_rca import analyze_input, evaluate, replay_report, score_rows
from tests.rca_scenarios import scenarios


class ScenarioContractTests(unittest.TestCase):
    def test_twelve_fixed_scenarios_have_valid_wire_events_and_separate_labels(self):
        first, second = scenarios(), scenarios()
        self.assertEqual(len(first), 12)
        self.assertEqual(first, second)
        self.assertEqual(len({case.case_id for case in first}), len(first))
        for case in first:
            with self.subTest(case=case.case_id):
                self.assertNotIn("labels", case.snapshot)
                self.assertNotIn("root_services", case.snapshot)
                for row in case.snapshot["records"]:
                    validate_event(row["event"])

    def test_algorithm_receives_evidence_and_alert_but_never_labels(self):
        seen = []
        def recorder(snapshot, **kwargs):
            seen.append((snapshot, kwargs))
            return {"status": "unknown", "candidates": [], "affected_services": [], "direct_anomalous_services": []}
        rows, _ = evaluate(recorder)
        self.assertEqual(len(seen), len(rows))
        for snapshot, kwargs in seen:
            self.assertEqual(set(kwargs), {"alert"})
            value = json.dumps([snapshot, kwargs])
            self.assertNotIn("expected_status", value)
            self.assertNotIn("root_services", value)
            self.assertNotIn("forbidden_edges", value)


class MetricAccountingTests(unittest.TestCase):
    def test_failed_executions_stay_in_denominator_and_multi_root_coverage_is_separate(self):
        def row(roots, prediction, expected="candidates", predicted="candidates", healthy=False, ok=True):
            return {"labels": {"root_services": roots, "expected_status": expected, "healthy": healthy},
                    "candidate_services": prediction, "predicted_status": predicted,
                    "execution_status": "ok" if ok else "error", "references_valid": ok}
        rows = [row(["a"], ["a"]), row(["b", "c"], ["x", "b"]),
                row(["d"], [], predicted=None, ok=False),
                row([], [], expected="unknown", predicted="unknown", healthy=True),
                row([], ["x"], expected="unknown")]
        metrics = score_rows(rows)
        self.assertEqual(metrics["top1_any_labelled_root_accuracy"], {"numerator": 1, "denominator": 3, "value": 1/3})
        self.assertEqual(metrics["top3_any_labelled_root_accuracy"], {"numerator": 2, "denominator": 3, "value": 2/3})
        self.assertEqual(metrics["multi_root_top3_coverage"]["value"], .5)
        self.assertEqual(metrics["multi_root_all_roots_in_top3"]["value"], 0)
        self.assertEqual(metrics["unknown_recall"]["value"], .5)
        self.assertEqual(metrics["healthy_false_positive_rate"]["value"], 0)
        self.assertEqual(metrics["execution_success_rate"]["value"], .8)

    def test_missing_references_fail_validation(self):
        case = scenarios()[0]
        def invalid(_snapshot, **_kwargs):
            return {"status": "candidates", "candidates": [{"service": "payment", "rank": 1,
                    "supporting_evidence_ids": ["nonexistent-evidence"], "against_evidence_ids": []}]}
        result = analyze_input(invalid, case.snapshot, case.alert)
        self.assertFalse(result["references_valid"])
        self.assertEqual(result["missing_evidence_ids"], ["nonexistent-evidence"])

    def test_unknown_has_no_candidate_label_denominator(self):
        self.assertIsNone(score_rows([])["top1_any_labelled_root_accuracy"]["value"])

    def test_archived_replay_is_unlabelled_and_hash_mismatch_is_rejected(self):
        case = scenarios()[4]
        def unknown(_snapshot, **_kwargs):
            return {"status": "unknown", "candidates": []}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            report = {"snapshot": case.snapshot, "alert": case.alert,
                      "audit": {"snapshot_sha256": digest(case.snapshot)}}
            path.write_text(json.dumps(report))
            replay = replay_report(path, unknown)
            self.assertFalse(replay["accuracy_eligible"])
            self.assertEqual(replay["source_kind"], "archived_unlabelled")
            report["audit"]["snapshot_sha256"] = "invalid"
            path.write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                replay_report(path, unknown)


class LabelledRCAEvaluationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows, cls.metrics = evaluate()
        cls.by_id = {row["case_id"]: row for row in cls.rows}

    def test_independent_expected_status_and_top_three_roots(self):
        for row in self.rows:
            with self.subTest(case=row["case_id"]):
                self.assertEqual(row["execution_status"], "ok")
                self.assertEqual(row["predicted_status"], row["labels"]["expected_status"])
                roots = set(row["labels"]["root_services"])
                if roots:
                    self.assertIn(row["candidate_services"][0], roots)
                    self.assertTrue(roots <= set(row["candidate_services"][:3]))
                else:
                    self.assertEqual(row["candidate_services"], [])

    def test_affected_callers_are_separate_from_root_candidates(self):
        for row in self.rows:
            with self.subTest(case=row["case_id"]):
                self.assertEqual(set(row["affected_services"]), set(row["labels"]["affected_services"]))
                self.assertFalse(set(row["affected_services"]) & set(row["labels"]["root_services"]))

    def test_all_candidates_cite_existing_same_service_evidence(self):
        cases = {case.case_id: case for case in scenarios()}
        for row in self.rows:
            with self.subTest(case=row["case_id"]):
                self.assertTrue(row["references_valid"])
                records = {r["event"]["event_id"]: r["event"] for r in cases[row["case_id"]].snapshot["records"]}
                for candidate in row["analysis"]["candidates"]:
                    refs = candidate["supporting_evidence_ids"]
                    self.assertTrue(any(records[ref]["service"] == candidate["service"] for ref in refs))

    def test_duplicates_and_order_do_not_change_candidate_or_affected_services(self):
        baseline = self.by_id["payment_503_cascade"]
        for name in ("duplicate_payment_events", "out_of_order_payment_events"):
            row = self.by_id[name]
            self.assertEqual(row["candidate_services"], baseline["candidate_services"])
            self.assertEqual(row["predicted_status"], baseline["predicted_status"])
            self.assertEqual(set(row["affected_services"]), set(baseline["affected_services"]))
            self.assertEqual(row["analysis"]["candidates"], baseline["analysis"]["candidates"])

    def test_no_unobserved_service_is_invented(self):
        cases = {case.case_id: case for case in scenarios()}
        for name in ("missing_child_insufficient", "orphan_abnormal_leaf"):
            observed = {row["event"]["service"] for row in cases[name].snapshot["records"]}
            prediction = self.by_id[name]
            self.assertTrue(set(prediction["candidate_services"]) <= observed)
            self.assertTrue(set(prediction["affected_services"]) <= observed)

    def test_dependency_edges_require_actual_same_trace_parent_pairs(self):
        cases = {case.case_id: case for case in scenarios()}
        for row in self.rows:
            with self.subTest(case=row["case_id"]):
                case = cases[row["case_id"]]
                events = {item["event"]["event_id"]: item["event"] for item in case.snapshot["records"]}
                edges = row["analysis"]["graph"]["edges"]
                links = {(edge["caller"], edge["callee"]) for edge in edges}
                self.assertFalse(links & {tuple(pair) for pair in case.labels["forbidden_edges"]})
                for edge in edges:
                    if edge.get("basis") != "parent_span":
                        continue
                    self.assertTrue(edge.get("observed_pairs"))
                    for pair in edge["observed_pairs"]:
                        caller, callee = events[pair["caller_event_id"]], events[pair["callee_event_id"]]
                        self.assertEqual(caller["trace_id"], callee["trace_id"])
                        self.assertEqual(pair["trace_id"], caller["trace_id"])
                        self.assertEqual(caller["span_id"], callee["parent_span_id"])
                        self.assertEqual((caller["service"], callee["service"]), (edge["caller"], edge["callee"]))


if __name__ == "__main__":
    unittest.main()
