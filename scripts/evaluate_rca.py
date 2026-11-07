"""Evaluate deterministic RCA on explicit synthetic labels and optional unlabelled replay."""
import argparse
from copy import deepcopy
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import sys
import time

from app.contracts import digest
from tests.rca_scenarios import scenarios


def evidence_references(value):
    refs = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key.endswith("evidence_ids") and isinstance(child, list):
                refs.extend(child)
            elif isinstance(child, (dict, list)):
                refs.extend(evidence_references(child))
    elif isinstance(value, list):
        for child in value:
            refs.extend(evidence_references(child))
    return refs


def score_rows(rows):
    """Metric denominators include failed executions; never omit difficult cases."""
    labelled = [row for row in rows if row["labels"]["expected_status"] == "candidates" and row["labels"]["root_services"]]
    unknown = [row for row in rows if row["labels"]["expected_status"] == "unknown"]
    healthy = [row for row in rows if row["labels"].get("healthy")]
    multiple = [row for row in labelled if len(row["labels"]["root_services"]) > 1]

    def metric(numerator, denominator):
        return {"numerator": numerator, "denominator": denominator,
                "value": numerator / denominator if denominator else None}

    def hit(row, k):
        return bool(set(row.get("candidate_services", [])[:k]) & set(row["labels"]["root_services"]))

    multi_covered = sum(len(set(row.get("candidate_services", [])[:3]) & set(row["labels"]["root_services"])) for row in multiple)
    multi_roots = sum(len(row["labels"]["root_services"]) for row in multiple)
    return {
        "top1_any_labelled_root_accuracy": metric(sum(hit(row, 1) for row in labelled), len(labelled)),
        "top3_any_labelled_root_accuracy": metric(sum(hit(row, 3) for row in labelled), len(labelled)),
        "multi_root_top3_coverage": metric(multi_covered, multi_roots),
        "multi_root_all_roots_in_top3": metric(sum(set(row["labels"]["root_services"]) <= set(row.get("candidate_services", [])[:3]) for row in multiple), len(multiple)),
        "unknown_recall": metric(sum(row.get("predicted_status") == "unknown" for row in unknown), len(unknown)),
        "healthy_false_positive_rate": metric(sum(row.get("predicted_status") == "candidates" or bool(row.get("candidate_services")) for row in healthy), len(healthy)),
        "execution_success_rate": metric(sum(row.get("execution_status") == "ok" for row in rows), len(rows)),
        "evidence_reference_validity": metric(sum(row.get("references_valid") is True for row in rows), len(rows)),
    }


def analyze_input(analyzer, snapshot, alert=None):
    before = time.perf_counter_ns()
    # No labels, scenario description, or expected root is passed to the algorithm.
    result = analyzer(deepcopy(snapshot), alert=deepcopy(alert))
    elapsed_ms = (time.perf_counter_ns() - before) / 1_000_000
    candidates = sorted(result.get("candidates", []), key=lambda item: (item.get("rank", 10**9), item.get("service", "")))
    event_ids = {row.get("event", row)["event_id"] for row in snapshot["records"]}
    refs = evidence_references(result)
    valid = all(isinstance(ref, str) and ref in event_ids for ref in refs)
    nonempty = all(bool(candidate.get("supporting_evidence_ids")) for candidate in candidates)
    return {"execution_status": "ok", "predicted_status": result.get("status"),
            "candidate_services": [item["service"] for item in candidates],
            "affected_services": result.get("affected_services", []),
            "direct_anomalous_services": result.get("direct_anomalous_services", []),
            "references_valid": valid and nonempty, "reference_count": len(refs),
            "missing_evidence_ids": sorted({str(ref) for ref in refs if not isinstance(ref, str) or ref not in event_ids}),
            "runtime_ms": round(elapsed_ms, 6), "analysis": result}


def evaluate(analyzer=None):
    if analyzer is None:
        from app.rca import analyze
        analyzer = analyze
    rows = []
    for case in scenarios():
        row = {"case_id": case.case_id, "family": case.family, "description": case.description,
               "source_kind": "synthetic_labelled", "labels": deepcopy(case.labels),
               "input_sha256": digest({"snapshot": case.snapshot, "alert": case.alert})}
        try:
            row.update(analyze_input(analyzer, case.snapshot, case.alert))
        except Exception as error:
            row.update(execution_status="error", error_type=type(error).__name__,
                       predicted_status=None, candidate_services=[], references_valid=False, runtime_ms=None)
        rows.append(row)
    return rows, score_rows(rows)


def replay_report(path, analyzer):
    raw = path.read_bytes()
    report = json.loads(raw)
    snapshot = report["snapshot"]
    expected_hash = report.get("audit", {}).get("snapshot_sha256")
    if expected_hash and digest(snapshot) != expected_hash:
        raise ValueError("archived snapshot hash mismatch")
    return {"source_kind": "archived_unlabelled", "path": str(path),
            "source_file_sha256": hashlib.sha256(raw).hexdigest(),
            "archived_snapshot_hash_present": expected_hash is not None,
            "snapshot_sha256": digest(snapshot), "accuracy_eligible": False,
            "note": "Historical evidence replay; no independent root-cause labels, excluded from accuracy denominators.",
            **analyze_input(analyzer, snapshot, report.get("alert"))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Fresh result directory; existing directories are preserved.")
    parser.add_argument("--archived-report", type=Path, action="append", default=[],
                        help="Optional historical report JSON; never counted as labelled accuracy.")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    from app.rca import analyze
    started = time.perf_counter()
    rows, metrics = evaluate(analyze)
    archived = []
    for path in args.archived_report:
        try:
            archived.append(replay_report(path, analyze))
        except Exception as error:
            archived.append({"source_kind": "archived_unlabelled", "path": str(path), "accuracy_eligible": False,
                             "execution_status": "error", "error_type": type(error).__name__})
    result = {"schema_version": 1, "performed_at_utc": datetime.now(timezone.utc).isoformat(),
              "environment": {"python": platform.python_version(), "platform": platform.platform()},
              "configuration": {"case_count": len(rows), "case_families": len({row["family"] for row in rows}),
                                "fixed_shuffle_seed": 173, "live_model_calls": 0},
              "elapsed_seconds": round(time.perf_counter() - started, 6), "metrics": metrics,
              "metric_definitions": {
                  "top1_top3": "Hit any labelled root within first k candidates. Denominator: every expected-candidates case with nonempty independent root labels, including execution errors.",
                  "multi_root_coverage": "Count of labelled roots recovered in top 3 / total labelled roots, restricted to multi-root cases; all-roots rate is also separate.",
                  "unknown_recall": "Predicted unknown / all cases labelled unknown, including healthy and insufficient-evidence cases.",
                  "healthy_false_positive_rate": "Predicted candidates or nonempty candidates / explicitly healthy cases only; execution failures are reported separately.",
                  "runtime_ms": "Measured in-process algorithm runtime including input deepcopy, excluding collection, Kafka/Flink and LLM; not production detection latency.",
                  "evidence_references": "All cited event IDs must exist in the input snapshot, and every returned candidate must have at least one supporting reference."},
              "limitations": ["Synthetic, constructed fixtures are not a representative labelled production benchmark.",
                  "Duplicate and reordered cases share a family; case rows are not statistically independent.",
                  "Service candidates are not proof of an internal component-level cause or calibrated confidence.",
                  "Archived evidence has no independent labels and is excluded from accuracy metrics."],
              "cases": rows, "archived_replays": archived}
    project = Path(__file__).resolve().parents[1]
    result["source_sha256"] = {name: hashlib.sha256((project / name).read_bytes()).hexdigest()
                               for name in ("app/rca.py", "scripts/evaluate_rca.py", "tests/rca_scenarios.py")}
    (args.output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    with (args.output / "cases.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["case_id", "family", "source_kind", "expected_status", "root_services",
            "predicted_status", "candidate_services", "references_valid", "runtime_ms", "execution_status"])
        writer.writeheader()
        for row in rows:
            writer.writerow({"case_id": row["case_id"], "family": row["family"], "source_kind": row["source_kind"],
                "expected_status": row["labels"]["expected_status"], "root_services": json.dumps(row["labels"]["root_services"]),
                **{key: json.dumps(row[key]) if key == "candidate_services" else row.get(key)
                   for key in ("predicted_status", "candidate_services", "references_valid", "runtime_ms", "execution_status")}})
    errors = sum(row.get("execution_status") != "ok" for row in rows + archived)
    print(json.dumps({"output": str(args.output), "cases": len(rows), "execution_errors": errors, "metrics": metrics}, ensure_ascii=False))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
