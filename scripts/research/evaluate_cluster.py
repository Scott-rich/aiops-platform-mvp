"""Summarize captured real faults without converting missing observations to successes."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from research.chaos.evaluate import evaluate
from research.chaos.plan import FAMILIES
from research.experiment import Experiment, sha256


def completeness(
    directory: Path, ground_truth: list[dict[str, Any]], metrics: dict[str, Any]
) -> dict[str, Any]:
    """A computable subset cannot pass the preregistered acquisition contract."""
    injected = [r for r in ground_truth if r.get("phase") == "injected"]
    ids = [r["case"]["id"] for r in injected]
    families = Counter(r["case"]["family"] for r in injected)
    checks = {
        "at_least_30_unique_faults": len(set(ids)) >= 30 and len(set(ids)) == len(ids),
        "six_families_at_least_five_each": all(families[f] >= 5 for f in FAMILIES)
        and set(families) == set(FAMILIES),
        "controller_confirmed": bool(injected)
        and all(
            any(
                c.get("type") == "AllInjected" and c.get("status") == "True"
                for c in r.get("controller_conditions", [])
            )
            for r in injected
        ),
        "cleanup_and_workload_ready": bool(ids)
        and all(
            any(
                r.get("phase") == phase and r.get("case", {}).get("id") == case_id
                for r in ground_truth
            )
            for case_id in ids
            for phase in ("fault_resource_removed", "workload_ready")
        ),
        "all_faults_have_observations": bool(metrics["cases"])
        and all(
            r["window_observations"] > 0 and r["observation_failures"] == 0
            for r in metrics["cases"]
        ),
        "all_faults_have_report_metrics": bool(ids)
        and all(
            metrics[key]["n_observed"] == len(ids)
            for key in ("root_cause_top1", "reference_validity", "diagnosis_latency")
        ),
        "latency_distributions_observed": all(
            metrics[key]["n_observed"] >= 2 for key in ("mttd", "mttr", "prometheus_mttd")
        ),
        "acquisition_terminal_hash": False,
        "acquisition_complete": False,
        "plan_complete_and_unchanged": False,
    }
    try:
        result_path = directory / "result.json"
        acquisition = json.loads(result_path.read_text())
        terminal = json.loads((directory / "completed.json").read_text())
        plan_path = directory / "plan.json"
        plan = json.loads(plan_path.read_text())
        checks["acquisition_terminal_hash"] = (
            terminal.get("result_sha256") == sha256(result_path)
            and terminal.get("status") == acquisition.get("status")
            and acquisition.get("status") in {"passed", "partial"}
        )
        checks["acquisition_complete"] = (
            acquisition.get("acquisition_complete") is True
            and acquisition.get("source_unchanged") is True
            and acquisition.get("config", {}).get("execute") is True
            and acquisition.get("metrics", {}).get("confirmed_injections") == len(ids)
        )
        checks["plan_complete_and_unchanged"] = acquisition.get("plan_sha256") == sha256(
            plan_path
        ) and sorted(plan, key=lambda c: c["id"]) == sorted(
            [r["case"] for r in injected], key=lambda c: c["id"]
        )
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "families": dict(families),
        "scope": "fault-evaluation gate only; scale experiment remains separate",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = [
        args.input / name for name in ("ground-truth.jsonl", "observations.jsonl", "reports.jsonl")
    ]
    with Experiment(
        args.output,
        {
            "input": str(args.input),
            "input_sha256": {
                p.name: sha256(p)
                for p in [
                    *paths,
                    *(args.input / n for n in ("result.json", "completed.json", "plan.json")),
                ]
                if p.exists()
            },
        },
        [2026],
    ) as run:
        if not all(p.exists() for p in paths[:2]):
            run.result.update(
                status="blocked",
                blocked_reason="No complete ground-truth and observation logs; cannot evaluate real faults",
            )
            data: list[list[dict[str, Any]]] = [[], [], []]
        else:
            data = [
                [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []
                for p in paths
            ]
        run.result["metrics"] = evaluate(*data)
        run.result["completeness"] = completeness(args.input, data[0], run.result["metrics"])
        if not run.result["metrics"]["n_confirmed_injections"]:
            run.result["status"] = "blocked"
        elif not run.result["completeness"]["passed"]:
            run.result["status"] = "partial"
        run.summary(
            [
                "只计算已确认故障，未观测项保留为null。",
                "样本与删失数量见metrics。",
                "同批故障的Prometheus固定阈值。",
                "观察到的延迟分位数附bootstrap CI；不代表删失总体。",
                "服务时间窗口匹配不是因果证明；RCA与引用指标缺完整报告时不得宣称通过。",
                "补完整报告采集与已恢复状态，并完成30次真实故障。",
            ]
        )
        incomplete = run.result["status"] in {"blocked", "partial"}
    return 2 if incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
