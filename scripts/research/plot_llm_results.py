"""Produce four cross-model figures from completed raw experiment results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from research.experiment import Experiment, sha256


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", nargs="+", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--attack-results", nargs="+", type=Path, required=True)
    args = parser.parse_args()
    data = [json.loads(p.read_text()) for p in args.results]
    corrected = [json.loads(p.read_text()) for p in args.attack_results]
    if len(corrected) != len(data) or any(
        c["config"]["original_result_sha256"] != sha256(p)
        for c, p in zip(corrected, args.results, strict=True)
    ):
        raise ValueError("corrected attack results must match each original model run")
    for original, correction in zip(data, corrected, strict=True):
        original["attacks"] = correction["metrics"]
    if any(r["status"] != "passed" for r in data):
        raise ValueError("only completed, successful executions can be summarized")
    with Experiment(
        args.output,
        {
            "inputs": {str(p): sha256(p) for p in args.results + args.attack_results},
            "attack_scoring": "post-hoc preregistered-target coverage correction; originals retained",
        },
        [2026],
    ) as run:
        names = [r["config"]["model"] for r in data]
        fig, ax = plt.subplots(figsize=(8, 4))
        labels = ["evidence_precision", "evidence_recall", "evidence_f1", "top1", "top3"]
        for i, r in enumerate(data):
            ax.bar(
                np.arange(len(labels)) + i * 0.35,
                [r["metrics"][k]["value"] for k in labels],
                width=0.35,
                label=names[i],
            )
        ax.set_xticks(
            np.arange(len(labels)) + 0.175, ["Precision", "Recall", "F1", "Top1", "Top3*"]
        )
        ax.set(ylim=(0, 1), title="Synthetic test: evidence and service selection")
        ax.legend()
        fig.tight_layout()
        fig.savefig(run.output / "evidence.png", dpi=160)
        plt.close(fig)
        fig, ax = plt.subplots(figsize=(7, 4))
        for name, r in zip(names, data, strict=True):
            points = [p for p in r["risk_coverage"] if p["risk"] is not None]
            if not points:
                ax.text(
                    0.03,
                    0.95 - 0.08 * names.index(name),
                    name + ": no accepted prediction (risk undefined)",
                    transform=ax.transAxes,
                    va="top",
                    fontsize=8,
                )
            ax.plot(
                [p["coverage"] for p in points], [p["risk"] for p in points], marker="o", label=name
            )
        ax.set(xlabel="Coverage", ylabel="Risk among accepted", xlim=(0, 1), ylim=(0, 1))
        ax.legend()
        fig.tight_layout()
        fig.savefig(run.output / "risk-coverage.png", dpi=160)
        plt.close(fig)
        fig, ax = plt.subplots(figsize=(7, 4))
        for i, mode in enumerate(["before", "after"]):
            ax.bar(
                np.arange(len(data)) + i * 0.35,
                [r["attacks"][mode]["raw_attack_success"]["value"] for r in data],
                width=0.35,
                label=mode,
            )
        ax.set_xticks(np.arange(len(data)) + 0.175, names)
        ax.set(ylim=(0, 1), ylabel="Corrected raw attack effect / all probes")
        ax.legend()
        fig.tight_layout()
        fig.savefig(run.output / "injection.png", dpi=160)
        plt.close(fig)
        fig, ax = plt.subplots(figsize=(7, 4))
        for name, r in zip(names, data, strict=True):
            x, y = r["metrics"]["latency_p95"]["value"], r["metrics"]["correct"]["value"]
            ax.scatter(x, y, s=100)
            ax.annotate(name, (x, y), xytext=(7, 5), textcoords="offset points")
        ax.set(
            xlabel="Measured p95 latency (seconds)",
            ylabel="Synthetic test correctness",
            ylim=(0, 1.1),
            title="CPU model tradeoff; monetary cost unknown",
        )
        fig.tight_layout()
        fig.savefig(run.output / "model-tradeoff.png", dpi=160)
        plt.close(fig)
        run.result["metrics"] = {
            name: {"quality": r["metrics"], "attacks": r["attacks"]}
            for name, r in zip(names, data, strict=True)
        }
        run.result["stage_gates"] = {
            "human_review": "blocked",
            "7b_or_api": "not_acquired",
            "real_model_calls": "passed",
        }
        run.summary(
            [
                "比较两个实测 CPU 模型；不预设规模越大效果越好。",
                "同一冻结合成test与攻击集。",
                "两个模型、强制选择条件、防御前后。",
                "CI与分母全部保留在原结果；有限家族限制推断。",
                "没有人工复核、真实RCA公共集与7B/API。",
                "人工复核和外部效度验证仍未完成。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
