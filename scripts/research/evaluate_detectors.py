"""Offline benchmark. All thresholds are fit without test labels."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import precision_recall_curve

from research.datasets.psm import PSM
from research.datasets.smd import SMD
from research.detectors import create
from research.experiment import Experiment
from research.metrics import describe, detection, paired_comparison, temporal_ci


def evaluate(args: argparse.Namespace) -> None:
    seeds = [int(s) for s in args.seeds.split(",")]
    names = args.detectors.split(",")
    entities = args.entities.split(",") if args.dataset == "smd" else ["pooled"]
    for values in (seeds, names, entities):
        if not values or len(values) != len(set(values)):
            raise ValueError("entities, detectors and seeds must be nonempty and unique")
    config = {
        "dataset": args.dataset,
        "entities": entities,
        "detectors": names,
        "data_root": str(args.data_root),
        "threshold_source": "train_only",
        "selection_rule": "CLI entity list; default is first five machine indices, prespecified before evaluation",
        "statistics": "entity CI resamples seed metrics; aggregate CI resamples entity means;"
        " deterministic seed CIs degenerate; separate seed0 temporal block CIs are also supplied",
        "spot_variant": "GPD MLE, refit every 64 accepted peaks, q=.001, init=.98; not original Grimshaw",
        "spot_ranking": "shares ZScore scores; only its sequential decision thresholds differ, so PR/ROC are identical",
    }
    with Experiment(args.output, config, seeds) as run:
        rows: list[dict[str, Any]] = []
        fig, axes = plt.subplots(1, len(entities), figsize=(5 * len(entities), 4), squeeze=False)
        metadata = []
        temporal = {}
        for ei, entity in enumerate(entities):
            loader = SMD(args.data_root, entity) if args.dataset == "smd" else PSM(args.data_root)
            train, _, test, labels, meta = loader.load()
            meta.update(
                n_train=len(train),
                n_test=len(test),
                n_features=train.shape[1],
                anomaly_points=int(labels.sum()),
            )
            metadata.append(meta)
            for name in names:
                for seed in seeds:
                    model = create(name, seed).fit(train)
                    scores = model.score(test)
                    pred = model.decide(scores)
                    row = {
                        "entity": entity,
                        "detector": name,
                        "seed": seed,
                        "deterministic": model.deterministic,
                        "threshold": model.threshold,
                        "metrics": detection(labels, scores, pred),
                        "scores_sha256": hashlib.sha256(scores.tobytes()).hexdigest(),
                        "prediction_sha256": hashlib.sha256(pred.tobytes()).hexdigest(),
                    }
                    rows.append(row)
                    with (run.output / "rows.jsonl").open("a") as stream:
                        stream.write(json.dumps(row, allow_nan=False) + "\n")
                    np.savez_compressed(
                        run.output / f"{entity}-{name}-{seed}.npz",
                        scores=scores,
                        predictions=pred,
                        labels=labels,
                    )
                    logging.info("%s %s seed=%d metrics=%s", entity, name, seed, row["metrics"])
                    if seed == seeds[0]:
                        temporal[f"{entity}/{name}"] = temporal_ci(labels, scores, pred)
                        temporal[f"{entity}/{name}"]["model_seed"] = seed
                        precision, recall, _ = precision_recall_curve(labels, scores)
                        axes[0, ei].plot(recall, precision, label=name)
            axes[0, ei].set(
                xlabel="Recall", ylabel="Precision", title=entity, xlim=(0, 1), ylim=(0, 1)
            )
            axes[0, ei].legend()
        fig.tight_layout()
        fig.savefig(run.output / "pr-curves.png", dpi=150)
        plt.close(fig)
        metrics = list(rows[0]["metrics"])
        per_entity: dict[str, Any] = {}
        aggregates: dict[str, Any] = {}
        for name in names:
            per_entity[name] = {
                e: {
                    metric: describe(
                        [
                            r["metrics"][metric]
                            for r in rows
                            if r["detector"] == name and r["entity"] == e
                        ]
                    )
                    for metric in metrics
                }
                for e in entities
            }
            aggregates[name] = {
                metric: describe([per_entity[name][e][metric]["mean"] for e in entities])
                for metric in metrics
            }
        comparison = {}
        if "zscore" in names:
            for name in names:
                if name != "zscore":
                    comparison[f"{name}_minus_zscore_pr_auc"] = paired_comparison(
                        [per_entity[name][e]["pr_auc"]["mean"] for e in entities],
                        [per_entity["zscore"][e]["pr_auc"]["mean"] for e in entities],
                    )
        run.result.update(
            rows=rows,
            datasets=metadata,
            metrics=aggregates,
            per_entity=per_entity,
            temporal_bootstrap=temporal,
            comparisons=comparison,
            metric_definitions={
                "pr_auc": "average precision, not trapezoid PR area",
                "range_recall": "mean fraction of positive points detected in each true interval; flat positional weighting, no cardinality penalty",
                "alarms": "contiguous predicted positive intervals",
                "pa20_f1": "fill a true interval only if >=20% points detected; false positives retained",
            },
        )
        run.summary(
            [
                "比较独立公共数据上的四种检测器；不预设 MAD 一定优于基线。",
                "各实体样本、维度、异常点与文件哈希见 datasets。",
                "固定 3σ、MAD、Isolation Forest、流式 POT 变体。",
                f"{len(seeds)} 个 seed；SMD 以实体均值做 bootstrap 与配对 Wilcoxon；PSM 单实体无法推断跨实体效度。时间块 CI 独立列出。",
                "匿名维度无服务语义；无因果标签；PA 会放大零星命中的效果。三项陷阱：零 MAD、数据泄漏、seed 伪重复。",
                "扩展全部 SMD 实体并检查不同时间块长度；不要把本表外推为生产准确率。",
            ]
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["smd", "psm"], required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--entities", default="machine-1-1,machine-1-2,machine-1-3,machine-1-4,machine-1-5"
    )
    parser.add_argument("--detectors", default="zscore,mad,iforest,spot")
    parser.add_argument("--seeds", default="0,1,2,3,4")
    parser.add_argument("--output", type=Path, required=True)
    evaluate(parser.parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
