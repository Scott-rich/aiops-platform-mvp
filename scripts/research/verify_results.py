"""Recompute saved P0 metrics and hashes; derived integrity helpers are not experiments."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

from research.experiment import Experiment, sha256
from research.metrics import detection


def inspect(folder: Path, expected: int) -> dict[str, Any]:
    value = json.loads((folder / "result.json").read_text())
    terminal = json.loads((folder / "completed.json").read_text())
    if terminal["result_sha256"] != sha256(folder / "result.json") or value["status"] != "passed":
        raise ValueError("invalid terminal result: " + str(folder))
    rows = [json.loads(line) for line in (folder / "rows.jsonl").read_text().splitlines()]
    logged = sum(
        " seed=" in line and "metrics=" in line
        for line in (folder / "run.log").read_text().splitlines()
    )
    arrays = list(folder.glob("*.npz"))
    if len(rows) != expected or logged != expected or len(arrays) != expected:
        raise ValueError("Incomplete row/log/prediction counts")
    for row in rows:
        filename = f"{row['entity']}-{row['detector']}-{row['seed']}.npz"
        with np.load(folder / filename, allow_pickle=False) as data:
            scores, pred, labels = data["scores"], data["predictions"], data["labels"]
            if (
                hashlib.sha256(scores.tobytes()).hexdigest() != row["scores_sha256"]
                or hashlib.sha256(pred.tobytes()).hexdigest() != row["prediction_sha256"]
            ):
                raise ValueError("prediction hash mismatch: " + filename)
            recomputed = detection(labels, scores, pred)
            for metric, score in recomputed.items():
                saved = row["metrics"][metric]
                if score is None and saved is None:
                    continue
                if score is None or saved is None or abs(saved - score) > 1e-9:
                    raise ValueError("metric mismatch: " + filename + ":" + metric)
    return {
        "rows": len(rows),
        "logs": logged,
        "prediction_files": len(arrays),
        "recomputed_metrics_equal_within_1e-9": True,
        "result_sha256": sha256(folder / "result.json"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smd", type=Path, required=True)
    parser.add_argument("--psm", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with Experiment(
        args.output, {"smd": str(args.smd), "psm": str(args.psm), "tolerance": 1e-9}, [2026]
    ) as run:
        run.result["metrics"] = {"smd": inspect(args.smd, 100), "psm": inspect(args.psm, 20)}
        run.summary(
            [
                "逐条重算公开数据检测指标并校验预测文件、日志数量和终态哈希。",
                "SMD与PSM行数见metrics。",
                "保存值对照重新计算值，容许浮点差1e-9。",
                "这是确定性完整性核验，不估计总体泛化。",
                "不能替代数据标签真实性、方法适用性与统计单位审查。",
                "按README从原始下载器独立复现实验。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
