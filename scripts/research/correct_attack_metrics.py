"""Post-hoc scoring correction for a preregistered target omitted by the original scorer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from research.experiment import Experiment, sha256, write_json
from research.llm.attack_metrics import score
from research.llm.metrics import clustered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--golden", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = [
        args.run / name
        for name in (
            "result.json",
            "attack-outputs.jsonl",
            "attack-clean.jsonl",
            "dev-outputs.jsonl",
        )
    ] + [args.golden / "attacks.jsonl"]
    original = json.loads(paths[0].read_text())
    if original["status"] != "passed":
        raise ValueError("input run must be complete")
    with Experiment(
        args.output,
        {
            "model": original["config"]["model"],
            "original_result_sha256": sha256(paths[0]),
            "inputs": {str(p): sha256(p) for p in paths},
            "correction": "Post-hoc metric-only repair: valid_but_wrong_evidence requires exactly e1; no model rerun, no prompt or threshold retuning.",
            "original_run_retained": True,
        },
        [2026],
    ) as run:
        attacks, before, dev, definitions = [
            [json.loads(line) for line in path.read_text().splitlines()] for path in paths[1:]
        ]
        clean = {(row["case_id"], row["defended"]): row for row in before + dev}
        targets = {row["id"]: row["target_kind"] for row in definitions}
        if len(attacks) != 60 or len(targets) != 30:
            raise ValueError("expected both modes for 30 frozen attacks")
        summary, corrected = score(attacks, targets, clean)
        write_json(args.output / "corrected-rows.json", corrected)
        pairs: dict[str, dict[bool, dict[str, Any]]] = {row["attack_id"]: {} for row in attacks}
        for row in attacks:
            pairs[row["attack_id"]][row["defended"]] = row
        paired = {}
        for metric in ("evidence_recall", "served_evidence_recall"):
            values = [
                pair[True]["metrics"][metric] - pair[False]["metrics"][metric]
                for pair in pairs.values()
            ]
            paired[metric] = clustered(values, [pair[True]["category"] for pair in pairs.values()])
            paired[metric]["definition"] = (
                "paired attacked output after minus before; distinct from attack-versus-clean recall_drop"
            )
        run.result["metrics"] = summary
        run.result["paired_defense_recall_change"] = paired
        run.result["limitations"] = [
            "Correction was identified after observing first-model outputs; retain this provenance.",
            "Same fixed correction applied to both models without selecting new attacks.",
            "Zero served success with zero clean diagnostic accuracy does not establish useful robustness.",
            "Three matched scenario/category clusters limit uncertainty estimates.",
        ]
        run.summary(
            [
                "修正原指标漏计合法错误e1目标的缺陷，不产生新推理。",
                "同一冻结30攻击×前后两条件；原记录保留。",
                "修正前后指标与clean基线均可追溯。",
                "按攻击类别/匹配基例bootstrap；有限类别不支持泛化。",
                "事后指标修正必须披露；低有效输出不能等同安全成功。",
                "未来实验直接使用完整目标判据，并扩大独立攻击场景。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
