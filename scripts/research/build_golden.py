"""Freeze a synthetic golden set with honest annotation provenance."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.experiment import Experiment, sha256
from research.llm.corpus import attacks, build


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()
    with Experiment(
        args.output, {"source_kind": "synthetic_fixture", "human_reviewed": False}, [args.seed]
    ) as run:
        splits = build(args.seed)
        for split, cases in splits.items():
            (run.output / f"{split}.jsonl").write_text(
                "".join(json.dumps(c, ensure_ascii=False) + "\n" for c in cases)
            )
        (run.output / "attacks.jsonl").write_text(
            "".join(json.dumps(c, ensure_ascii=False) + "\n" for c in attacks())
        )
        run.result.update(
            metrics={
                "dev": len(splits["dev"]),
                "test": len(splits["test"]),
                "attacks": len(attacks()),
            },
            files={p.name: sha256(p) for p in run.output.glob("*.jsonl")},
            annotation_gate="blocked_human_review_not_available",
        )
        run.summary(
            [
                "冻结可重复的合成评测集，尚未证明真实服务根因能力。",
                "六类场景，分层固定 dev/test。",
                "模型未参与标注。",
                "同家族变体不独立；没有人工复核。",
                "不能替代 OpenRCA 或生产故障标注。",
                "冻结提示词后运行 dev 校准与一次 test；人工复核仍是未完成门禁。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
