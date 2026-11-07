"""Fresh-directory experiment records with retained failures and source fingerprints."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.metadata
import json
import logging
import platform
import shlex
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Literal

ROOT = Path(__file__).resolve().parents[1]


def sha256(path: Path) -> str:
    """Hash bytes without loading large datasets into memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def source_hashes() -> dict[str, str]:
    """Fingerprint executable sources; exclude mutable outputs and downloaded data."""
    files = [
        p
        for folder in ("research", "scripts", "app", "flink", "tests")
        for p in (ROOT / folder).rglob("*.py")
        if "__pycache__" not in p.parts
    ]
    files += list(ROOT.glob("requirements*.txt"))
    files += [
        ROOT / name
        for name in ("wire_contract.py", "pyproject.toml", "Dockerfile", "Makefile")
        if (ROOT / name).is_file()
    ]
    return {str(p.relative_to(ROOT)): sha256(p) for p in sorted(files)}


def write_json(path: Path, value: Any) -> None:
    """Reject NaN/Infinity and write UTF-8 JSON with an atomic rename."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    temporary.replace(path)


class Experiment:
    """A run never overwrites an earlier run, even when it fails."""

    def __init__(self, output: Path, config: dict[str, Any], seeds: list[int]) -> None:
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.started = time.monotonic()
        self.console = (self.output / "console.log").open("a", encoding="utf-8", buffering=1)
        self.redirects = contextlib.ExitStack()
        self.handler = logging.FileHandler(self.output / "run.log", encoding="utf-8")
        self.handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logging.getLogger().addHandler(self.handler)
        logging.getLogger().setLevel(logging.INFO)
        entry = Path(sys.argv[0]).resolve()
        module = (
            ".".join(entry.relative_to(ROOT).with_suffix("").parts)
            if entry.is_relative_to(ROOT)
            else sys.argv[0]
        )
        self.result: dict[str, Any] = {
            "schema_version": 1,
            "status": "running",
            "config": config,
            "seeds": seeds,
            "command": shlex.join([sys.executable, "-m", module, *sys.argv[1:]]),
            "argv": sys.argv,
            "cwd": str(Path.cwd()),
            "started_at_utc": datetime.now(UTC).isoformat(),
            "environment": {
                "python": sys.version,
                "platform": platform.platform(),
                "dependencies": sorted(
                    f"{d.metadata['Name']}=={d.version}" for d in importlib.metadata.distributions()
                ),
            },
            "source_sha256": source_hashes(),
            "metrics": {},
            "timings": {
                "deterministic": False,
                "reason": "Wall-clock measurements depend on host load.",
            },
        }
        logging.info("command=%s", self.result["command"])
        write_json(self.output / "result.json", self.result)

    def __enter__(self) -> Experiment:
        self.redirects.enter_context(contextlib.redirect_stdout(self.console))
        self.redirects.enter_context(contextlib.redirect_stderr(self.console))
        return self

    def summary(self, answers: list[str]) -> None:
        questions = ["证明了什么", "数据与样本", "对比对象", "不确定性", "失效边界", "下一步"]
        text = "# 实验记录\n\n命令：`" + self.result["command"] + "`\n\n"
        text += "\n\n".join(f"**{q}**：{a}" for q, a in zip(questions, answers, strict=True))
        (self.output / "summary.md").write_text(
            text + "\n\n数值和完整配置：[result.json](result.json)。\n"
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        if exc is not None:
            self.result.update(status="failed", error_type=type(exc).__name__, error=str(exc))
            logging.error("Experiment failed", exc_info=(type(exc), exc, traceback))
        elif self.result["status"] == "running":
            self.result["status"] = "passed"
        self.result["ended_at_utc"] = datetime.now(UTC).isoformat()
        self.result["timings"]["elapsed_seconds"] = time.monotonic() - self.started
        self.result["source_unchanged"] = source_hashes() == self.result["source_sha256"]
        if not self.result["source_unchanged"]:
            self.result["status"] = "failed"
            self.result["source_error"] = "Executable source changed during this run"
        logging.info("status=%s", self.result["status"])
        if not (self.output / "summary.md").exists():
            self.summary(
                [
                    "本次执行未完成，不能作为成功证据。",
                    "见 config。",
                    "未形成比较结论。",
                    "未估计。",
                    str(self.result.get("error", "未写摘要")),
                    "保留失败目录并修复后用新目录重跑。",
                ]
            )
        logging.getLogger().removeHandler(self.handler)
        self.handler.close()
        self.redirects.close()
        self.console.close()
        write_json(self.output / "result.json", self.result)
        write_json(
            self.output / "completed.json",
            {"status": self.result["status"], "result_sha256": sha256(self.output / "result.json")},
        )
        if exc is None and self.result["status"] == "failed":
            raise RuntimeError("experiment failed; see retained result.json")
        return False
