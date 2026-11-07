"""Execute local gates and controlled red-to-green probes; remote CI is separate."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from research.experiment import ROOT, Experiment


def execute(command: list[str], cwd: Path, log: Path, env: dict[str, str] | None = None) -> int:
    with log.open("w") as stream:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            timeout=1200,
            check=False,
        )
    return result.returncode


def probes(output: Path) -> dict[str, Any]:
    """Mutations exist only in a fresh throwaway project; original assertions untouched."""
    result: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="aiops-gate-probe-") as folder:
        root = Path(folder)
        program = root / "probe.py"
        program.write_text("import os\n")
        red = execute(
            [sys.executable, "-m", "ruff", "check", str(program), "--select", "F401", "--isolated"],
            root,
            output / "ruff-red.log",
        )
        program.write_text("value = 1\n")
        green = execute(
            [sys.executable, "-m", "ruff", "check", str(program), "--select", "F401", "--isolated"],
            root,
            output / "ruff-green.log",
        )
        result["ruff"] = {"red_exit": red, "green_exit": green, "passed": red != 0 and green == 0}
        program.write_text("def identity(value):\n    return value\n")
        red = execute(
            [sys.executable, "-m", "mypy", "--disallow-untyped-defs", str(program)],
            root,
            output / "mypy-red.log",
        )
        program.write_text("def identity(value: int) -> int:\n    return value\n")
        green = execute(
            [sys.executable, "-m", "mypy", "--disallow-untyped-defs", str(program)],
            root,
            output / "mypy-green.log",
        )
        result["mypy"] = {"red_exit": red, "green_exit": green, "passed": red != 0 and green == 0}
        (root / "sample.py").write_text(
            "def used():\n    return 1\n\ndef hidden():\n    value = 0\n"
            + "".join(f"    value += {i}\n" for i in range(20))
            + "    return value\n"
        )
        exercise = root / "exercise.py"
        exercise.write_text("import sample\nassert sample.used() == 1\n")
        command = [sys.executable, "-m", "coverage", "run", "--source=sample", str(exercise)]
        execute(command, root, output / "coverage-red-execution.log")
        red = execute(
            [sys.executable, "-m", "coverage", "report", "--fail-under=80"],
            root,
            output / "coverage-red.log",
        )
        exercise.write_text(
            "import sample\nassert sample.used() == 1\nassert sample.hidden() == 190\n"
        )
        execute(command, root, output / "coverage-green-execution.log")
        green = execute(
            [sys.executable, "-m", "coverage", "report", "--fail-under=80"],
            root,
            output / "coverage-green.log",
        )
        result["coverage"] = {
            "red_exit": red,
            "green_exit": green,
            "passed": red != 0 and green == 0,
        }
        big = root / "large.bin"
        with big.open("wb") as stream:
            stream.truncate(5 * 1024 * 1024 + 1)
        red = execute(
            [sys.executable, str(ROOT / "scripts/research/check_file_size.py"), str(big)],
            root,
            output / "size-red.log",
        )
        big.write_bytes(b"ok")
        green = execute(
            [sys.executable, str(ROOT / "scripts/research/check_file_size.py"), str(big)],
            root,
            output / "size-green.log",
        )
        result["size"] = {"red_exit": red, "green_exit": green, "passed": red != 0 and green == 0}
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--helm", default="helm")
    args = parser.parse_args()
    args.output = args.output.resolve()
    with Experiment(
        args.output,
        {
            "coverage_threshold": 80,
            "python": sys.executable,
            "remote_ci": "not_run",
            "helm": args.helm,
        },
        [2026],
    ) as run:
        env = dict(os.environ, COVERAGE_FILE=str(args.output / ".coverage"))
        commands = {
            "pip-check": [sys.executable, "-m", "pip", "check"],
            "ruff": [sys.executable, "-m", "ruff", "check", "app", "research", "scripts/research"],
            "format": [
                sys.executable,
                "-m",
                "ruff",
                "format",
                "--check",
                "app",
                "research",
                "scripts/research",
            ],
            "mypy": [sys.executable, "-m", "mypy"],
            "tests": [
                sys.executable,
                "-m",
                "coverage",
                "run",
                "-m",
                "pytest",
                "tests",
                "research/tests",
                "-q",
            ],
            "coverage": [sys.executable, "-m", "coverage", "report"],
            "coverage-json": [
                sys.executable,
                "-m",
                "coverage",
                "json",
                "-o",
                str(args.output / "coverage.json"),
            ],
            "coverage-xml": [
                sys.executable,
                "-m",
                "coverage",
                "xml",
                "-o",
                str(args.output / "coverage.xml"),
            ],
        }
        statuses = {}
        for name, command in commands.items():
            statuses[name] = {
                "exit_code": execute(command, ROOT, args.output / (name + ".log"), env),
                "command": command,
            }
        run.result["metrics"]["checks"] = statuses
        run.result["metrics"]["red_green"] = probes(args.output)
        if (args.output / "coverage.json").exists():
            run.result["metrics"]["coverage"] = json.loads(
                (args.output / "coverage.json").read_text()
            )["totals"]
        if shutil.which(args.helm):
            code = execute(
                [args.helm, "lint", "deploy/helm/aiops"], ROOT, args.output / "helm-lint.log"
            )
            run.result["metrics"]["helm_lint_exit"] = code
            invalid = execute(
                [args.helm, "template", "aiops", "deploy/helm/aiops", "--set", "app.replicas=2"],
                ROOT,
                args.output / "helm-invalid-replica.log",
            )
            run.result["metrics"]["helm_rejects_unsafe_sqlite_scaling"] = invalid != 0
        else:
            run.result["metrics"]["helm_lint_exit"] = None
        run.result["metrics"]["docker_build"] = (
            "not_executed_no_daemon"
            if not shutil.which("docker")
            else "not_executed_by_this_local_gate"
        )
        run.result["metrics"]["remote_ci"] = "not_run_no_connected_remote_repository"
        passed = (
            all(r["exit_code"] == 0 for r in statuses.values())
            and all(r["passed"] for r in run.result["metrics"]["red_green"].values())
            and run.result["metrics"]["helm_lint_exit"] == 0
            and run.result["metrics"].get("helm_rejects_unsafe_sqlite_scaling") is True
        )
        run.result["status"] = "passed" if passed else "failed"
        run.summary(
            [
                "本地质量门禁及临时项目红→绿验证；不等同远程CI运行。",
                "测试与覆盖率精确数值见原始日志和metrics。",
                "与原始断言/80%门槛保持一致。",
                "功能测试不是生产可靠性或模型泛化证明。",
                "Docker/kind和远程工作流未运行时必须单列。",
                "在可用容器主机与远程仓库执行剩余门禁。",
            ]
        )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
