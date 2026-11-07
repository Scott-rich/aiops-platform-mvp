"""Fast, network-local fixture + real HTTP API smoke; explicitly not Kafka/Flink or LLM."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

from app.diagnosis import diagnose, markdown
from app.storage import Store
from flink.rules import evaluate_window
from research.experiment import Experiment, write_json
from scripts.demo import fixture


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with Experiment(
        args.output,
        {
            "mode": "synthetic fixture + real loopback API; no Kafka/Flink/model",
            "sampling": "fixed 40 requests per synthetic service",
            "timestamp_determinism": "event times and report IDs change; counts and root ranking deterministic",
        },
        [2026],
    ) as run:
        original = {key: os.environ.pop(key) for key in list(os.environ) if key.startswith("LLM_")}
        try:
            store = Store(args.output / "data")
            events, start, end = fixture()
            for event in events:
                store.ingest(
                    event, {"transport": "offline-fixture", "fixture": "payment-observation-v1"}
                )
            alerts = evaluate_window(events, "gateway", start, end)
            alert = next(row for row in alerts if row["rule_id"] == "high_error_rate")
            store.alert(
                alert, {"transport": "offline-rule-evaluation", "engine": "python-shared-rules"}
            )
            report = diagnose(store, alert["alert_id"])
            write_json(args.output / "report.json", report)
            (args.output / "report.md").write_text(markdown(report))
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            env = dict(
                os.environ,
                DATA_DIR=str((args.output / "data").resolve()),
                EXECUTION_MODE="local-http-python-rules",
                API_AUTH_MODE="local",
            )
            with (args.output / "api.log").open("w") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "app.api:app",
                        "--no-proxy-headers",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--no-access-log",
                    ],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                try:
                    with httpx.Client(
                        base_url=f"http://127.0.0.1:{port}", timeout=5, trust_env=False
                    ) as client:
                        for _ in range(100):
                            try:
                                if client.get("/v1/live").status_code == 200:
                                    break
                            except httpx.HTTPError:
                                pass
                            if process.poll() is not None:
                                raise RuntimeError("API exited during startup")
                            time.sleep(0.1)
                        else:
                            raise TimeoutError("API did not start")
                        response = client.get("/v1/reports/" + report["report_id"])
                        response.raise_for_status()
                        verification = client.get("/v1/reports/" + report["report_id"] + "/verify")
                        verification.raise_for_status()
                        verdict = verification.json()
                        required = (
                            "full_evidence_valid",
                            "snapshot_sha256_valid",
                            "rca_sha256_valid",
                            "rca_evidence_references_valid",
                            "rca_evidence_set_valid",
                        )
                        if not all(verdict.get(key) is True for key in required):
                            raise AssertionError("report verification failed")
                        metrics = client.get("/v1/metrics")
                        metrics.raise_for_status()
                        (args.output / "metrics.prom").write_text(metrics.text)
                        write_json(args.output / "api-verification.json", verification.json())
                        top = response.json()["rca"]["candidates"][0]["service"]
                        if top != "payment" or "aiops_http_requests_total" not in metrics.text:
                            raise AssertionError("demo root ranking or metric export failed")
                        run.result["metrics"] = {
                            "events": store.stats()["events"],
                            "alerts": store.stats()["alerts"],
                            "candidate_service": top,
                            "http_status": response.status_code,
                            "versioned_api": True,
                            "prometheus_export": True,
                            "model_executed": False,
                        }
                        logging.info(
                            "Actual API response: root=%s, events=%s, status=%d",
                            top,
                            store.stats()["events"],
                            response.status_code,
                        )
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
        finally:
            os.environ.update(original)
        run.summary(
            [
                "真实启动API并读取本次生成的证据报告与指标。",
                "合成fixture；数量见metrics，不能代表外部故障。",
                "无性能/准确率对照，属于功能smoke。",
                "时钟、UUID及启动耗时不确定，核心计数与根因排序固定。",
                "未运行Kafka/Flink或模型；不可用此证明分布式链路。",
                "完整实测见研究结果与原生验收记录。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
