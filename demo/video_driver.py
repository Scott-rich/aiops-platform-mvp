"""Five-minute live local HTTP demonstration; synthetic business services, no Chaos Mesh."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


def emit(label: str, value: object = None) -> None:
    print(label, flush=True)
    if value is not None:
        print(
            value if isinstance(value, str) else json.dumps(value, ensure_ascii=True, indent=2),
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    for base in range(39100, 39900, 10):
        sockets = []
        try:
            for port in [base, *range(base + 1, base + 5)]:
                sock = socket.socket()
                sockets.append(sock)
                sock.bind(("127.0.0.1", port))
            break
        except OSError:
            continue
        finally:
            for sock in sockets:
                sock.close()
    else:
        raise RuntimeError("No free demonstration port range")
    command = [
        sys.executable,
        "-m",
        "scripts.local_platform",
        "--port",
        str(base),
        "--base-port",
        str(base + 1),
        "--data-dir",
        str(args.output / "platform-data"),
        "--rps",
        "2",
    ]
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("LLM_", "API_", "TELEMETRY_", "SIMULATOR_"))
    }
    env.update(API_AUTH_MODE="local", PYTHONUNBUFFERED="1")
    log = (args.output / "platform.log").open("w")
    process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    responses = []
    manifest = {
        "schema_version": 1,
        "status": "running",
        "config": {
            "mode": "live loopback HTTP using synthetic business services; actual local GGUF inference",
            "not_external_benchmark": True,
            "model_sha256": hashlib.sha256(args.model.read_bytes()).hexdigest(),
        },
        "command": [sys.executable, "-m", "demo.video_driver", *sys.argv[1:]],
        "seeds": [2026],
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "dependencies": sorted(
                f"{d.metadata['Name']}=={d.version}" for d in importlib.metadata.distributions()
            ),
        },
        "source_sha256": {
            "demo/video_driver.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        },
        "metrics": {},
        "timings": {
            "deterministic": False,
            "reason": "Actual HTTP times, event identifiers and scheduling vary.",
        },
    }
    (args.output / "result.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def until(second: float) -> None:
        while time.monotonic() - start < second:
            if process.poll() is not None:
                raise RuntimeError("Platform stopped unexpectedly")
            time.sleep(min(1, max(0, second - (time.monotonic() - start))))

    def get(path: str, client: httpx.Client) -> object:
        response = client.get(path)
        response.raise_for_status()
        data = response.json()
        responses.append(
            {
                "elapsed": time.monotonic() - start,
                "method": "GET",
                "path": path,
                "status": response.status_code,
                "response": data,
            }
        )
        return data

    try:
        emit("AIOps Research | recorded live process output | 5 minute session")
        emit("Scope: four SYNTHETIC business services over real loopback HTTP.")
        emit("This video does not claim a Kubernetes or Chaos Mesh experiment.")
        emit("Starting real processes", command)
        with httpx.Client(
            base_url=f"http://127.0.0.1:{base}", timeout=15, trust_env=False
        ) as client:
            for _ in range(120):
                try:
                    ready = get("/v1/ready", client)
                    break
                except httpx.HTTPError:
                    time.sleep(0.25)
            else:
                raise TimeoutError("Platform startup timed out")
            emit("Actual readiness response", ready)
            until(30)
            emit("00:30 | Enable controlled synthetic payment error (not infrastructure chaos).")
            fault = client.post(
                f"http://127.0.0.1:{base + 4}/fault",
                json={"kind": "error", "duration_seconds": 150, "error_rate": 1.0},
            )
            fault.raise_for_status()
            emit("Actual fault-control response", fault.json())
            responses.append(
                {
                    "elapsed": time.monotonic() - start,
                    "method": "POST",
                    "path": "payment/fault",
                    "response": fault.json(),
                }
            )
            until(60)
            alerts = get("/v1/alerts?limit=100", client)
            emit(
                "01:00 | Actual alerts",
                [
                    {"service": a["service"], "rule": a["rule_id"], "id": a["alert_id"]}
                    for a in alerts
                ],
            )
            until(90)
            incidents = get("/v1/incidents?limit=100", client)
            emit(
                "01:30 | Actual incident correlation",
                [
                    {
                        "id": i["incident_id"],
                        "services": i["affected_services"],
                        "status": i["status"],
                    }
                    for i in incidents
                ],
            )
            if not incidents:
                raise RuntimeError("No incident was observed")
            selected = next(
                i
                for i in incidents
                if i.get("root_anchor") == "payment" and i.get("family") == "availability"
            )
            versions = get("/v1/incidents/" + selected["incident_id"] + "/reports", client)
            if not versions:
                raise RuntimeError("No report was produced")
            report = get(
                "/v1/report-versions/" + max(versions, key=lambda v: v["version"])["report_id"],
                client,
            )
            until(120)
            emit("02:00 | Real local 0.5B inference on evidence from this live report.")
            emit(
                "Rules and model output are reported separately. Abstention is not a correct root-cause claim."
            )
            records = [r["event"] for r in report["snapshot"]["records"]]
            span = next(
                e
                for e in records
                if e["event_type"] == "span"
                and e["service"] == "payment"
                and e.get("status_code", 200) >= 500
            )
            evidence = [
                e
                for e in records
                if e.get("trace_id") == span["trace_id"] and e["event_type"] == "span"
            ][:8]
            from llama_cpp import Llama

            from scripts.research.evaluate_llm import infer

            model = Llama(
                model_path=str(args.model),
                n_ctx=4096,
                n_threads=2,
                n_threads_batch=2,
                logits_all=False,
                seed=2026,
                verbose=False,
            )
            try:
                case = {
                    "case_id": "live-video-payment",
                    "family": "dependency",
                    "payload": {"records": evidence, "alert": report["alert"]},
                    "gold": {
                        "root_services": ["payment"],
                        "evidence_ids": [span["event_id"]],
                        "should_abstain": False,
                    },
                }
                result = infer(model, case, True)
            finally:
                model.close()
            (args.output / "live-model-output.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2) + "\n"
            )
            emit("Actual model output", result["audit"]["raw_output"])
            emit(
                "Validation and mapping",
                {
                    "valid": result["metrics"]["valid"],
                    "selected_evidence": result["mapping"],
                    "latency_seconds": result["latency_seconds"],
                },
            )
            verification = get("/v1/report-versions/" + report["report_id"] + "/verify", client)
            emit("Actual immutable report verification", verification)
            manifest["metrics"].update(
                model_valid=result["metrics"]["valid"],
                model_abstained=result["metrics"]["abstained"],
                model_latency_seconds=result["latency_seconds"],
                report_verification=verification,
            )
            until(180)
            emit("03:00 | Clear the synthetic fault; continue real healthy HTTP traffic.")
            clear = client.post(
                f"http://127.0.0.1:{base + 4}/fault", json={"kind": "none", "duration_seconds": 1}
            )
            clear.raise_for_status()
            emit("Actual clear response", clear.json())
            responses.append(
                {
                    "elapsed": time.monotonic() - start,
                    "method": "POST",
                    "path": "payment/fault",
                    "response": clear.json(),
                }
            )
            until(215)
            emit("03:35 | Recovery is determined by fresh evidence and a hold period.")
            states = get("/v1/incidents?limit=100", client)
            emit(
                "Actual incident state",
                [
                    {k: i.get(k) for k in ("incident_id", "status", "root_service", "resolved_at")}
                    for i in states
                ],
            )
            until(250)
            versions = get("/v1/incidents/" + selected["incident_id"] + "/reports", client)
            emit(
                "04:10 | Immutable report versions",
                [
                    {"version": v["version"], "stage": v["stage"], "report_id": v["report_id"]}
                    for v in versions
                ],
            )
            latest = max(versions, key=lambda v: v["version"])
            latest_verification = get(
                "/v1/report-versions/" + latest["report_id"] + "/verify", client
            )
            emit("Latest evidence verification", latest_verification)
            final_incident = get("/v1/incidents/" + selected["incident_id"], client)
            emit(
                "Selected payment incident recovery",
                {
                    k: final_incident.get(k)
                    for k in ("incident_id", "family", "root_anchor", "status", "resolved_at")
                },
            )
            manifest["metrics"]["selected_incident_final_status"] = final_incident["status"]
            if final_incident["status"] != "resolved":
                raise RuntimeError(
                    "Demonstration did not observe payment-incident recovery; preserve this run"
                )
            if not all(
                latest_verification.get(k) is True
                for k in (
                    "full_evidence_valid",
                    "snapshot_sha256_valid",
                    "rca_sha256_valid",
                    "rca_evidence_references_valid",
                    "rca_evidence_set_valid",
                )
            ):
                raise RuntimeError("Report integrity verification failed")
            until(270)
            emit("04:30 | Research results and limits (read from saved result.json).")
            for name in ("p0-smd-03", "p0-psm-03"):
                data = json.loads((ROOT / "research/results" / name / "result.json").read_text())
                emit(name, {k: round(v["pr_auc"]["mean"], 6) for k, v in data["metrics"].items()})
            for name in ("p1-05b-01", "p1-15b-01"):
                path = ROOT / "research/results" / name / "result.json"
                if path.exists():
                    data = json.loads(path.read_text())
                    emit(
                        name,
                        {
                            "n_test": data["metrics"]["correct"]["n"],
                            "correctness": data["metrics"]["correct"]["value"],
                            "abstention": data["metrics"]["abstained"]["value"],
                        },
                    )
            emit("Public anomaly datasets are separate from synthetic RCA labels.")
            emit(
                "Human label review, real cluster faults and remote CI remain separate unpassed gates."
            )
            until(299)
            emit("04:59 | End. All displayed responses came from this execution; outputs retained.")
        manifest["status"] = "passed"
        manifest["metrics"].update(
            http_observations=len(responses), report_versions_observed=len(versions)
        )
    except BaseException as error:
        manifest.update(status="failed", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        process.terminate()
        try:
            process.wait(timeout=35)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        log.close()
        manifest["ended_at_utc"] = datetime.now(timezone.utc).isoformat()
        manifest["timings"]["elapsed_seconds"] = time.monotonic() - start
        (args.output / "http-responses.json").write_text(
            json.dumps(responses, ensure_ascii=False, indent=2) + "\n"
        )
        (args.output / "result.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (args.output / "completed.json").write_text(
            json.dumps(
                {
                    "status": manifest["status"],
                    "result_sha256": hashlib.sha256(
                        (args.output / "result.json").read_bytes()
                    ).hexdigest(),
                },
                indent=2,
            )
            + "\n"
        )
        (args.output / "summary.md").write_text(
            "# 实时演示记录\n\n1. 证明：真实进程、HTTP、GGUF 与证据核对可串联演示；不证明外部准确率。\n2. 样本：一次本机合成 payment 故障，时间戳与响应均实录。\n3. 对照：没有对照组，不报告优越性。\n4. 不确定性：单次演示不计算泛化 CI。\n5. 局限：不使用 Kubernetes/Chaos Mesh，终端渲染并非浏览器视频。\n6. 下一步：依 deploy/README.md 在可运行的集群验证真实故障。\n\n完整命令、种子与版本见 [result.json](result.json)；stdout 时间线在上一层 session.cast。\n"
        )


if __name__ == "__main__":
    main()
