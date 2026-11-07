"""Preflight, retain seeded plans, or run real Chaos Mesh faults; no simulation fallback."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx

from research.chaos.plan import make_plan, manifest
from research.experiment import Experiment, sha256, write_json


def command(args: list[str], stdin: str | None = None) -> str:
    result = subprocess.run(
        args, input=stdin, capture_output=True, text=True, timeout=180, check=False
    )
    logging.info(
        "command=%s exit=%d stdout=%s stderr=%s",
        args,
        result.returncode,
        result.stdout,
        result.stderr,
    )
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args}")
    return result.stdout


def append(path: Path, row: dict[str, Any]) -> None:
    with path.open("a") as stream:
        stream.write(json.dumps(row, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


@contextmanager
def managed_fault(
    prefix: list[str], doc: dict[str, Any], case: dict[str, Any], output: Path
) -> Iterator[None]:
    """Delete only the object this call exclusively created, even if journal fsync fails."""
    command(prefix + ["create", "-f", "-"], json.dumps(doc))
    try:
        append(
            output / "ground-truth.jsonl",
            {"phase": "submitted", "at_ms": time.time_ns() // 1_000_000, "case": case},
        )
        yield
    finally:
        command(prefix + ["delete", doc["kind"], case["id"], "--wait=true", "--timeout=120s"])
        append(
            output / "ground-truth.jsonl",
            {"phase": "fault_resource_removed", "at_ms": time.time_ns() // 1_000_000, "case": case},
        )


def observe(api: str, prometheus: str, output: Path, token: str | None) -> None:
    """Record polling timestamps separately from service event time; no zero imputation."""
    row: dict[str, Any] = {"observed_at_ms": time.time_ns() // 1_000_000}
    with httpx.Client(timeout=5, trust_env=False) as client:
        for key, url in {
            "alerts": api + "/v1/alerts?limit=500",
            "incidents": api + "/v1/incidents?limit=500",
            "prometheus_alerts": prometheus
            + "/api/v1/query?"
            + urlencode({"query": 'ALERTS{alertstate="firing"}'}),
        }.items():
            try:
                headers = (
                    {"Authorization": "Bearer " + token}
                    if token and key != "prometheus_alerts"
                    else {}
                )
                response = client.get(url, headers=headers)
                response.raise_for_status()
                row[key] = response.json()
            except (httpx.HTTPError, ValueError) as error:
                row[key] = None
                row[key + "_error"] = type(error).__name__
    append(output / "observations.jsonl", row)


def collect_reports(api: str, output: Path, token: str | None) -> None:
    """Capture immutable versions and integrity verdicts after the observation window."""
    headers = {"Authorization": "Bearer " + token} if token else {}
    with httpx.Client(timeout=60, trust_env=False, headers=headers) as client:
        response = client.get(api + "/v1/incidents?limit=500")
        response.raise_for_status()
        incidents = response.json()
        if len(incidents) >= 500:
            raise RuntimeError("incident list may be truncated; split the run")
        for incident in incidents:
            listing = client.get(api + "/v1/incidents/" + incident["incident_id"] + "/reports")
            listing.raise_for_status()
            versions = listing.json()
            if len(versions) >= 500:
                raise RuntimeError("report list may be truncated; split the run")
            for version in versions:
                path = api + "/v1/report-versions/" + version["report_id"]
                detail, verification = client.get(path), client.get(path + "/verify")
                detail.raise_for_status()
                verification.raise_for_status()
                append(
                    output / "reports.jsonl",
                    {"report": detail.json(), "verification": verification.json()},
                )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--namespace", default="boutique")
    parser.add_argument("--context", default="kind-aiops-research")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    args = parser.parse_args()
    if args.context != "kind-aiops-research" or args.namespace != "boutique":
        raise ValueError(
            "executor is restricted to the named disposable research cluster/namespace"
        )
    for url in (args.api_url, args.prometheus_url):
        if urlparse(url).hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("use local port-forwarded experiment endpoints")
    plan = make_plan(args.seed, args.repeats)
    with Experiment(
        args.output,
        vars(args)
        | {
            "output": str(args.output),
            "physical_hosts": 1,
            "kubernetes_nodes_requested": 3,
            "dependency_semantics": "payment gRPC outage via network partition; not literal HTTP 503",
        },
        [args.seed],
    ) as run:
        write_json(args.output / "plan.json", plan)
        run.result["plan_sha256"] = sha256(args.output / "plan.json")
        run.result["metrics"] = {
            "planned_injections": len(plan),
            "confirmed_injections": 0,
            "mttd": None,
            "mttr": None,
            "root_cause_top1": None,
            "root_cause_top3": None,
            "load_200_1000_3000_eps_600s": None,
        }
        available = {name: shutil.which(name) for name in ("docker", "kind", "kubectl")}
        run.result["preflight"] = available
        if not all(available.values()):
            run.result["status"] = "blocked"
            run.result["blocked_reason"] = (
                "Missing required cluster tools; no real injections attempted"
            )
        elif not args.execute:
            run.result["status"] = "blocked"
            run.result["blocked_reason"] = (
                "Plan only; --execute required for actual injection evidence"
            )
        else:
            prefix = ["kubectl", "--context", args.context, "-n", args.namespace]
            namespace = json.loads(
                command(
                    [
                        "kubectl",
                        "--context",
                        args.context,
                        "get",
                        "namespace",
                        args.namespace,
                        "-o",
                        "json",
                    ]
                )
            )
            if namespace["metadata"].get("labels", {}).get("aiops-chaos-target") != "true":
                raise ValueError("namespace must be labelled aiops-chaos-target=true")
            nodes = json.loads(
                command(["kubectl", "--context", args.context, "get", "nodes", "-o", "json"])
            )
            if len(nodes["items"]) < 3:
                raise ValueError("require at least three Kubernetes nodes; still one physical host")
            write_json(args.output / "nodes.json", nodes)
            inventory = json.loads(command(prefix + ["get", "pods", "-o", "json"]))
            write_json(args.output / "pods-before.json", inventory)
            command(prefix + ["get", "stresschaos,networkchaos,podchaos"])
            token = os.getenv("API_READ_TOKEN")
            for case in plan:
                start_wait = time.monotonic()
                while time.monotonic() - start_wait < case["wait_seconds"]:
                    observe(args.api_url, args.prometheus_url, args.output, token)
                    time.sleep(1)
                doc = manifest(case, args.namespace)
                with managed_fault(prefix, doc, case, args.output):
                    confirmed = False
                    deadline = time.monotonic() + 90
                    while time.monotonic() < deadline:
                        status = json.loads(
                            command(prefix + ["get", doc["kind"], case["id"], "-o", "json"])
                        )
                        append(
                            args.output / "controller-status.jsonl",
                            {
                                "case_id": case["id"],
                                "at_ms": time.time_ns() // 1_000_000,
                                "resource": status,
                            },
                        )
                        conditions = status.get("status", {}).get("conditions", [])
                        if any(
                            c.get("type") == "AllInjected" and c.get("status") == "True"
                            for c in conditions
                        ):
                            confirmed = True
                            run.result["metrics"]["confirmed_injections"] += 1
                            append(
                                args.output / "ground-truth.jsonl",
                                {
                                    "phase": "injected",
                                    "at_ms": time.time_ns() // 1_000_000,
                                    "case": case,
                                    "controller_conditions": conditions,
                                },
                            )
                            break
                        observe(args.api_url, args.prometheus_url, args.output, token)
                        time.sleep(1)
                    if not confirmed:
                        raise TimeoutError("Chaos controller did not confirm injection")
                    until = time.monotonic() + case["duration_seconds"]
                    while time.monotonic() < until:
                        observe(args.api_url, args.prometheus_url, args.output, token)
                        time.sleep(1)
                command(
                    prefix + ["rollout", "status", "deployment/" + case["target"], "--timeout=120s"]
                )
                append(
                    args.output / "ground-truth.jsonl",
                    {"phase": "workload_ready", "at_ms": time.time_ns() // 1_000_000, "case": case},
                )
                until = time.monotonic() + case["recovery_observation_seconds"]
                while time.monotonic() < until:
                    observe(args.api_url, args.prometheus_url, args.output, token)
                    time.sleep(1)
            collect_reports(args.api_url, args.output, token)
            run.result["acquisition_complete"] = True
            run.result["status"] = "partial"
            run.result["blocked_reason"] = (
                "Injection acquisition complete; run evaluator for censored detection/recovery metrics. Scale and process profiling remain separate gates."
            )
        run.summary(
            [
                "只计 Chaos 控制器明确确认的真实注入；计划不算执行。",
                "计划及实测数量分别见 metrics。",
                "同一故障期间同时采集 Prometheus 阈值告警。",
                "轮询时间有误差；漏检和未恢复保留为删失，不填0。",
                "当前环境缺集群工具；kind 三节点仍为一台物理机。",
                "具备隔离集群后执行，然后运行分布指标与规模评测。",
            ]
        )
        blocked = run.result["status"] in {"blocked", "partial"}
    return 2 if blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
