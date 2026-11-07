"""Bounded HTTP workload and real resource sampling; requested RPS is not observed EPS."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import numpy as np
from kafka import KafkaConsumer, TopicPartition

from research.chaos.resources import derive
from research.experiment import Experiment, write_json
from scripts.research.cluster_experiment import append, command


async def workload(url: str, rate: int, duration: int, seed: int) -> dict[str, Any]:
    """No unbounded backlog: record locally dropped requests when 128 slots are full."""
    counts = {"scheduled": 0, "started": 0, "success": 0, "failed": 0, "dropped_due_to_capacity": 0}
    latencies: list[float] = []
    rng = random.Random(seed)
    pending: set[asyncio.Task[None]] = set()
    start = time.monotonic()
    async with httpx.AsyncClient(
        timeout=5,
        trust_env=False,
        limits=httpx.Limits(max_connections=128, max_keepalive_connections=128),
    ) as client:

        async def request() -> None:
            mark = time.monotonic()
            try:
                response = await client.get(url)
                response.raise_for_status()
                counts["success"] += 1
            except httpx.HTTPError:
                counts["failed"] += 1
            value = time.monotonic() - mark
            completed = counts["success"] + counts["failed"]
            if len(latencies) < 20000:
                latencies.append(value)
            else:
                selected = rng.randrange(completed)
                if selected < len(latencies):
                    latencies[selected] = value

        while time.monotonic() - start < duration:
            scheduled = min(rate * duration, int((time.monotonic() - start) * rate))
            count = scheduled - counts["scheduled"]
            counts["scheduled"] = scheduled
            available = min(count, max(0, 128 - len(pending)))
            counts["dropped_due_to_capacity"] += count - available
            for _ in range(available):
                task = asyncio.create_task(request())
                pending.add(task)
                task.add_done_callback(pending.discard)
                counts["started"] += 1
            await asyncio.sleep(0.01)
        if pending:
            await asyncio.gather(*pending)
    elapsed = time.monotonic() - start
    return {
        **counts,
        "elapsed_seconds": elapsed,
        "target_http_requests_per_second": rate,
        "achieved_http_requests_per_second": counts["started"] / elapsed,
        "latency_sample_n": len(latencies),
        "latency_reservoir_limit": 20000,
        "p50_seconds": float(np.quantile(latencies, 0.5)) if latencies else None,
        "p95_seconds": float(np.quantile(latencies, 0.95)) if latencies else None,
    }


def resource_sample(args: argparse.Namespace) -> dict[str, Any]:
    row: dict[str, Any] = {"observed_at_ms": time.time_ns() // 1_000_000}
    code = "import pathlib,json; paths=['/proc/1/status','/sys/fs/cgroup/cpu.stat','/sys/fs/cgroup/memory.current']; print(json.dumps({p:pathlib.Path(p).read_text() if pathlib.Path(p).exists() else None for p in paths}))"
    for container in ("api", "worker"):
        cmd = [
            "kubectl",
            "--context",
            "kind-aiops-research",
            "-n",
            "aiops",
            "exec",
            "deployment/aiops-app",
            "-c",
            container,
            "--",
            "python",
            "-c",
            code,
        ]
        try:
            value = subprocess.run(cmd, text=True, capture_output=True, check=True, timeout=10)
            row[container] = json.loads(value.stdout)
        except (subprocess.SubprocessError, ValueError) as error:
            row[container] = None
            row[container + "_error"] = type(error).__name__
    with httpx.Client(timeout=5, trust_env=False) as client:
        for name, url in {
            "database": args.api_url + "/v1/metrics",
            "flink_jobs": args.flink_url + "/jobs/overview",
        }.items():
            try:
                token = os.getenv("API_READ_TOKEN") if name == "database" else None
                headers = {"Authorization": "Bearer " + token} if token else {}
                response = client.get(url, headers=headers)
                response.raise_for_status()
                row[name] = response.text if name == "database" else response.json()
                if name == "flink_jobs":
                    checkpoints = {}
                    for job in row[name].get("jobs", []):
                        check = client.get(args.flink_url + "/jobs/" + job["jid"] + "/checkpoints")
                        check.raise_for_status()
                        checkpoints[job["jid"]] = check.json()
                    row["flink_checkpoints"] = checkpoints
            except (httpx.HTTPError, ValueError) as error:
                row[name] = None
                row[name + "_error"] = type(error).__name__
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=args.kafka.split(","),
            group_id="aiops-evidence-v1",
            enable_auto_commit=False,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=3000,
        )
        try:
            partitions = [
                TopicPartition(topic, p)
                for topic in (
                    "telemetry.metrics",
                    "telemetry.logs",
                    "telemetry.spans",
                    "ops.alerts",
                )
                for p in consumer.partitions_for_topic(topic) or []
            ]
            ends = consumer.end_offsets(partitions)
            row["kafka"] = [
                {
                    "topic": p.topic,
                    "partition": p.partition,
                    "end_offset": ends[p],
                    "committed": consumer.committed(p),
                }
                for p in partitions
            ]
        finally:
            consumer.close()
    except Exception as error:
        row["kafka"] = None
        row["kafka_error"] = type(error).__name__
    return row


async def measured_level(args: argparse.Namespace, rate: int, output: Path) -> dict[str, Any]:
    job = asyncio.create_task(workload(args.url, rate, args.duration, args.seed))
    while not job.done():
        sample = await asyncio.to_thread(resource_sample, args)
        append(output / f"resources-{rate}.jsonl", sample)
        await asyncio.sleep(5)
    return await job


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url", default="http://127.0.0.1:8080/")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--flink-url", default="http://127.0.0.1:8081")
    parser.add_argument("--kafka", default="127.0.0.1:19092")
    parser.add_argument("--rates", default="200,1000,3000")
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    rates = [int(x) for x in args.rates.split(",")]
    if len(rates) != 3 or any(not 1 <= r <= 3000 for r in rates) or args.duration != 600:
        raise ValueError("three bounded rates and exactly 600 seconds per level required")
    if any(
        urlparse(url).hostname not in {"localhost", "127.0.0.1"}
        for url in (args.url, args.api_url, args.flink_url)
    ):
        raise ValueError("only local port forwards to the isolated cluster are permitted")
    with Experiment(
        args.output,
        vars(args)
        | {
            "output": str(args.output),
            "measurement_scope": "API/worker container cgroup CPU, PID1 RSS; Kafka/Flink JVM RSS and external LLM excluded",
        },
        [args.seed],
    ) as run:
        if not args.execute or not shutil.which("kubectl") or not shutil.which("docker"):
            run.result.update(
                status="blocked",
                blocked_reason="Real cluster execution unavailable or not requested",
            )
            run.result["metrics"] = {"levels": [], "observed_eps": None}
        else:
            namespace = json.loads(
                command(
                    [
                        "kubectl",
                        "--context",
                        "kind-aiops-research",
                        "get",
                        "namespace",
                        "boutique",
                        "-o",
                        "json",
                    ]
                )
            )
            if namespace["metadata"].get("labels", {}).get("aiops-chaos-target") != "true":
                raise ValueError("wrong target namespace")
            levels = [asyncio.run(measured_level(args, rate, args.output)) for rate in rates]
            summaries = []
            for rate in rates:
                rows = [
                    json.loads(line)
                    for line in (args.output / f"resources-{rate}.jsonl").read_text().splitlines()
                ]
                derived = derive(rows)
                write_json(args.output / f"resources-{rate}-derived.json", derived)
                summaries.append(
                    {key: value for key, value in derived.items() if key != "points"}
                    | {"target_http_rps": rate}
                )
            run.result["metrics"] = {
                "levels": levels,
                "resources": summaries,
                "observed_eps": [item["observed_eps_mean"] for item in summaries],
            }
            run.result.update(
                status="partial",
                blocked_reason="Requested HTTP RPS is not a guaranteed 200/1000/3000 telemetry EPS intervention; inspect measured throughput and resource coverage",
            )
        run.summary(
            [
                "只有真实负载与资源采样能产生结果。",
                "三个600秒档位，未执行时指标为空。",
                "比较同一集群三个请求速率；实际事件速率需看Kafka offsets。",
                "延迟为有界reservoir样本；时间与主机负载不确定。",
                "HTTP RPS不等于事件EPS；容器PID1 RSS遗漏其他进程。",
                "检查原始资源与offset记录再判断200/1000/3000 EPS目标是否达到。",
            ]
        )
        incomplete = run.result["status"] in {"blocked", "partial"}
    return 2 if incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
