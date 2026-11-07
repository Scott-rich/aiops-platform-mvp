"""Explicit dependency probes; liveness alone never certifies the stream pipeline."""

import json
import os
import urllib.request
from pathlib import Path
from typing import Any

from .worker import check_health


def kafka_ready() -> Any:
    from kafka import KafkaAdminClient

    client = None
    try:
        client = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092").split(","),
            api_version_auto_timeout_ms=1500,
            request_timeout_ms=3000,
        )
        client.describe_cluster()
        return {"ready": True}
    except Exception as exc:
        return {"ready": False, "error_type": type(exc).__name__}
    finally:
        if client is not None:
            client.close()


def flink_ready() -> Any:
    name = os.getenv("FLINK_JOB_NAME", "aiops-event-time-detector-v3")
    base = os.getenv("FLINK_REST_URL", "http://jobmanager:8081").rstrip("/")
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
            base + "/jobs/overview", timeout=3
        ) as response:
            jobs = json.load(response)["jobs"]
        active = [
            j
            for j in jobs
            if j.get("name") == name
            and j.get("state") in {"RUNNING", "RESTARTING", "CREATED", "INITIALIZING"}
        ]
        ready = False
        if len(active) == 1:
            tasks = active[0].get("tasks", {})
            ready = (
                active[0]["state"] == "RUNNING"
                and tasks.get("total", 0) > 0
                and tasks.get("running", 0) == tasks["total"]
            )
        return {
            "ready": ready,
            "jobs": [
                {"job_id": j["jid"], "state": j["state"], "tasks": j.get("tasks", {})}
                for j in active
            ],
        }
    except Exception as exc:
        return {"ready": False, "error_type": type(exc).__name__}


def dependencies(data_dir: Any) -> Any:
    mode = os.getenv("EXECUTION_MODE", "kafka-worker")
    if mode in {"local-http", "local-http-python-rules"}:
        return {
            "ready": True,
            "execution_mode": mode,
            "note": "local Python mode; Kafka/Flink probes not applicable",
        }
    state = {
        "execution_mode": mode,
        "kafka": kafka_ready(),
        "flink": flink_ready(),
        "worker": {"ready": check_health(Path(data_dir) / "worker-health.json")},
    }
    state["ready"] = all(state[key]["ready"] for key in ("kafka", "flink", "worker"))
    state["note"] = (
        "Dependency readiness is a point-in-time probe; it does not certify throughput or all windows completed"
    )
    return state
