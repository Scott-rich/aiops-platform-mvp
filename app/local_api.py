"""Explicit local HTTP ingestion mode; not enabled on the Kafka production API."""

import asyncio
import json
import logging
import os
import socket
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response

from .api import app as query_app
from .api import storage_unavailable
from .api_runtime import install_boundary
from .contracts import now_ms, validate_event
from .engine import Pipeline
from .linux_agent import cpu_ticks, memory_percent
from .local_detection import LocalWindowDetector
from .observability import install_features
from .storage import Store

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: Any) -> Any:
    from flink.adaptive import DetectionConfig

    from .runtime_config import RuntimeConfig
    from .runtime_logging import configure_logging

    configure_logging(RuntimeConfig.from_env())
    config = DetectionConfig.from_env()
    pipeline = Pipeline(Store())
    try:
        detector = LocalWindowDetector(pipeline, config=config)
    except Exception:
        pipeline.close()
        raise
    app.state.pipeline = pipeline
    health: dict[str, Any] = {
        "running": True,
        "last_success": 0,
        "last_ok": False,
        "last_error_type": None,
    }
    app.state.local_pipeline_health = health
    stopped = asyncio.Event()

    async def advance() -> Any:
        next_host = 0
        previous_cpu = cpu_ticks() if Path("/proc/stat").exists() else None
        while not stopped.is_set():
            try:
                if previous_cpu is not None and now_ms() >= next_host:
                    current_cpu = cpu_ticks()
                    total, idle = current_cpu[0] - previous_cpu[0], current_cpu[1] - previous_cpu[1]
                    previous_cpu = current_cpu
                    event = {
                        "schema_version": 1,
                        "event_id": uuid.uuid4().hex,
                        "event_time": now_ms(),
                        "event_type": "host",
                        "service": "linux-node",
                        "instance_id": socket.gethostname(),
                        "trace_id": "",
                        "cpu_percent": max(0, min(100, 100 * (1 - idle / max(1, total)))),
                        "memory_percent": memory_percent(),
                        "load1": os.getloadavg()[0],
                    }
                    await asyncio.to_thread(
                        pipeline.ingest_batch, [(event, {"transport": "local-proc"})]
                    )
                    next_host = now_ms() + 5000
                await asyncio.to_thread(detector.advance)
                await asyncio.to_thread(pipeline.tick)
                health.update(last_success=time.monotonic(), last_ok=True, last_error_type=None)
            except Exception as exc:
                health.update(last_ok=False, last_error_type=type(exc).__name__)
                log.exception("Local pipeline tick failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(stopped.wait(), 0.5)

    task = asyncio.create_task(advance())
    try:
        yield
    finally:
        health["running"] = False
        stopped.set()
        try:
            await task
        finally:
            await asyncio.to_thread(pipeline.close)
            log.info("local_pipeline_stopped")


app = FastAPI(title="智能运维平台 · 本地HTTP运行", version="0.2.0", lifespan=lifespan)
app.include_router(query_app.router)
app.add_exception_handler(sqlite3.Error, storage_unavailable)

install_boundary(app)


@app.post("/demo/ingest", status_code=204)
async def ingest(request: Request) -> Response:
    raw = await request.body()
    if len(raw) > 1_000_000:
        raise HTTPException(413, "telemetry too large") from None
    try:
        body = json.loads(raw)
        event, topic = body["event"], body["topic"]
        expected = {
            "telemetry.metrics": ("request", "host"),
            "telemetry.logs": ("log",),
            "telemetry.spans": ("span",),
        }
        if event.get("event_type") not in expected.get(topic, ()):
            raise ValueError("topic mismatch")
        validate_event(event)
        source = {"transport": "local-http", "topic": topic, "receipt_id": event["event_id"]}
        await asyncio.to_thread(request.app.state.pipeline.ingest_batch, [(event, source)])
    except (ValueError, KeyError, TypeError, AttributeError):
        raise HTTPException(400, "invalid telemetry") from None
    return Response(status_code=204)


install_features(app)
