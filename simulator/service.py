"""One HTTP service process; deploy four instances to form a real call graph."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
import os
import hmac
import logging
from pathlib import Path
import random
import re
import socket
import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, ConfigDict, Field

from .faults import FaultState
from .telemetry import Telemetry


@dataclass
class Settings:
    service_name: str = field(default_factory=lambda: os.getenv("SERVICE_NAME", "gateway"))
    instance_id: str = field(default_factory=lambda: os.getenv("INSTANCE_ID", socket.gethostname()))
    downstream_timeout_seconds: float = field(default_factory=lambda: float(os.getenv("SIMULATOR_DOWNSTREAM_TIMEOUT_SECONDS", "3")))
    control_token: str = field(default_factory=lambda: os.getenv("SIMULATOR_CONTROL_TOKEN", ""))
    audit_dir: str = field(default_factory=lambda: os.getenv("SIMULATOR_FAULT_AUDIT_DIR", ""))
    downstream_urls: dict[str, str] = field(default_factory=lambda: {
        "order": os.getenv("ORDER_URL", "http://order:8000"),
        "payment": os.getenv("PAYMENT_URL", "http://payment:8000"),
        "inventory": os.getenv("INVENTORY_URL", "http://inventory:8000"),
    })


class FaultSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str = Field(default="none", pattern="^(latency|error|timeout|unavailable|none)$")
    duration_seconds: int = Field(default=120, ge=1, le=600)
    latency_ms: int = Field(default=1500, ge=0, le=10000)
    error_rate: float = Field(default=1.0, ge=0.0, le=1.0)


def _identifier(value: str | None, length: int) -> str:
    return value if value and re.fullmatch(f"[0-9a-f]{{{length}}}", value) else uuid.uuid4().hex[:length]


def create_app(settings: Settings | None = None, telemetry: Telemetry | None = None,
               transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    cfg = settings or Settings()
    if cfg.service_name not in {"gateway", "order", "payment", "inventory"}:
        raise ValueError("SERVICE_NAME must be gateway, order, payment, or inventory")
    sink = telemetry or Telemetry()
    if not 0.05 <= cfg.downstream_timeout_seconds <= 30:
        raise ValueError("downstream timeout must be 0.05..30 seconds")
    faults = FaultState(service=cfg.service_name,
                        audit_path=Path(cfg.audit_dir)/(cfg.service_name+".jsonl") if cfg.audit_dir else None)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(sink.start)
        app.state.client = httpx.AsyncClient(
            timeout=cfg.downstream_timeout_seconds, transport=transport,
            trust_env=False,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=40))
        async def expire_faults():
            while True:
                await asyncio.sleep(0.1)
                try:
                    faults.snapshot()
                except OSError:
                    logging.getLogger(__name__).exception("fault audit failed")
        expiry_task = asyncio.create_task(expire_faults())
        try:
            yield
        finally:
            expiry_task.cancel()
            await asyncio.gather(expiry_task, return_exceptions=True)
            try:
                faults.close()
            except OSError:
                logging.getLogger(__name__).exception("fault shutdown audit failed")
            await app.state.client.aclose()
            await asyncio.to_thread(sink.close)

    app = FastAPI(title=f"AIOps simulated {cfg.service_name}", lifespan=lifespan)
    app.state.telemetry = sink
    app.state.faults = faults
    app.state.settings = cfg

    @app.middleware("http")
    async def observe_work(request: Request, call_next):
        if request.url.path != "/work":
            return await call_next(request)
        trace_id = _identifier(request.headers.get("x-trace-id"), 32)
        span_id = uuid.uuid4().hex[:16]
        parent_span_id = request.headers.get("x-parent-span-id")
        if parent_span_id and not re.fullmatch("[0-9a-f]{16}", parent_span_id):
            parent_span_id = None
        request.state.trace_id = trace_id
        request.state.span_id = span_id
        request.state.error_message = None
        request.state.error_code = None
        request.state.dependency_service = None
        request.state.downstream = []
        started_ms = int(time.time() * 1000)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            request.state.error_message = "unhandled service error"
            request.state.error_code = "INTERNAL_ERROR"
            response = JSONResponse({"detail": "internal service error"}, status_code=500)
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        ended_ms = int(time.time() * 1000)
        common = {
            "schema_version": 1,
            "event_time": ended_ms,
            "service": cfg.service_name,
            "instance_id": cfg.instance_id,
            "trace_id": trace_id,
        }
        failed = response.status_code >= 500
        metric = {**common, "event_id": uuid.uuid4().hex,
                  "event_type": "request", "latency_ms": duration_ms,
                  "error": failed, "status_code": response.status_code,
                  "span_id": span_id}
        span = {**common, "event_id": uuid.uuid4().hex,
                "event_type": "span", "span_id": span_id,
                "parent_span_id": parent_span_id, "start_time": started_ms,
                "end_time": ended_ms, "latency_ms": duration_ms,
                "status_code": response.status_code,
                "downstream": request.state.downstream,
                "dependency_service": request.state.dependency_service}
        log = {**common, "event_id": uuid.uuid4().hex,
               "event_type": "log", "span_id": span_id,
               "level": "ERROR" if failed else "INFO",
               "message": request.state.error_message or "request completed",
               "error_code": request.state.error_code,
               "dependency_service": request.state.dependency_service}
        # Kafka's potentially blocking metadata lookup stays off the HTTP event loop.
        def publish_events():
            sink.publish("telemetry.metrics", metric)
            sink.publish("telemetry.spans", span)
            sink.publish("telemetry.logs", log)
        publish_started = time.perf_counter()
        await asyncio.to_thread(publish_events)
        # Keep the original span/request semantics: the handler includes child
        # requests, but excludes this service's own synchronous publication.
        # Expose the overhead separately; do not subtract it or queue it away.
        response.headers["x-handler-downstream-ms"] = str(duration_ms)
        response.headers["x-telemetry-publish-ms"] = str(round((time.perf_counter()-publish_started)*1000, 3))
        response.headers["x-observed-total-ms"] = str(round((time.perf_counter()-started)*1000, 3))
        response.headers["x-trace-id"] = trace_id
        response.headers["x-span-id"] = span_id
        return response

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": cfg.service_name,
                "telemetry_mode": sink.mode, "telemetry_sent": sink.sent,
                "telemetry_failed": sink.failed}

    def authorize(request):
        if cfg.control_token and not hmac.compare_digest(
                request.headers.get("x-simulator-token", ""), cfg.control_token):
            raise HTTPException(403, "invalid simulator control token")

    @app.get("/fault/history")
    async def fault_history(request: Request):
        authorize(request)
        return {"service": cfg.service_name, "records": faults.history(), "retained_limit": 256}

    @app.get("/fault")
    async def get_fault(request: Request):
        authorize(request)
        return {"service": cfg.service_name, **asdict(faults.snapshot())}

    @app.post("/fault")
    async def set_fault(spec: FaultSpec, request: Request):
        authorize(request)
        try:
            snapshot = faults.set(spec.kind, spec.duration_seconds,
                                  spec.latency_ms, spec.error_rate)
        except OSError:
            logging.getLogger(__name__).exception("fault audit write failed")
            raise HTTPException(503, "fault audit unavailable")
        return {"service": cfg.service_name, **asdict(snapshot)}

    async def call_dependency(request: Request, service: str):
        request.state.dependency_service = service
        request.state.downstream.append(service)
        try:
            response = await app.state.client.get(
                cfg.downstream_urls[service].rstrip("/") + "/work",
                headers={"x-trace-id": request.state.trace_id,
                         "x-parent-span-id": request.state.span_id})
            if response.status_code >= 400:
                request.state.error_message = f"dependency {service} returned HTTP {response.status_code}"
                request.state.error_code = "UPSTREAM_ERROR"
                raise HTTPException(502, detail=f"dependency {service} unavailable")
            return response.json()
        except httpx.TimeoutException:
            request.state.error_message = f"dependency {service} request timed out"
            request.state.error_code = "UPSTREAM_TIMEOUT"
            raise HTTPException(504, detail=f"dependency {service} timed out")
        except httpx.RequestError:
            request.state.error_message = f"dependency {service} connection failed"
            request.state.error_code = "UPSTREAM_UNAVAILABLE"
            raise HTTPException(502, detail=f"dependency {service} unavailable")

    @app.get("/work")
    @app.post("/work")
    async def work(request: Request):
        snapshot = faults.snapshot()
        if snapshot.kind in {"latency", "timeout"}:
            deadline = time.monotonic() + snapshot.latency_ms / 1000
            while time.monotonic() < deadline:
                if faults.snapshot().fault_id != snapshot.fault_id:
                    break  # Explicit clear, replacement or TTL cancels outstanding waits.
                await asyncio.sleep(min(0.05, max(0, deadline-time.monotonic())))
        if snapshot.kind == "unavailable" or (snapshot.kind == "error" and random.random() < snapshot.error_rate):
            request.state.error_message = "service temporarily unavailable"
            request.state.error_code = "SERVICE_UNAVAILABLE"
            raise HTTPException(503, detail="service temporarily unavailable")
        await asyncio.sleep(random.uniform(0.008, 0.030))
        downstream = {}
        if cfg.service_name == "gateway":
            downstream["order"] = await call_dependency(request, "order")
        elif cfg.service_name == "order":
            downstream["inventory"] = await call_dependency(request, "inventory")
            downstream["payment"] = await call_dependency(request, "payment")
        return {"ok": True, "service": cfg.service_name,
                "trace_id": request.state.trace_id, "downstream": downstream}

    return app


app = create_app()
