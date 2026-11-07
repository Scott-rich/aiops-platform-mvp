import json
import logging
import os
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field

from .api_runtime import api_lifespan, install_boundary
from .contracts import digest
from .diagnosis import markdown
from .evidence_api import router as evidence_router
from .observability import install_features
from .rca_integrity import verify_attachment
from .storage import Store

app = FastAPI(
    lifespan=api_lifespan,
    title="分布式系统智能运维平台",
    version="0.2.0",
    description="实时检测、证据快照、案例检索与辅助诊断。交互入口 /docs；报告接口返回完整可追溯JSON。",
)

install_boundary(app)


@app.exception_handler(sqlite3.Error)
async def storage_unavailable(_request: Any, error: Any) -> Any:
    logging.getLogger(__name__).error("storage_unavailable error_type=%s", type(error).__name__)
    return JSONResponse(
        {"detail": "storage_unavailable", "error_type": type(error).__name__}, status_code=503
    )


def storage() -> Any:
    return Store()


@app.get("/")
def index() -> Any:
    page = Path(__file__).parent / "static" / "index.html"
    if page.is_file():
        return FileResponse(page)
    return {
        "name": app.title,
        "api_docs": "/docs",
        "alerts": "/alerts",
        "incidents": "/incidents",
        "status": "/health",
    }


@app.get("/live")
def live() -> Any:
    return {"status": "alive"}


@app.get("/health")
def health() -> Any:
    return {
        "status": "ok",
        "storage": storage().stats(),
        "execution_mode": os.getenv("EXECUTION_MODE", "kafka-worker"),
        "llm_configured": bool(os.getenv("LLM_BASE_URL") and os.getenv("LLM_MODEL")),
        "note": "API存活状态不代表Kafka/Flink已就绪，请同时检查Flink作业与worker日志",
    }


@app.get("/ready")
def ready(request: Request) -> Any:
    from .readiness import dependencies

    try:
        store = storage()
        store.stats()  # Exercise a real read, rather than trusting a local-mode flag.
        state = dependencies(store.root)
        if state.get("execution_mode") in {"local-http", "local-http-python-rules"}:
            import time

            from .runtime_config import RuntimeConfig

            health = getattr(request.app.state, "local_pipeline_health", {})
            age = time.monotonic() - health.get("last_success", 0)
            healthy = bool(
                health.get("running")
                and health.get("last_ok")
                and 0 <= age <= RuntimeConfig.from_env().ready_max_age
            )
            state.update(
                ready=healthy,
                local_pipeline={"ready": healthy, "last_error_type": health.get("last_error_type")},
            )
        return JSONResponse(state, status_code=200 if state["ready"] else 503)
    except Exception as exc:
        return JSONResponse({"ready": False, "error_type": type(exc).__name__}, status_code=503)


@app.get("/services")
async def services() -> Any:
    results = {}
    async with httpx.AsyncClient(timeout=1, trust_env=False) as client:

        async def check(service: Any) -> Any:
            base = os.getenv(service.upper() + "_URL", f"http://{service}:8000")
            try:
                response = await client.get(base.rstrip("/") + "/health")
                response.raise_for_status()
                results[service] = response.json()
            except (httpx.HTTPError, ValueError):
                results[service] = {"service": service, "status": "unknown"}

        import asyncio

        await asyncio.gather(
            *(check(name) for name in ("gateway", "order", "inventory", "payment"))
        )
    return {"services": results, "note": "进程存活状态；异常影响见事件与请求指标"}


@app.get("/incidents")
def incidents(limit: int = Query(100, ge=1, le=500)) -> Any:
    from .incidents import IncidentManager

    return IncidentManager(storage()).list(limit)


@app.get("/incidents/{incident_id}")
def incident(incident_id: str) -> Any:
    from .incidents import IncidentManager

    try:
        value = IncidentManager(storage()).get(incident_id)
        if value is None:
            raise KeyError(incident_id)
        return value
    except KeyError:
        raise HTTPException(404, "incident not found") from None


@app.get("/incidents/{incident_id}/reports")
def incident_reports(incident_id: str) -> Any:
    from .reporting import ReportManager

    return ReportManager(storage()).list_versions(incident_id)


@app.get("/report-versions/{report_id}")
def report_version(report_id: str) -> Any:
    from .reporting import ReportManager

    try:
        value = ReportManager(storage()).get(report_id)
        if value is None:
            raise KeyError(report_id)
        return value
    except KeyError:
        raise HTTPException(404, "report version not found") from None


@app.get("/report-versions/{report_id}/verify")
def verify_version(report_id: str) -> Any:
    value = report_version(report_id)
    from .evidence_api import verify_report_evidence

    return {
        **verify_report_evidence(storage(), value),
        "report_id": report_id,
        "snapshot_sha256_valid": digest(value["snapshot"]) == value["audit"]["snapshot_sha256"],
        **verify_attachment(value),
        "note": "数据一致性校验，不是外部数字签名",
    }


def rca_attachment(value: Any) -> Any:
    if "rca" not in value:
        raise HTTPException(404, "RCA is not recorded in this historical report version") from None
    return {
        "report_id": value["report_id"],
        "rca": value["rca"],
        "snapshot_sha256": value["audit"]["snapshot_sha256"],
        **verify_attachment(value),
    }


@app.get("/report-versions/{report_id}/rca")
def version_rca(report_id: str) -> Any:
    return rca_attachment(report_version(report_id))


@app.get("/incidents/{incident_id}/topology")
def incident_topology(incident_id: str) -> Any:
    from .reporting import ReportManager

    incident(incident_id)
    value = ReportManager(storage()).latest(incident_id)
    if value is None:
        raise HTTPException(404, "incident has no report snapshot") from None
    result = rca_attachment(value)
    valid = {
        key: result[key]
        for key in ("rca_sha256_valid", "rca_evidence_references_valid", "rca_evidence_set_valid")
    }
    if not all(valid.values()):
        return JSONResponse(
            {
                "incident_id": incident_id,
                "report_id": value["report_id"],
                "detail": "RCA attachment failed integrity verification",
                **valid,
            },
            status_code=409,
        )
    return {
        "incident_id": incident_id,
        "report_id": value["report_id"],
        "graph": result["rca"]["graph"],
        "evidence_set_id": result["rca"]["evidence_set_id"],
        **valid,
        "note": "Observed caller-to-callee edges in this report snapshot; not a complete infrastructure inventory",
    }


@app.get("/report-versions/{report_id}/markdown", response_class=PlainTextResponse)
def version_markdown(report_id: str) -> Any:
    value = report_version(report_id)
    prefix = f"事件：{value['incident_id']} / 版本：{value['version']} / 阶段：{value['stage']}\n\n"
    return prefix + markdown(value)


@app.get("/notifications")
def notifications(incident_id: str | None = None) -> Any:
    from .incidents import IncidentManager

    return IncidentManager(storage()).notifications(incident_id)


@app.get("/alerts")
def alerts(limit: int = Query(100, ge=1, le=500)) -> Any:
    return storage().list_alerts(limit)


@app.get("/reports")
def reports(limit: int = Query(50, ge=1, le=100)) -> Any:
    return storage().list_reports(limit)


@app.get("/reports/{report_id}")
def report(report_id: str) -> Any:
    try:
        return storage().get_report(report_id)
    except KeyError:
        raise HTTPException(404, "report not found") from None


@app.get("/reports/{report_id}/markdown", response_class=PlainTextResponse)
def report_markdown(report_id: str) -> Any:
    return markdown(report(report_id))


@app.get("/reports/{report_id}/verify")
def verify(report_id: str) -> Any:
    r = report(report_id)
    from .evidence_api import verify_report_evidence

    return {
        **verify_report_evidence(storage(), r),
        "report_id": report_id,
        "snapshot_sha256_valid": digest(r["snapshot"]) == r["audit"]["snapshot_sha256"],
        **verify_attachment(r),
        "note": "校验数据一致性；不构成数字签名或外部防篡改证明",
    }


@app.get("/reports/{report_id}/rca")
def legacy_report_rca(report_id: str) -> Any:
    return rca_attachment(report(report_id))


class FaultSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str = Field(default="none", pattern="^(latency|error|timeout|unavailable|none)$")
    duration_seconds: int = Field(default=120, ge=1, le=600)
    latency_ms: int = Field(default=1500, ge=0, le=10000)
    error_rate: float = Field(default=1, ge=0, le=1)


@app.post("/faults/{service}")
def inject(service: str, spec: FaultSpec) -> Any:
    if service not in {"gateway", "order", "inventory", "payment"}:
        raise HTTPException(404, "unknown simulated service") from None
    # Fixed allowlist; never accepts arbitrary URLs or shell commands.
    base = os.getenv(service.upper() + "_URL", f"http://{service}:8000")
    request = urllib.request.Request(
        base.rstrip("/") + "/fault",
        json.dumps(spec.model_dump()).encode(),
        {
            "Content-Type": "application/json",
            "x-simulator-token": os.getenv("SIMULATOR_CONTROL_TOKEN", ""),
        },
    )
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
            request, timeout=5
        ) as response:
            return json.load(response)
    except (urllib.error.URLError, TimeoutError):
        raise HTTPException(502, "simulated service unavailable") from None


app.include_router(evidence_router)


install_features(app)
