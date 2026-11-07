"""Bounded API metrics, per-IP/route token buckets and compatible v1 aliases."""

from __future__ import annotations

import math
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.routing import APIRoute
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.middleware import Middleware
from starlette.routing import BaseRoute, Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send


@dataclass(frozen=True)
class RateConfig:
    rate: float = 2000.0
    burst: int = 10000
    max_keys: int = 10000

    @classmethod
    def from_env(cls) -> RateConfig:
        value = cls(
            float(os.getenv("API_RATE_PER_SECOND", "2000")),
            int(os.getenv("API_RATE_BURST", "10000")),
            int(os.getenv("API_RATE_MAX_KEYS", "10000")),
        )
        if not math.isfinite(value.rate) or not 0 < value.rate <= 1e6:
            raise ValueError("invalid API_RATE_PER_SECOND")
        if not 1 <= value.burst <= 1000000 or not 1 <= value.max_keys <= 100000:
            raise ValueError("invalid rate bucket capacity")
        return value


class TokenBuckets:
    def __init__(self, config: RateConfig) -> None:
        self.config = config
        self.entries: OrderedDict[tuple[str, str], tuple[float, float]] = OrderedDict()
        self.lock = threading.Lock()

    def allow(self, key: tuple[str, str], now: float | None = None) -> bool:
        clock = time.monotonic() if now is None else now
        with self.lock:
            tokens, last = self.entries.pop(key, (float(self.config.burst), clock))
            tokens = min(
                float(self.config.burst), tokens + max(0.0, clock - last) * self.config.rate
            )
            allowed = tokens >= 1
            self.entries[key] = (tokens - 1 if allowed else tokens, max(last, clock))
            while len(self.entries) > self.config.max_keys:
                self.entries.popitem(last=False)
            return allowed


class Monitor:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "aiops_http_requests_total",
            "API requests",
            ["method", "route", "status"],
            registry=self.registry,
        )
        self.latency = Histogram(
            "aiops_http_request_seconds",
            "API request duration",
            ["method", "route"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
            registry=self.registry,
        )
        self.events = Gauge(
            "aiops_events_stored",
            "Current persisted events (not lifetime ingest counter)",
            registry=self.registry,
        )
        self.alerts = Gauge(
            "aiops_alerts_stored", "Current persisted alerts", registry=self.registry
        )
        self.db_bytes = Gauge(
            "aiops_database_bytes", "SQLite database plus WAL/SHM bytes", registry=self.registry
        )
        self.limited = Counter(
            "aiops_rate_limited_total", "Rejected requests", registry=self.registry
        )
        self.buckets = TokenBuckets(RateConfig.from_env())


def route_label(scope: Scope, routes: list[BaseRoute]) -> str:
    for route in routes:
        match, _ = route.matches(scope)
        if match == Match.FULL and isinstance(route, APIRoute):
            return route.path.removeprefix("/v1") or "/"
    return "unmatched"


class RequestPolicy:
    """Runs after authentication/body checks; cannot bypass security headers or audits."""

    def __init__(self, app: ASGIApp, monitor: Monitor, routes: list[BaseRoute]) -> None:
        self.app, self.monitor, self.routes = app, monitor, routes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        route_name = route_label(scope, self.routes)
        method = scope["method"]
        client = scope.get("client")
        ip = str(client[0]) if client else "unknown"
        if route_name != "/live" and not self.monitor.buckets.allow((ip, route_name)):
            self.monitor.limited.inc()
            retry = str(max(1, math.ceil(1 / self.monitor.buckets.config.rate)))
            await JSONResponse(
                {"detail": "rate_limit_exceeded", "code": "RATE_LIMITED"},
                status_code=429,
                headers={"Retry-After": retry},
            )(scope, receive, send)
            return
        # Opt-in for legacy GET/HEAD redirects; writes retain method/body and compatibility.
        if (
            os.getenv("API_LEGACY_GET_REDIRECT", "0") == "1"
            and method in {"GET", "HEAD"}
            and not scope["path"].startswith("/v1/")
            and route_name not in {"/", "/live", "/metrics", "unmatched"}
        ):
            query = scope.get("query_string", b"").decode("ascii")
            location = "/v1" + scope["path"] + (("?" + query) if query else "")
            await RedirectResponse(location, status_code=301)(scope, receive, send)
            return
        await self.app(scope, receive, send)


class Instrumentation:
    def __init__(self, app: ASGIApp, monitor: Monitor, routes: list[BaseRoute]) -> None:
        self.app, self.monitor, self.routes = app, monitor, routes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        route_name = route_label(scope, self.routes)
        method = (
            scope["method"]
            if scope["method"] in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
            else "OTHER"
        )
        started, status = time.monotonic(), 500

        async def observed(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = list(message.get("headers", []))
                if not scope["path"].startswith("/v1/") and route_name not in {
                    "/",
                    "/live",
                    "/metrics",
                    "unmatched",
                }:
                    headers += [
                        (b"deprecation", b"true"),
                        (b"sunset", b"Thu, 04 Feb 2027 00:00:00 GMT"),
                    ]
                if status >= 400:
                    headers.append((b"x-aiops-error-code", f"http_{status}".encode()))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, observed)
        finally:
            self.monitor.requests.labels(method, route_name, str(status)).inc()
            self.monitor.latency.labels(method, route_name).observe(time.monotonic() - started)


def metrics(request: Request) -> Response:
    from .storage import Store

    monitor: Monitor = request.app.state.monitor
    store = Store()
    stats = store.stats()
    monitor.events.set(stats.get("events", 0))
    monitor.alerts.set(stats.get("alerts", 0))
    size = 0
    for suffix in ("", "-wal", "-shm"):
        try:
            size += (store.root / ("aiops.sqlite3" + suffix)).stat().st_size
        except FileNotFoundError:
            # Checkpointing can remove WAL/SHM between directory lookup and stat.
            continue
    monitor.db_bytes.set(size)
    return Response(
        generate_latest(monitor.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
    )


def install_features(app: FastAPI) -> None:
    """Install once after all routes, including local ingestion, have been registered."""
    monitor = Monitor()
    app.state.monitor = monitor
    if not any(getattr(r, "path", None) == "/metrics" for r in app.routes):
        app.add_api_route("/metrics", metrics, methods=["GET"], include_in_schema=True)
    existing = {getattr(route, "path", "") for route in app.routes}
    for route in list(app.routes):
        if not isinstance(route, APIRoute) or route.path == "/" or route.path.startswith("/v1/"):
            continue
        if "/v1" + route.path in existing:
            continue
        app.add_api_route(
            "/v1" + route.path,
            route.endpoint,
            methods=list(route.methods),
            response_model=route.response_model,
            status_code=route.status_code,
            response_class=route.response_class,
            dependencies=route.dependencies,
            name="v1_" + route.name,
            tags=["v1"],
        )
    # Installed before startup: append places policy inside the existing security boundary.
    app.user_middleware.append(Middleware(RequestPolicy, monitor=monitor, routes=app.router.routes))
    app.add_middleware(Instrumentation, monitor=monitor, routes=app.router.routes)
