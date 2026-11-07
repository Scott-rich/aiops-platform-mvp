"""ASGI request boundary for the query API and the explicit local ingest API."""

import asyncio
import hmac
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.datastructures import Headers

from .runtime_config import RuntimeConfig
from .runtime_logging import configure_logging

log = logging.getLogger(__name__)
MAX_BODY = 1_000_000


def origin_tuple(value: Any) -> Any:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise ValueError("invalid origin")
    return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


class RequestBoundary:
    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> Any:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request_id = uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        headers = Headers(scope=scope)
        method = scope["method"]
        path = scope["path"]
        status = 500
        auth_path = path[3:] if path.startswith("/v1/") else path
        started = time.monotonic()

        async def respond(code: Any, detail: Any) -> Any:
            await JSONResponse({"detail": detail, "request_id": request_id}, status_code=code)(
                scope, receive, wrapped_send
            )

        async def wrapped_send(message: Any) -> Any:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message = dict(message)
                message["headers"] = [
                    *message.get("headers", []),
                    (b"x-request-id", request_id.encode()),
                    (b"x-content-type-options", b"nosniff"),
                ]
            await send(message)

        try:
            try:
                cfg = RuntimeConfig.from_env()
            except ValueError:
                await respond(503, "invalid_runtime_configuration")
                return
            if cfg.auth_mode == "token" and auth_path != "/live":
                auth = headers.getlist("authorization")
                token = auth[0][7:] if len(auth) == 1 and auth[0].startswith("Bearer ") else ""

                def matches(expected: str) -> bool:
                    return hmac.compare_digest(token.encode(), expected.encode())

                role = (
                    "control"
                    if matches(cfg.control_token)
                    else "read"
                    if matches(cfg.read_token)
                    else "ingest"
                    if matches(cfg.ingest_token)
                    else None
                )
                if role is None:
                    await respond(401, "authentication_required")
                    return
                allowed = (
                    role == "control"
                    or (role == "read" and method in {"GET", "HEAD"})
                    or (role == "ingest" and auth_path == "/demo/ingest" and method == "POST")
                )
                if not allowed:
                    await respond(403, "insufficient_permission")
                    return
            # Browser writes must be same-origin, including local unauthenticated mode.
            if method not in {"GET", "HEAD", "OPTIONS"} and headers.get("origin"):
                try:
                    allowed = origin_tuple(headers["origin"]) == origin_tuple(
                        scope["scheme"] + "://" + headers.get("host", "")
                    )
                except ValueError:
                    allowed = False
                if not allowed:
                    await respond(403, "cross_origin_write_denied")
                    return
            # Fully validate the bounded body before any endpoint can mutate state.
            # Chunked bodies are counted too; a dishonest Content-Length cannot bypass it.
            if method not in {"GET", "HEAD"}:
                try:
                    lengths = headers.getlist("content-length")
                    if len(lengths) > 1 or (lengths and int(lengths[0]) < 0):
                        raise ValueError
                    length = int(lengths[0]) if lengths else 0
                except ValueError:
                    await respond(400, "invalid_content_length")
                    return
                if length > MAX_BODY:
                    await respond(413, "request_too_large")
                    return
                chunks = []
                size = 0
                deadline = time.monotonic() + cfg.body_timeout
                while True:
                    try:
                        message = await asyncio.wait_for(
                            receive(), max(0, deadline - time.monotonic())
                        )
                    except TimeoutError:
                        await respond(408, "request_body_timeout")
                        return
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    size += len(chunk)
                    if size > MAX_BODY:
                        await respond(413, "request_too_large")
                        return
                    chunks.append(chunk)
                    if not message.get("more_body", False):
                        break
                body = b"".join(chunks)
                del chunks
                delivered = False

                async def replay() -> Any:
                    nonlocal delivered
                    if not delivered:
                        delivered = True
                        return {"type": "http.request", "body": body, "more_body": False}
                    return await receive()

                await self.app(scope, replay, wrapped_send)
            else:
                await self.app(scope, receive, wrapped_send)
        finally:
            route = getattr(scope.get("route"), "path", "unmatched")
            log.info(
                "http_request request_id=%s method=%s route=%s status=%s elapsed_ms=%.3f",
                request_id,
                method,
                route,
                status,
                1000 * (time.monotonic() - started),
            )


async def validation_error(request: Any, exc: Any) -> Any:
    # Retain FastAPI's detail list/locations, but omit input and arbitrary ctx objects.
    return JSONResponse(
        {
            "detail": [{"type": e["type"], "loc": e["loc"], "msg": e["msg"]} for e in exc.errors()],
            "request_id": getattr(request.state, "request_id", None),
        },
        status_code=422,
    )


def install_boundary(app: Any) -> Any:
    app.add_middleware(RequestBoundary)
    app.add_exception_handler(RequestValidationError, validation_error)


@asynccontextmanager
async def api_lifespan(app: Any) -> Any:
    cfg = RuntimeConfig.from_env()
    configure_logging(cfg)
    from .storage import Store

    Store()  # Initialize/validate before admitting HTTP traffic; existing backup policy applies.
    log.info("api_started auth_mode=%s", cfg.auth_mode)
    if cfg.auth_mode == "local":
        log.warning("local_mode_requires_trusted_loopback_no_user_authentication")
    try:
        yield
    finally:
        log.info("api_stopped")
