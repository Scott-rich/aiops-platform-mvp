"""Bounded OpenAI-compatible HTTP transport, with real async wall-clock timeout."""

import asyncio
import hashlib
import json
import logging
import math
import threading
import time
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import httpx

from .analysis_schema import json_schema
from .contracts import canonical, now_ms
from .llm_config import LLMConfig, ModelError, redact

log = logging.getLogger(__name__)
_SESSION = ContextVar("model_session", default=None)
_RATE_LOCK = threading.Lock()
_RATE_TIMES: deque[float] = deque(maxlen=1000)


@contextmanager
def model_scope(session: Any) -> Any:
    token = _SESSION.set(session)
    try:
        yield
    finally:
        _SESSION.reset(token)


def _strict_json(raw: Any) -> Any:
    def invalid(_: Any) -> Any:
        raise ValueError("non-finite JSON")

    def pairs(values: Any) -> Any:
        obj = {}
        for key, value in values:
            if key in obj:
                raise ValueError("duplicate JSON key")
            obj[key] = value
        return obj

    value = json.loads(raw, parse_constant=invalid, object_pairs_hook=pairs)
    canonical(value).encode("utf-8")
    return value


def _reserve_local(config: Any) -> Any:
    with _RATE_LOCK:
        now = time.monotonic()
        while _RATE_TIMES and _RATE_TIMES[0] <= now - 60:
            _RATE_TIMES.popleft()
        if len(_RATE_TIMES) >= config.requests_per_minute:
            raise ModelError("local_rate_limited")
        _RATE_TIMES.append(now)


def _retry_delay(header: Any, config: Any) -> Any:
    try:
        value = float(header) if header else config.retry_backoff_s
    except (TypeError, ValueError):
        value = config.retry_backoff_s
    return min(5, max(0, value)) if math.isfinite(value) else config.retry_backoff_s


def _usage_and_cost(completion: Any, config: Any) -> Any:
    raw = completion.get("usage")
    usage = None
    if isinstance(raw, dict):
        fields = ("prompt_tokens", "completion_tokens", "total_tokens")
        if (
            all(type(raw.get(k)) is int and 0 <= raw[k] <= 1_000_000_000 for k in fields)
            and raw["total_tokens"] >= raw["prompt_tokens"] + raw["completion_tokens"]
        ):
            usage = {k: raw[k] for k in fields}
    cost = None
    if (
        usage is not None
        and config.input_price_per_million is not None
        and config.output_price_per_million is not None
    ):
        cost = {
            "amount": round(
                (
                    usage["prompt_tokens"] * config.input_price_per_million
                    + usage["completion_tokens"] * config.output_price_per_million
                )
                / 1_000_000,
                10,
            ),
            "currency": config.cost_currency,
            "estimated": True,
            "pricing_source": "configured_prices_per_million_tokens",
        }
    return usage, cost


def constrained_schema(payload: Any) -> Any:
    schema = json_schema()
    ids = sorted({e["event_id"] for e in payload.get("records", [])})
    services = sorted({e["service"] for e in payload.get("records", [])})
    properties = schema["$defs"]["Hypothesis"]["properties"]
    if ids:
        for key in ("supporting_evidence_ids", "against_evidence_ids"):
            properties[key]["items"]["enum"] = ids
    if services:
        properties["service"]["enum"] = services
        schema["properties"]["involved_services"]["items"]["enum"] = services
    return schema


def _audit_totals(metadata: Any, config: Any, started: Any) -> Any:
    known = [a["usage"] for a in metadata["attempts"] if a.get("usage") is not None]
    missing = len(metadata["attempts"]) - len(known)
    total = {
        key: sum(row[key] for row in known)
        for key in ("prompt_tokens", "completion_tokens", "total_tokens")
    }
    metadata.update(
        elapsed_ms=round((time.monotonic() - started) * 1000, 3),
        reported_usage_totals=total,
        attempts_without_usage=missing,
        cost_coverage_complete=not missing and bool(known),
    )
    # Unknown/missing usage on any billable attempt keeps total cost unknown.
    _, estimate = _usage_and_cost({"usage": total}, config)
    metadata["estimated_cost_for_reported_usage_only"] = estimate if known else None
    metadata["estimated_cost"] = estimate if known and not missing else None


async def _complete(payload: Any, system: Any, config: Any, session: Any) -> Any:
    started = time.monotonic()
    attempts: list[dict[str, Any]] = []
    safe_payload = redact(payload, config.api_key)
    protocol = config.output_protocol
    response_schema = None
    if protocol == "evidence-selection-v1":
        from .model_grounding import SELECTION_SYSTEM, selection_payload, selection_schema

        response_schema = selection_schema(safe_payload)
        safe_payload = selection_payload(safe_payload)
        system = SELECTION_SYSTEM
    if len(canonical(safe_payload).encode()) > config.max_context_bytes:
        raise ModelError("context_too_large", {"attempts": [], "elapsed_ms": 0})
    body = {
        "model": config.model,
        "temperature": 0,
        "max_tokens": config.max_tokens,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": canonical(safe_payload)},
        ],
    }
    if config.response_format == "json_object":
        body["response_format"] = {"type": "json_object"}
    elif config.response_format == "json_schema":
        body["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "aiops_evidence_selection_v1" if response_schema else "aiops_diagnosis_v2",
                "strict": True,
                "schema": response_schema or constrained_schema(safe_payload),
            },
        }
    metadata = {
        "requested_model": config.model,
        "prompt_version": payload.get("prompt_version"),
        "evidence_set_id": payload.get("evidence_set", {}).get("id"),
        "input_sha256": hashlib.sha256(
            canonical({"system": system, "payload": safe_payload}).encode()
        ).hexdigest(),
        "base_url_sha256": hashlib.sha256(config.base_url.encode()).hexdigest(),
        "attempts": attempts,
        "request_body": body,
        "usage": None,
        "estimated_cost": None,
        "output_protocol": protocol,
        "selection_prompt_version": safe_payload.get("selection_prompt_version"),
        "cost_note": "provider-reported token usage; estimates use configured prices, exclude hardware cost, and are not billing receipts",
    }
    headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
    if config.api_key:
        headers["Authorization"] = "Bearer " + config.api_key
    try:
        async with asyncio.timeout(config.total_timeout_s):
            async with httpx.AsyncClient(
                timeout=config.timeout_s,
                trust_env=config.trust_env,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=1),
            ) as client:
                maximum = min(config.max_attempts, config.max_calls_per_incident)
                for number in range(1, maximum + 1):
                    if session:
                        ordinal = await asyncio.to_thread(session.reserve)
                    else:
                        _reserve_local(config)
                        ordinal = number
                    mark = time.monotonic()
                    item = {
                        "ordinal": ordinal,
                        "started_at": now_ms(),
                        "http_status": None,
                        "outcome": "total_timeout",
                        "requested_model": config.model,
                        "evidence_set_id": metadata["evidence_set_id"],
                        "prompt_version": metadata["prompt_version"],
                        "input_sha256": metadata["input_sha256"],
                    }
                    error, retry, delay, completion, output = (
                        None,
                        False,
                        config.retry_backoff_s,
                        None,
                        None,
                    )
                    try:
                        async with client.stream(
                            "POST",
                            config.base_url + "/chat/completions",
                            json=body,
                            headers=headers,
                        ) as response:
                            item["http_status"] = response.status_code
                            if response.status_code != 200:
                                code = (
                                    "provider_rate_limited"
                                    if response.status_code == 429
                                    else "provider_http_error"
                                )
                                error = ModelError(code)
                                retry = response.status_code in {429, 502, 503, 504}
                                delay = _retry_delay(response.headers.get("Retry-After"), config)
                            else:
                                if response.headers.get(
                                    "Content-Encoding", "identity"
                                ).lower() not in {"", "identity"}:
                                    raise ModelError("unsupported_response_encoding")
                                raw = bytearray()
                                async for block in response.aiter_raw():
                                    raw.extend(block)
                                    if len(raw) > config.max_response_bytes:
                                        raise ModelError("response_too_large")
                                try:
                                    completion = _strict_json(bytes(raw))
                                    item["usage"], item["estimated_cost"] = _usage_and_cost(
                                        completion, config
                                    )
                                    choice = completion["choices"][0]
                                    output = choice["message"].get("content")
                                    metadata.update(
                                        returned_model=str(completion.get("model", ""))[:256],
                                        response_id=str(completion.get("id", ""))[:256],
                                        usage=item["usage"],
                                        raw_output=output if isinstance(output, str) else None,
                                        finish_reason=str(choice.get("finish_reason", ""))[:32],
                                    )
                                    if choice.get("finish_reason") == "length":
                                        raise ModelError("output_truncated")
                                    if choice["message"].get("refusal"):
                                        raise ModelError("model_refusal")
                                    if not isinstance(output, str):
                                        raise ValueError("non-text completion")
                                    candidate = _strict_json(output)
                                    if not isinstance(candidate, dict):
                                        raise ValueError("non-object diagnosis")
                                except ModelError:
                                    raise
                                except (
                                    ValueError,
                                    TypeError,
                                    KeyError,
                                    IndexError,
                                    AttributeError,
                                    RecursionError,
                                ) as exc:
                                    raise ModelError("invalid_model_json") from exc
                    except httpx.TimeoutException:
                        error = ModelError(
                            "request_timeout"
                        )  # Outcome may be billable; default no retry.
                    except (httpx.NetworkError, httpx.RemoteProtocolError):
                        error, retry = ModelError("network_error"), True
                    except httpx.HTTPError:
                        error = ModelError("http_client_error")
                    except ModelError as exc:
                        error = exc
                    finally:
                        item.update(
                            duration_ms=round((time.monotonic() - mark) * 1000, 3),
                            outcome=(error.code if error else "transport_success")
                            if output is not None or error
                            else "total_timeout",
                        )
                        attempts.append(item)
                        if session:
                            await asyncio.shield(
                                asyncio.to_thread(
                                    session.finish_attempt, ordinal, item["outcome"], item
                                )
                            )
                    if error:
                        if (
                            retry
                            and number < maximum
                            and time.monotonic() - started + delay < config.total_timeout_s
                        ):
                            await asyncio.sleep(delay)
                            continue
                        raise error
                    if completion is None:
                        raise ModelError("invalid_model_json")
                    usage, cost = _usage_and_cost(completion, config)
                    metadata.update(
                        returned_model=str(completion.get("model", ""))[:256],
                        response_id=str(completion.get("id", ""))[:256],
                        raw_output=output,
                        usage=usage,
                        estimated_cost=cost,
                    )
                    result = redact(candidate, config.api_key)
                    _audit_totals(metadata, config, started)
                    log.info(
                        "model_transport_success attempts=%d elapsed_ms=%s",
                        len(attempts),
                        metadata["elapsed_ms"],
                    )
                    return result, redact(metadata, config.api_key)
    except TimeoutError as exc:
        _audit_totals(metadata, config, started)
        metadata.update(failure_code="total_timeout")
        raise ModelError("total_timeout", redact(metadata, config.api_key)) from exc
    except ModelError as exc:
        _audit_totals(metadata, config, started)
        metadata.update(failure_code=exc.code)
        raise ModelError(exc.code, redact(metadata, config.api_key)) from exc
    raise ModelError("attempt_budget_exhausted", redact(metadata, config.api_key))


def complete(payload: Any, system: Any) -> Any:
    config = LLMConfig.from_env()
    if not config.configured:
        raise ModelError("configuration_missing", {"attempts": [], "elapsed_ms": 0})
    try:
        return asyncio.run(_complete(payload, system, config, _SESSION.get()))
    except ModelError as exc:
        log.warning(
            "model_transport_failure code=%s attempts=%d",
            exc.code,
            len(exc.metadata.get("attempts", [])),
        )
        raise
