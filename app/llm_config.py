"""Environment-only, bounded settings; repr never prints credentials."""

import math
import os
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit


class ModelError(Exception):
    def __init__(self, code: Any, metadata: Any = None) -> None:
        super().__init__(code)
        self.code = code
        self.metadata = metadata or {}


def redact(value: Any, secret: Any = None) -> Any:
    secret = os.getenv("LLM_API_KEY", "") if secret is None else secret
    if isinstance(value, str):
        return value.replace(secret, "[REDACTED]") if secret else value
    if isinstance(value, list):
        return [redact(v, secret) for v in value]
    if isinstance(value, dict):
        return {
            k: (
                "[REDACTED]"
                if str(k).lower() in {"authorization", "api_key", "password"}
                else redact(v, secret)
            )
            for k, v in value.items()
        }
    return value


@dataclass(frozen=True)
class LLMConfig:
    base_url: str = ""
    model: str = ""
    api_key: str = field(default="", repr=False)
    timeout_s: float = 20
    total_timeout_s: float = 30
    max_attempts: int = 2
    max_calls_per_incident: int = 2
    requests_per_minute: int = 10
    retry_backoff_s: float = 0.5
    max_response_bytes: int = 1_000_000
    max_context_records: int = 48
    max_context_bytes: int = 64_000
    max_tokens: int = 1500
    response_format: str = "none"
    output_protocol: str = "analysis-v2"
    trust_env: bool = False
    input_price_per_million: float | None = None
    output_price_per_million: float | None = None
    cost_currency: str = "USD"

    @property
    def configured(self) -> Any:
        return bool(self.base_url and self.model)

    @classmethod
    def from_env(cls, environ: Any = None) -> Any:
        env = os.environ if environ is None else environ

        def numeric(
            name: Any, default: Any, minimum: Any, maximum: Any, integer: Any = False
        ) -> Any:
            raw = env.get(name, str(default))
            try:
                number = int(raw) if integer else float(raw)
            except (ValueError, TypeError, OverflowError) as exc:
                raise ModelError("invalid_configuration", {"configuration_field": name}) from exc
            if not math.isfinite(number) or not minimum <= number <= maximum:
                raise ModelError("invalid_configuration", {"configuration_field": name})
            return number

        def price(name: Any) -> Any:
            return numeric(name, 0, 0, 100000) if env.get(name, "").strip() else None

        base, model = (
            env.get("LLM_BASE_URL", "").strip().rstrip("/"),
            env.get("LLM_MODEL", "").strip(),
        )
        try:
            parsed = urlsplit(base)
            _ = parsed.port  # Validate malformed or out-of-range port.
        except ValueError as exc:
            raise ModelError(
                "invalid_configuration", {"configuration_field": "LLM_BASE_URL"}
            ) from exc
        if base and (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ModelError("invalid_configuration", {"configuration_field": "LLM_BASE_URL"})
        if len(model) > 256:
            raise ModelError("invalid_configuration", {"configuration_field": "LLM_MODEL"})
        key = env.get("LLM_API_KEY", "")
        if len(key) > 8192 or any(ord(c) < 32 or ord(c) == 127 for c in key):
            raise ModelError("invalid_configuration", {"configuration_field": "LLM_API_KEY"})
        response_format = env.get("LLM_RESPONSE_FORMAT", "none").strip()
        if response_format not in {"none", "json_object", "json_schema"}:
            raise ModelError(
                "invalid_configuration", {"configuration_field": "LLM_RESPONSE_FORMAT"}
            )
        output_protocol = env.get("LLM_OUTPUT_PROTOCOL", "analysis-v2").strip()
        if output_protocol not in {"analysis-v2", "evidence-selection-v1"}:
            raise ModelError(
                "invalid_configuration", {"configuration_field": "LLM_OUTPUT_PROTOCOL"}
            )
        trust_env = env.get("LLM_TRUST_ENV", "false").lower()
        if trust_env not in {"false", "true"}:
            raise ModelError("invalid_configuration", {"configuration_field": "LLM_TRUST_ENV"})
        currency = env.get("LLM_COST_CURRENCY", "USD")
        if len(currency) > 8 or not currency.isalpha():
            raise ModelError("invalid_configuration", {"configuration_field": "LLM_COST_CURRENCY"})
        return cls(
            base_url=base,
            model=model,
            api_key=key,
            timeout_s=numeric("LLM_TIMEOUT_SECONDS", 20, 0.05, 600),
            total_timeout_s=numeric("LLM_TOTAL_TIMEOUT_SECONDS", 30, 0.05, 600),
            max_attempts=numeric("LLM_MAX_ATTEMPTS", 2, 1, 3, True),
            max_calls_per_incident=numeric("LLM_MAX_CALLS_PER_INCIDENT", 2, 1, 5, True),
            requests_per_minute=numeric("LLM_REQUESTS_PER_MINUTE", 10, 1, 1000, True),
            retry_backoff_s=numeric("LLM_RETRY_BACKOFF_SECONDS", 0.5, 0, 5),
            max_response_bytes=numeric("LLM_MAX_RESPONSE_BYTES", 1_000_000, 256, 2_000_000, True),
            max_context_records=numeric("LLM_MAX_CONTEXT_RECORDS", 48, 1, 200, True),
            max_context_bytes=numeric("LLM_MAX_CONTEXT_BYTES", 64000, 4096, 262144, True),
            max_tokens=numeric("LLM_MAX_TOKENS", 1500, 64, 4096, True),
            response_format=response_format,
            output_protocol=output_protocol,
            trust_env=trust_env == "true",
            input_price_per_million=price("LLM_INPUT_PRICE_PER_MILLION"),
            output_price_per_million=price("LLM_OUTPUT_PRICE_PER_MILLION"),
            cost_currency=currency.upper(),
        )


ENV_KEYS = frozenset(
    {
        "LLM_BASE_URL",
        "LLM_MODEL",
        "LLM_API_KEY",
        "LLM_TIMEOUT_SECONDS",
        "LLM_TOTAL_TIMEOUT_SECONDS",
        "LLM_MAX_ATTEMPTS",
        "LLM_MAX_CALLS_PER_INCIDENT",
        "LLM_REQUESTS_PER_MINUTE",
        "LLM_RETRY_BACKOFF_SECONDS",
        "LLM_MAX_RESPONSE_BYTES",
        "LLM_MAX_CONTEXT_RECORDS",
        "LLM_MAX_CONTEXT_BYTES",
        "LLM_MAX_TOKENS",
        "LLM_RESPONSE_FORMAT",
        "LLM_TRUST_ENV",
        "LLM_INPUT_PRICE_PER_MILLION",
        "LLM_OUTPUT_PRICE_PER_MILLION",
        "LLM_COST_CURRENCY",
        "LLM_OUTPUT_PROTOCOL",
    }
)
