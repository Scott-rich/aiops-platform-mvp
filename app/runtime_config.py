"""Validated process settings. Domain-specific model/detector contracts remain separate."""

import math
import os
import re
from dataclasses import dataclass, field
from typing import Any

ENV_KEYS = frozenset(
    {
        "DB_TIMEOUT_SECONDS",
        "EVIDENCE_OPERATION_TIMEOUT_SECONDS",
        "EVIDENCE_RETENTION_DAYS",
        "FAST_STATE_MAX_KEYS",
        "LOG_LEVEL",
        "LOG_FORMAT",
        "API_AUTH_MODE",
        "API_READ_TOKEN",
        "API_CONTROL_TOKEN",
        "TELEMETRY_INGEST_TOKEN",
        "LOCAL_READY_MAX_AGE_SECONDS",
        "API_BODY_TIMEOUT_SECONDS",
    }
)


def numeric(env: Any, key: Any, default: Any, low: Any, high: Any, integer: Any = False) -> Any:
    try:
        value = int(env.get(key, str(default))) if integer else float(env.get(key, str(default)))
    except (ValueError, TypeError, OverflowError):
        raise ValueError(f"invalid {key}") from None
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(f"invalid {key}")
    return value


@dataclass(frozen=True)
class RuntimeConfig:
    db_timeout: float = 3
    evidence_timeout: float = 30
    retention_days: int = 30
    fast_state_keys: int = 512
    log_level: str = "INFO"
    log_format: str = "json"
    auth_mode: str = "local"
    read_token: str = field(default="", repr=False)
    control_token: str = field(default="", repr=False)
    ingest_token: str = field(default="", repr=False)
    ready_max_age: float = 10
    body_timeout: float = 5

    @classmethod
    def from_env(cls, env: Any = None) -> Any:
        env = os.environ if env is None else env
        mode = env.get("API_AUTH_MODE", "local")
        level, fmt = env.get("LOG_LEVEL", "INFO").upper(), env.get("LOG_FORMAT", "json")
        if mode not in {"local", "token"}:
            raise ValueError("invalid API_AUTH_MODE")
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("invalid LOG_LEVEL")
        if fmt not in {"json", "text"}:
            raise ValueError("invalid LOG_FORMAT")
        keys = ["API_READ_TOKEN", "API_CONTROL_TOKEN", "TELEMETRY_INGEST_TOKEN"]
        tokens = [env.get(k, "") for k in keys]
        for key, token in zip(keys, tokens, strict=False):
            if (mode == "token" and not token) or (
                token and not re.fullmatch(r"[A-Za-z0-9_-]{32,512}", token)
            ):
                raise ValueError(f"invalid {key}; require 32..512 URL-safe characters")
        if mode == "token" and len(set(tokens)) != len(tokens):
            raise ValueError("API tokens must be distinct")
        return cls(
            numeric(env, "DB_TIMEOUT_SECONDS", 3, 0.05, 30),
            numeric(env, "EVIDENCE_OPERATION_TIMEOUT_SECONDS", 30, 1, 120),
            numeric(env, "EVIDENCE_RETENTION_DAYS", 30, 7, 3650, True),
            numeric(env, "FAST_STATE_MAX_KEYS", 512, 1, 4096, True),
            level,
            fmt,
            mode,
            tokens[0],
            tokens[1],
            tokens[2],
            numeric(env, "LOCAL_READY_MAX_AGE_SECONDS", 10, 1, 120),
            numeric(env, "API_BODY_TIMEOUT_SECONDS", 5, 0.05, 30),
        )
