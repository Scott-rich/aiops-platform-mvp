"""Process logging: bounded messages, known-secret redaction, no exception values or locals."""

import json
import logging
import os
import re
from datetime import UTC, datetime
from typing import Any

SECRET_NAME = re.compile(
    r"(?:^|_)(?:SECRET|PASSWORD|PASSWD|TOKEN|CREDENTIALS?|PRIVATE_KEY|API_KEY)(?:_|$)", re.I
)


def redact_log(text: Any, env: Any = None) -> Any:
    env = os.environ if env is None else env
    # Redact before truncation; truncating first could expose a partial credential.
    for secret in sorted(
        {v for k, v in env.items() if v and SECRET_NAME.search(k)}, key=len, reverse=True
    ):
        text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9_.~+/=-]+", r"\1[REDACTED]", text)
    text = re.sub(r"(https?://)[^\s/@]+:[^\s/@]+@", r"\1[REDACTED]@", text)
    text = re.sub(r"(https?://[^\s?]+)\?[^\s]*", r"\1?[REDACTED]", text)
    return text[:8192]


class SafeFormatter(logging.Formatter):
    def __init__(self, style: Any = "json") -> None:
        super().__init__()
        self.style = style

    def format(self, record: Any) -> Any:
        message = redact_log(record.getMessage())
        value = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        if record.exc_info:
            value["error_type"] = record.exc_info[0].__name__
            # File/line/function only: no raw exception message, source text or locals.
            trace = record.exc_info[2]
            frames: list[dict[str, Any]] = []
            while trace is not None and len(frames) < 16:
                frames.append(
                    {
                        "file": os.path.basename(trace.tb_frame.f_code.co_filename),
                        "line": trace.tb_lineno,
                        "function": trace.tb_frame.f_code.co_name,
                    }
                )
                trace = trace.tb_next
            value["frames"] = frames
        if self.style == "json":
            return json.dumps(value, ensure_ascii=False)
        return (
            f"{value['timestamp']} {value['level']} {value['logger']} {json.dumps(message, ensure_ascii=False)}"
            + (f" error_type={value['error_type']}" if "error_type" in value else "")
        )


def configure_logging(config: Any) -> Any:
    handler = logging.StreamHandler()
    handler.setFormatter(SafeFormatter(config.log_format))
    logging.basicConfig(level=config.log_level, handlers=[handler], force=True)
    for name in ("uvicorn", "uvicorn.error"):
        log = logging.getLogger(name)
        log.handlers.clear()
        log.propagate = True
    # Raw access lines contain query strings. Middleware emits route-template logs instead.
    log = logging.getLogger("uvicorn.access")
    log.handlers.clear()
    log.propagate = False
    log.disabled = True
    logging.getLogger("httpx").setLevel(logging.WARNING)
