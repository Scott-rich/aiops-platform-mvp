"""Crash-safe local detector sidecar; never modifies existing database schema."""

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from filelock import FileLock

from wire_contract import canonical

MAX_STATE_BYTES = 64 * 1024 * 1024


class DetectionStateFile:
    def __init__(self, root: Any, config: Any) -> None:
        self.path = Path(root) / "detection-state-v1.json"
        self.config = config
        self.lock = FileLock(str(self.path) + ".lock", timeout=10)

    def load(self, next_end: Any) -> Any:
        if not self.path.exists():
            return {
                "schema_version": 1,
                "config_id": self.config.fingerprint,
                "next_end": next_end,
                "services": {},
                "pending": [],
            }
        if self.path.stat().st_size > MAX_STATE_BYTES:
            raise ValueError("detector state exceeds safety size limit")
        document = json.loads(self.path.read_text())
        payload = document["payload"]
        digest = hashlib.sha256(canonical(payload).encode()).hexdigest()
        if digest != document.get("sha256"):
            raise ValueError("detector state checksum mismatch; preserve file and investigate")
        if (
            payload.get("schema_version") != 1
            or payload.get("config_id") != self.config.fingerprint
        ):
            raise ValueError(
                "detector config changed; back up and explicitly reset detection-state-v1.json"
            )
        return payload

    def save(self, payload: Any) -> Any:
        body = canonical(
            {"payload": payload, "sha256": hashlib.sha256(canonical(payload).encode()).hexdigest()}
        )
        if len(body.encode()) > MAX_STATE_BYTES:
            raise ValueError("detector state exceeds safety size limit")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=".detection-",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            if os.name == "posix":
                fd = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
