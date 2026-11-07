"""JSON Kafka telemetry with an explicit in-memory mode for tests."""

from collections import deque
import json
import logging
import os
from threading import Lock
from typing import Any
import urllib.request

LOG = logging.getLogger(__name__)


class Telemetry:
    def __init__(self, bootstrap_servers: str | None = None,
                 mode: str | None = None):
        self.mode = mode or os.getenv("TELEMETRY_MODE", "kafka")
        if self.mode not in {"kafka", "memory", "http"}:
            raise ValueError("TELEMETRY_MODE must be kafka, memory, or http")
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
        self.http_url = os.getenv("TELEMETRY_HTTP_URL", "http://127.0.0.1:18100/demo/ingest")
        self.ingest_token = os.getenv("TELEMETRY_INGEST_TOKEN", "")
        self._producer = None
        self._lock = Lock()
        self.events: deque[tuple[str, dict[str, Any]]] = deque(maxlen=20000)
        self.sent = 0
        self.failed = 0

    def start(self) -> None:
        if self.mode in {"memory", "http"}:
            return
        from kafka import KafkaProducer
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, allow_nan=False).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all", retries=5, linger_ms=5,
            max_in_flight_requests_per_connection=1,
            max_block_ms=5000, request_timeout_ms=10000,
        )

    def publish(self, topic: str, value: dict[str, Any]) -> None:
        if self.mode == "memory":
            self.events.append((topic, dict(value)))
            self.sent += 1
            return
        if self.mode == "http":
            try:
                body = json.dumps({"topic": topic, "event": value},
                                  ensure_ascii=False, allow_nan=False).encode("utf-8")
                request = urllib.request.Request(
                    self.http_url, data=body,
                    headers={"Content-Type": "application/json", **({"Authorization":"Bearer "+self.ingest_token} if self.ingest_token else {})}, method="POST")
                with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=3) as response:
                    if not 200 <= response.status < 300:
                        raise RuntimeError("HTTP telemetry endpoint rejected event")
                self._delivered(None)
            except Exception as exc:
                self._error(exc)
            return
        if self._producer is None:
            self._error(RuntimeError("telemetry producer is not started"))
            return
        try:
            self._producer.send(topic, key=value["service"], value=value).add_callback(
                self._delivered).add_errback(self._error)
        except Exception as exc:
            self._error(exc)

    def _delivered(self, _metadata) -> None:
        with self._lock:
            self.sent += 1

    def _error(self, exc: Exception) -> None:
        with self._lock:
            self.failed += 1
        LOG.error("telemetry delivery failed: %s", type(exc).__name__)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=10)
            self._producer.close(timeout=5)
