"""Persist a poll before committing offsets; failed batches are replayed on restart."""

import json
import logging
import os
import signal
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .contracts import canonical, now_ms, validate_alert, validate_event
from .storage import Store

log = logging.getLogger(__name__)
TOPICS = ("telemetry.metrics", "telemetry.logs", "telemetry.spans", "ops.alerts", "ops.deadletter")
EXPECTED = {
    "telemetry.metrics": ("request", "host"),
    "telemetry.logs": ("log",),
    "telemetry.spans": ("span",),
}


def decode_record(topic: Any, value: Any) -> Any:
    if not isinstance(value, bytes) or len(value) > 1_000_000:
        raise ValueError("tombstone or event larger than 1MB")
    event = json.loads(value)
    if not isinstance(event, dict):
        raise ValueError("record must be a JSON object")
    if topic == "ops.alerts":
        validate_alert(event)
    elif topic == "ops.deadletter":
        if not isinstance(event.get("reason"), str) or not event["reason"]:
            raise ValueError("deadletter must have a reason")
    else:
        if event.get("event_type") not in EXPECTED.get(topic, ()):
            raise ValueError("event type does not match topic")
        validate_event(event)
    # json.loads accepts NaN and escaped lone surrogates. Reject them while the
    # caller can still quarantine this record, before any persistence operation.
    canonical(event).encode("utf-8")
    return event


def process_record(store: Any, topic: Any, value: Any, source: Any) -> Any:
    """Compatibility helper used by the original offline tests."""
    event = decode_record(topic, value)
    if topic == "ops.alerts":
        store.alert(event, source)
    elif topic == "ops.deadletter":
        store.deadletter(event["reason"], canonical(event), source)
    else:
        store.ingest(event, source)


def persist_batch(pipeline: Any, batches: Any) -> Any:
    events, alerts = [], []
    rejected = 0
    for records in batches.values():
        for record in records:
            source = {
                "transport": "kafka",
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "kafka_timestamp": record.timestamp,
            }
            try:
                event = decode_record(record.topic, record.value)
            except (
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                OverflowError,
                RecursionError,
            ) as exc:
                raw = (
                    record.value.decode("utf-8", errors="replace")
                    if isinstance(record.value, bytes)
                    else repr(record.value)
                )
                pipeline.store.deadletter(str(exc), raw, source)
                log.warning(
                    "Rejected event topic=%s partition=%s offset=%s reason=%s",
                    record.topic,
                    record.partition,
                    record.offset,
                    type(exc).__name__,
                )
                rejected += 1
                continue
            if record.topic == "ops.alerts":
                alerts.append((event, source))
            elif record.topic == "ops.deadletter":
                # Retain detector envelope and original Kafka coordinates, plus this DLQ offset.
                pipeline.store.deadletter(event["reason"], canonical(event), source)
                rejected += 1
            else:
                events.append((event, source))
    if events or alerts:
        pipeline.ingest_batch(events, alerts)
    return {
        "telemetry_received": len(events),
        "alerts_received": len(alerts),
        "deadletters_received": rejected,
    }


def write_health(path: Any, state: Any) -> Any:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(canonical({**state, "updated_at": now_ms()}), encoding="utf-8")
    temporary.replace(path)


def check_health(path: Any, max_age_ms: Any = 30_000) -> Any:
    try:
        state = json.loads(Path(path).read_text(encoding="utf-8"))
        return (
            isinstance(state, dict)
            and state.get("status") == "running"
            and 0 <= now_ms() - state["updated_at"] <= max_age_ms
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


def retryable_storage(error: Any) -> Any:
    # Corruption, schema and programming failures are not retried.
    code = getattr(error, "sqlite_errorcode", None)
    if code is not None:
        return code & 255 in {
            sqlite3.SQLITE_BUSY,
            sqlite3.SQLITE_LOCKED,
            sqlite3.SQLITE_CANTOPEN,
            sqlite3.SQLITE_IOERR,
        }
    return isinstance(error, sqlite3.OperationalError) and any(
        word in str(error).lower() for word in ("locked", "busy", "access lock unavailable")
    )


def storage_operation(
    operation: Any, stop: Any, health_path: Any, counters: Any, attempts: Any
) -> Any:
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            counters["storage_failures"] += 1
            write_health(
                health_path,
                {
                    "status": "degraded",
                    **counters,
                    "error_type": type(exc).__name__,
                    "attempt": attempt,
                },
            )
            log.warning(
                "storage_operation_failed attempt=%d/%d type=%s",
                attempt,
                attempts,
                type(exc).__name__,
            )
            if not retryable_storage(exc) or attempt == attempts:
                raise
            if stop.wait(min(0.2 * 2 ** (attempt - 1), 2)):
                raise RuntimeError("stopped before storage operation completed") from exc


def run_consumer(
    consumer: Any,
    pipeline: Any,
    stop: Any,
    health_path: Any,
    max_failures: Any = 5,
    storage_attempts: Any = 3,
    commit_timeout_ms: Any = 15_000,
) -> Any:
    counters = {
        "telemetry_received": 0,
        "alerts_received": 0,
        "deadletters_received": 0,
        "committed_polls": 0,
        "poll_failures": 0,
        "storage_failures": 0,
        "probe_failures": 0,
    }
    failures = 0
    next_broker_check = 0.0
    while not stop.is_set():
        try:
            batches = consumer.poll(timeout_ms=500)
            if not batches and time.monotonic() >= next_broker_check:
                # In pinned kafka-python 2.2.12 topics() retries metadata
                # without a total deadline. end_offsets uses a Timer bounded
                # by request_timeout_ms, including metadata refresh retries.
                from kafka import TopicPartition

                partitions = consumer.assignment() or [TopicPartition(TOPICS[0], 0)]
                try:
                    consumer.end_offsets(partitions)
                except Exception:
                    counters["probe_failures"] += 1
                    raise
                next_broker_check = time.monotonic() + 5
        except Exception as exc:
            failures += 1
            counters["poll_failures"] += 1
            write_health(
                health_path, {"status": "degraded", **counters, "error_type": type(exc).__name__}
            )
            log.warning(
                "Kafka poll failed attempt=%d/%d type=%s",
                failures,
                max_failures,
                type(exc).__name__,
            )
            if failures >= max_failures:
                raise RuntimeError("Kafka poll failure budget exhausted") from exc
            if stop.wait(min(2 ** (failures - 1), 10)):
                return
            continue
        failures = 0
        if batches:
            values = storage_operation(
                lambda batches=batches: persist_batch(pipeline, batches),
                stop,
                health_path,
                counters,
                storage_attempts,
            )
            # Database/commit failures escape: do not poll later batches or advance offsets.
            consumer.commit(timeout_ms=commit_timeout_ms)
            counters["committed_polls"] += 1
            for key, value in values.items():
                counters[key] += value
        storage_operation(pipeline.tick, stop, health_path, counters, storage_attempts)
        write_health(health_path, {"status": "running", **counters})


def positive_env(name: Any, default: Any, maximum: Any) -> Any:
    value = int(os.getenv(name, str(default)))
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be in 1..{maximum}")
    return value


def main() -> Any:
    from kafka import KafkaConsumer

    from .engine import Pipeline
    from .runtime_config import RuntimeConfig
    from .runtime_logging import configure_logging

    configure_logging(RuntimeConfig.from_env())
    store = Store()
    health_path = store.root / "worker-health.json"
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    consumer = pipeline = None
    write_health(health_path, {"status": "starting"})
    try:
        attempts = positive_env("WORKER_CONNECT_ATTEMPTS", 12, 100)
        for attempt in range(1, attempts + 1):
            if stop.is_set():
                return
            try:
                consumer = KafkaConsumer(
                    *TOPICS,
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092").split(
                        ","
                    ),
                    group_id=os.getenv("WORKER_GROUP_ID", "aiops-evidence-v1"),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    api_version_auto_timeout_ms=5000,
                    request_timeout_ms=15_000,
                    session_timeout_ms=10_000,
                    max_poll_records=1000,
                    max_poll_interval_ms=300_000,
                )
                break
            except Exception as exc:
                log.warning(
                    "Kafka startup failed attempt=%d/%d type=%s",
                    attempt,
                    attempts,
                    type(exc).__name__,
                )
                if attempt == attempts:
                    raise RuntimeError("Kafka startup retry budget exhausted") from exc
                stop.wait(2)
        pipeline = Pipeline(store)
        run_consumer(
            consumer,
            pipeline,
            stop,
            health_path,
            max_failures=positive_env("WORKER_POLL_FAILURES", 5, 100),
            storage_attempts=positive_env("WORKER_STORAGE_ATTEMPTS", 3, 5),
            commit_timeout_ms=positive_env("WORKER_COMMIT_TIMEOUT_MS", 15_000, 60_000),
        )
    except Exception as exc:
        write_health(health_path, {"status": "failed", "error_type": type(exc).__name__})
        log.error("Worker stopped without committing a failed batch type=%s", type(exc).__name__)
        raise
    else:
        write_health(health_path, {"status": "stopped"})
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)
        if pipeline is not None:
            pipeline.close()


if __name__ == "__main__":
    import sys

    if "--healthcheck" in sys.argv:
        path = Path(os.getenv("DATA_DIR", "data")) / "worker-health.json"
        raise SystemExit(0 if check_health(path) else 1)
    main()
