"""Kafka OTLP JSON bridge with durable publish-before-offset-commit semantics."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
from pathlib import Path
from types import FrameType
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from research.experiment import Experiment
from research.otel.normalize import normalize
from wire_contract import digest

TOPICS = {
    "span": "telemetry.spans",
    "log": "telemetry.logs",
    "host": "telemetry.metrics",
    "request": "telemetry.metrics",
}


def process_record(record: Any, producer: Any) -> tuple[int, int, bool]:
    """All events acknowledged before caller may commit; failures remain replayable."""
    raw = record.value
    origin = {"topic": record.topic, "partition": record.partition, "offset": record.offset}
    try:
        if len(raw) > 1_000_000:
            raise ValueError("OTLP message exceeds 1 MB")
        events, unsupported = normalize(json.loads(raw), record.topic.rsplit(".", 1)[-1])
        if len(events) > 10000:
            raise ValueError("OTLP batch exceeds 10000 projected events")
    except (
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        AttributeError,
        RecursionError,
    ) as error:
        # Preserve raw payload upstream; DLQ references its exact topic/partition/offset/hash.
        producer.send(
            "otel.deadletter",
            value={
                "origin": origin,
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "reason": type(error).__name__ + ": " + str(error)[:256],
            },
        ).get(timeout=30)
        return 0, 0, True
    for event in events:
        producer.send(TOPICS[event["event_type"]], key=event["event_id"].encode(), value=event).get(
            timeout=30
        )
    logging.info(
        "bridged origin=%s count=%d unmapped=%d batch_sha=%s",
        origin,
        len(events),
        unsupported,
        digest(events),
    )
    return len(events), unsupported, False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    )
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.max_messages < 0:
        raise ValueError("max-messages must be nonnegative")
    stopped = False

    def stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with Experiment(
        args.output,
        vars(args)
        | {
            "output": str(args.output),
            "raw_retention": "Kafka retention; no fabricated host values",
        },
        [2026],
    ) as run:
        consumer = KafkaConsumer(
            "otel.traces",
            "otel.logs",
            "otel.metrics",
            bootstrap_servers=args.bootstrap.split(","),
            group_id="aiops-otel-bridge-v1",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1,
            max_poll_interval_ms=300000,
        )
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap.split(","),
            acks="all",
            retries=5,
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, allow_nan=False
            ).encode(),
        )
        counts = {"messages": 0, "events": 0, "unmapped_metric_groups": 0, "deadletters": 0}
        try:
            while not stopped and (not args.max_messages or counts["messages"] < args.max_messages):
                batches = consumer.poll(timeout_ms=1000, max_records=1)
                for records in batches.values():
                    for record in records:
                        events, unsupported, bad = process_record(record, producer)
                        consumer.commit()
                        counts["messages"] += 1
                        counts["events"] += events
                        counts["unmapped_metric_groups"] += unsupported
                        counts["deadletters"] += int(bad)
        finally:
            producer.close(timeout=30)
            consumer.close()
        run.result["metrics"] = counts
        run.summary(
            [
                "完成已读取消息的持久发布再提交消费位点。",
                "计数见 metrics，原始 OTLP 保留在 Kafka。",
                "无性能对照。",
                "运行计数，不估计 CI。",
                "至少一次投递；下游稳定 ID 去重；不把 gRPC 投影当真实 HTTP。",
                "在实际集群执行故障与吞吐实验。",
            ]
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
