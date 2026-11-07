"""Kafka -> partition event-time windows -> audited, idempotent alert events.

The source bridge preserves Kafka coordinates before Python validation. Checkpoint
state is v3: submit a fresh job when upgrading from the old v2 topology.
"""
import json
import logging
import os
from pathlib import Path

from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, OutputTag, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink, KafkaSource
from pyflink.datastream.functions import AggregateFunction, KeyedProcessFunction, ProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.java_gateway import get_gateway

from aggregate import WindowAccumulator, evaluate_accumulator, summarize_accumulator
from adaptive import DetectionConfig, evaluate_adaptive, fresh_state
from stream_support import deadletter, event_fingerprint, parse_metric_envelope

LOGGER = logging.getLogger("aiops.flink")
INVALID = OutputTag("invalid-metrics-v3", Types.STRING())
LATE = OutputTag("late-metrics-v3", Types.PICKLED_BYTE_ARRAY())


def integer_setting(name, default, minimum=1, maximum=86_400_000):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


class ParseMetric(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            yield parse_metric_envelope(value)
        except (ValueError, TypeError, KeyError, OverflowError, RecursionError) as error:
            LOGGER.warning("invalid_metric reason=%s", str(error))
            yield INVALID, deadletter(str(error), value=value)


class AuditedDeduplicate(KeyedProcessFunction):
    """First-observed payload wins; conflicting IDs and capacity overflow go to DLQ.

    IDs hash into a fixed number of buckets, with a hard cap per bucket and a
    processing-time TTL. This bounds state instead of accumulating IDs forever.
    WindowAccumulator keeps its own exact per-window deduplication as a final
    guard. Checkpoints persist this filter and its cleanup timers.
    """
    def __init__(self, ttl_ms, capacity):
        self.ttl_ms = ttl_ms
        self.capacity = capacity

    def open(self, runtime_context):
        self.entries = runtime_context.get_map_state(MapStateDescriptor("event-payloads-v3", Types.STRING(), Types.PICKLED_BYTE_ARRAY()))
        self.count = runtime_context.get_state(ValueStateDescriptor("event-count-v3", Types.LONG()))
        self.timer = runtime_context.get_state(ValueStateDescriptor("cleanup-timer-v3", Types.LONG()))

    def process_element(self, event, ctx):
        now = ctx.timer_service().current_processing_time()
        event_id = event["event_id"]
        previous = self.entries.get(event_id)
        fingerprint = event_fingerprint(event)
        if previous is not None and previous["expires_at"] > now:
            if previous["fingerprint"] != fingerprint:
                LOGGER.warning("conflicting_duplicate event_id=%s", event_id)
                yield INVALID, deadletter("conflicting_duplicate_event_id", event=event, previous=previous)
            return
        count = self.count.value() or 0
        if previous is None and count >= self.capacity:
            yield INVALID, deadletter("deduplication_bucket_capacity_exceeded", event=event)
            return
        if previous is None:
            self.count.update(count + 1)
        self.entries.put(event_id, {"fingerprint": fingerprint, "expires_at": now + self.ttl_ms,
                                    "origin": event.get("__kafka_origin")})
        if self.timer.value() is None:
            timer = now + min(self.ttl_ms, 10_000)
            ctx.timer_service().register_processing_time_timer(timer)
            self.timer.update(timer)
        yield event

    def on_timer(self, timestamp, ctx):
        now = ctx.timer_service().current_processing_time()
        expired = [event_id for event_id, row in self.entries.items() if row["expires_at"] <= now]
        for event_id in expired:
            self.entries.remove(event_id)
        remaining = max(0, (self.count.value() or 0) - len(expired))
        self.count.update(remaining)
        self.timer.clear()
        if remaining:
            timer = now + min(self.ttl_ms, 10_000)
            ctx.timer_service().register_processing_time_timer(timer)
            self.timer.update(timer)
        return []


class RuleAggregate(AggregateFunction):
    def create_accumulator(self):
        return WindowAccumulator()

    def add(self, value, accumulator):
        return accumulator.add_or_quarantine(value)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, acc_a, acc_b):
        if len(acc_a.seen) < len(acc_b.seen):
            acc_a, acc_b = acc_b, acc_a
        return acc_a.merge(acc_b)


class DetectAggregateWindow(ProcessWindowFunction):
    def __init__(self, config=None):
        self.config = config or DetectionConfig()

    def open(self, runtime_context):
        if self.config.mode == 'dynamic':
            from pyflink.datastream.state import StateTtlConfig
            self.descriptor = ValueStateDescriptor('adaptive-baseline-v1', Types.PICKLED_BYTE_ARRAY())
            ttl = (StateTtlConfig.new_builder(Time.milliseconds(self.config.max_age_ms))
                   .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                   .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build())
            self.descriptor.enable_time_to_live(ttl)

    def process(self, key, context, elements):
        for accumulator in elements:
            if self.config.mode == 'dynamic':
                handle = context.global_state().get_state(self.descriptor)
                state = handle.value() or fresh_state(self.config)
                result = evaluate_adaptive(summarize_accumulator(accumulator), key,
                    context.window().start, context.window().end, self.config, state)
                handle.update(state)
                output = result['alerts']
            else:
                output = evaluate_accumulator(accumulator, key, context.window().start, context.window().end)
            for alert in output:
                yield json.dumps(alert)
            if accumulator.quarantine_count:
                yield json.dumps({"schema_version": 1, "event_type": "deadletter", "source": "flink-detector",
                    "reason": "window_conflicting_duplicate_event_id", "service": key,
                    "window_start": context.window().start, "window_end": context.window().end,
                    "quarantine_count": accumulator.quarantine_count,
                    "samples": accumulator.quarantine_samples,
                    "samples_truncated": accumulator.quarantine_count > len(accumulator.quarantine_samples)})


def kafka_sink(brokers, topic):
    return (KafkaSink.builder().set_bootstrap_servers(brokers)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema()).build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE).build())


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    detection_config = DetectionConfig.from_env()
    LOGGER.info("detector mode=%s config_id=%s", detection_config.mode, detection_config.fingerprint)
    parallelism = integer_setting("FLINK_PARALLELISM", 2, maximum=128)
    window_ms = integer_setting("FLINK_WINDOW_MS", 30_000)
    slide_ms = integer_setting("FLINK_SLIDE_MS", 10_000)
    disorder_ms = integer_setting("FLINK_OUT_OF_ORDER_MS", 5_000, minimum=0)
    idle_ms = integer_setting("FLINK_IDLE_MS", 10_000)
    ttl_ms = integer_setting("FLINK_DEDUP_TTL_MS", 300_000)
    if ttl_ms < window_ms + disorder_ms:
        raise ValueError("FLINK_DEDUP_TTL_MS must cover the event-time window and disorder allowance")
    if window_ms % slide_ms:
        raise ValueError("FLINK_WINDOW_MS must be a multiple of FLINK_SLIDE_MS")
    buckets = integer_setting("FLINK_DEDUP_BUCKETS", 64, maximum=4096)
    capacity = integer_setting("FLINK_DEDUP_BUCKET_CAPACITY", 1000, maximum=100_000)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    bridge = os.environ.get("FLINK_SOURCE_BRIDGE_JAR")
    if bridge:
        env.add_jars(Path(bridge).resolve().as_uri() if not bridge.startswith("file:") else bridge)
    env.enable_checkpointing(integer_setting("FLINK_CHECKPOINT_INTERVAL_MS", 10_000, minimum=100), CheckpointingMode.EXACTLY_ONCE)
    checkpoints = env.get_checkpoint_config()
    # PyFlink 1.20.3's Python enum adapter still points to the removed nested
    # Java enum; the actual 1.20.3 enum is in org.apache.flink.configuration.
    checkpoints._j_checkpoint_config.setExternalizedCheckpointRetention(
        get_gateway().jvm.org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
    checkpoints.set_checkpoint_timeout(integer_setting("FLINK_CHECKPOINT_TIMEOUT_MS", 60_000, minimum=1000))
    checkpoints.set_min_pause_between_checkpoints(integer_setting("FLINK_CHECKPOINT_MIN_PAUSE_MS", 2_000, minimum=0))
    checkpoints.set_checkpoint_storage(FileSystemCheckpointStorage(os.environ.get("FLINK_CHECKPOINT_PATH", "file:///opt/flink/checkpoints")))

    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    source_builder = (KafkaSource.builder().set_bootstrap_servers(brokers)
                      .set_topics(os.environ.get("FLINK_METRICS_TOPIC", "telemetry.metrics"))
                      .set_group_id(os.environ.get("FLINK_CONSUMER_GROUP", "aiops-flink-detector-v3"))
                      .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                      .set_property("commit.offsets.on.checkpoint", "true"))
    # The Python 1.20 builder only exposes value-only deserialization. The small
    # compiled Java schema is the supported Java connector extension point.
    bridge_class = get_gateway().jvm.aiops.SourceEnvelope
    source_builder._j_builder.setDeserializer(bridge_class())
    watermarks = WatermarkStrategy(bridge_class.watermarks(disorder_ms, idle_ms))
    raw = env.from_source(source_builder.build(), watermarks, "Kafka metrics with coordinates").uid("metrics-envelope-source-v3")
    parsed = raw.process(ParseMetric(), output_type=Types.PICKLED_BYTE_ARRAY()).uid("parse-metrics-v3")
    parsed.get_side_output(INVALID).sink_to(kafka_sink(brokers, "ops.deadletter")).name("Invalid metrics DLQ").uid("invalid-dlq-sink-v3")
    accepted = (parsed.key_by(lambda e: int(event_fingerprint({"event_id": e["event_id"]})[:8], 16) % buckets, key_type=Types.INT())
                .process(AuditedDeduplicate(ttl_ms, capacity), output_type=Types.PICKLED_BYTE_ARRAY()).uid("audited-dedup-v3"))
    accepted.get_side_output(INVALID).sink_to(kafka_sink(brokers, "ops.deadletter")).name("Conflict and capacity DLQ").uid("dedup-dlq-sink-v3")
    alerts = (accepted.key_by(lambda event: event["service"], key_type=Types.STRING())
              .window(SlidingEventTimeWindows.of(Time.milliseconds(window_ms), Time.milliseconds(slide_ms)))
              .allowed_lateness(0).side_output_late_data(LATE)
              .aggregate(RuleAggregate(), window_function=DetectAggregateWindow(detection_config),
                         accumulator_type=Types.PICKLED_BYTE_ARRAY(), output_type=Types.STRING()).uid("window-rules-v3" if detection_config.mode == "fixed" else "adaptive-window-v1"))
    alerts.filter(lambda value: json.loads(value)["event_type"] == "alert").sink_to(kafka_sink(brokers, "ops.alerts")).name("Alerts at least once").uid("alerts-sink-v3")
    alerts.filter(lambda value: json.loads(value)["event_type"] == "deadletter").sink_to(kafka_sink(brokers, "ops.deadletter")).name("Window conflict audit DLQ").uid("window-conflict-dlq-v3")
    alerts.get_side_output(LATE).map(lambda event: deadletter("event_time_older_than_watermark", event=event), output_type=Types.STRING()) \
        .sink_to(kafka_sink(brokers, "ops.deadletter")).name("Late metrics DLQ").uid("late-dlq-sink-v3")
    env.execute(os.environ.get("FLINK_JOB_NAME", "aiops-event-time-detector-v3"))


if __name__ == "__main__":
    main()
