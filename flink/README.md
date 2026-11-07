# Streaming implementation v3

The detector preserves the schema-v1 event and alert interfaces. `wire_contract.py`
is dependency-free and used by API ingestion and Flink. Timestamps must now be
integer Unix milliseconds, at most 60 seconds ahead of the clock. Identifiers are
limited to 256 Unicode code points; all JSON numbers must be finite. `trace_id`
remains optional for legacy v1 events, while the supplied request/span producers
send it. The reserved `__kafka_origin` field cannot be supplied by producers.

`java/aiops/SourceEnvelope.java` implements the actual Kafka connector Java
extension point. It retains topic, partition, offset and Kafka timestamp, and
installs watermarks at the source so each Kafka partition progresses separately.
Its timestamp guard is tested against the Python metric contract. Invalid input
gets `Long.MIN_VALUE` and cannot advance the watermark; Python sends it to DLQ
with its original coordinates. UTF-8 decoding is strict; invalid bytes are retained
as base64 with encoding_error, while a legitimate U+FFFD character is accepted.
The narrow Java extension is needed because the
PyFlink 1.20 Kafka builder only exposes value-only deserialization. It is compiled
against the fixed SQL connector, whose Kafka client classes are shaded.

## Configuration

All values below are environment variables, with integer millisecond durations.

| Variable | Default | Meaning |
| --- | --- | --- |
| KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | Kafka bootstrap addresses |
| FLINK_METRICS_TOPIC | telemetry.metrics | Source topic |
| FLINK_CONSUMER_GROUP | aiops-flink-detector-v3 | Offset group |
| FLINK_JOB_NAME | aiops-event-time-detector-v3 | Submitted job name |
| FLINK_PARALLELISM | 2 | Source/operator parallelism |
| FLINK_WINDOW_MS | 30000 | Sliding window length |
| FLINK_SLIDE_MS | 10000 | Window slide; must divide window length |
| FLINK_OUT_OF_ORDER_MS | 5000 | Partition out-of-order allowance |
| FLINK_IDLE_MS | 10000 | Idle source-partition timeout |
| FLINK_DEDUP_TTL_MS | 300000 | Processing-time deduplication retention; at least window + disorder |
| FLINK_DEDUP_BUCKETS | 64 | Fixed event-ID hash buckets |
| FLINK_DEDUP_BUCKET_CAPACITY | 1000 | Hard maximum IDs per bucket |
| FLINK_CHECKPOINT_INTERVAL_MS | 10000 | Checkpoint frequency |
| FLINK_CHECKPOINT_TIMEOUT_MS | 60000 | Checkpoint timeout |
| FLINK_CHECKPOINT_MIN_PAUSE_MS | 2000 | Minimum checkpoint pause |
| FLINK_CHECKPOINT_PATH | file:///opt/flink/checkpoints | Shared checkpoint URI |
| FLINK_SOURCE_BRIDGE_JAR | unset | Optional JAR path; unnecessary if installed in Flink lib |
| LOG_LEVEL | INFO | Python logging level |

The Python files shipped using `-pyfs` are `rules.py`, `aggregate.py`,
`stream_support.py`, and the root `wire_contract.py`. The bridge JAR and fixed
connector JAR must be available to client and TaskManager class loaders.

## Duplicate and late-input policy

First-observed event payload wins for an event ID. Equal payloads at different
Kafka coordinates are discarded before aggregation; different payloads are
quarantined with both origins. The fixed bucket capacity and processing-time TTL
bound this state. A capacity rejection is explicit DLQ, not a successful detection
input. To handle watermarks stalled longer than the TTL, the window accumulator
has a second first-wins guard: it records conflict totals and at most ten audit
samples without changing the first observation's metric values. Window-close
records mark whether these samples were truncated. Its original strict `add` and
`merge` APIs remain available and tested.

Watermark lateness is evaluated per assigned sliding window. An old event may
still contribute to a later overlapping window; the `late-metrics-v3` side output
contains events for which every assigned window has expired. When every input
stops, event time does not advance and the final windows can stay open. No
heartbeat or arrival-time anomaly is claimed by this job.

Kafka output is at-least-once. The unchanged alert ID hashes service, rule and
window boundaries; downstream persists by this ID. Internal checkpoint mode does
not imply end-to-end exactly-once.

DLQ Kafka values have a hard 128 KiB UTF-8 limit applied to the final serialized
record, including raw text, base64, event fields and metadata. Oversized records
set `truncated=true`, record `original_serialized_bytes` and
`original_payload_sha256`, and retain bounded UTF-8 prefixes, raw/event lengths
and SHA256 hashes plus the Kafka coordinates. Prefixes end at a valid UTF-8
boundary. The full original input must be fetched by topic/partition/offset before
Kafka retention expires; a truncated DLQ record is not a complete archive.

## Upgrade and validation boundaries

Source, parser, deduplication and window state UIDs are now v3. **Do not restore a
v2 checkpoint into this new topology.** Back up persistent state, stop the v2 job,
then submit v3 afresh and replay the retained source topic. Existing alert IDs
remain stable for downstream business deduplication. No database migration or
data deletion is needed for this source change. Restore tests for checkpoints
created by v3 are separate from migration compatibility. A v3 restore must use
compatible window/slide settings and the same deduplication bucket count.
The PyFlink 1.20.3 enum adapter references an obsolete nested Java enum; the
implementation invokes the tested top-level Java enum from
`org.apache.flink.configuration` directly. Externalized checkpoints are retained
on cancellation for explicit restore;
operators must manage old job checkpoint directories and should not delete active
state or the only recovery copy.

The multistage `Dockerfile` builds the Java source and pins PyFlink 1.20.3 plus
setuptools 80.9.0; newer setuptools can remove `pkg_resources`, required by the
bundled Beam worker. Docker image building is not validated merely by a successful
native run.

Pure Python checks:

```bash
python -m unittest discover -s tests -p test_wire_contract.py -v
python -m unittest discover -s tests -p test_flink_accumulator.py -v
```

Optional matching PyFlink callback tests (no Kafka/Flink service launch):

```bash
python -m unittest discover -s tests -p test_stream_operators.py -v
AIOPS_SOURCE_BRIDGE_JAR=/absolute/aiops-kafka-source.jar \
AIOPS_KAFKA_CONNECTOR_JAR=/absolute/flink-sql-connector-kafka-3.3.0-1.20.jar \
python -m unittest discover -s tests -p test_source_bridge.py -v
```

To compile outside Docker, use `python flink/build_source_bridge.py --help` with
matching Flink libs, the connector and a JDK, or an explicit ECJ compiler JAR.
Real service-chain results and commands are recorded separately in the engineering
upgrade validation reports; the unit and Java-construction tests do not establish
message delivery or restart recovery.

## 第四阶段动态基线

设置 `DETECTION_MODE=dynamic` 启用同窗口特征的服务/指标/UTC分桶median/MAD检测。固定模式仍为默认；新增adaptive.py必须和aggregate.py一起通过`-pyfs`发布（提交器和Dockerfile已更新）。动态窗口使用独立UID及keyed全局基线状态，切换模式需备份后新作业重放，不能直接跨模式恢复。部署、暖机、阈值和真实恢复测试见[第四阶段指南](../docs/phase4/动态检测部署与评估.md)。
