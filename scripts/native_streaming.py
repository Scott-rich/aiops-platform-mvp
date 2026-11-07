"""Real, isolated Kafka + Flink integration acceptance test (Linux/Java 17).

Run bootstrap_native first, then this module with its isolated Python 3.11. No
Docker result is inferred from this test. Every daemon belongs to this invocation
and is stopped in finally; broker data and test logs remain in the output folder.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
TOPICS = ("telemetry.metrics", "telemetry.logs", "telemetry.spans", "ops.alerts", "ops.deadletter")


def http_json(url, body=None, timeout=5):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=timeout) as response:
        return json.load(response)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(label, predicate, timeout=90, interval=1):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except Exception as error:
            last_error = f"{type(error).__name__}: {error}"
        time.sleep(interval)
    raise TimeoutError(f"{label} did not become ready in {timeout}s; last error={last_error}")


def metric(event_id, timestamp, service="stream-compare", status=503, latency=1001):
    return {"schema_version": 1, "event_id": event_id, "event_time": timestamp,
            "event_type": "request", "service": service, "instance_id": service + "-1",
            "trace_id": "", "latency_ms": latency, "status_code": status}


class NativeAcceptance:
    def __init__(self, runtime, output):
        self.runtime, self.output = runtime, output
        self.output.mkdir(parents=True, exist_ok=False)
        self.logs = self.output / "logs"
        self.logs.mkdir()
        self.processes, self.handles = [], []
        self.results = {"schema_version": 1, "execution_kind": "real_native_kafka_flink",
            "started_at_utc": datetime.now(timezone.utc).isoformat(), "status": "running",
            "environment": {"platform": platform.platform(), "python": sys.version,
                            "cpu_count_visible": os.cpu_count()},
            "configuration": {"kafka": "3.9.1", "flink": "1.20.3", "connector": "3.3.0-1.20",
                              "partitions": 3, "parallelism": 2, "window_ms": 30000,
                              "slide_ms": 10000, "out_of_order_ms": 5000,
                              "checkpoint_interval_ms": 10000,
                              "delivery_semantics": "Flink checkpoint exactly-once internally; Kafka sink at-least-once; ID dedup in worker"},
            "checks": [], "milestones": [], "sent_records": [], "output_records": [],
            "limitations": ["Native Linux processes; Docker image build/Compose deployment is a separate unverified acceptance item.",
                            "Single broker and local checkpoint disk are not high availability.",
                            "Test input includes explicitly synthetic telemetry; HTTP scenario uses actual simulator processes.",
                            "No external model call is made by this test."]}
        self.started = time.monotonic()
        self.consumer = self.producer = self.admin = None
        self.rest = self.brokers = None
        self.last_watermark_ms = 0
        files = [ROOT / "wire_contract.py", ROOT / "app/worker.py", ROOT / "app/storage.py",
                 ROOT / "flink/detector.py", ROOT / "flink/aggregate.py", ROOT / "flink/stream_support.py",
                 ROOT / "flink/java/aiops/SourceEnvelope.java", Path(__file__)]
        self.results["source_sha256"] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                         for path in files if path.exists()}

    def mark(self, name, **details):
        value = {"name": name, "elapsed_seconds": round(time.monotonic() - self.started, 3), **details}
        self.results["milestones"].append(value)
        print(json.dumps(value, ensure_ascii=False), flush=True)
        self.save()

    def check(self, name, condition, **details):
        value = {"name": name, "passed": bool(condition), **details}
        self.results["checks"].append(value)
        self.save()
        if not condition:
            raise AssertionError(json.dumps(value, ensure_ascii=False))

    def save(self):
        target = self.output / "result.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.results, ensure_ascii=False, indent=2) + "\n")
        temporary.replace(target)

    def start(self, name, command, env=None):
        log = (self.logs / (name + ".log")).open("w")
        self.handles.append(log)
        process = subprocess.Popen([str(v) for v in command], cwd=ROOT,
                                   env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        self.processes.append((name, process))
        self.mark("process_started", process=name, pid=process.pid)
        return process

    @staticmethod
    def stop(process):
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=12)
            except ProcessLookupError:
                pass
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)

    def command(self, name, command, env=None, timeout=90):
        with (self.logs / (name + ".log")).open("w") as log:
            completed = subprocess.run([str(v) for v in command], cwd=ROOT, env=env,
                                       stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
        if completed.returncode:
            raise RuntimeError(f"{name} exited {completed.returncode}; see logs/{name}.log")

    def setup(self):
        from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
        from kafka.admin import NewTopic
        metadata = json.loads((self.runtime / "runtime.json").read_text())
        self.flink, self.kafka = Path(metadata["flink_home"]), Path(metadata["kafka_home"])
        self.python = Path(metadata["python"])
        self.results["runtime_artifacts"] = metadata
        freeze = self.runtime / "dependency-freeze.txt"
        if freeze.exists():
            shutil.copy2(freeze, self.output / freeze.name)
        self.results["environment"]["java"] = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True).strip()
        for name in ("memory.max", "cpu.max"):
            path = Path("/sys/fs/cgroup") / name
            if path.exists():
                self.results["environment"][name] = path.read_text().strip()
        broker_port, controller_port = free_port(), free_port()
        self.brokers = f"127.0.0.1:{broker_port}"
        kafka_config = self.output / "kafka.properties"
        kafka_config.write_text("\n".join([
            "process.roles=broker,controller", "node.id=1",
            f"controller.quorum.voters=1@127.0.0.1:{controller_port}",
            f"listeners=PLAINTEXT://127.0.0.1:{broker_port},CONTROLLER://127.0.0.1:{controller_port}",
            f"advertised.listeners=PLAINTEXT://{self.brokers}",
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "controller.listener.names=CONTROLLER", "inter.broker.listener.name=PLAINTEXT",
            f"log.dirs={self.output / 'kafka-data'}", "num.partitions=3",
            "offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0",
            "auto.create.topics.enable=false", "num.network.threads=3", "num.io.threads=4",
            "log.retention.hours=24", "log.segment.bytes=16777216"]) + "\n")
        kafka_env = os.environ.copy()
        kafka_env["KAFKA_HEAP_OPTS"] = "-Xms256m -Xmx384m"
        self.command("kafka-format", [self.kafka / "bin/kafka-storage.sh", "format", "-t",
                     uuid.uuid4().hex[:22], "-c", kafka_config], kafka_env)
        self.broker = self.start("kafka", [self.kafka / "bin/kafka-server-start.sh", kafka_config], kafka_env)
        def kafka_ready():
            client = KafkaAdminClient(bootstrap_servers=self.brokers, request_timeout_ms=6000,
                                      api_version_auto_timeout_ms=3000)
            client.list_topics()
            return client
        self.admin = wait_until("Kafka broker", kafka_ready, timeout=90)
        self.results["kafka_runtime_cluster"] = self.admin.describe_cluster()
        self.admin.create_topics([NewTopic(name, 3, 1) for name in TOPICS], timeout_ms=20000)
        self.producer = KafkaProducer(bootstrap_servers=self.brokers, acks="all", retries=3,
                                      max_in_flight_requests_per_connection=1,
                                      request_timeout_ms=10000, max_block_ms=10000)
        self.consumer = KafkaConsumer("ops.alerts", "ops.deadletter", bootstrap_servers=self.brokers,
                                      auto_offset_reset="earliest", enable_auto_commit=False,
                                      group_id="native-acceptance-" + uuid.uuid4().hex,
                                      request_timeout_ms=15000, consumer_timeout_ms=1000)
        self.mark("kafka_ready", brokers=self.brokers, topics=list(TOPICS))
        conf = self.output / "flink-conf"
        conf.mkdir()
        for source in (self.flink / "conf").iterdir():
            if source.is_file():
                shutil.copy2(source, conf / source.name)
        from ruamel.yaml import YAML
        original = YAML(typ="safe").load((conf / "config.yaml").read_text())
        rest_port = free_port()
        config = {"env.java.opts.all": original["env"]["java"]["opts"]["all"],
            "jobmanager.rpc.address": "127.0.0.1", "jobmanager.bind-host": "127.0.0.1",
            "jobmanager.rpc.port": free_port(), "rest.address": "127.0.0.1",
            "rest.bind-address": "127.0.0.1", "rest.port": rest_port,
            "taskmanager.bind-host": "127.0.0.1", "taskmanager.host": "127.0.0.1",
            "taskmanager.numberOfTaskSlots": 2, "jobmanager.memory.process.size": "768m",
            "taskmanager.memory.process.size": "1536m", "parallelism.default": 2,
            "python.client.executable": str(self.python), "python.executable": str(self.python),
            "heartbeat.timeout": 10000, "heartbeat.interval": 1000,
            "restart-strategy.type": "fixed-delay", "restart-strategy.fixed-delay.attempts": 6,
            "restart-strategy.fixed-delay.delay": "3 s",
            "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
            "state.checkpoints.dir": (self.output / "checkpoints").as_uri(),
            "state.savepoints.dir": (self.output / "savepoints").as_uri()}
        (conf / "config.yaml").write_text("\n".join(k + ": " + json.dumps(v) for k, v in config.items()) + "\n")
        self.env = os.environ.copy()
        self.env.update(FLINK_HOME=str(self.flink), FLINK_CONF_DIR=str(conf),
                        FLINK_LOG_DIR=str(self.logs), FLINK_PID_DIR=str(self.output),
                        FLINK_IDENT_STRING="aiops-native-" + uuid.uuid4().hex[:8],
                        KAFKA_BOOTSTRAP_SERVERS=self.brokers, FLINK_PARALLELISM="2",
                        PYTHONPATH=str(ROOT), PYFLINK_CLIENT_EXECUTABLE=str(self.python),
                        PYFLINK_PYTHON=str(self.python),
                        FLINK_CHECKPOINT_PATH=(self.output / "checkpoints").as_uri(),
                        FLINK_CHECKPOINT_INTERVAL_MS="10000", FLINK_CHECKPOINT_MIN_PAUSE_MS="2000")
        # Do not let optional model configuration turn a stream test into a paid call.
        for key in tuple(self.env):
            if key.startswith("LLM_") or key.endswith("API_KEY"):
                self.env.pop(key, None)
        self.rest = f"http://127.0.0.1:{rest_port}"
        bridge = self.runtime / "aiops-kafka-source.jar"
        self.command("build-source-bridge", [self.python, ROOT / "flink/build_source_bridge.py",
            "--flink-home", self.flink, "--connector", self.runtime / "downloads/flink-sql-connector-kafka-3.3.0-1.20.jar",
            "--output", bridge, "--ecj", self.runtime / "downloads/ecj-3.33.0.jar"], self.env)
        self.results["source_bridge_sha256"] = hashlib.sha256(bridge.read_bytes()).hexdigest()
        shutil.copy2(bridge, self.flink / "lib" / bridge.name)
        self.jobmanager = self.start("jobmanager", [self.flink / "bin/jobmanager.sh", "start-foreground"], self.env)
        self.taskmanager = self.start("taskmanager", [self.flink / "bin/taskmanager.sh", "start-foreground"], self.env)
        wait_until("Flink TaskManager", lambda: http_json(self.rest + "/overview").get("taskmanagers", 0), timeout=90)
        self.results["flink_runtime_overview"] = http_json(self.rest + "/overview")
        self.mark("flink_cluster_ready", rest=self.rest)
        files = [ROOT / "flink" / name for name in ("rules.py", "aggregate.py", "adaptive.py", "stream_support.py")]
        files.append(ROOT / "wire_contract.py")
        self.command("submit-job", [self.flink / "bin/flink", "run", "-m", f"127.0.0.1:{rest_port}", "-d",
            "-py", ROOT / "flink/detector.py", "-pyfs", ",".join(str(p) for p in files)], self.env, timeout=180)
        def running_job():
            jobs = http_json(self.rest + "/jobs/overview")["jobs"]
            return next((j for j in jobs if j["name"] == "aiops-event-time-detector-v3" and j["state"] == "RUNNING"
                         and j.get("tasks", {}).get("total", 0) > 0
                         and j["tasks"].get("running") == j["tasks"]["total"]), None)
        job = wait_until("Flink detector RUNNING", running_job, timeout=90)
        self.job_id = job["jid"]
        self.results["job_id"] = self.job_id
        self.mark("flink_job_running", job=job)
        self.check("real_broker_and_flink_running", self.broker.poll() is None and self.jobmanager.poll() is None)
        worker_env = self.env.copy()
        worker_env["DATA_DIR"] = str(self.output / "app-data")
        worker_env["FLINK_REST_URL"] = self.rest
        worker_env["EXECUTION_MODE"] = "kafka-worker"
        self.worker = self.start("worker", [self.python, "-m", "app.worker"], worker_env)
        self.worker_env = worker_env
        self.api_port = free_port()
        self.api = self.start("api", [self.python, "-m", "uvicorn", "app.api:app", "--no-proxy-headers", "--host", "127.0.0.1", "--port", self.api_port], worker_env)
        self.api_url = f"http://127.0.0.1:{self.api_port}"
        wait_until("query API", lambda: http_json(self.api_url + "/health"), timeout=30)

    def send(self, event, partition=0, raw=None):
        payload = raw if raw is not None else json.dumps(event, separators=(",", ":"))
        wire = payload if isinstance(payload, bytes) else payload.encode()
        metadata = self.producer.send("telemetry.metrics", value=wire, partition=partition).get(timeout=15)
        item = {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset,
                "event": event, "raw": (base64.b64encode(payload).decode() if isinstance(payload, bytes) else payload) if raw is not None else None,
                "raw_encoding": "base64" if isinstance(payload, bytes) else "utf-8",
                "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
                "sent_at_ms": int(time.time() * 1000)}
        self.results["sent_records"].append(item)
        return item

    def advance_watermark(self, timestamp):
        self.last_watermark_ms = max(self.last_watermark_ms, timestamp)
        for partition in range(3):
            self.send(metric(uuid.uuid4().hex, timestamp, f"watermark-{partition}", 200, 1), partition)
        self.producer.flush(timeout=15)

    def collect(self, seconds=1):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            batches = self.consumer.poll(timeout_ms=500)
            for records in batches.values():
                for record in records:
                    payload = json.loads(record.value)
                    self.results["output_records"].append({"topic": record.topic, "partition": record.partition,
                        "offset": record.offset, "kafka_timestamp": record.timestamp, "payload": payload,
                        "wire_bytes": len(record.value),
                        "received_at_ms": int(time.time() * 1000)})
        self.save()
        return self.results["output_records"]

    def alerts_for(self, service, start=None, end=None):
        self.collect()
        return [r["payload"] for r in self.results["output_records"]
                if r["topic"] == "ops.alerts" and r["payload"].get("service") == service
                and (start is None or r["payload"].get("window_start") == start)
                and (end is None or r["payload"].get("window_end") == end)]

    def streaming_scenarios(self):
        from flink.rules import evaluate_window
        base = (int(time.time() * 1000) // 10000) * 10000 - 90000
        self.results["test_window"] = {"start": base, "end": base + 30000}
        # All partitions first become active. p0 then advances while p2 is behind;
        # p0 and p2 share a source subtask at parallelism 2, testing partition WMs.
        self.advance_watermark(base + 100)
        self.send(metric(uuid.uuid4().hex, base + 70000, "watermark-0", 200, 1), 0)
        time.sleep(2)
        events = [metric(f"compare-{i}", base + 500 + i * 150, status=503 if i < 6 else 200,
                         latency=1001 + i) for i in range(20)]
        # Deliberate in-bound arrival reordering, plus identical redelivery.
        for event in reversed(events):
            self.send(event, 2)
        for event in events[:5]:
            self.send(event, 2)
        conflict_source = self.send({**events[0], "latency_ms": 5, "status_code": 200}, 2)
        normal = [metric(f"normal-{i}", base + 500 + i * 150, "stream-normal", 200, 20) for i in range(20)]
        for event in normal:
            self.send(event, 1)
        invalid = metric("poison-number", base + 1000)
        invalid["latency_ms"] = 10 ** 1000
        poison_source = self.send(invalid, 1)
        json_source = self.send(None, 1, raw="{invalid-json")
        utf8_source = self.send(None, 1, raw=b"\xff")
        large_json_source = self.send(None, 1, raw="{" + 600000 * '"')
        large_utf8_source = self.send(None, 1, raw=b"\xff" * 600001)
        future_source = self.send({**metric("invalid-future", base + 1000), "event_time": int(time.time() * 1000) + 120000}, 1)
        self.advance_watermark(base + 80000)
        actual = wait_until("Flink comparison alerts", lambda: self.alerts_for("stream-compare", base, base + 30000), timeout=80)
        expected = evaluate_window(events, "stream-compare", base, base + 30000)
        by_rule = {a["rule_id"]: a for a in actual}
        self.results["python_reference"] = expected
        self.results["flink_comparison"] = actual
        self.check("python_flink_rule_decisions_and_ids", set(by_rule) == {a["rule_id"] for a in expected}
                   and {a["alert_id"] for a in actual} == {a["alert_id"] for a in expected},
                   python_rules=[a["rule_id"] for a in expected], flink_rules=list(by_rule))
        self.check("duplicate_and_conflict_do_not_change_sample_count", all(a["evidence"]["sample_count"] == 20 for a in actual))
        self.check("out_of_order_and_skewed_partition_events_retained", bool(actual))
        errors = []
        for reference in expected:
            measured = by_rule[reference["rule_id"]]
            errors.append(measured["evidence"]["p95_latency_ms"] - reference["evidence"]["p95_latency_ms"])
        self.check("p95_histogram_precision", all(0 <= value < 25 for value in errors), measured_errors_ms=errors)
        self.check("healthy_window_not_alerted", not self.alerts_for("stream-normal"))
        late_source = self.send(metric("too-late", base - 120000, "late-service"), 1)
        targets = [("poison", poison_source, ("latency", "number", "finite")),
                   ("invalid_json", json_source, ("Expecting", "JSON", "property")),
                   ("invalid_utf8", utf8_source, ("utf", "encoding")),
                   ("large_invalid_json", large_json_source, ("Expecting", "JSON", "property")),
                   ("large_invalid_utf8", large_utf8_source, ("utf", "encoding")),
                   ("future", future_source, ("future", "within 60 seconds of now")),
                   ("conflict", conflict_source, ("conflicting_duplicate",)),
                   ("late", late_source, ("watermark", "late"))]
        def matches(item, source):
            origin = item.get("origin") or {}
            return all(origin.get(key) == source[key] for key in ("topic", "partition", "offset"))
        def dlqs():
            self.collect()
            values = [r["payload"] for r in self.results["output_records"] if r["topic"] == "ops.deadletter"]
            return values if all(any(matches(item, source) for item in values) for _, source, _ in targets) else None
        deadletters = wait_until("invalid/conflict/late DLQ", dlqs, timeout=40)
        self.results["flink_deadletters"] = deadletters
        reasons = [item.get("reason", "") for item in deadletters]
        self.check("poison_invalid_conflict_late_dlq", len(deadletters) >= len(targets), reasons=reasons)
        per_input = []
        for name, source, reason_parts in targets:
            matching = [item for item in deadletters if matches(item, source)]
            valid = any(any(part.lower() in item.get("reason", "").lower() for part in reason_parts) for item in matching)
            per_input.append({"scenario": name, "origin": {key: source[key] for key in ("topic", "partition", "offset")},
                              "reasons": [item.get("reason") for item in matching], "passed": valid})
        self.check("every_bad_input_has_own_correct_dlq", all(item["passed"] for item in per_input), inputs=per_input)
        utf8_dlq = next(item for item in deadletters if matches(item, utf8_source))
        self.check("invalid_utf8_bytes_preserved", utf8_dlq.get("raw_base64") == "/w==")
        large_details = []
        for name, source in (("large_invalid_json", large_json_source), ("large_invalid_utf8", large_utf8_source)):
            item = next(item for item in deadletters if matches(item, source))
            large_details.append({"scenario": name, "passed": item.get("truncated") is True
                                  and item.get("raw_byte_length") == source["wire_bytes"]
                                  and item.get("raw_sha256") == source["wire_sha256"],
                                  "raw_byte_length": item.get("raw_byte_length"),
                                  "truncated": item.get("truncated"),
                                  "sha256_valid": item.get("raw_sha256") == source["wire_sha256"]})
        self.check("large_dlq_bounded_and_original_hashes_valid", all(item["passed"] for item in large_details)
                   and all(row["wire_bytes"] <= 131072 for row in self.results["output_records"] if row["topic"] == "ops.deadletter"),
                   inputs=large_details)
        self.check("dlq_original_kafka_coordinates", all(isinstance(item.get("origin"), dict)
            and item["origin"].get("topic") == "telemetry.metrics"
            and isinstance(item["origin"].get("offset"), int) for item in deadletters))
        self.check("job_survives_bad_inputs", http_json(self.rest + f"/jobs/{self.job_id}")["state"] == "RUNNING")
        self.mark("streaming_scenarios_complete")

    def checkpoint(self):
        return http_json(self.rest + f"/jobs/{self.job_id}/checkpoints")

    def restart_scenario(self):
        start = (int(time.time() * 1000) // 10000) * 10000 - 10000
        events = [metric(f"recovery-{i}", start + 1000 + i * 50, "stream-recovery", 503, 1201)
                  for i in range(20)]
        previous = self.checkpoint().get("latest", {}).get("completed", {}) or {}
        before_id = previous.get("id", -1)
        for event in events[:10]:
            self.send(event, 0)
        checkpoint_input_offset = self.results["sent_records"][-1]["offset"]
        self.producer.flush(timeout=15)
        time.sleep(3)
        def completed_after_inputs():
            value = self.checkpoint().get("latest", {}).get("completed") or {}
            return value if value.get("id", -1) > before_id and value.get("status") == "COMPLETED" else None
        checkpoint = wait_until("checkpoint with pending window", completed_after_inputs, timeout=80)
        def committed_input_offsets():
            offsets = self.admin.list_consumer_group_offsets("aiops-flink-detector-v3")
            values = [{"topic": key.topic, "partition": key.partition, "offset": value.offset} for key, value in offsets.items()]
            return values if any(row["topic"] == "telemetry.metrics" and row["partition"] == 0
                                 and row["offset"] > checkpoint_input_offset for row in values) else None
        self.results["kafka_offsets_at_checkpoint"] = wait_until("checkpoint source offsets committed", committed_input_offsets, timeout=20)
        self.results["checkpoint_before_failure"] = checkpoint
        self.check("completed_checkpoint_persisted", checkpoint["id"] > before_id and bool(checkpoint.get("external_path")), checkpoint_id=checkpoint["id"])
        self.mark("taskmanager_stop", checkpoint_id=checkpoint["id"])
        recovery_started = time.monotonic()
        self.stop(self.taskmanager)
        time.sleep(2)
        self.taskmanager = self.start("taskmanager-restarted", [self.flink / "bin/taskmanager.sh", "start-foreground"], self.env)
        def restored():
            value = self.checkpoint().get("latest", {}).get("restored") or {}
            detail = http_json(self.rest + f"/jobs/{self.job_id}")
            ready = bool(detail.get("vertices")) and all(vertex["status"] == "RUNNING" for vertex in detail["vertices"])
            return value if value.get("id", -1) >= checkpoint["id"] and detail.get("state") == "RUNNING" and ready else None
        restore = wait_until("TaskManager checkpoint restore", restored, timeout=120)
        self.results["checkpoint_restored"] = restore
        self.results["recovery_seconds"] = round(time.monotonic() - recovery_started, 3)
        for event in events[10:]:
            self.send(event, 0)
        self.advance_watermark(start + 45000)
        alerts = wait_until("stateful recovery window", lambda: self.alerts_for("stream-recovery", start, start + 30000), timeout=80)
        self.check("checkpoint_window_state_and_offsets_restored", all(a["evidence"]["sample_count"] == 20 for a in alerts),
                   checkpoint_id=checkpoint["id"], restored_id=restore["id"], alerts=alerts)
        self.mark("taskmanager_recovery_complete", seconds=self.results["recovery_seconds"])

    def http_scenario(self):
        # Keep actual HTTP event timestamps beyond previous synthetic watermarks.
        latest = self.last_watermark_ms
        if latest > int(time.time() * 1000):
            wait_until("wall clock catches up with test watermarks", lambda: int(time.time() * 1000) >= latest + 1000,
                       timeout=65, interval=1)
        ports = {name: free_port() for name in ("inventory", "payment", "order", "gateway")}
        service_env = self.env.copy()
        service_env.update(TELEMETRY_MODE="kafka", **{name.upper() + "_URL": f"http://127.0.0.1:{port}" for name, port in ports.items()})
        for name in ("inventory", "payment", "order", "gateway"):
            env = service_env.copy()
            env.update(SERVICE_NAME=name, INSTANCE_ID=name + "-native")
            self.start(name, [self.python, "-m", "uvicorn", "simulator.service:app", "--host", "127.0.0.1", "--port", ports[name]], env)
            wait_until(name + " ready", lambda n=name: http_json(f"http://127.0.0.1:{ports[n]}/health"), timeout=40)
        gateway = f"http://127.0.0.1:{ports['gateway']}"
        for _ in range(5):
            http_json(gateway + "/work", timeout=10)
        http_json(f"http://127.0.0.1:{ports['payment']}/fault", {"kind": "error", "duration_seconds": 90, "error_rate": 1})
        statuses = Counter()
        for _ in range(25):
            try:
                http_json(gateway + "/work", timeout=10)
                statuses[200] += 1
            except urllib.error.HTTPError as error:
                statuses[error.code] += 1
        self.results["actual_http_status_counts"] = dict(statuses)
        self.check("real_http_cascade", statuses[502] == 25)
        http_json(f"http://127.0.0.1:{ports['payment']}/fault", {"kind": "none"})
        self.advance_watermark(int(time.time() * 1000) + 40000)
        actual_alerts = wait_until("actual HTTP Flink alert", lambda: self.alerts_for("payment"), timeout=80)
        self.check("http_telemetry_through_real_flink", any(a["rule_id"] == "high_error_rate" for a in actual_alerts), alerts=actual_alerts)
        def reports_ready():
            incidents = http_json(self.api_url + "/incidents")
            for incident in incidents:
                if "payment" in incident.get("services", []) or incident.get("root_service") == "payment":
                    versions = http_json(self.api_url + f"/incidents/{incident['incident_id']}/reports")
                    if versions and any(version.get('stage') == 'enriched' for version in versions):
                        return {"incident": incident, "versions": versions}
            return None
        reports = wait_until("worker diagnostic report", reports_ready, timeout=50)
        self.results["actual_http_incident"] = reports
        report_id = reports["versions"][-1]["report_id"]
        report = http_json(self.api_url + "/report-versions/" + report_id)
        (self.output / "actual-http-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        records = report["snapshot"]["records"]
        self.check("worker_report_has_real_kafka_provenance", bool(records) and all(r["source"].get("transport") == "kafka" for r in records))
        self.check("worker_report_hash_verifies", http_json(self.api_url + f"/report-versions/{report_id}/verify")["snapshot_sha256_valid"])
        ids = {row["event"]["event_id"] for row in records}
        hypotheses = report["analysis"].get("hypotheses", [])
        references = [ref for hypothesis in hypotheses for key in ("supporting_evidence_ids", "against_evidence_ids")
                      for ref in hypothesis.get(key, [])]
        self.check("diagnostic_support_and_against_refs_exist", bool(hypotheses) and bool(references)
                   and all(ref in ids for ref in references), referenced_count=len(references))
        rca = report.get('rca', {})
        self.check('real_rca_candidate_and_affected_roles',
                   bool(rca.get('candidates')) and rca['candidates'][0]['service'] == 'payment'
                   and {'gateway', 'order'} <= set(rca.get('affected_services', [])))
        verified = http_json(self.api_url + f'/report-versions/{report_id}/verify')
        self.check('real_rca_hash_and_evidence', all(verified.get(key) is True for key in
                   ('rca_sha256_valid', 'rca_evidence_references_valid', 'rca_evidence_set_valid')))
        topology = http_json(self.api_url + f"/incidents/{reports['incident']['incident_id']}/topology")
        observed_edges = {(edge['caller'], edge['callee']) for edge in topology['graph']['edges'] if edge['basis'] == 'parent_span'}
        self.check('real_rca_topology_api', {('gateway', 'order'), ('order', 'payment')} <= observed_edges)
        self.results["worker_stats"] = http_json(self.api_url + "/health")["storage"]
        ready = wait_until("platform readiness", lambda: http_json(self.api_url + "/ready"), timeout=30)
        self.results["platform_readiness"] = ready
        self.check("real_dependency_readiness", ready.get("ready") is True, readiness=ready)
        self.results["simulator_health"] = {name: http_json(f"http://127.0.0.1:{port}/health") for name, port in ports.items()}
        self.check("simulator_kafka_send_failures_zero", all(value.get("telemetry_failed", -1) == 0 for value in self.results["simulator_health"].values()))
        self.mark("actual_http_diagnosis_complete")

    def close(self):
        errors = []
        for client in (self.consumer, self.producer, self.admin):
            if client:
                try:
                    if client is self.producer:
                        client.close(timeout=5)
                    else:
                        client.close()
                except Exception as error:
                    errors.append({"component": type(client).__name__, "error": type(error).__name__})
        for name, process in reversed(self.processes):
            try:
                self.stop(process)
            except Exception as error:
                errors.append({"component": name, "error": type(error).__name__})
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                except Exception as final_error:
                    errors.append({"component": name, "error": type(final_error).__name__})
        for handle in self.handles:
            try:
                handle.close()
            except Exception as error:
                errors.append({"component": "log_handle", "error": type(error).__name__})
        self.results["cleanup"] = {"errors": errors, "all_processes_stopped": all(process.poll() is not None for _, process in self.processes)}
        if errors or not self.results["cleanup"]["all_processes_stopped"]:
            self.results["status"] = "failed"
        self.results["elapsed_seconds"] = round(time.monotonic() - self.started, 3)
        self.results["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        self.results["check_counts"] = {"passed": sum(c["passed"] for c in self.results["checks"]),
                                        "failed": sum(not c["passed"] for c in self.results["checks"]),
                                        "errors": int(bool(self.results.get("error"))), "cleanup_errors": len(errors)}
        self.results["process_exit_codes"] = {name: process.poll() for name, process in self.processes}
        self.save()


def main():
    def requested_stop(signum, frame):
        raise KeyboardInterrupt(f"received signal {signum}; stopping test-owned processes")
    signal.signal(signal.SIGTERM, requested_stop)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=Path("/tmp/aiops-native-runtime"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--skip-http", action="store_true", help="Only validate stream/restore; explicitly recorded.")
    parser.add_argument("--skip-restart", action="store_true", help="Developer smoke run; not full acceptance.")
    args = parser.parse_args()
    output = (args.output or ROOT / "artifacts" / ("native-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))).resolve()
    acceptance = NativeAcceptance(args.runtime_dir.resolve(), output)
    acceptance.results["requested_scenarios"] = {"http": not args.skip_http, "restart": not args.skip_restart}
    try:
        acceptance.setup()
        acceptance.streaming_scenarios()
        if not args.skip_restart:
            acceptance.restart_scenario()
        if not args.skip_http:
            acceptance.http_scenario()
        acceptance.collect(2)
        acceptance.check("healthy_window_not_alerted_at_end", not acceptance.alerts_for("stream-normal"))
        acceptance.results["status"] = "passed"
    except BaseException as error:
        acceptance.results["status"] = "failed"
        acceptance.results["error"] = {"type": type(error).__name__, "message": str(error), "traceback": traceback.format_exc()}
        print(traceback.format_exc(), file=sys.stderr, flush=True)
    finally:
        acceptance.close()
    print(str(output / "result.json"), flush=True)
    if acceptance.results["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
