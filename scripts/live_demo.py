"""Real local HTTP run; shared window rules, NO Kafka/Flink engine or LLM.

python -m scripts.live_demo --output examples
Four uvicorn child processes are stopped after normal/fault/recovery phases.
"""
import argparse
import asyncio
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import httpx
from app.contracts import digest, now_ms
from app.diagnosis import diagnose, markdown
from app.linux_agent import cpu_ticks, memory_percent
from app.storage import Store
from app.worker import process_record
from flink.rules import evaluate_window

SERVICES = ("gateway", "order", "inventory", "payment")


class Collector:
    def __init__(self, store, run_id):
        self.store, self.run_id = store, run_id
        self.lock = threading.Lock()
        self.events = []
        self.sequence = 0

    def ingest(self, topic, event):
        with self.lock:
            self.sequence += 1
            source = {"transport": "local-http", "run_id": self.run_id, "receipt_sequence": self.sequence}
            process_record(self.store, topic, json.dumps(event).encode(), source)
            self.events.append(event)

    def copy(self):
        with self.lock:
            return list(self.events)


def server_for(collector, port):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/demo/ingest":
                self.send_error(404)
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 1_000_000:
                    raise ValueError("invalid body length")
                data = json.loads(self.rfile.read(size))
                collector.ingest(data["topic"], data["event"])
            except Exception:
                self.send_error(400, "invalid telemetry")
                return
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def service_stats(events, at, width=5000):
    result = {}
    for service in SERVICES:
        rows = [e for e in events if e["event_type"] == "request" and e["service"] == service
                and at-width <= e["event_time"] <= at]
        values = sorted(e["latency_ms"] for e in rows)
        result[service] = {"count": len(rows), "error_rate": sum(e["status_code"] >= 500 for e in rows)/max(1, len(rows)),
                           "p95_ms": round(values[math.ceil(len(values)*0.95)-1], 2) if values else 0}
    return result


async def run(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parent.parent
    run_id = uuid.uuid4().hex
    # All artifacts use real event times and explicitly local provenance.
    data_dir = Path(tempfile.mkdtemp(prefix="aiops-live-"))
    store = Store(data_dir)
    collector = Collector(store, run_id)
    ports = {name: args.base_port+i for i, name in enumerate(SERVICES)}
    collector_port = args.base_port+10
    for port in [*ports.values(), collector_port]:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
    server = server_for(collector, collector_port)
    children, logs, frames, milestones, windows = [], [], [], [], []
    reports = []
    started, start_clock = None, None
    fault_at, clear_at = args.normal, args.normal+args.fault
    duration = args.normal+args.fault+args.recovery
    original_model = os.environ.pop("LLM_BASE_URL", None)
    try:
        for service in reversed(SERVICES):
            env = os.environ.copy()
            # Local service traffic must not pass through external environment proxies.
            for key in list(env):
                if key.lower().endswith("_proxy"):
                    env.pop(key)
            env.update(SERVICE_NAME=service, TELEMETRY_MODE="http",
                       TELEMETRY_HTTP_URL=f"http://127.0.0.1:{collector_port}/demo/ingest",
                       **{s.upper()+"_URL": f"http://127.0.0.1:{p}" for s, p in ports.items()})
            log = (data_dir/(service+".log")).open("w")
            logs.append(log)
            child = subprocess.Popen([sys.executable, "-m", "uvicorn", "simulator.service:app",
                "--host", "127.0.0.1", "--port", str(ports[service]), "--log-level", "warning", "--no-access-log"],
                cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
            children.append(child)
        async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
            for _ in range(60):
                ready = 0
                for service, port in ports.items():
                    try:
                        response = await client.get(f"http://127.0.0.1:{port}/health")
                        ready += response.status_code == 200
                    except httpx.RequestError:
                        pass
                if ready == 4:
                    break
                if any(p.poll() is not None for p in children):
                    raise RuntimeError("a service failed startup; inspect "+str(data_dir))
                await asyncio.sleep(0.2)
            else:
                raise RuntimeError("services not ready")
            started, start_clock = now_ms(), time.monotonic()
            phases = [{"name": "healthy", "start_s": 0, "end_s": fault_at},
                      {"name": "fault", "start_s": fault_at, "end_s": clear_at},
                      {"name": "recovery", "start_s": clear_at, "end_s": duration}]
            milestones.append({"t": 0, "kind": "start", "label": "四个HTTP服务就绪，健康流量开始"})
            print(json.dumps({"status": "running", "services": ports, "duration_seconds": duration,
                              "mode": "actual-local-http; shared-python-rules; no Kafka/Flink/LLM"}), flush=True)
            stop = asyncio.Event()
            request_counts = Counter()
            async def load():
                while not stop.is_set():
                    began = time.monotonic()
                    try:
                        response = await client.get(f"http://127.0.0.1:{ports['gateway']}/work")
                        request_counts[str(response.status_code)] += 1
                    except httpx.RequestError:
                        request_counts["transport_error"] += 1
                    try:
                        await asyncio.wait_for(stop.wait(), max(0.001, 1/args.rps-(time.monotonic()-began)))
                    except asyncio.TimeoutError:
                        pass
            load_task = asyncio.create_task(load())
            next_end = (started//10_000+1)*10_000
            next_frame, next_host, last_progress = 0, 0, -15
            injected, cleared = False, False
            cpu_previous = cpu_ticks()
            report_task = None
            try:
                while True:
                    elapsed = time.monotonic()-start_clock
                    if elapsed >= duration:
                        break
                    if elapsed >= fault_at and not injected:
                        response = await client.post(f"http://127.0.0.1:{ports['payment']}/fault",
                            json={"kind": "error", "duration_seconds": math.ceil(args.fault)+10, "error_rate": 1})
                        response.raise_for_status()
                        injected = True
                        milestones.append({"t": round(elapsed, 2), "kind": "fault", "label": "注入支付服务503错误"})
                    if elapsed >= clear_at and not cleared:
                        response = await client.post(f"http://127.0.0.1:{ports['payment']}/fault", json={"kind": "none"})
                        response.raise_for_status()
                        cleared = True
                        milestones.append({"t": round(elapsed, 2), "kind": "recovery", "label": "清除故障，继续健康请求"})
                    if elapsed >= next_host:
                        current = cpu_ticks()
                        total, idle = current[0]-cpu_previous[0], current[1]-cpu_previous[1]
                        cpu_previous = current
                        host = {"schema_version": 1, "event_id": uuid.uuid4().hex, "event_time": now_ms(),
                                "event_type": "host", "service": "linux-node", "instance_id": socket.gethostname(), "trace_id": "",
                                "cpu_percent": round(max(0, min(100, 100*(1-idle/max(1, total)))), 2),
                                "memory_percent": round(memory_percent(), 2), "load1": os.getloadavg()[0], "scope": "proc-visible-node"}
                        store.ingest(host, {"transport": "local-proc", "run_id": run_id})
                        next_host += 5
                    events = collector.copy()
                    metrics = [e for e in events if e["event_type"] == "request"]
                    watermark = max((e["event_time"] for e in metrics), default=started)-5000
                    while next_end <= watermark:
                        for service in SERVICES:
                            rows = [e for e in metrics if e["service"] == service and next_end-30_000 <= e["event_time"] < next_end]
                            alerts = evaluate_window(rows, service, next_end-30_000, next_end)
                            windows.append({"service": service, "start": next_end-30_000, "end": next_end,
                                            "sample_count": len(rows), "error_rate": sum(e["status_code"] >= 500 for e in rows)/max(1, len(rows)),
                                            "rule_ids": [a["rule_id"] for a in alerts], "evaluated_at_s": round(elapsed, 2)})
                            for alert in alerts:
                                store.alert(alert, {"transport": "local-rule-evaluator", "run_id": run_id,
                                                    "engine": "shared-python-rules-not-flink"})
                                milestones.append({"t": round(elapsed, 2), "kind": "alert", "service": service,
                                                   "label": service+" 错误率窗口告警", "alert_id": alert["alert_id"]})
                        next_end += 10_000
                    if report_task is not None and report_task.done():
                        report = report_task.result()
                        reports.append(report)
                        milestones.append({"t": round(elapsed, 2), "kind": "report", "label": "证据诊断报告生成", "report_id": report["report_id"]})
                        report_task = None
                    # Produce one representative gateway report; raw alerts remain queryable.
                    if not reports and report_task is None:
                        pending = [a for a in store.pending(8000) if store.get_alert(a)[0]["service"] == "gateway"]
                        if pending:
                            report_task = asyncio.create_task(asyncio.to_thread(diagnose, store, pending[0]))
                    if elapsed >= next_frame:
                        stats = store.stats()
                        frames.append({"t": round(elapsed, 2), "phase": "recovery" if cleared else "fault" if injected else "healthy",
                                       "services": service_stats(events, now_ms()), "events_count": stats["events"],
                                       "alerts_count": stats["alerts"], "reports_count": stats["reports"],
                                       "host": {"cpu_percent": host["cpu_percent"], "memory_percent": host["memory_percent"]}})
                        next_frame += 5
                    if elapsed-last_progress >= 15:
                        print(json.dumps({"elapsed_s": round(elapsed, 1), "stage": frames[-1]["phase"],
                                          "gateway_http_counts": dict(request_counts), "stored": store.stats()}), flush=True)
                        last_progress = elapsed
                    await asyncio.sleep(0.25)
            finally:
                stop.set()
                await load_task
                if report_task is not None:
                    reports.append(await report_task)
            # Child telemetry is synchronous: completed requests have delivered all3events.
            events = collector.copy()
            stats = store.stats()
            frames.append({"t": round(duration, 2), "phase": "recovery", "services": service_stats(events, now_ms()),
                           "events_count": stats["events"], "alerts_count": stats["alerts"], "reports_count": stats["reports"],
                           "host": {"cpu_percent": host["cpu_percent"], "memory_percent": host["memory_percent"]}})
            health = {}
            for service, port in ports.items():
                health[service] = (await client.get(f"http://127.0.0.1:{port}/health")).json()
            representative = reports[0] if reports else None
            report_view = None
            if representative:
                candidate = representative["analysis"]["hypotheses"][0]["service"]
                records = [r["event"] for r in representative["snapshot"]["records"]]
                leaf = next(e for e in records if e["event_type"] == "span" and e["service"] == candidate and e["status_code"] >= 500)
                trace = leaf["trace_id"]
                report_milestone = next((m["t"] for m in milestones if m["kind"] == "report"), duration)
                report_view = {"report_id": representative["report_id"], "created_s": report_milestone,
                               "candidate": candidate, "analysis_mode": representative["audit"]["analysis_mode"],
                               "evidence_count": len(records), "coverage": representative["snapshot"]["coverage"],
                               "hash_valid": digest(representative["snapshot"]) == representative["audit"]["snapshot_sha256"],
                               "snapshot_sha256": representative["audit"]["snapshot_sha256"],
                               "example_trace": {"trace_id": trace, "spans": [{k: e.get(k) for k in ("service", "status_code", "latency_ms", "span_id", "parent_span_id")}
                                   for e in records if e["event_type"] == "span" and e["trace_id"] == trace]}}
                (output/"live-report.json").write_text(json.dumps(representative, ensure_ascii=False, indent=2))
                (output/"live-report.md").write_text(markdown(representative))
            phase_stats = {}
            for phase in phases:
                phase_stats[phase["name"]] = service_stats(events, started+int(phase["end_s"]*1000)-1,
                                                           int((phase["end_s"]-phase["start_s"])*1000))
            result = {"summary": {"run_id": run_id, "started_at": started, "duration_seconds": duration,
                                   "mode": "actual-local-tcp-http + shared-python-rules; no Kafka/Flink/LLM", "phases": phases,
                                   "event_count": stats["events"], "trace_count": len({e["trace_id"] for e in events}),
                                   "alert_count": stats["alerts"], "report_count": stats["reports"],
                                   "http_counts": dict(request_counts), "service_health": health,
                                   "watermark_delay_seconds": 5, "window_seconds": 30, "slide_seconds": 10,
                                   "report_policy": "first gateway alert only; local demonstration orchestration"},
                      "frames": frames, "milestones": milestones, "report": report_view,
                      "phase_statistics": phase_stats, "evaluated_windows": windows}
            (output/"live-run.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
            print(json.dumps({"completed": True, "summary": result["summary"], "report": report_view}, ensure_ascii=False), flush=True)
            return result
    finally:
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs:
            log.close()
        server.shutdown()
        server.server_close()
        if original_model is not None:
            os.environ["LLM_BASE_URL"] = original_model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="examples")
    parser.add_argument("--normal", type=int, default=35)
    parser.add_argument("--fault", type=int, default=30)
    parser.add_argument("--recovery", type=int, default=45)
    parser.add_argument("--rps", type=float, default=3)
    parser.add_argument("--base-port", type=int, default=18081)
    args = parser.parse_args()
    if min(args.normal, args.fault, args.recovery) < 1 or not 0 < args.rps <= 10:
        parser.error("positive phase durations and 0<rps<=10 required")
    asyncio.run(run(args))
