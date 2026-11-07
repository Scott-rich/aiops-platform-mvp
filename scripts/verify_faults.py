"""Phase5 real loopback fault scenarios. New output only; no Kafka/Flink claims."""
import argparse
import asyncio
import csv
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import time
import traceback
import uuid

import httpx
from app.storage import Store
from simulator.network import DelayProxy
from simulator.pressure import run_pressure
from simulator.scenarios import SCENARIOS, message_cases
from scripts.local_platform import simulator_environment

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ("gateway", "order", "inventory", "payment")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Suite:
    def __init__(self, output, allow_pressure=False, kafka_home=None):
        self.output = Path(output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        (self.output/"logs").mkdir()
        self.allow_pressure = allow_pressure
        self.kafka_home = str(Path(kafka_home).resolve()) if kafka_home else None
        self.ports = {}
        for name in ("api", *SERVICES):
            while (port := free_port()) in self.ports.values():
                pass
            self.ports[name] = port
        self.urls = {name: f"http://127.0.0.1:{port}" for name, port in self.ports.items()}
        self.children, self.handles, self.spawned = {}, [], []
        self.token = uuid.uuid4().hex
        self.proxy = DelayProxy("127.0.0.1", self.ports["payment"])
        self.result = {"schema_version": 1, "execution_kind": "real_loopback_http_and_bounded_processes",
            "started_at_ms": int(time.time()*1000), "environment": {"python": sys.version, "platform": platform.platform()},
            "configuration": {"resource_pressure_enabled": allow_pressure, "kafka": False, "flink": False,
                              "llm_calls": 0, "service_timeout_seconds": 3, "kafka_subtest_requested": bool(kafka_home)},
            "scenarios": [], "cleanup": {}, "status": "running"}
        self.current = None
        self.save()

    def save(self):
        temp = self.output/"result.tmp"
        temp.write_text(json.dumps(self.result, indent=2, ensure_ascii=False)+"\n", encoding="utf-8")
        temp.replace(self.output/"result.json")

    def check(self, name, condition, **details):
        item = {"name": name, "passed": bool(condition), **details}
        self.current["checks"].append(item)
        if not condition:
            raise AssertionError(name + ": " + json.dumps(details, ensure_ascii=False))

    def spawn(self, name):
        environment = simulator_environment(os.environ)
        environment.update(DATA_DIR=str(self.output/"data"), EXECUTION_MODE="local-http-python-rules",
            SIMULATOR_CONTROL_TOKEN=self.token, SIMULATOR_FAULT_AUDIT_DIR=str(self.output/"fault-audit"),
            SIMULATOR_DOWNSTREAM_TIMEOUT_SECONDS="3", DETECTION_MODE="fixed",
            TELEMETRY_MODE="http", TELEMETRY_HTTP_URL=self.urls["api"]+"/demo/ingest")
        environment.update({key.upper()+"_URL": value for key, value in self.urls.items() if key != "api"})
        environment["PAYMENT_URL"] = f"http://127.0.0.1:{self.proxy_port}"
        if name != "api":
            environment["SERVICE_NAME"] = name
        module = "app.local_api:app" if name == "api" else "simulator.service:app"
        log = (self.output/"logs"/(name+".log")).open("a", encoding="utf-8")
        self.handles.append(log)
        child = subprocess.Popen([sys.executable, "-m", "uvicorn", module, "--no-proxy-headers", "--host", "127.0.0.1",
            "--port", str(self.ports[name]), "--log-level", "info", "--no-access-log"],
            env=environment, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        self.children[name] = child
        self.spawned.append((name, child))

    async def stop(self, name):
        child = self.children.get(name)
        if child and child.poll() is None:
            child.terminate()
            try:
                await asyncio.to_thread(child.wait, 5)
            except subprocess.TimeoutExpired:
                child.kill()
                await asyncio.to_thread(child.wait, 2)

    async def ready(self, name):
        for _ in range(100):
            if self.children[name].poll() is not None:
                raise RuntimeError(f"{name} exited; see logs/{name}.log")
            try:
                response = await self.client.get(self.urls[name]+"/health", timeout=0.5)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
        raise TimeoutError(name+" startup")

    async def fault(self, kind, **options):
        response = await self.client.post(self.urls["payment"]+"/fault",
            json={"kind": kind, "duration_seconds": 5, **options}, headers={"x-simulator-token": self.token})
        response.raise_for_status()
        return response.json()

    async def request(self, service="payment", **options):
        start = time.monotonic()
        response = await self.client.get(self.urls[service]+"/work", **options)
        return response, (time.monotonic()-start)*1000

    async def http_5xx(self):
        await self.fault("error")
        response, _ = await self.request()
        self.check("observed_503", response.status_code == 503, status=response.status_code)

    async def latency(self):
        await self.fault("latency", latency_ms=220)
        response, ms = await self.request()
        self.check("observed_latency", response.status_code == 200 and ms >= 180, response_ms=ms)

    async def timeout(self):
        await self.fault("timeout", latency_ms=1200)
        started = time.monotonic()
        try:
            await self.request(timeout=0.1)
        except httpx.ReadTimeout:
            self.check("real_read_timeout", True, elapsed_ms=(time.monotonic()-started)*1000)
        else:
            self.check("real_read_timeout", False)
        # Verify actual downstream timeout classification, not only a direct client timeout.
        await self.fault("timeout", latency_ms=4500)
        response, _ = await self.request("order", timeout=5)
        self.check("dependency_timeout_504", response.status_code == 504, status=response.status_code)

    async def unavailable(self):
        await self.stop("payment")
        self.current["affected_scope"] = ["payment-process", "order", "gateway"]
        try:
            await self.request(timeout=0.3)
        except httpx.ConnectError:
            self.check("connection_refused", True)
        else:
            self.check("connection_refused", False)
        response, _ = await self.request("gateway")
        self.check("cascade_connection_error", response.status_code == 502, status=response.status_code)

    async def network_delay(self):
        self.proxy.set_delay(240, 5)
        before = self.proxy.delayed_connections
        # The same connection also sees delay: injection applies to TCP request chunks.
        response, ms = await self.request("gateway")
        self.check("real_tcp_delay", response.status_code == 200 and ms >= 200
                   and self.proxy.delayed_connections > before, response_ms=ms,
                   forwarded_bytes=self.proxy.forwarded_bytes)

    async def client_request_via_proxy(self):
        async with httpx.AsyncClient(trust_env=False, timeout=5) as client:
            started = time.monotonic()
            response = await client.get(f"http://127.0.0.1:{self.proxy_port}/work")
            return response, (time.monotonic()-started)*1000

    async def cpu(self):
        value = await asyncio.to_thread(run_pressure, "cpu", allow=self.allow_pressure)
        self.check("bounded_cpu_measured", value["cpu_seconds"] >= 0.04 and value["process_reaped"], measurement=value)

    async def memory(self):
        value = await asyncio.to_thread(run_pressure, "memory", allow=self.allow_pressure)
        self.check("bounded_memory_measured", value["allocated_bytes"] == 16*1024**2
                   and value["rss_after_allocation_bytes"]-value["rss_before_bytes"] >= 8*1024**2
                   and value["process_reaped"], measurement=value)

    async def cascade(self):
        await self.fault("error")
        for _ in range(6):
            response, _ = await self.request("gateway")
            self.check("gateway_502", response.status_code == 502)
        trace_id = response.headers["x-trace-id"]
        store = Store(self.output/"data")
        with store.db() as db:
            events = [json.loads(row[0]) for row in db.execute("SELECT payload FROM events WHERE trace_id=?", (trace_id,))]
        statuses = {event["service"]: event["status_code"] for event in events if event["event_type"] == "request"}
        self.check("actual_trace_propagation", statuses == {"payment":503,"order":502,"gateway":502,"inventory":200},
                   trace_id=trace_id, statuses=statuses)
        for _ in range(30):
            alerts = (await self.client.get(self.urls["api"]+"/alerts")).json()
            if {"payment", "order", "gateway"}.issubset({a["service"] for a in alerts}):
                break
            await asyncio.sleep(0.1)
        self.check("existing_detection_observed", {"payment", "order", "gateway"}.issubset({a["service"] for a in alerts}),
                   alert_count=len(alerts))
        self.current["evidence"] = events

    async def messages(self):
        cases = message_cases(int(time.time()*1000), uuid.uuid4().hex)
        for event in [*cases["valid"], *cases["duplicates"], cases["conflict"]]:
            response = await self.client.post(self.urls["api"]+"/demo/ingest", json={"topic":"telemetry.metrics","event":event})
            response.raise_for_status()
        rejected = []
        for raw in cases["invalid"]:
            response = await self.client.post(self.urls["api"]+"/demo/ingest",
                content=b'{"topic":"telemetry.metrics","event":'+raw+b'}', headers={"Content-Type":"application/json"})
            rejected.append(response.status_code)
        store = Store(self.output/"data")
        with store.db() as db:
            count = db.execute("SELECT COUNT(*) FROM events WHERE service='phase5-input'").fetchone()[0]
            conflicts = db.execute("SELECT COUNT(*) FROM deadletters WHERE reason LIKE 'event_id_payload_conflict:%'").fetchone()[0]
        self.check("unique_and_conflict", count == 5 and conflicts == 1, unique=count, conflicts=conflicts)
        self.check("invalid_inputs_rejected", rejected == [400]*len(cases["invalid"]), http_statuses=rejected)
        self.check("input_is_out_of_order", any(a["event_time"] > b["event_time"] for a,b in zip(cases["valid"], cases["valid"][1:])))
        self.current["input_event_ids"] = [e["event_id"] for e in cases["valid"]]
        self.current["limitation"] = "HTTP persistence/dedup validation; Flink watermark/late-event policy is covered by existing separate tests."

    async def backlog(self):
        output = self.output/"kafka"
        with (self.output/"logs"/"kafka-suite.log").open("w") as log:
            child = await asyncio.create_subprocess_exec(sys.executable, "-m", "scripts.verify_fault_kafka",
                "--kafka-home", self.kafka_home, "--output", str(output), cwd=ROOT,
                env=simulator_environment(os.environ), stdout=log, stderr=asyncio.subprocess.STDOUT)
            try:
                await asyncio.wait_for(child.wait(), 100)
            finally:
                if child.returncode is None:
                    child.terminate()
                    try:
                        await asyncio.wait_for(child.wait(), 15)
                    except asyncio.TimeoutError:
                        child.kill(); await child.wait()
            value = json.loads((output/"result.json").read_text())
            self.current["result_path"] = "kafka/result.json"
            self.current["checks"].extend(value["checks"])
            self.current["kafka_cleanup"] = value["cleanup"]
            if value["status"] == "blocked":
                raise RuntimeError(value["reason"])
            self.check("kafka_subprocess_passed_and_cleaned", child.returncode == 0
                and value["status"] == "passed" and value["cleanup"]["broker_stopped"]
                and not value["cleanup"]["errors"])

    async def run_one(self, name):
        item = {"name":name,"expected":SCENARIOS[name],"started_at_ms":int(time.time()*1000),
                "affected_scope":["owned-local-test-processes"],"checks":[],"status":"running"}
        self.result["scenarios"].append(item)
        self.current = item
        try:
            if name == "backlog" and not self.kafka_home:
                item.update(status="blocked", reason="Pass --kafka-home for isolated Kafka subtest, or run scripts.verify_fault_kafka separately.")
                return
            if name in {"cpu","memory"} and not self.allow_pressure:
                item.update(status="skipped", reason="resource pressure disabled by default")
                return
            await getattr(self, name)()
            item["status"] = "passed"
        except BaseException as error:
            item.update(status="failed", error_type=type(error).__name__, error=str(error))
            (self.output/"logs"/(name+"-error.log")).write_text(traceback.format_exc())
            if isinstance(error, (KeyboardInterrupt, asyncio.CancelledError)):
                raise
        finally:
            try:
                self.proxy.clear()
                if self.children["payment"].poll() is not None:
                    self.spawn("payment")
                    await self.ready("payment")
                state = await self.fault("none")
                self.check("fault_state_cleared", state["kind"] == "none" and self.proxy.delay_ms == 0)
                response, ms = await self.request("gateway")
                self.check("cleared_business_200", response.status_code == 200, response_ms=ms)
                history = await self.client.get(self.urls["payment"]+"/fault/history", headers={"x-simulator-token":self.token})
                history.raise_for_status()
                item["fault_history"] = history.json()["records"]
            except Exception as error:
                item.update(status="failed", cleanup_error=str(error))
            item["ended_at_ms"] = int(time.time()*1000)
            self.save()
            print(json.dumps({"scenario":name,"status":item["status"]}), flush=True)

    async def run(self, scenarios):
        try:
            self.proxy_port = await self.proxy.start()
            async with httpx.AsyncClient(timeout=6, trust_env=False) as client:
                self.client = client
                for name in ("api", "payment", "inventory", "order", "gateway"):
                    self.spawn(name)
                    await self.ready(name)
                for name in scenarios:
                    await self.run_one(name)
        except BaseException as error:
            self.result.update(status="failed", error_type=type(error).__name__, error=str(error))
            if isinstance(error, (KeyboardInterrupt, asyncio.CancelledError)):
                self.result["interrupted"] = True
        finally:
            errors = []
            for name in ("gateway", "order", "inventory", "payment", "api"):
                try:
                    await self.stop(name)
                except Exception as error:
                    errors.append(f"{name}: {type(error).__name__}")
            try:
                await self.proxy.close()
            except Exception as error:
                errors.append("proxy: " + type(error).__name__)
            for handle in self.handles:
                handle.close()
            self.result["cleanup"] = {"errors":errors,"all_processes_stopped":all(p.poll() is not None for _,p in self.spawned),
                "proxy_tasks_remaining":len(self.proxy.tasks),"processes":[{"name":n,"pid":p.pid,"exit_code":p.poll()} for n,p in self.spawned]}
            if not self.result["cleanup"]["all_processes_stopped"] or self.proxy.tasks:
                errors.append("incomplete process/proxy cleanup")
            self.result["ended_at_ms"] = int(time.time()*1000)
            counts = {key:sum(s["status"] == key for s in self.result["scenarios"]) for key in ("passed","failed","skipped","blocked")}
            self.result["counts"] = counts
            if self.result["status"] != "failed":
                self.result["status"] = "failed" if counts["failed"] or errors else "partial" if counts["blocked"] or counts["skipped"] else "passed"
            self.save()
            # Write an immutable terminal record, distinct from progress checkpoints.
            with (self.output/"completed.json").open("x", encoding="utf-8") as terminal:
                json.dump(self.result, terminal, ensure_ascii=False, indent=2)
                terminal.flush()
                os.fsync(terminal.fileno())
            print(json.dumps({"status":self.result["status"], "counts":counts,
                              "cleanup":self.result["cleanup"]}), flush=True)
            with (self.output/"scenarios.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["name","status","started_at_ms","ended_at_ms","expected"])
                writer.writeheader()
                writer.writerows({key:s[key] for key in writer.fieldnames} for s in self.result["scenarios"])
        return 1 if self.result["status"] == "failed" else 2 if self.result["status"] == "partial" else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--scenario", choices=["all",*SCENARIOS], default="all")
    parser.add_argument("--allow-resource-pressure", action="store_true")
    parser.add_argument("--kafka-home", help="Optional Kafka binary directory; starts an owned loopback broker")
    args = parser.parse_args()
    suite = Suite(args.output,args.allow_resource_pressure,args.kafka_home)
    # asyncio.run handles Ctrl+C; SIGTERM requests the same bounded cleanup.
    async def execute():
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        if hasattr(loop,"add_signal_handler"):
            try:
                loop.add_signal_handler(signal.SIGTERM, task.cancel)
            except NotImplementedError:
                pass
        return await suite.run(list(SCENARIOS) if args.scenario == "all" else [args.scenario])
    raise SystemExit(asyncio.run(execute()))


if __name__ == "__main__":
    main()
