"""Start the local operational UI +4 real HTTP services; Ctrl+C stops children."""
import argparse
import os
import re
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import httpx
from app.llm_config import ENV_KEYS
from app.runtime_support import require_supported_runtime, UnsupportedPythonRuntime
from flink.adaptive import ENV_KEYS as DETECTION_ENV_KEYS

SERVICES = ("gateway", "order", "inventory", "payment")
MODEL_ENV_KEYS = ENV_KEYS
from app.runtime_config import ENV_KEYS as LOCAL_CONFIG_KEYS, RuntimeConfig
PROVIDER_CREDENTIAL_KEYS = {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY",
                            "GOOGLE_API_KEY", "AZURE_OPENAI_API_KEY"}


def platform_environment(inherited, config):
    """Read model configuration without evaluating shell; inherited values win."""
    environment = dict(inherited)
    if config.exists():
        for line in config.read_text().splitlines():
            key, sep, value = line.partition("=")
            if sep and key.strip() in MODEL_ENV_KEYS | DETECTION_ENV_KEYS | LOCAL_CONFIG_KEYS | {"SIMULATOR_CONTROL_TOKEN", "SIMULATOR_FAULT_AUDIT_DIR", "SIMULATOR_DOWNSTREAM_TIMEOUT_SECONDS"}:
                environment.setdefault(key.strip(), value.strip().strip("\"'"))
    return environment


def simulator_environment(common):
    """Business simulators/loadgen do not need model configuration or credentials."""
    # Only the explicit simulator control token needs to cross this boundary.
    # Credential names vary across vendors; filter common generic forms as well.
    def allowed(key):
        name = key.upper()
        if name in {"SIMULATOR_CONTROL_TOKEN", "TELEMETRY_INGEST_TOKEN"}:
            return True
        return (not key.lower().endswith("_proxy")
                and not name.startswith("LLM_")
                and name not in PROVIDER_CREDENTIAL_KEYS
                and not re.search(r"(?:^|_)(?:SECRET|PASSWORD|PASSWD|TOKEN|CREDENTIALS?|PRIVATE_KEY|API_KEY)(?:_|$)", name))
    return {key: value for key, value in common.items() if allowed(key)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--base-port", type=int, default=18081)
    parser.add_argument("--data-dir", default="data-local")
    parser.add_argument("--rps", type=float, default=3)
    parser.add_argument("--no-load", action="store_true")
    args = parser.parse_args()
    try:
        require_supported_runtime()
    except UnsupportedPythonRuntime as exc:
        parser.error(str(exc))
    if not 0 < args.rps <= 100:
        parser.error("0<rps<=100 required")
    root = Path(__file__).resolve().parent.parent
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    urls = {name: f"http://127.0.0.1:{args.base_port+i}" for i, name in enumerate(SERVICES)}
    for port in [args.port, *range(args.base_port, args.base_port+4)]:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
    common = platform_environment(os.environ, root/".env")
    RuntimeConfig.from_env(common)  # Fail before launching any child processes.
    common.update(DATA_DIR=str(data_dir), EXECUTION_MODE="local-http-python-rules")
    common.setdefault("SIMULATOR_FAULT_AUDIT_DIR", str(data_dir/"fault-audit"))
    common.update({name.upper()+"_URL": url for name, url in urls.items()})
    common["NO_PROXY"] = "127.0.0.1,localhost"
    common["no_proxy"] = "127.0.0.1,localhost"
    children, logs = [], []
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    def spawn(name, command, env):
        log = (data_dir/(name+".log")).open("a", encoding="utf-8")
        logs.append(log)
        child = subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        children.append(child)
    def uvicorn(module, port):
        return [sys.executable, "-m", "uvicorn", module, "--no-proxy-headers", "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning", "--no-access-log"]
    try:
        spawn("api", uvicorn("app.local_api:app", args.port), common)
        for service in reversed(SERVICES):
            env = simulator_environment(common)
            env.update(SERVICE_NAME=service, TELEMETRY_MODE="http", TELEMETRY_HTTP_URL=f"http://127.0.0.1:{args.port}/demo/ingest")
            spawn(service, uvicorn("simulator.service:app", int(urls[service].rsplit(":", 1)[1])), env)
        health_headers = {"Authorization":"Bearer "+common["API_READ_TOKEN"]} if common.get("API_AUTH_MODE")=="token" else {}
        with httpx.Client(timeout=1, trust_env=False) as client:
            ready = False
            for _ in range(100):
                if stopped:
                    break
                try:
                    ready = all(client.get(url+"/health", headers=health_headers if url==f"http://127.0.0.1:{args.port}" else {}).status_code == 200 for url in [f"http://127.0.0.1:{args.port}", *urls.values()])
                except httpx.HTTPError:
                    ready = False
                if ready:
                    break
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("a process failed; inspect logs in "+str(data_dir))
                time.sleep(0.2)
            if not ready:
                raise RuntimeError("startup timed out; inspect logs in "+str(data_dir))
        if not args.no_load:
            env = simulator_environment(common)
            env.update(GATEWAY_URL=urls["gateway"], LOAD_RPS=str(args.rps))
            spawn("loadgen", [sys.executable, "-m", "simulator.loadgen"], env)
        print(f"控制台已启动：http://127.0.0.1:{args.port}\n模式：真实本机HTTP + Python检测规则；未运行Kafka/Flink。\n按Ctrl+C停止，数据保留在{data_dir}", flush=True)
        while not stopped:
            if any(child.poll() is not None for child in children):
                raise RuntimeError("a process exited; inspect logs in "+str(data_dir))
            time.sleep(0.5)
    finally:
        for child in reversed(children):
            child.terminate()
        for child in reversed(children):
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
