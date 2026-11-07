"""Bounded, idempotent submission; explicit restore is separate from replay."""
import json
import logging
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

log = logging.getLogger(__name__)
ACTIVE = {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING", "RECONCILING"}


def get(base, path):
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(base + path, timeout=3) as response:
        return json.load(response)


def command(environ):
    cli = environ.get("FLINK_CLI", "/opt/flink/bin/flink")
    endpoint = environ.get("FLINK_REST_URL", "http://jobmanager:8081").rstrip("/")
    job = environ.get("FLINK_JOB_FILE", "/opt/flink/jobs/detector.py")
    files = environ.get("FLINK_PY_FILES", "/opt/flink/jobs/rules.py,/opt/flink/jobs/aggregate.py,/opt/flink/jobs/adaptive.py,/opt/flink/jobs/stream_support.py,/opt/flink/jobs/wire_contract.py")
    args = [cli, "run", "-m", endpoint, "-d"]
    restore = environ.get("FLINK_RESTORE_PATH", "").strip()
    if restore:
        args += ["-s", restore]
    return args + ["-py", job, "-pyfs", files]


def wait_ready(base, name, deadline, fetch=get, sleep=time.sleep):
    last_error = "no TaskManager"
    while time.monotonic() < deadline:
        try:
            jobs = fetch(base, "/jobs/overview").get("jobs", [])
            active = [job for job in jobs if job.get("name") == name and job.get("state") in ACTIVE]
            if len(active) > 1:
                raise RuntimeError("multiple active detector jobs; manual inspection required")
            if active:
                return active[0]
            if fetch(base, "/overview").get("taskmanagers", 0) > 0:
                return None
        except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError, OSError) as exc:
            last_error = type(exc).__name__
        sleep(1)
    raise TimeoutError(f"Flink did not become ready: {last_error}")


def wait_running(base, name, deadline, fetch=get, sleep=time.sleep):
    while time.monotonic() < deadline:
        try:
            jobs = fetch(base, "/jobs/overview").get("jobs", [])
            matched = [job for job in jobs if job.get("name") == name and job.get("state") in ACTIVE]
            if len(matched) > 1:
                raise RuntimeError("multiple active detector jobs; manual inspection required")
            if len(matched) == 1 and matched[0]["state"] == "RUNNING":
                tasks = matched[0].get("tasks", {})
                if tasks.get("total", 0) > 0 and tasks.get("running", 0) == tasks["total"]:
                    return matched[0]
            if not matched and any(job.get("name") == name and job.get("state") == "FAILED" for job in jobs):
                raise RuntimeError("detector failed; inspect Flink exceptions endpoint/logs")
        except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError, OSError) as exc:
            log.warning("Waiting for detector type=%s", type(exc).__name__)
        sleep(1)
    raise TimeoutError("detector did not reach RUNNING within deadline")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    base = os.getenv("FLINK_REST_URL", "http://jobmanager:8081").rstrip("/")
    name = os.getenv("FLINK_JOB_NAME", "aiops-event-time-detector-v3")
    timeout = int(os.getenv("FLINK_STARTUP_TIMEOUT_SECONDS", "120"))
    if not 5 <= timeout <= 600:
        raise ValueError("FLINK_STARTUP_TIMEOUT_SECONDS must be 5..600")
    existing = wait_ready(base, name, time.monotonic()+timeout)
    if existing:
        if os.getenv("FLINK_RESTORE_PATH"):
            raise RuntimeError("restore requested while detector active; stop it explicitly first")
        log.info("Detector already active jid=%s state=%s", existing["jid"], existing["state"])
    else:
        args = command(os.environ)
        if not Path(args[0]).is_file():
            raise FileNotFoundError("FLINK_CLI does not exist")
        subprocess.run(args, check=True, timeout=timeout)
    running = wait_running(base, name, time.monotonic()+timeout)
    log.info("Detector RUNNING jid=%s restore=%s", running["jid"], bool(os.getenv("FLINK_RESTORE_PATH")))


if __name__ == "__main__":
    main()
