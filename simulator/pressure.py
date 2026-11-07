"""Opt-in bounded Linux worker. Never stress an existing process or the host."""
import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

MAX_SECONDS = 10
MAX_MIB = 32
MAX_DUTY = 0.25


def validate(kind, seconds, memory_mib, duty):
    if kind not in {"cpu", "memory"} or not math.isfinite(seconds) or not 0.2 <= seconds <= MAX_SECONDS:
        raise ValueError("kind cpu/memory, duration 0.2..10s required")
    if type(memory_mib) is not int or not 1 <= memory_mib <= MAX_MIB:
        raise ValueError("memory allocation must be 1..32MiB")
    if not math.isfinite(duty) or not 0.01 <= duty <= MAX_DUTY:
        raise ValueError("CPU duty must be 0.01..0.25 of one core")


def run_pressure(kind, *, allow=False, seconds=1.2, memory_mib=16, duty=0.2):
    validate(kind, seconds, memory_mib, duty)
    if not allow:
        raise PermissionError("resource pressure disabled; explicit --allow-resource-pressure required")
    if sys.platform != "linux":
        raise RuntimeError("Linux resource limits required")
    command = [sys.executable, "-m", "simulator.pressure", "--worker", "--allow-resource-pressure",
               "--kind", kind, "--seconds", str(seconds), "--memory-mib", str(memory_mib), "--duty", str(duty)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               cwd=Path(__file__).resolve().parents[1], start_new_session=True,
                               env={"PATH": os.defpath, "PYTHONUNBUFFERED": "1"})
    try:
        stdout, stderr = process.communicate(timeout=seconds+3)
        if process.returncode:
            raise RuntimeError(f"pressure worker failed ({process.returncode}): {stderr[-1000:]}")
        result = json.loads(stdout)
        result.update(exit_code=process.returncode, process_reaped=process.poll() is not None)
        return result
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)


def worker(args):
    if not args.allow_resource_pressure or sys.platform != "linux":
        raise PermissionError("Linux and explicit opt-in required")
    validate(args.kind, args.seconds, args.memory_mib, args.duty)
    import resource
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (192*1024**2, 192*1024**2))
    resource.setrlimit(resource.RLIMIT_CPU, (4, 4))
    signal.alarm(math.ceil(args.seconds)+2)  # Independent of the parent process.
    stop = False
    def stopped(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, stopped)
    started_at = int(time.time()*1000)
    start, cpu_start = time.monotonic(), time.process_time()
    def resident_bytes():
        return int(Path("/proc/self/statm").read_text().split()[1])*os.sysconf("SC_PAGE_SIZE")
    rss_before = resident_bytes()
    memory = None
    allocated = 0
    if args.kind == "memory":
        allocated = args.memory_mib*1024**2
        memory = bytearray(allocated)
        for offset in range(0, len(memory), 4096):
            memory[offset] = 1
    rss_after_allocation = resident_bytes()
    while not stop and time.monotonic()-start < args.seconds:
        frame = time.monotonic()
        if args.kind == "cpu":
            until = min(frame+0.05*args.duty, start+args.seconds)
            while time.monotonic() < until:
                _ = sum(i*i for i in range(32))
        time.sleep(max(0, min(0.05-(time.monotonic()-frame), start+args.seconds-time.monotonic())))
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    del memory
    signal.alarm(0)
    print(json.dumps({"kind": args.kind, "scope": "dedicated-child-process", "pid": os.getpid(),
        "started_at_ms": started_at, "ended_at_ms": int(time.time()*1000),
        "elapsed_seconds": time.monotonic()-start, "cpu_seconds": time.process_time()-cpu_start,
        "rss_before_bytes": rss_before, "rss_after_allocation_bytes": rss_after_allocation, "peak_rss_bytes": peak, "allocated_bytes": allocated,
        "address_space_limit_bytes": 192*1024**2, "cpu_time_limit_seconds": 4,
        "duty_target": args.duty, "end_reason": "terminated" if stop else "duration_elapsed"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=["cpu", "memory"], required=True)
    parser.add_argument("--seconds", type=float, default=1.2)
    parser.add_argument("--memory-mib", type=int, default=16)
    parser.add_argument("--duty", type=float, default=0.2)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--allow-resource-pressure", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker(args)
    else:
        print(json.dumps(run_pressure(args.kind, allow=args.allow_resource_pressure, seconds=args.seconds,
                                      memory_mib=args.memory_mib, duty=args.duty)))


if __name__ == "__main__":
    main()
