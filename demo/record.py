"""Capture real stdout/stderr timestamps as an asciicast-v2 file, without generated outputs."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("command", nargs=argparse.REMAINDER)
a = parser.parse_args()
if not a.command:
    parser.error("command required after --")
command = a.command[1:] if a.command[0] == "--" else a.command
a.output.mkdir(parents=True, exist_ok=False)
start = time.monotonic()
with (a.output / "session.cast").open("x") as cast, (a.output / "session.log").open("x") as log:
    cast.write(
        json.dumps(
            {
                "version": 2,
                "width": 120,
                "height": 30,
                "timestamp": int(time.time()),
                "env": {"TERM": "xterm-256color"},
                "title": "AIOps live local research demonstration",
            }
        )
        + "\n"
    )
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=dict(os.environ, PYTHONUNBUFFERED="1"),
    )
    try:
        for line in process.stdout:
            elapsed = time.monotonic() - start
            cast.write(
                json.dumps([elapsed, "o", line.replace("\n", "\r\n")], ensure_ascii=False) + "\n"
            )
            cast.flush()
            log.write(line)
            log.flush()
        code = process.wait()
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=45)
    result = {
        "command": command,
        "exit_code": code,
        "elapsed_seconds": time.monotonic() - start,
        "capture": "actual subprocess stdout/stderr with monotonic timestamps; no pre-recorded response replay",
        "cast_sha256": hashlib.sha256((a.output / "session.cast").read_bytes()).hexdigest(),
        "recorder_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    (a.output / "recording.json").write_text(json.dumps(result, indent=2) + "\n")
sys.exit(code)
