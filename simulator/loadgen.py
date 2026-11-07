"""A bounded concurrent HTTP load generator; start after the service graph."""

import asyncio
from collections import Counter
import json
import os
import signal
import time

import httpx


async def main():
    url = os.getenv("GATEWAY_URL", "http://gateway:8000").rstrip("/") + "/work"
    rps = float(os.getenv("LOAD_RPS", "5"))
    max_concurrency = int(os.getenv("LOAD_MAX_CONCURRENCY", "50"))
    if not 0 < rps <= 100 or not 1 <= max_concurrency <= 200:
        raise ValueError("LOAD_RPS must be 0..100 and LOAD_MAX_CONCURRENCY 1..200")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    pending = set()
    counts = Counter()
    async with httpx.AsyncClient(timeout=12, trust_env=False,
                                limits=httpx.Limits(max_connections=max_concurrency)) as client:
        async def request():
            try:
                response = await client.get(url)
                counts[str(response.status_code)] += 1
            except httpx.RequestError:
                counts["connection_or_timeout"] += 1
        last_print = time.monotonic()
        while not stop.is_set():
            if len(pending) < max_concurrency:
                task = asyncio.create_task(request())
                pending.add(task)
                task.add_done_callback(pending.discard)
            else:
                counts["skipped_at_concurrency_limit"] += 1
            if time.monotonic() - last_print >= 5:
                print(json.dumps({"loadgen": dict(counts), "in_flight": len(pending)}), flush=True)
                last_print = time.monotonic()
            try:
                await asyncio.wait_for(stop.wait(), timeout=1 / rps)
            except asyncio.TimeoutError:
                pass
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
