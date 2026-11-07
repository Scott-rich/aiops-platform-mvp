"""Loopback-only TCP delay proxy. No tc/iptables, no host network mutations."""
import asyncio
import ipaddress
import time


def require_loopback(host):
    try:
        valid = ipaddress.ip_address(host).is_loopback
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("numeric loopback address required")


class DelayProxy:
    def __init__(self, target_host, target_port, *, max_connections=32):
        require_loopback(target_host)
        if not 1 <= target_port <= 65535 or not 1 <= max_connections <= 32:
            raise ValueError("invalid bounded proxy settings")
        self.target = (target_host, target_port)
        self.max_connections = max_connections
        self.server = None
        self.tasks = set()
        self.delay_ms, self.expires = 0, 0
        self.generation = 0
        self.connections = self.forwarded_bytes = self.delayed_connections = 0

    async def start(self):
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    def set_delay(self, delay_ms, duration_seconds):
        if not 0 <= delay_ms <= 2000 or not 0 < duration_seconds <= 30:
            raise ValueError("proxy delay 0..2000ms, duration 0..30s required")
        self.delay_ms, self.expires = delay_ms, time.monotonic() + duration_seconds
        self.generation += 1

    def clear(self):
        self.delay_ms, self.expires = 0, 0
        self.generation += 1

    async def _accept(self, reader, writer):
        task = asyncio.current_task()
        upstream = None
        if len(self.tasks) >= self.max_connections:
            writer.close()
            await writer.wait_closed()
            return
        self.tasks.add(task)
        self.connections += 1
        try:
            async with asyncio.timeout(15):
                source, upstream = await asyncio.open_connection(*self.target)
                async def pump(src, dst, inject=False):
                    while data := await src.read(16384):
                        if inject and time.monotonic() < self.expires and self.delay_ms:
                            self.delayed_connections += 1
                            generation = self.generation
                            until = min(time.monotonic()+self.delay_ms/1000, self.expires)
                            while time.monotonic() < until and self.generation == generation:
                                await asyncio.sleep(min(0.02, max(0, until-time.monotonic())))
                        self.forwarded_bytes += len(data)
                        dst.write(data)
                        await dst.drain()
                pumps = [asyncio.create_task(pump(reader, upstream, True)),
                         asyncio.create_task(pump(source, writer))]
                try:
                    await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for item in pumps:
                        item.cancel()
                    await asyncio.gather(*pumps, return_exceptions=True)
        except (OSError, TimeoutError):
            pass  # Connection failure is observable by the calling service.
        finally:
            for stream in (upstream, writer):
                if stream:
                    stream.close()
                    try:
                        await stream.wait_closed()
                    except (OSError, asyncio.CancelledError):
                        pass
            self.tasks.discard(task)

    async def close(self):
        self.clear()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
