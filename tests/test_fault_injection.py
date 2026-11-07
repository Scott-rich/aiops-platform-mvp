import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx
from simulator.faults import FaultState
from simulator.network import DelayProxy, require_loopback
from simulator.pressure import run_pressure, validate
from simulator.service import Settings, create_app
from simulator.telemetry import Telemetry
from simulator.scenarios import message_cases
from wire_contract import validate_event


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.clock = 10.0
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/"audit.jsonl"
        self.state = FaultState(lambda:self.clock, service="payment", audit_path=self.path,
                                wall_clock=lambda:1000+self.clock)

    def test_expiry_and_persistent_scope(self):
        first = self.state.set("timeout", 2, 1000, 1)
        self.clock += 3
        self.assertEqual(self.state.snapshot().kind, "none")
        rows = [json.loads(line) for line in self.path.read_text().splitlines()]
        self.assertEqual([r["action"] for r in rows], ["started","ended"])
        self.assertEqual(rows[1]["fault_id"], first.fault_id)
        self.assertEqual(rows[1]["end_reason"], "expired")
        self.assertEqual(rows[0]["scope"], ["payment:/work"])
        self.assertGreater(rows[1]["ended_at_ms"],rows[0]["started_at_ms"])

    def test_replace_clear_and_shutdown(self):
        self.state.set("latency",2,10,1)
        self.state.set("error",2,0,1)
        self.state.set("none",2,0,0)
        self.state.set("unavailable",2,0,1)
        self.state.close()
        self.assertEqual([r["end_reason"] for r in self.state.history() if r["action"] == "ended"],
                         ["replaced","cleared","shutdown"])
        self.assertEqual(self.state.snapshot().kind,"none")

    def test_history_bounded(self):
        for _ in range(150):
            self.state.set("error",1,0,1)
        self.state.close()
        self.assertEqual(len(self.state.history()),256)
        self.assertEqual(len(self.path.read_text().splitlines()),300)

    def test_invalid_injection_cannot_replace_current(self):
        original=self.state.set("error",2,0,1)
        for args in [("cpu",1,0,1),("error",601,0,1),("error",2,0,float('nan'))]:
            with self.assertRaises(ValueError):self.state.set(*args)
        self.assertEqual(self.state.snapshot().fault_id,original.fault_id)

    def test_audit_write_failure_fail_closed(self):
        with patch.object(self.state,"_record",side_effect=OSError("disk full")):
            with self.assertRaises(OSError):self.state.set("error",1,0,1)
        self.assertEqual(self.state.snapshot().kind,"none")

    def test_failed_end_audit_still_clears(self):
        self.state.set("error",1,0,1)
        with patch.object(self.state,"_record",side_effect=OSError("disk full")):
            with self.assertRaises(OSError):self.state.close()
        self.assertEqual(self.state.snapshot().kind,"none")


class BoundsTests(unittest.TestCase):
    def test_pressure_default_off(self):
        with self.assertRaises(PermissionError):run_pressure("cpu")

    def test_resource_limits_validate(self):
        for args in [("cpu",11,16,.2),("memory",1,33,.2),("cpu",1,16,.9),("cpu",float('nan'),16,.2)]:
            with self.assertRaises(ValueError):validate(*args)

    def test_loopback_only(self):
        for host in ["localhost","0.0.0.0","example.com","10.0.0.1","::"]:
            with self.assertRaises(ValueError):require_loopback(host)
        require_loopback("127.0.0.1")
        require_loopback("::1")

    def test_fixture_contract_and_reproducibility(self):
        import time
        epoch=int(time.time()*1000)
        data=message_cases(epoch)
        self.assertEqual(data,message_cases(epoch))
        for row in [*data["valid"],*data["duplicates"],data["conflict"]]:validate_event(row)
        for row in data["invalid"]:
            with self.assertRaises((ValueError,UnicodeError)):validate_event(json.loads(row))


class ServiceControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = create_app(Settings(service_name="payment",control_token="test-token"),Telemetry(mode="memory"))
        self.life = self.app.router.lifespan_context(self.app)
        await self.life.__aenter__()
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),base_url="http://payment")

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.life.__aexit__(None,None,None)

    async def fault(self,body):
        return await self.client.post("/fault",json=body,headers={"x-simulator-token":"test-token"})

    async def test_control_token_is_required(self):
        self.assertEqual((await self.client.post("/fault",json={"kind":"error"})).status_code,403)
        self.assertEqual((await self.client.get("/fault/history")).status_code,403)
        self.assertEqual((await self.client.get("/health")).status_code,200)

    async def test_clear_cancels_pending_delay(self):
        await self.fault({"kind":"timeout","latency_ms":2000})
        request=asyncio.create_task(self.client.get("/work"))
        await asyncio.sleep(.08)
        await self.fault({"kind":"none"})
        response=await asyncio.wait_for(request,.5)
        self.assertEqual(response.status_code,200)

    async def test_unavailable_leaves_health_usable(self):
        await self.fault({"kind":"unavailable"})
        self.assertEqual((await self.client.get("/work")).status_code,503)
        self.assertEqual((await self.client.get("/health")).status_code,200)

    async def test_expiry_without_traffic(self):
        await self.fault({"kind":"error","duration_seconds":1})
        await asyncio.sleep(1.2)
        self.assertEqual(self.app.state.faults._kind,"none")
        self.assertEqual(self.app.state.faults.history()[-1]["end_reason"],"expired")

    async def test_audit_failure_returns_503(self):
        with patch.object(self.app.state.faults,"_record",side_effect=OSError("disk full")):
            with self.assertLogs("simulator.service",level="ERROR"):
                response=await self.fault({"kind":"error"})
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.app.state.faults.snapshot().kind,"none")


class NetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_tcp_delay_clear_and_teardown(self):
        writers=set()
        async def echo(reader,writer):
            writers.add(writer)
            try:
                while data:=await reader.read(100):
                    writer.write(data);await writer.drain()
            finally:
                writer.close();await writer.wait_closed();writers.discard(writer)
        server=await asyncio.start_server(echo,"127.0.0.1",0)
        proxy=DelayProxy("127.0.0.1",server.sockets[0].getsockname()[1])
        port=await proxy.start()
        try:
            reader,writer=await asyncio.open_connection("127.0.0.1",port)
            proxy.set_delay(100,1)
            before=asyncio.get_running_loop().time()
            writer.write(b"first");await writer.drain()
            self.assertEqual(await reader.readexactly(5),b"first")
            self.assertGreaterEqual(asyncio.get_running_loop().time()-before,.08)
            proxy.set_delay(1000,2)
            writer.write(b"next");await writer.drain()
            await asyncio.sleep(.05);proxy.clear()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(4),.4),b"next")
            writer.close();await writer.wait_closed()
        finally:
            await proxy.close();server.close();await server.wait_closed()
            for writer in list(writers):writer.close()
        self.assertEqual(len(proxy.tasks),0)

class ConfigurationTests(unittest.TestCase):
    def test_local_config_includes_only_named_simulator_settings(self):
        from scripts.local_platform import platform_environment
        with tempfile.TemporaryDirectory() as folder:
            config=Path(folder)/'.env'
            config.write_text('SIMULATOR_CONTROL_TOKEN=local\nSIMULATOR_DOWNSTREAM_TIMEOUT_SECONDS=1.5\nUNRELATED_SECRET=hidden\n')
            actual=platform_environment({'SIMULATOR_CONTROL_TOKEN':'inherited'},config)
        self.assertEqual(actual['SIMULATOR_CONTROL_TOKEN'],'inherited')
        self.assertEqual(actual['SIMULATOR_DOWNSTREAM_TIMEOUT_SECONDS'],'1.5')
        self.assertNotIn('UNRELATED_SECRET',actual)

    def test_two_services_require_two_window_accumulators(self):
        from flink.aggregate import WindowAccumulator
        import time
        cases=message_cases(int(time.time()*1000))
        rows=[*cases['valid'],*cases['duplicates'],cases['conflict'],
              {**cases['valid'][0],'service':'second','event_id':'second-1'}]
        accumulators={}
        for row in rows:
            accumulators.setdefault(row['service'],WindowAccumulator()).add_or_quarantine(row)
        self.assertEqual(sum(a.request_count for a in accumulators.values()),6)
        self.assertEqual(sum(a.quarantine_count for a in accumulators.values()),1)
