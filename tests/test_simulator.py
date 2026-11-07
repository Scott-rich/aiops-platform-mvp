"""Exercise real HTTP request handling across four in-process service apps.

ASGI transport replaces the network only; the application routes, dependency
HTTP calls, fault controls, middleware, and telemetry are the production code.
Kafka is explicitly replaced by the simulator's memory sink.
"""

from contextlib import AsyncExitStack
import json
import os
import unittest
from unittest.mock import patch
from urllib.error import URLError

import httpx

from simulator.service import Settings, create_app
from simulator.telemetry import Telemetry


SERVICES = ("gateway", "order", "inventory", "payment")


class ServiceRouter(httpx.AsyncBaseTransport):
    def __init__(self):
        self.apps = {}

    async def handle_async_request(self, request):
        service = request.url.host
        if service not in self.apps:
            raise httpx.ConnectError("unknown test service", request=request)
        transport = httpx.ASGITransport(app=self.apps[service])
        return await transport.handle_async_request(request)


class TelemetryHTTPTests(unittest.TestCase):
    def test_http_envelope_success_and_failure_counts(self):
        event = {"schema_version": 1, "event_id": "http-example",
                 "service": "payment", "event_type": "request",
                 "latency_ms": 100.0, "status_code": 200}
        with patch.dict(os.environ, {"TELEMETRY_HTTP_URL": "http://localhost:18100/demo/ingest", "TELEMETRY_INGEST_TOKEN":"test-ingest-token"}):
            sink = Telemetry(mode="http")
        sink.start()
        with patch("simulator.telemetry.urllib.request.build_opener") as opener:
            send = opener.return_value.open
            send.return_value.__enter__.return_value.status = 202
            sink.publish("telemetry.metrics", event)
            request = send.call_args.args[0]
            self.assertEqual(request.get_header("Authorization"), "Bearer test-ingest-token")
            self.assertEqual(opener.call_args.args[0].proxies, {})
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "http://localhost:18100/demo/ingest")
            self.assertEqual(request.get_header("Content-type"), "application/json")
            self.assertEqual(json.loads(request.data),
                             {"topic": "telemetry.metrics", "event": event})
            self.assertEqual(send.call_args.kwargs["timeout"], 3)
            self.assertEqual((sink.sent, sink.failed), (1, 0))
            send.side_effect = URLError("demo coordinator unavailable")
            with self.assertLogs("simulator.telemetry", level="ERROR"):
                sink.publish("telemetry.metrics", event)
            self.assertEqual((sink.sent, sink.failed), (1, 1))
        sink.close()


class SimulatorHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = AsyncExitStack()
        self.router = ServiceRouter()
        self.sinks = {name: Telemetry(mode="memory") for name in SERVICES}
        for name in SERVICES:
            settings = Settings(
                service_name=name, instance_id=f"test-{name}",
                downstream_urls={service: f"http://{service}"
                                 for service in ("order", "inventory", "payment")})
            self.router.apps[name] = create_app(settings, self.sinks[name], self.router)
        for app in self.router.apps.values():
            await self.stack.enter_async_context(app.router.lifespan_context(app))
        self.client = await self.stack.enter_async_context(
            httpx.AsyncClient(transport=self.router, base_url="http://gateway"))

    async def asyncTearDown(self):
        await self.stack.aclose()

    def events(self, service, topic=None):
        return [event for event_topic, event in self.sinks[service].events
                if topic is None or event_topic == topic]

    def metric(self, service):
        return self.events(service, "telemetry.metrics")[-1]

    async def test_four_service_trace_and_parent_links(self):
        response = await self.client.get("/work")
        self.assertEqual(response.status_code, 200)
        trace_id = response.headers["x-trace-id"]
        self.assertEqual(response.json()["trace_id"], trace_id)
        spans = {}
        all_ids = []
        for name in SERVICES:
            events = self.events(name)
            self.assertEqual(len(events), 3, name)
            self.assertTrue(all(event["trace_id"] == trace_id for event in events))
            self.assertTrue(all(event["service"] == name for event in events))
            self.assertTrue(all(event["schema_version"] == 1 for event in events))
            self.assertTrue(all(isinstance(event["event_time"], int) for event in events))
            self.assertEqual(self.metric(name)["status_code"], 200)
            spans[name] = self.events(name, "telemetry.spans")[0]
            all_ids.extend(event["event_id"] for event in events)
        self.assertEqual(len(all_ids), len(set(all_ids)))
        self.assertIsNone(spans["gateway"]["parent_span_id"])
        self.assertEqual(spans["order"]["parent_span_id"], spans["gateway"]["span_id"])
        for leaf in ("inventory", "payment"):
            self.assertEqual(spans[leaf]["parent_span_id"], spans["order"]["span_id"])
            self.assertEqual(spans[leaf]["downstream"], [])
        self.assertEqual(spans["gateway"]["downstream"], ["order"])
        self.assertEqual(spans["order"]["downstream"], ["inventory", "payment"])
        self.assertGreaterEqual(self.metric("gateway")["latency_ms"],
                                self.metric("payment")["latency_ms"])

    async def test_payment_error_propagates_and_reset_restores(self):
        injected = await self.client.post("http://payment/fault", json={
            "kind": "error", "duration_seconds": 120, "error_rate": 1.0})
        self.assertEqual(injected.status_code, 200)
        response = await self.client.get("/work")
        self.assertEqual(response.status_code, 502)
        self.assertEqual(self.metric("payment")["status_code"], 503)
        self.assertEqual(self.metric("order")["status_code"], 502)
        self.assertEqual(self.metric("gateway")["status_code"], 502)
        self.assertEqual(self.metric("inventory")["status_code"], 200)
        order_log = self.events("order", "telemetry.logs")[-1]
        self.assertIn("payment returned HTTP 503", order_log["message"])
        self.assertEqual(order_log["dependency_service"], "payment")
        cleared = await self.client.post("http://payment/fault", json={"kind": "none"})
        self.assertEqual(cleared.json()["kind"], "none")
        restored = await self.client.get("/work")
        self.assertEqual(restored.status_code, 200)
        for name in SERVICES:
            self.assertEqual(self.metric(name)["status_code"], 200)

    async def test_leaf_latency_is_visible_in_leaf_and_gateway(self):
        injected = await self.client.post("http://payment/fault", json={
            "kind": "latency", "duration_seconds": 120, "latency_ms": 100})
        self.assertEqual(injected.status_code, 200)
        response = await self.client.get("/work")
        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(self.metric("payment")["latency_ms"], 90)
        self.assertGreaterEqual(self.metric("gateway")["latency_ms"], 90)

    async def test_injected_control_state_is_not_telemetry_evidence(self):
        await self.client.post("http://payment/fault", json={
            "kind": "error", "duration_seconds": 120, "error_rate": 1.0})
        response = await self.client.get("/work")
        self.assertEqual(response.status_code, 502)
        prohibited_keys = {"kind", "fault", "fault_kind", "injected_fault",
                           "duration_seconds", "remaining_seconds", "error_rate"}
        for name in SERVICES:
            for event in self.events(name):
                self.assertFalse(prohibited_keys.intersection(event), event)
                serialized = json.dumps(event).lower()
                self.assertNotIn("injected", serialized)
                self.assertNotIn("fault", serialized)

    async def test_controls_validate_and_do_not_generate_request_metrics(self):
        for payload in ({"kind": "cpu_burn"},
                        {"kind": "latency", "duration_seconds": 601},
                        {"kind": "latency", "latency_ms": 10001},
                        {"kind": "error", "error_rate": 1.1},
                        {"kind": "error", "unexpected": "field"}):
            with self.subTest(payload=payload):
                response = await self.client.post("http://payment/fault", json=payload)
                self.assertEqual(response.status_code, 422)
        state = await self.client.get("http://payment/fault")
        health = await self.client.get("http://payment/health")
        self.assertEqual(state.json()["kind"], "none")
        self.assertEqual(health.status_code, 200)
        for name in SERVICES:
            self.assertEqual(self.events(name), [])


if __name__ == "__main__":
    unittest.main()
