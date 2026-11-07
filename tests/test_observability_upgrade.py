"""Runtime compatibility, bounded metric labels and limiter isolation tests."""

from __future__ import annotations

import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.observability import RateConfig, TokenBuckets, install_features


class BucketTests(unittest.TestCase):
    def test_budget_refill_and_backward_clock(self) -> None:
        buckets = TokenBuckets(RateConfig(rate=2, burst=2, max_keys=2))
        self.assertTrue(buckets.allow(("ip", "route"), 10))
        self.assertTrue(buckets.allow(("ip", "route"), 10))
        self.assertFalse(buckets.allow(("ip", "route"), 9))
        self.assertTrue(buckets.allow(("ip", "route"), 10.5))
        self.assertFalse(buckets.allow(("ip", "route"), 10.5))

    def test_bucket_storage_bounded(self) -> None:
        buckets = TokenBuckets(RateConfig(max_keys=2))
        for i in range(20):
            buckets.allow((str(i), "/route"), 0)
        self.assertEqual(len(buckets.entries), 2)

    def test_concurrent_budget_is_atomic(self) -> None:
        buckets = TokenBuckets(RateConfig(rate=1, burst=5))
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: buckets.allow(("ip", "/path"), 0), range(100)))
        self.assertEqual(sum(values), 5)

    def test_invalid_configuration_fails(self) -> None:
        for name, value in [
            ("API_RATE_PER_SECOND", "nan"),
            ("API_RATE_BURST", "0"),
            ("API_RATE_MAX_KEYS", "-1"),
        ]:
            with patch.dict(os.environ, {name: value}), self.assertRaises(ValueError):
                RateConfig.from_env()


def tiny_app() -> FastAPI:
    app = FastAPI()

    @app.get("/live")
    def live() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/items/{item_id}")
    def item(item_id: str) -> dict[str, str]:
        return {"id": item_id}

    @app.post("/items")
    def put(value: dict[str, str]) -> dict[str, str]:
        return value

    install_features(app)
    return app


class APIFeatureTests(unittest.TestCase):
    def test_alias_preserves_parameters_and_body(self) -> None:
        with TestClient(tiny_app()) as client:
            self.assertEqual(client.get("/v1/items/abc").json(), {"id": "abc"})
            self.assertEqual(client.post("/v1/items", json={"value": "x"}).json(), {"value": "x"})
            self.assertEqual(client.get("/items/abc").status_code, 200)
            self.assertEqual(client.get("/items/abc").headers["deprecation"], "true")

    def test_optional_301_preserves_query(self) -> None:
        with (
            patch.dict(os.environ, {"API_LEGACY_GET_REDIRECT": "1"}),
            TestClient(tiny_app()) as client,
        ):
            response = client.get("/items/a?limit=2", follow_redirects=False)
            self.assertEqual(response.status_code, 301)
            self.assertEqual(response.headers["location"], "/v1/items/a?limit=2")
            self.assertEqual(client.post("/items", json={"v": "body"}).status_code, 200)

    def test_rate_limit_uses_direct_ip_and_canonical_route(self) -> None:
        app = tiny_app()
        app.state.monitor.buckets = TokenBuckets(RateConfig(rate=0.01, burst=2))
        with TestClient(app) as client:
            self.assertEqual(client.get("/items/one").status_code, 200)
            self.assertEqual(client.get("/v1/items/two").status_code, 200)
            response = client.get("/items/three", headers={"x-forwarded-for": "different-client"})
            self.assertEqual(response.status_code, 429)
            self.assertIn("retry-after", response.headers)
            self.assertEqual(client.get("/live").status_code, 200)

    def test_metrics_and_label_cardinality(self) -> None:
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict(os.environ, {"DATA_DIR": folder}),
            TestClient(tiny_app()) as client,
        ):
            for i in range(15):
                client.get(f"/unrecognised-{i}")
                client.get(f"/items/{i}")
            text = client.get("/v1/metrics").text
            self.assertIn("aiops_http_request_seconds_bucket", text)
            self.assertIn("aiops_events_stored 0.0", text)
            self.assertIn("aiops_database_bytes", text)
            self.assertNotIn("unrecognised-0", text)
            self.assertIn('route="/items/{item_id}"', text)

    def test_versioned_ingest_keeps_role_boundary(self) -> None:
        from app.local_api import app

        env = {
            "API_AUTH_MODE": "token",
            "API_READ_TOKEN": "r" * 32,
            "API_CONTROL_TOKEN": "c" * 32,
            "TELEMETRY_INGEST_TOKEN": "i" * 32,
            "EXECUTION_MODE": "local-http",
        }
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict(os.environ, {**env, "DATA_DIR": folder}),
            TestClient(app) as client,
        ):
            ingest = {"Authorization": "Bearer " + "i" * 32}
            self.assertEqual(client.get("/v1/metrics", headers=ingest).status_code, 403)
            self.assertEqual(
                client.get(
                    "/v1/metrics", headers={"Authorization": "Bearer " + "r" * 32}
                ).status_code,
                200,
            )
            self.assertEqual(client.get("/v1/live").status_code, 200)
            bad = client.post("/v1/demo/ingest", json={}, headers=ingest)
            self.assertEqual(bad.status_code, 400)
            self.assertEqual(client.post("/v1/demo/ingest", json={}).status_code, 401)


if __name__ == "__main__":
    unittest.main()


class CombinedBoundaryTests(unittest.TestCase):
    def test_auth_precedes_rate_and_redirect_but_all_responses_are_observed(self) -> None:
        from app.api_runtime import install_boundary

        app = FastAPI()

        @app.get("/items")
        def items() -> dict[str, bool]:
            return {"ok": True}

        install_boundary(app)
        install_features(app)
        app.state.monitor.buckets = TokenBuckets(RateConfig(rate=0.001, burst=1))
        env = {
            "API_AUTH_MODE": "token",
            "API_READ_TOKEN": "r" * 32,
            "API_CONTROL_TOKEN": "c" * 32,
            "TELEMETRY_INGEST_TOKEN": "i" * 32,
            "API_LEGACY_GET_REDIRECT": "1",
        }
        with patch.dict(os.environ, env), TestClient(app) as client:
            denied = client.get("/items", follow_redirects=False)
            self.assertEqual(denied.status_code, 401)
            allowed = client.get(
                "/items", headers={"Authorization": "Bearer " + "r" * 32}, follow_redirects=False
            )
            self.assertEqual(allowed.status_code, 301)
            limited = client.get("/v1/items", headers={"Authorization": "Bearer " + "r" * 32})
            self.assertEqual(limited.status_code, 429)
            for response in (denied, allowed, limited):
                self.assertIn("x-request-id", response.headers)
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
            values = list(app.state.monitor.requests.collect())[0].samples
            self.assertTrue(any(s.labels.get("status") == "401" for s in values))


class ServerBoundaryTests(unittest.TestCase):
    def test_packaged_server_ignores_forged_forwarded_client(self) -> None:
        import json
        import socket
        import subprocess
        import sys
        import time
        from pathlib import Path
        import httpx

        root = Path(__file__).resolve().parents[1]
        line = next(
            line
            for line in (root / "Dockerfile").read_text().splitlines()
            if line.startswith("CMD ")
        )
        cmd = json.loads(line[4:])
        self.assertIn("--no-proxy-headers", cmd)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        cmd[cmd.index("--host") + 1] = "127.0.0.1"
        cmd[cmd.index("--port") + 1] = str(port)
        with tempfile.TemporaryDirectory() as folder:
            env = {k: v for k, v in os.environ.items() if not k.startswith(("API_", "TELEMETRY_"))}
            env.update(
                DATA_DIR=folder,
                API_AUTH_MODE="local",
                API_RATE_PER_SECOND="0.001",
                API_RATE_BURST="1",
            )
            child = subprocess.Popen(
                [sys.executable, "-m", *cmd],
                cwd=root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                with httpx.Client(
                    base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=5
                ) as client:
                    for _ in range(100):
                        try:
                            if client.get("/live").status_code == 200:
                                break
                        except httpx.HTTPError:
                            pass
                        time.sleep(0.05)
                    else:
                        self.fail("packaged server did not start")
                    responses = [
                        client.get("/v1/alerts", headers={"X-Forwarded-For": address}).status_code
                        for address in ("198.51.100.1", "198.51.100.2")
                    ]
                    self.assertEqual(responses, [200, 429])
            finally:
                child.terminate()
                child.wait(timeout=15)
