"""Acceptance checks must fail visibly, including Python's optimized mode."""
from contextlib import redirect_stdout
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.contracts import digest
from scripts.local_platform import platform_environment, simulator_environment
from scripts.verify_optimized import (VerificationError, report_integrity,
                                      require_checks, run, services_ready, telemetry_healthy)


def ready_services():
    return {name: {"status": "ok"} for name in ("gateway", "order", "inventory", "payment")}


def report_fixture():
    snapshot = {"records": [{"event": {"event_id": "support"}}, {"event": {"event_id": "counter"}}]}
    return {"report_id": "fixture-report", "snapshot": snapshot,
            "audit": {"snapshot_sha256": digest(snapshot)},
            "analysis": {"hypotheses": [{"supporting_evidence_ids": ["support"],
                                           "against_evidence_ids": ["counter"]}]}}


class IntegrityChecksTests(unittest.TestCase):
    def test_valid_both_citation_types_and_hashes(self):
        checks = report_integrity(report_fixture(), {"snapshot_sha256_valid": True})
        require_checks(checks)
        self.assertTrue(all(checks.values()))

    def test_invented_support_or_counter_evidence_and_tampering_fail(self):
        for field, check in (("supporting_evidence_ids", "supporting_references_valid"),
                             ("against_evidence_ids", "against_references_valid")):
            with self.subTest(field=field):
                report = report_fixture()
                report["analysis"]["hypotheses"][0][field] = ["invented"]
                checks = report_integrity(report, {"snapshot_sha256_valid": True})
                self.assertFalse(checks[check])
                with self.assertRaises(VerificationError):
                    require_checks(checks)
        report = report_fixture()
        report["snapshot"]["records"][0]["event"]["event_id"] = "tampered"
        self.assertFalse(report_integrity(report, {"snapshot_sha256_valid": True})["snapshot_hash_valid"])
        self.assertFalse(report_integrity(report_fixture(), {"snapshot_sha256_valid": False})["api_snapshot_hash_valid"])

    def test_missing_citations_or_hypotheses_never_pass_vacuously(self):
        for hypotheses in ([], [{"supporting_evidence_ids": ["support"]}],
                           [{"supporting_evidence_ids": "support", "against_evidence_ids": []}]):
            report = report_fixture()
            report["analysis"]["hypotheses"] = hypotheses
            with self.subTest(hypotheses=hypotheses), self.assertRaises(VerificationError):
                require_checks(report_integrity(report, {"snapshot_sha256_valid": True}))
        with self.assertRaises(VerificationError):
            require_checks({"not_executed": None})

    def test_missing_or_failed_service_telemetry_does_not_report_success(self):
        services = {"services": {name: {"telemetry_failed": 0}
                                for name in ("gateway", "order", "inventory", "payment")}}
        self.assertTrue(telemetry_healthy(services))
        for value in (1, None, False, "0"):
            changed = copy.deepcopy(services)
            changed["services"]["payment"]["telemetry_failed"] = value
            self.assertFalse(telemetry_healthy(changed), value)
        self.assertFalse(telemetry_healthy({"services": {}}))
        del services["services"]["payment"]["telemetry_failed"]
        self.assertFalse(telemetry_healthy(services))

    def test_startup_requires_all_four_healthy_services(self):
        self.assertTrue(services_ready(ready_services()))
        for states in ({}, {"payment": {"status": "ok"}}, None):
            self.assertFalse(services_ready(states))
        states = ready_services()
        states["inventory"]["status"] = "unavailable"
        self.assertFalse(services_ready(states))

    def test_optimized_python_still_rejects_invented_counter_evidence(self):
        report = report_fixture()
        report["analysis"]["hypotheses"][0]["against_evidence_ids"] = ["invented"]
        code = ("import json; from scripts.verify_optimized import report_integrity, require_checks; "
                "report=json.loads("+repr(json.dumps(report))+"); "
                "require_checks(report_integrity(report, {'snapshot_sha256_valid': True}))")
        result = subprocess.run([sys.executable, "-O", "-c", code], capture_output=True, text=True,
                                cwd=Path(__file__).resolve().parent.parent, timeout=15, env=os.environ.copy())
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("VerificationError", result.stderr)
        self.assertIn("against_references_valid", result.stderr)


class FailureArtifactTests(unittest.TestCase):
    def setUp(self):
        self.output = tempfile.TemporaryDirectory()
        self.addCleanup(self.output.cleanup)
        self.data = tempfile.TemporaryDirectory()
        self.addCleanup(self.data.cleanup)
        self.args = SimpleNamespace(output=self.output.name, port=18000, base_port=18181)
        self.directory = patch("scripts.verify_optimized.tempfile.mkdtemp", return_value=self.data.name)
        self.directory.start()
        self.addCleanup(self.directory.stop)

    def result(self):
        return json.loads((Path(self.output.name)/"optimized-run.json").read_text())

    def test_launcher_creation_failure_is_saved_without_secret_text(self):
        with patch("scripts.verify_optimized.subprocess.Popen", side_effect=OSError("sensitive-exception-value")), \
             redirect_stdout(io.StringIO()), self.assertRaises(OSError):
            run(self.args)
        result = self.result()
        self.assertEqual((result["status"], result["completed"], result["failed_phase"]), ("failed", False, "launch"))
        self.assertEqual(result["error"]["type"], "OSError")
        self.assertNotIn("sensitive-exception-value", json.dumps(result))
        self.assertTrue(all(value is None for value in result["checks"].values()))
        self.assertTrue(result["cleanup"]["launcher_stopped"])
        self.assertEqual(json.loads((Path(self.output.name)/"optimized-report-versions.json").read_text()), [])

    def test_early_http_failure_records_checks_and_stops_launcher(self):
        child = Mock()
        client = Mock()
        client.get.side_effect = [SimpleNamespace(status_code=200),
                                 Mock(json=lambda: {"services": ready_services()}),
                                 SimpleNamespace(status_code=503, text="unavailable")]
        with patch("scripts.verify_optimized.subprocess.Popen", return_value=child), \
             patch("scripts.verify_optimized.httpx.Client") as factory, redirect_stdout(io.StringIO()):
            factory.return_value.__enter__.return_value = client
            with self.assertRaises(VerificationError):
                run(self.args)
        result = self.result()
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["checks"]["ui_http_200"])
        self.assertFalse(result["checks"]["ui_content_valid"])
        self.assertIsNone(result["checks"]["all_hashes_and_references_valid"])
        child.terminate.assert_called_once()
        child.wait.assert_called_once_with(timeout=20)

    def test_failed_check_still_persists_forced_shutdown_result(self):
        child = Mock()
        child.wait.side_effect = [subprocess.TimeoutExpired("launcher", 20), 0]
        client = Mock()
        client.get.side_effect = [SimpleNamespace(status_code=200),
                                 Mock(json=lambda: {"services": ready_services()}),
                                 SimpleNamespace(status_code=200, text="wrong page")]
        with patch("scripts.verify_optimized.subprocess.Popen", return_value=child), \
             patch("scripts.verify_optimized.httpx.Client") as factory, redirect_stdout(io.StringIO()):
            factory.return_value.__enter__.return_value = client
            with self.assertRaises(VerificationError):
                run(self.args)
        result = self.result()
        self.assertTrue(result["cleanup"]["forced_kill"])
        self.assertTrue(result["cleanup"]["launcher_stopped"])
        self.assertFalse(result["completed"])
        child.kill.assert_called_once()

    def test_non_http_fault_clear_failure_still_stops_launcher(self):
        child = Mock()
        client = Mock()
        client.get.side_effect = [SimpleNamespace(status_code=200),
                                 Mock(json=lambda: {"services": ready_services()}),
                                 SimpleNamespace(status_code=200, text="智能运维"),
                                 RuntimeError("observation failed")]
        main_context, cleanup_context = Mock(), Mock()
        main_context.__enter__ = Mock(return_value=client)
        main_context.__exit__ = Mock(return_value=False)
        cleanup_context.__enter__ = Mock(side_effect=RuntimeError("cleanup setup failed"))
        cleanup_context.__exit__ = Mock(return_value=False)
        ticks = iter((0, 0, 35, 35))
        with patch("scripts.verify_optimized.subprocess.Popen", return_value=child), \
             patch("scripts.verify_optimized.httpx.Client", side_effect=[main_context, cleanup_context]), \
             patch("scripts.verify_optimized.time.monotonic", side_effect=lambda: next(ticks, 40)), \
             redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "observation failed"):
            run(self.args)
        result = self.result()
        self.assertEqual(result["failed_phase"], "fault_cycle")
        self.assertTrue(result["cleanup"]["fault_clear_attempted"])
        self.assertFalse(result["cleanup"]["fault_cleared"])
        self.assertEqual(result["cleanup"]["fault_clear_error"], "RuntimeError")
        self.assertTrue(result["cleanup"]["launcher_stopped"])
        self.assertEqual(result["elapsed_seconds"], 40)
        child.terminate.assert_called_once()


class ChildConfigurationTests(unittest.TestCase):
    def test_environment_wins_over_file_including_explicitly_empty_values(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory)/"model.env"
            config.write_text("LLM_BASE_URL='http://file/v1'\nLLM_MODEL=from-file\nLLM_API_KEY=file-value\n"
                              "UNRELATED_SECRET=ignored\nLLM_OTHER=ignored\n")
            inherited = {"LLM_API_KEY": "from-environment", "LLM_MODEL": "", "PATH": "/bin"}
            environment = platform_environment(inherited, config)
        self.assertEqual(environment["LLM_BASE_URL"], "http://file/v1")
        self.assertEqual(environment["LLM_MODEL"], "")
        self.assertEqual(environment["LLM_API_KEY"], "from-environment")
        self.assertNotIn("UNRELATED_SECRET", environment)
        self.assertNotIn("LLM_OTHER", environment)
        self.assertNotIn("LLM_BASE_URL", inherited)

    def test_simulators_and_loadgen_do_not_inherit_model_credentials(self):
        common = {"LLM_BASE_URL": "http://model/v1", "LLM_MODEL": "model", "LLM_API_KEY": "test-value",
                  "LLM_FUTURE_SECRET": "test-value", "OPENAI_API_KEY": "test-value",
                  "ANTHROPIC_API_KEY": "test-value", "HTTPS_PROXY": "http://proxy",
                  "no_proxy": "localhost", "PYTHONPATH": "/runtime", "GATEWAY_URL": "http://gateway",
                  "TELEMETRY_MODE": "http"}
        environment = simulator_environment(common)
        self.assertEqual(environment, {"PYTHONPATH": "/runtime", "GATEWAY_URL": "http://gateway", "TELEMETRY_MODE": "http"})
        self.assertIn("LLM_API_KEY", common)


if __name__ == "__main__":
    unittest.main()
