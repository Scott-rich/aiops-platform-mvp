import os
import tempfile
import unittest
from unittest.mock import patch
from app.readiness import dependencies
from app.worker import write_health


class ReadinessTests(unittest.TestCase):
    def test_local_mode_explicitly_does_not_probe_distributed_runtime(self):
        for mode in ("local-http", "local-http-python-rules"):
            with self.subTest(mode=mode), patch.dict(os.environ, {"EXECUTION_MODE": mode}), patch("app.readiness.kafka_ready") as probe:
                self.assertTrue(dependencies("unused")["ready"])
                probe.assert_not_called()

    def test_ready_requires_each_dependency_and_fresh_worker(self):
        with tempfile.TemporaryDirectory() as data, patch.dict(os.environ, {"EXECUTION_MODE": "kafka-worker"}), \
             patch("app.readiness.kafka_ready", return_value={"ready": True}), \
             patch("app.readiness.flink_ready", return_value={"ready": True}):
            self.assertFalse(dependencies(data)["ready"])
            write_health(data+"/worker-health.json", {"status": "running"})
            self.assertTrue(dependencies(data)["ready"])
            with patch("app.readiness.flink_ready", return_value={"ready": False}):
                self.assertFalse(dependencies(data)["ready"])
