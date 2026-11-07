"""This test starts actual native Kafka/Flink; it never substitutes mocks."""
import json
import os
from pathlib import Path
import subprocess
import unittest
import uuid


@unittest.skipUnless(os.getenv("RUN_NATIVE_STREAMING") == "1",
                     "real Kafka/Flink test: set RUN_NATIVE_STREAMING=1 after bootstrap_native")
class NativeStreamingIntegration(unittest.TestCase):
    def test_real_stream_restore_http_and_diagnosis(self):
        runtime = Path(os.getenv("AIOPS_NATIVE_RUNTIME", "/tmp/aiops-native-runtime")).resolve()
        root = Path(__file__).resolve().parents[2]
        # Retain failures and logs instead of deleting the test evidence on exit.
        output = Path(os.getenv("AIOPS_NATIVE_OUTPUT", str(root / "artifacts" / ("native-unittest-" + uuid.uuid4().hex[:10])))).resolve()
        process = subprocess.Popen([str(runtime / "venv/bin/python"), "-m", "scripts.native_streaming",
                                    "--runtime-dir", str(runtime), "--output", str(output)], cwd=root,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            stdout, stderr = process.communicate(timeout=900)
        except subprocess.TimeoutExpired:
            # The harness handles SIGTERM and cleans its own process groups.
            process.terminate()
            stdout, stderr = process.communicate(timeout=180)
        output.mkdir(exist_ok=True)
        (output / "wrapper-stdout.log").write_text(stdout)
        (output / "wrapper-stderr.log").write_text(stderr)
        report = json.loads((output / "result.json").read_text()) if (output / "result.json").exists() else {}
        self.assertEqual(process.returncode, 0, stdout + stderr + json.dumps(report.get("error")))
        self.assertEqual(report.get("execution_kind"), "real_native_kafka_flink")
        self.assertEqual(report.get("status"), "passed")
        self.assertEqual(report.get("check_counts", {}).get("failed"), 0)
        self.assertTrue(report.get("checkpoint_restored"))
        self.assertTrue(report.get("actual_http_incident"))


if __name__ == "__main__":
    unittest.main()
