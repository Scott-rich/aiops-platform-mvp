import hashlib
from pathlib import Path
import tempfile
import unittest

from scripts.bootstrap_native import verify_published_sha512
from scripts.native_streaming import NativeAcceptance, metric


class NativeToolingTests(unittest.TestCase):
    def test_apache_grouped_checksum_and_wrong_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact, checksum = root / "sample.tgz", root / "sample.sha512"
            artifact.write_bytes(b"actual artifact")
            digest = hashlib.sha512(artifact.read_bytes()).hexdigest()
            checksum.write_text("sample.tgz: " + " \n".join(digest[i:i+8] for i in range(0, 128, 8)))
            self.assertEqual(verify_published_sha512(artifact, checksum), digest)
            artifact.write_bytes(b"corrupted")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                verify_published_sha512(artifact, checksum)

    def test_failure_check_is_not_disabled_by_optimized_python(self):
        with tempfile.TemporaryDirectory() as directory:
            acceptance = NativeAcceptance(Path(directory), Path(directory) / "run")
            with self.assertRaises(AssertionError):
                acceptance.check("actually_failed", False, reason="intentional test")
            acceptance.results["status"] = "failed"
            acceptance.close()
            import json
            report = json.loads((Path(directory) / "run/result.json").read_text())
            self.assertEqual(report["check_counts"], {"passed": 0, "failed": 1, "errors": 0, "cleanup_errors": 0})
            self.assertEqual(report["status"], "failed")

    def test_fixture_is_explicit_v1_without_injection_label(self):
        from wire_contract import validate_metric
        value = metric("test-id", 1000)
        self.assertIs(validate_metric(value), value)
        self.assertNotIn("root_cause", value)
        self.assertNotIn("fault_kind", value)


if __name__ == "__main__":
    unittest.main()
