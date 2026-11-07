"""Explicit real-provider integration; never calls a provider during unit tests."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


@unittest.skipUnless(os.getenv('AIOPS_REAL_LLM_TEST')=='1','opt-in real model integration; use scripts.verify_llm for retained artifacts')
class RealModelTests(unittest.TestCase):
    def test_configured_model_on_archived_actual_evidence(self):
        root=Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'real-result'
            run=subprocess.run([sys.executable,'-m','scripts.verify_llm','--output',str(output)],cwd=root,timeout=700)
            self.assertEqual(run.returncode,0,(output/'result.json').read_text())
