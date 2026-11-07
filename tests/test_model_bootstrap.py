"""Pinned download behavior with tiny explicit fixtures; no weight downloads."""
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from scripts import bootstrap_local_model as bootstrap


class ModelBootstrapTests(unittest.TestCase):
    def test_verified_existing_model_never_redownloads(self):
        raw=b'explicit-small-test-model'
        with tempfile.TemporaryDirectory() as directory,patch.object(bootstrap,'BYTES',len(raw)), \
             patch.object(bootstrap,'SHA256',hashlib.sha256(raw).hexdigest()),patch.object(bootstrap.urllib.request,'urlopen') as network:
            target=Path(directory)/'fixture.gguf';target.write_bytes(raw)
            bootstrap.fetch_model(target);network.assert_not_called()

    def test_bad_existing_model_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/'fixture.gguf';target.write_bytes(b'preserve-me')
            with self.assertRaisesRegex(ValueError,'preserved'):bootstrap.fetch_model(target)
            self.assertEqual(target.read_bytes(),b'preserve-me')

    def test_download_is_verified_before_atomic_rename(self):
        raw=b'explicit-small-test-model'
        with tempfile.TemporaryDirectory() as directory,patch.object(bootstrap,'BYTES',len(raw)), \
             patch.object(bootstrap,'SHA256',hashlib.sha256(raw).hexdigest()), \
             patch.object(bootstrap.urllib.request,'urlopen',return_value=io.BytesIO(raw)):
            target=Path(directory)/'fixture.gguf';bootstrap.fetch_model(target)
            self.assertEqual(target.read_bytes(),raw);self.assertFalse(target.with_suffix('.partial').exists())

    def test_checksum_failure_never_publishes_corrupt_model(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(bootstrap,'BYTES',3), \
             patch.object(bootstrap.urllib.request,'urlopen',return_value=io.BytesIO(b'bad')):
            target=Path(directory)/'fixture.gguf'
            with self.assertRaisesRegex(ValueError,'checksum'):bootstrap.fetch_model(target)
            self.assertFalse(target.exists());self.assertTrue(target.with_suffix('.partial').exists())
