"""Independent regression tests for dataset leakage and retained experiment failures."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from research.datasets.base import clean
from research.datasets.psm import PSM
from research.datasets.smd import SMD
from research.experiment import Experiment, sha256
from research.metrics import describe, detection, temporal_ci


class DatasetContractTests(unittest.TestCase):
    def test_train_only_missing_values(self) -> None:
        train = np.array([[1.0, 2.0], [3.0, np.nan]])
        test = np.array([[1000.0, 1000.0], [np.nan, np.nan]])
        x, y, meta = clean(train, test)
        np.testing.assert_array_equal(x, [[1, 2], [3, 2]])
        np.testing.assert_array_equal(y[1], [2, 2])
        self.assertEqual(meta["test_imputed"], 2)

    def test_invalid_matrices(self) -> None:
        for train, test in [(np.ones((0, 2)), np.ones((2, 2))), (np.ones((3, 2)), np.ones((3, 3)))]:
            with self.assertRaises(ValueError):
                clean(train, test)

    def test_psm_timestamp_is_not_a_feature(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "train.csv").write_text("timestamp,a,b\n1,2,3\n2,4,5\n")
            (root / "test.csv").write_text("timestamp,a,b\n1000,2,3\n2000,4,5\n")
            (root / "test_label.csv").write_text("timestamp,label\n1000,0\n2000,1\n")
            x, y, z, labels, meta = PSM(root).load()
            self.assertEqual(x.shape, (2, 2))
            np.testing.assert_array_equal(z, x)
            np.testing.assert_array_equal(labels, [0, 1])
            self.assertEqual(len(meta["sha256"]), 3)
            self.assertFalse(y.any())

    def test_smd_invalid_labels_and_paths(self) -> None:
        with self.assertRaises(ValueError):
            SMD(Path("."), "machine-../../wrong")
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for split in ("train", "test", "test_label"):
                (root / split).mkdir()
                (root / split / "machine-1-1.txt").write_text(
                    "1,2\n3,4\n" if split != "test_label" else "0\n0.5\n"
                )
            with self.assertRaises(ValueError):
                SMD(root, "machine-1-1").load()


class ExperimentContractTests(unittest.TestCase):
    def test_immutable_output_and_complete_terminal_hash(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "run"
            with Experiment(output, {}, [0]) as run:
                sys.stdout.write("stdout_probe\n")
                sys.stderr.write("stderr_probe\n")
                run.summary(["a"] * 6)
            with self.assertRaises(FileExistsError):
                Experiment(output, {}, [0])
            terminal = json.loads((output / "completed.json").read_text())
            self.assertEqual(terminal["result_sha256"], sha256(output / "result.json"))
            self.assertIn("stdout_probe", (output / "console.log").read_text())
            self.assertIn("stderr_probe", (output / "console.log").read_text())

    def test_failure_retains_summary(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "run"
            with self.assertRaisesRegex(ValueError, "intentional"), Experiment(output, {}, []):
                raise ValueError("intentional failure probe")
            self.assertEqual(json.loads((output / "result.json").read_text())["status"], "failed")
            self.assertTrue((output / "summary.md").exists())

    def test_source_change_is_not_a_success_exit(self) -> None:
        with (
            tempfile.TemporaryDirectory() as folder,
            patch("research.experiment.source_hashes", side_effect=[{"a": "1"}, {"a": "2"}]),
            self.assertRaises(RuntimeError),
            Experiment(Path(folder) / "run", {}, []),
        ):
            pass


class UncertaintyTests(unittest.TestCase):
    def test_single_entity_has_no_population_ci(self) -> None:
        self.assertEqual(describe([0.5])["ci95"], [None, None])
        self.assertIsNone(describe([0.5])["std"])

    def test_temporal_bootstrap_replay(self) -> None:
        labels = np.array([0, 0, 1, 1, 1, 0, 0, 0], dtype=np.int64)
        scores = np.array([0.1, 0.2, 0.9, 0.8, 0.3, 0.2, 0.1, 0.1])
        pred = (scores > 0.5).astype(np.int64)
        a = temporal_ci(labels, scores, pred, block_size=3, resamples=20)
        b = temporal_ci(labels, scores, pred, block_size=3, resamples=20)
        self.assertEqual(a, b)
        self.assertEqual(a["resamples"], 20)

    def test_nonbinary_predictions_rejected(self) -> None:
        with self.assertRaises(ValueError):
            detection(np.ones(2, dtype=np.int64), np.ones(2), np.array([0, 2], dtype=np.int64))


if __name__ == "__main__":
    unittest.main()
