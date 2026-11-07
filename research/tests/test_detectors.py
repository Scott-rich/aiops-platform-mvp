"""Shape, cold-start, replay and extreme-value regression tests."""

import unittest

import numpy as np

from research.detectors import create
from research.metrics import adjusted, detection, intervals, paired_comparison

NAMES = ("zscore", "mad", "iforest", "spot")


class DetectorTests(unittest.TestCase):
    def test_constant_never_alarms(self) -> None:
        for name in NAMES:
            with self.subTest(name=name):
                x = np.ones((300, 2))
                model = create(name).fit(x)
                self.assertEqual(int(model.decide(model.score(x)).sum()), 0)

    def test_monotonic_shift_alarms(self) -> None:
        train = np.random.default_rng(4).normal(0, 0.1, (500, 2))
        test = np.tile(np.linspace(1, 20, 100)[:, None], (1, 2))
        for name in NAMES:
            with self.subTest(name=name):
                model = create(name).fit(train)
                self.assertGreater(int(model.decide(model.score(test)).sum()), 50)

    def test_shape_and_replay(self) -> None:
        x = np.random.default_rng(3).normal(size=(600, 3))
        for name in NAMES:
            with self.subTest(name=name):
                model = create(name, 7).fit(x)
                scores = model.score(x[:19])
                self.assertEqual(scores.shape, (19,))
                np.testing.assert_array_equal(model.decide(scores), model.decide(scores))
                with self.assertRaises(ValueError):
                    model.score(np.ones((3, 2)))
                with self.assertRaises(ValueError):
                    model.score(np.array([[np.nan] * 3]))

    def test_not_fitted_and_empty_train(self) -> None:
        for name in NAMES:
            with self.assertRaises((ValueError, AttributeError)):
                create(name).score(np.ones((3, 2)))
            with self.assertRaises(ValueError):
                create(name).fit(np.ones((1, 2)))


class MetricTests(unittest.TestCase):
    def test_pa_inflation_counterexample(self) -> None:
        y = np.r_[np.ones(100, dtype=np.int64), np.zeros(100, dtype=np.int64)]
        pred = np.zeros(200, dtype=np.int64)
        pred[0] = 1
        m = detection(y, pred.astype(float), pred)
        self.assertEqual(m["pa_f1"], 1.0)
        value = m["pa20_f1"]
        assert value is not None
        self.assertLess(value, 0.03)
        self.assertEqual(m["range_recall"], 0.01)
        self.assertEqual(m["alarms_per_event"], 1.0)

    def test_interval_edges_and_false_positives(self) -> None:
        y = np.array([1, 1, 0, 1], dtype=np.int64)
        self.assertEqual(intervals(y), [(0, 2), (3, 4)])
        pred = np.array([0, 1, 1, 0], dtype=np.int64)
        np.testing.assert_array_equal(adjusted(y, pred, 0.2), [1, 1, 1, 0])
        self.assertIsNone(paired_comparison([1], [0])["wilcoxon_two_sided_p"])

    def test_undefined_metrics_not_zero(self) -> None:
        y = np.zeros(10, dtype=np.int64)
        m = detection(y, y.astype(float), y)
        self.assertIsNone(m["roc_auc"])
        self.assertIsNone(m["alarms_per_event"])


if __name__ == "__main__":
    unittest.main()
