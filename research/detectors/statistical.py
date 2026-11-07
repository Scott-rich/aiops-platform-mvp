"""Transparent max-per-dimension statistics and sklearn Isolation Forest."""

from __future__ import annotations

import numpy as np
from sklearn.ensemble import IsolationForest

from research.datasets.base import FloatArray
from research.detectors.base import Detector


class ZScore(Detector):
    def fit(self, x: FloatArray) -> ZScore:
        x = self.validate(x, fitting=True)
        self.center = np.mean(x, axis=0)
        self.scale = np.maximum(np.std(x, axis=0), 1e-8)
        return self

    def score(self, x: FloatArray) -> FloatArray:
        return np.max(np.abs((self.validate(x) - self.center) / self.scale), axis=1)


class MAD(ZScore):
    def fit(self, x: FloatArray) -> MAD:
        x = self.validate(x, fitting=True)
        self.center = np.median(x, axis=0)
        mad = 1.4826 * np.median(np.abs(x - self.center), axis=0)
        # A discrete feature can have zero MAD without being constant.
        self.scale = np.maximum(np.where(mad > 1e-8, mad, np.std(x, axis=0)), 1e-8)
        return self


class IForest(Detector):
    deterministic = False

    def fit(self, x: FloatArray) -> IForest:
        x = self.validate(x, fitting=True)
        self.model = IsolationForest(
            n_estimators=100,
            max_samples=256,
            random_state=self.seed,
            n_jobs=1,
            contamination="auto",
        )
        self.model.fit(x)
        self.threshold = float(np.quantile(self.score(x), 0.99))
        return self

    def score(self, x: FloatArray) -> FloatArray:
        return np.asarray(-self.model.score_samples(self.validate(x)), dtype=np.float64)
