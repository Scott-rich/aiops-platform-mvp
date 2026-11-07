"""Detector protocol and validation; labels are absent from the fit API."""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np

from research.datasets.base import FloatArray, IntArray


class Detector(ABC):
    deterministic = True

    def __init__(self, seed: int = 0) -> None:
        self.seed = seed
        self.features: int | None = None
        self.threshold = 3.0

    def validate(self, x: FloatArray, *, fitting: bool = False) -> FloatArray:
        x = np.asarray(x, dtype=np.float64)
        if x.ndim != 2 or not x.shape[1] or not np.isfinite(x).all():
            raise ValueError("expected a finite 2D matrix with features")
        if fitting:
            if len(x) < 2:
                raise ValueError("at least two train observations required")
            self.features = x.shape[1]
        elif self.features is None or self.features != x.shape[1]:
            raise ValueError("not fitted or feature count mismatch")
        return x

    @abstractmethod
    def fit(self, x: FloatArray) -> Detector: ...

    @abstractmethod
    def score(self, x: FloatArray) -> FloatArray: ...

    def decide(self, scores: FloatArray) -> IntArray:
        if self.features is None or scores.ndim != 1 or not np.isfinite(scores).all():
            raise ValueError("invalid scores or unfitted detector")
        return (scores > self.threshold).astype(np.int64)
