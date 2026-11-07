"""Streaming POT on max-z scores; periodic GPD-MLE refits, not original Grimshaw SPOT."""

from __future__ import annotations

import numpy as np
from scipy.stats import genpareto

from research.datasets.base import FloatArray, IntArray
from research.detectors.statistical import ZScore


class SPOT(ZScore):
    """Train initializes a 98% tail; accepted observations update it without labels.

    q=1e-3, refit every 64 admitted tail observations. Alarms never update the tail.
    Repeated decide calls reset stream state, making replay deterministic.
    """

    def fit(self, x: FloatArray) -> SPOT:
        super().fit(x)
        scores = self.score(x)
        self.initial_n = len(scores)
        self.base = float(np.quantile(scores, 0.98))
        self.initial_peaks = scores[scores > self.base] - self.base
        self.threshold = self.extreme(self.initial_peaks, self.initial_n)
        return self

    def extreme(self, peaks: FloatArray, n: int) -> float:
        if len(peaks) < 8 or float(np.max(peaks, initial=0.0)) < 1e-8:
            return self.base + max(float(np.max(peaks, initial=0.0)) * 2, 1e-8)
        shape, _, scale = genpareto.fit(peaks, floc=0)
        p = min(0.999999, max(1e-10, 0.001 * n / len(peaks)))
        value = self.base + float(genpareto.isf(p, shape, loc=0, scale=scale))
        if not np.isfinite(value):
            raise ValueError("nonfinite POT threshold")
        return max(self.base, value)

    def decide(self, scores: FloatArray) -> IntArray:
        super().decide(scores)  # Validate without changing state.
        peaks = self.initial_peaks.tolist()
        n, threshold, accepted = self.initial_n, self.threshold, 0
        predictions = np.zeros(len(scores), dtype=np.int64)
        for i, value in enumerate(scores):
            if value > threshold:
                predictions[i] = 1
                continue
            n += 1
            if value > self.base:
                peaks.append(float(value - self.base))
                accepted += 1
                if accepted % 64 == 0:
                    threshold = self.extreme(np.asarray(peaks), n)
        return predictions
