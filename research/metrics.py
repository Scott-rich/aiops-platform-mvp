"""Explicit point, adjusted, interval, and engineering metrics; no label tuning."""

from __future__ import annotations

from typing import Any

import numpy as np
from scipy.stats import wilcoxon
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

from research.datasets.base import FloatArray, IntArray


def intervals(y: IntArray) -> list[tuple[int, int]]:
    """Half-open contiguous positive runs."""
    changes = np.diff(np.r_[0, y, 0])
    return list(
        zip(np.where(changes == 1)[0].tolist(), np.where(changes == -1)[0].tolist(), strict=True)
    )


def adjusted(y: IntArray, pred: IntArray, k: float) -> IntArray:
    value = pred.copy()
    for start, end in intervals(y):
        count = int(pred[start:end].sum())
        if count > 0 and count / (end - start) >= k:
            value[start:end] = 1
    return value


def detection(y: IntArray, scores: FloatArray, pred: IntArray) -> dict[str, float | None]:
    if y.shape != scores.shape or y.shape != pred.shape or not np.isfinite(scores).all():
        raise ValueError("unaligned or nonfinite predictions")
    if not np.isin(y, [0, 1]).all() or not np.isin(pred, [0, 1]).all():
        raise ValueError("binary labels/predictions required")
    events, alarms = intervals(y), intervals(pred)
    overlap = [float(pred[s:e].mean()) for s, e in events]
    return {
        "pr_auc": float(average_precision_score(y, scores)) if y.sum() else None,
        "roc_auc": float(roc_auc_score(y, scores)) if len(np.unique(y)) == 2 else None,
        "point_f1": float(f1_score(y, pred, zero_division=0)),
        "pa_f1": float(f1_score(y, adjusted(y, pred, 0), zero_division=0)),
        "pa20_f1": float(f1_score(y, adjusted(y, pred, 0.2), zero_division=0)),
        "range_recall": float(np.mean(overlap)) if events else None,
        "event_recall": float(np.mean(np.array(overlap) > 0)) if events else None,
        "alarms": float(len(alarms)),
        "events": float(len(events)),
        "alarms_per_event": len(alarms) / len(events) if events else None,
    }


def describe(values: list[float | None], seed: int = 2026, resamples: int = 2000) -> dict[str, Any]:
    x = np.asarray([v for v in values if v is not None], dtype=float)
    if not len(x):
        return {"n": 0, "mean": None, "std": None, "ci95": [None, None]}
    if len(x) == 1:
        return {
            "n": 1,
            "mean": float(x[0]),
            "std": None,
            "ci95": [None, None],
            "reason": "One statistical unit cannot estimate between-unit uncertainty.",
        }
    boot = np.random.default_rng(seed).choice(x, size=(resamples, len(x))).mean(axis=1)
    return {
        "n": len(x),
        "mean": float(x.mean()),
        "std": float(x.std(ddof=1)) if len(x) > 1 else 0.0,
        "ci95": np.quantile(boot, [0.025, 0.975]).tolist(),
        "bootstrap_seed": seed,
        "bootstrap_resamples": resamples,
    }


def paired_comparison(a: list[float], b: list[float]) -> dict[str, Any]:
    """Entities, not repeated seeds, are the paired statistical unit."""
    delta = np.asarray(a) - np.asarray(b)
    if len(a) != len(b) or not len(a):
        raise ValueError("paired samples required")
    p = float(wilcoxon(delta, method="auto").pvalue) if len(a) >= 5 and np.any(delta) else None
    return {
        "delta": describe(delta.tolist()),
        "wilcoxon_two_sided_p": p,
        "unit": "entity_mean_over_seeds",
        "exploratory_uncorrected": True,
        "warning": "Small entity sample; seeds are not independent machines. PSM has one entity.",
    }


def temporal_ci(
    y: IntArray,
    scores: FloatArray,
    pred: IntArray,
    seed: int = 2026,
    block_size: int = 256,
    resamples: int = 200,
) -> dict[str, Any]:
    """Moving-block bootstrap conditional on a fitted model and one observed trace.

    Block concatenation can alter event boundaries; interval-count CIs are approximate.
    This does not establish independence, stationarity, or out-of-distribution coverage.
    """
    rng = np.random.default_rng(seed)
    n = len(y)
    size = min(block_size, n)
    samples: dict[str, list[float]] = {}
    for _ in range(resamples):
        starts = rng.integers(0, n - size + 1, size=(n + size - 1) // size)
        indices = (starts[:, None] + np.arange(size)).ravel()[:n]
        for key, value in detection(y[indices], scores[indices], pred[indices]).items():
            if value is not None:
                samples.setdefault(key, []).append(value)
    return {
        "unit": "moving_time_blocks",
        "block_size": size,
        "seed": seed,
        "model_seed": 0,
        "resamples": resamples,
        "n_test_points": n,
        "caveat": "Conditional on fitted model. Block joins alter interval counts; assumes local stationarity.",
        "metrics": {
            key: {
                "ci95": np.quantile(values, [0.025, 0.975]).tolist(),
                "valid_resamples": len(values),
            }
            for key, values in samples.items()
        },
    }
