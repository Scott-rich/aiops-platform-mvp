"""Evidence metrics, risk/coverage and calibration without converting 'low' to a probability."""

from __future__ import annotations

from typing import Any

import numpy as np


def score(
    case: dict[str, Any], mapping: list[dict[str, Any]], raw_keys: list[str], valid: bool
) -> dict[str, float]:
    gold = case["gold"]
    selected = {m["event_id"] for m in mapping}
    expected = set(gold["evidence_ids"])
    services = list(dict.fromkeys(m["service"] for m in mapping))
    precision = (
        len(selected & expected) / len(set(raw_keys)) if raw_keys else float(not expected and valid)
    )
    recall = len(selected & expected) / len(expected) if expected else float(not selected and valid)
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    roots = set(gold["root_services"])
    correct = (
        (not services and valid)
        if gold["should_abstain"]
        else bool(valid and services and services[0] in roots)
    )
    n = len(case["payload"]["records"])
    unknown = sum(key not in {f"e{i}" for i in range(1, n + 1)} for key in raw_keys)
    return {
        "evidence_precision": precision,
        "evidence_recall": recall,
        "evidence_f1": f1,
        "correct": float(correct),
        "top1": float(bool(services and services[0] in roots)),
        "top3": float(bool(set(services[:3]) & roots)),
        "abstained": float(not services),
        "valid": float(valid),
        "unsupported_reference_rate": unknown / len(raw_keys) if raw_keys else 0.0,
        "reference_count": float(len(raw_keys)),
        "unsupported_reference_count": float(unknown),
    }


def ece(confidence: list[float], correct: list[float], bins: int = 10) -> float:
    p, y = np.array(confidence), np.array(correct)
    if not len(p) or len(p) != len(y) or np.any((p < 0) | (p > 1)):
        raise ValueError("aligned probabilities in [0,1] are required")
    groups = np.minimum((p * bins).astype(int), bins - 1)
    return float(
        sum(
            np.sum(groups == b) / len(p) * abs(p[groups == b].mean() - y[groups == b].mean())
            for b in range(bins)
            if np.any(groups == b)
        )
    )


def risk_coverage(
    confidence: list[float], correct: list[float], eligible: list[bool]
) -> list[dict[str, Any]]:
    p, y, mask = np.array(confidence), np.array(correct), np.array(eligible)
    points = [{"threshold": None, "coverage": 0.0, "risk": None, "accepted": 0}]
    for threshold in sorted(set(confidence), reverse=True):
        accepted = (p >= threshold) & mask
        if accepted.any():
            points.append(
                {
                    "threshold": threshold,
                    "coverage": float(accepted.mean()),
                    "risk": float(1 - y[accepted].mean()),
                    "accepted": int(accepted.sum()),
                }
            )
    return points


def clustered(
    values: list[float], families: list[str], seed: int = 2026, resamples: int = 2000
) -> dict[str, Any]:
    """Bootstrap scenario families, not synthetic variations as independent worlds."""
    if len(values) != len(families) or not values:
        raise ValueError("aligned nonempty metric samples required")
    groups = sorted(set(families))
    mapping = {g: [v for v, f in zip(values, families, strict=True) if f == g] for g in groups}
    rng = np.random.default_rng(seed)
    draws = [
        np.mean([v for g in rng.choice(groups, len(groups)) for v in mapping[g]])
        for _ in range(resamples)
    ]
    return {
        "value": float(np.mean(values)),
        "n": len(values),
        "n_families": len(groups),
        "ci95": np.quantile(draws, [0.025, 0.975]).tolist(),
        "bootstrap_seed": seed,
        "resamples": resamples,
        "unit": "synthetic_family",
        "external_validity": "not established",
    }
