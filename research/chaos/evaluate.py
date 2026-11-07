"""Fault-window metrics with explicit right censoring and ambiguity disclosure."""

from __future__ import annotations

from typing import Any

import numpy as np


def latency_summary(values: list[float | None], seed: int = 2026) -> dict[str, Any]:
    """Detected-only percentiles never hide the denominator or censored faults."""
    observed = np.asarray([v for v in values if v is not None], dtype=float)
    result: dict[str, Any] = {
        "n_total": len(values),
        "n_observed": len(observed),
        "n_right_censored": sum(v is None for v in values),
        "unit": "seconds",
        "estimand": "conditional_on_observed_detection_or_recovery",
        "p50": None,
        "p95": None,
        "p50_ci95": None,
        "p95_ci95": None,
    }
    if len(observed):
        result["p50"], result["p95"] = [float(x) for x in np.quantile(observed, [0.5, 0.95])]
    if len(observed) >= 2:
        draws = np.random.default_rng(seed).choice(observed, (2000, len(observed)), replace=True)
        for q, key in [(0.5, "p50_ci95"), (0.95, "p95_ci95")]:
            result[key] = np.quantile(np.quantile(draws, q, axis=1), [0.025, 0.975]).tolist()
    return result


def evaluate(
    ground_truth: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    reports: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    injected = [row for row in ground_truth if row["phase"] == "injected"]
    cases = []
    ordered = sorted(observations, key=lambda row: row["observed_at_ms"])
    for index, fault in enumerate(injected):
        case, start = fault["case"], fault["at_ms"]
        next_start = injected[index + 1]["at_ms"] if index + 1 < len(injected) else float("inf")
        horizon = min(
            next_start,
            start + (case["duration_seconds"] + case["recovery_observation_seconds"] + 120) * 1000,
        )
        window = [r for r in ordered if start <= r["observed_at_ms"] < horizon]
        alerts: set[str] = set()
        detected = recovered = baseline = None
        observation_failures = 0
        for row in window:
            when = row["observed_at_ms"]
            if row.get("alerts") is None or row.get("incidents") is None:
                observation_failures += 1
            for alert in row.get("alerts") or []:
                # Window overlap is a proxy, not proof the injection caused the alert.
                if (
                    alert.get("service") == case["target"]
                    and start <= alert.get("window_end", 0) <= horizon
                ):
                    alerts.add(alert["alert_id"])
                    detected = when if detected is None else detected
            for incident in row.get("incidents") or []:
                if (
                    case["target"] in incident.get("affected_services", [])
                    and start <= incident.get("last_anomaly_at", 0) <= horizon
                    and incident.get("status") == "resolved"
                    and incident.get("resolved_at", 0) >= start
                ):
                    recovered = when if recovered is None else recovered
            for value in ((row.get("prometheus_alerts") or {}).get("data") or {}).get("result", []):
                metric = value.get("metric", {})
                if (
                    metric.get("baseline") == "prometheus-fixed"
                    and metric.get("service_name") == case["target"]
                ):
                    baseline = when if baseline is None else baseline
        cases.append(
            {
                "case_id": case["id"],
                "family": case["family"],
                "target": case["target"],
                "mttd_s": (detected - start) / 1000 if detected is not None else None,
                "mttr_s": (recovered - start) / 1000 if recovered is not None else None,
                "prometheus_mttd_s": (baseline - start) / 1000 if baseline is not None else None,
                "alert_count": len(alerts),
                "observation_failures": observation_failures,
                "window_observations": len(window),
            }
        )
    # First available incident report after injection; never select the best later diagnosis.
    top1: list[float | None] = []
    top3: list[float | None] = []
    validity: list[float | None] = []
    diagnosis: list[float | None] = []
    for index, fault in enumerate(injected):
        start, case = fault["at_ms"], fault["case"]
        end = injected[index + 1]["at_ms"] if index + 1 < len(injected) else start + 300000
        candidates = sorted(
            (
                r
                for r in reports or []
                if start <= r["report"].get("created_at", 0) < end
                and case["target"] in r["report"].get("rca", {}).get("affected_services", [])
            ),
            key=lambda r: r["report"]["created_at"],
        )
        chosen = candidates[0] if candidates else None
        if chosen is None:
            top1.append(None)
            top3.append(None)
            validity.append(None)
            diagnosis.append(None)
            continue
        ranking = chosen["report"].get("rca", {}).get("candidates", [])
        # Rank ties follow the original RCA competition ranks, not arbitrary list order.
        top1.append(
            float(any(c["service"] == case["target"] and c.get("rank", 999) <= 1 for c in ranking))
        )
        top3.append(
            float(any(c["service"] == case["target"] and c.get("rank", 999) <= 3 for c in ranking))
        )
        verdict = chosen.get("verification", {})
        checks = [
            verdict.get(k)
            for k in (
                "full_evidence_valid",
                "snapshot_sha256_valid",
                "rca_sha256_valid",
                "rca_evidence_references_valid",
                "rca_evidence_set_valid",
            )
        ]
        validity.append(float(all(checks)) if all(isinstance(c, bool) for c in checks) else None)
        diagnosis.append((chosen["report"]["created_at"] - start) / 1000)
    return {
        "n_confirmed_injections": len(cases),
        "cases": cases,
        "mttd": latency_summary([r["mttd_s"] for r in cases]),
        "mttr": latency_summary([r["mttr_s"] for r in cases]),
        "prometheus_mttd": latency_summary([r["prometheus_mttd_s"] for r in cases]),
        "alerts_per_event": sum(r["alert_count"] for r in cases) / len(cases) if cases else None,
        "root_cause_top1": proportion_summary(top1),
        "root_cause_top3": proportion_summary(top3),
        "reference_validity": proportion_summary(validity),
        "diagnosis_latency": latency_summary(diagnosis),
        "limits": [
            "Matched by target and temporal overlap; propagating-only detections may be missed.",
            "Already firing Prometheus alerts can confound attribution; inspect pre-injection baseline.",
            "Top-K conditional on a matching report; missing reports are counted separately.",
            "Diagnostic latency is injection-to-first-report wall time, not isolated LLM inference time.",
        ],
    }


def proportion_summary(values: list[float | None], seed: int = 2026) -> dict[str, Any]:
    observed = np.asarray([v for v in values if v is not None])
    return {
        "n_total": len(values),
        "n_observed": len(observed),
        "n_missing": sum(v is None for v in values),
        "value": float(observed.mean()) if len(observed) else None,
        "ci95": np.quantile(
            np.random.default_rng(seed)
            .choice(observed, (2000, len(observed)), replace=True)
            .mean(axis=1),
            [0.025, 0.975],
        ).tolist()
        if len(observed) >= 2
        else None,
    }
