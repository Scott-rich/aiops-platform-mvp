"""Seeded schedules and explicit Chaos Mesh CRDs for an isolated research cluster."""

from __future__ import annotations

import random
import re
from typing import Any

FAMILIES = (
    "pod_kill",
    "network_delay",
    "packet_loss",
    "dependency_unavailable",
    "cpu_pressure",
    "memory_pressure",
)
TARGETS = (
    "checkoutservice",
    "currencyservice",
    "frontend",
    "paymentservice",
    "productcatalogservice",
    "recommendationservice",
)


def make_plan(
    seed: int, repeats: int = 5, targets: tuple[str, ...] = TARGETS
) -> list[dict[str, Any]]:
    """Targets and waits depend only on the seed; absolute timestamps are observed."""
    if (
        not 5 <= repeats <= 20
        or not targets
        or any(not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", x) for x in targets)
    ):
        raise ValueError("require 5..20 repeats and valid Kubernetes app selectors")
    rng = random.Random(seed)
    families = [family for family in FAMILIES for _ in range(repeats)]
    rng.shuffle(families)
    result = []
    for index, family in enumerate(families):
        result.append(
            {
                "id": f"fault-{index:03d}",
                "family": family,
                "target": "paymentservice"
                if family == "dependency_unavailable"
                else rng.choice(targets),
                "wait_seconds": rng.randint(5, 15),
                "duration_seconds": 30,
                "recovery_observation_seconds": 120,
                "delay_ms": rng.choice([100, 300]),
                "loss_percent": rng.choice([10, 30]),
            }
        )
    return result


def manifest(case: dict[str, Any], namespace: str) -> dict[str, Any]:
    """Dependency failure is a real partition/gRPC outage, not an HTTP 503 claim."""
    spec: dict[str, Any] = {
        "mode": "one",
        "selector": {"namespaces": [namespace], "labelSelectors": {"app": case["target"]}},
        "duration": f"{case['duration_seconds']}s",
    }
    family = case["family"]
    if family == "pod_kill":
        kind = "PodChaos"
        spec["action"] = "pod-kill"
        spec.pop("duration")
    elif family in {"network_delay", "packet_loss", "dependency_unavailable"}:
        kind = "NetworkChaos"
        spec["direction"] = "both"
        if family == "network_delay":
            spec.update(
                action="delay",
                delay={"latency": f"{case['delay_ms']}ms", "correlation": "0", "jitter": "0ms"},
            )
        elif family == "packet_loss":
            spec.update(action="loss", loss={"loss": str(case["loss_percent"]), "correlation": "0"})
        else:
            spec.update(
                action="partition", target={"mode": "all", "selector": {"namespaces": [namespace]}}
            )
    elif family in {"cpu_pressure", "memory_pressure"}:
        kind = "StressChaos"
        spec["stressors"] = (
            {"cpu": {"workers": 1, "load": 80}}
            if family == "cpu_pressure"
            else {"memory": {"workers": 1, "size": "64MB"}}
        )
    else:
        raise ValueError("unknown fault family")
    return {
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": kind,
        "metadata": {
            "name": case["id"],
            "namespace": namespace,
            "labels": {"aiops-experiment": "research"},
        },
        "spec": spec,
    }
