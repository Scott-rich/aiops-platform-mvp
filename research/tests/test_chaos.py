"""Do not confuse deterministic plans, controller-confirmed faults, and missing data."""

from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from research.chaos.evaluate import evaluate, latency_summary
from research.chaos.plan import FAMILIES, make_plan, manifest


def test_plan_replays_every_family_with_seed() -> None:
    plan = make_plan(2026)
    assert plan == make_plan(2026)
    assert plan != make_plan(2027)
    assert Counter(p["family"] for p in plan) == dict.fromkeys(FAMILIES, 5)
    assert len({p["id"] for p in plan}) == 30
    assert all(
        p["target"] == "paymentservice" for p in plan if p["family"] == "dependency_unavailable"
    )


def test_real_crds_are_bounded_to_target_namespace() -> None:
    for case in make_plan(2026):
        doc = manifest(case, "boutique")
        assert doc["spec"]["selector"]["namespaces"] == ["boutique"]
        assert doc["metadata"]["namespace"] == "boutique"
        assert doc["kind"] in {"StressChaos", "NetworkChaos", "PodChaos"}
        if case["family"] == "dependency_unavailable":
            assert doc["spec"]["action"] == "partition"
        if case["family"] == "cpu_pressure":
            assert doc["spec"]["stressors"]["cpu"]["load"] == 80
    with pytest.raises(ValueError):
        make_plan(1, 1)
    with pytest.raises(ValueError):
        make_plan(1, targets=("not a selector",))


def test_censored_faults_are_never_zero_or_removed_from_denominator() -> None:
    stats = latency_summary([1.0, 3.0, None])
    assert stats["n_total"] == 3
    assert stats["n_right_censored"] == 1
    assert stats["p50"] == 2.0
    assert latency_summary([None])["p50"] is None
    assert latency_summary([1.0])["p50_ci95"] is None


def test_only_confirmed_injection_counts_and_existing_events_do_not_match() -> None:
    case = make_plan(2026)[0]
    gt = [{"phase": "submitted", "at_ms": 1000, "case": case}]
    assert evaluate(gt, [])["n_confirmed_injections"] == 0
    gt.append({"phase": "injected", "at_ms": 2000, "case": case})
    obs = [
        {
            "observed_at_ms": 3000,
            "alerts": [{"alert_id": "old", "service": case["target"], "window_end": 1500}],
            "incidents": [],
        },
        {
            "observed_at_ms": 4000,
            "alerts": [{"alert_id": "new", "service": case["target"], "window_end": 3500}],
            "incidents": [],
        },
    ]
    stats = evaluate(gt, obs)
    assert stats["mttd"]["p50"] == 2.0
    assert stats["mttr"]["n_right_censored"] == 1
    assert stats["root_cause_top1"]["value"] is None
    assert stats["root_cause_top1"]["n_missing"] == 1


def test_resource_missing_values_and_offset_resets_not_zero_filled() -> None:
    from research.chaos.resources import derive

    rows = [
        {
            "observed_at_ms": 1000,
            "kafka": [
                {"topic": "telemetry.spans", "partition": 0, "end_offset": 10, "committed": None}
            ],
        },
        {
            "observed_at_ms": 2000,
            "kafka": [
                {"topic": "telemetry.spans", "partition": 0, "end_offset": 30, "committed": 20}
            ],
        },
        {
            "observed_at_ms": 3000,
            "kafka": [
                {"topic": "telemetry.spans", "partition": 0, "end_offset": 5, "committed": 0}
            ],
        },
    ]
    result = derive(rows)
    assert result["points"][0]["kafka_lag"] is None
    assert result["points"][1]["telemetry_eps"] == 20
    assert result["points"][1]["kafka_lag"] == 10
    assert result["points"][2]["telemetry_eps"] is None
    assert result["observed_eps_mean"] == 20
    assert result["points"][1]["api_pid1_rss_bytes"] is None


def test_complete_acquisition_required_and_hash_tampering_rejected(tmp_path: Path) -> None:
    import json

    from research.experiment import sha256
    from scripts.research.evaluate_cluster import completeness

    plan = make_plan(2026)
    gt = [
        {
            "phase": phase,
            "case": case,
            "controller_conditions": [{"type": "AllInjected", "status": "True"}],
        }
        for case in plan
        for phase in ("injected", "fault_resource_removed", "workload_ready")
    ]
    metrics: dict[str, Any] = {
        key: {"n_observed": 30}
        for key in (
            "root_cause_top1",
            "reference_validity",
            "diagnosis_latency",
            "mttd",
            "mttr",
            "prometheus_mttd",
        )
    }
    metrics["cases"] = [{"window_observations": 10, "observation_failures": 0}] * 30
    assert not completeness(tmp_path, gt, metrics)["passed"]
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    result = {
        "status": "partial",
        "source_unchanged": True,
        "acquisition_complete": True,
        "config": {"execute": True},
        "metrics": {"confirmed_injections": 30},
        "plan_sha256": sha256(tmp_path / "plan.json"),
    }
    (tmp_path / "result.json").write_text(json.dumps(result))
    (tmp_path / "completed.json").write_text(
        json.dumps({"status": "partial", "result_sha256": sha256(tmp_path / "result.json")})
    )
    assert completeness(tmp_path, gt, metrics)["passed"]
    # A single surviving fault and plausible report values cannot satisfy the full protocol.
    small = completeness(tmp_path, gt[:3], metrics)
    assert not small["passed"]
    assert not small["checks"]["at_least_30_unique_faults"]
    assert not small["checks"]["six_families_at_least_five_each"]
    (tmp_path / "result.json").write_text(json.dumps(result) + " ")
    assert not completeness(tmp_path, gt, metrics)["checks"]["acquisition_terminal_hash"]


def test_scale_read_token_only_sent_to_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse
    from types import SimpleNamespace
    from unittest.mock import Mock

    import httpx

    from scripts.research import scale_experiment

    requests = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"jobs": []})

    real_client = httpx.Client(transport=httpx.MockTransport(handle))
    monkeypatch.setenv("API_READ_TOKEN", "test-read-token")
    monkeypatch.setattr(scale_experiment.httpx, "Client", lambda **kwargs: real_client)
    monkeypatch.setattr(
        scale_experiment.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout="{}")
    )
    consumer = Mock()
    consumer.partitions_for_topic.return_value = set()
    consumer.end_offsets.return_value = {}
    monkeypatch.setattr(scale_experiment, "KafkaConsumer", lambda **kwargs: consumer)
    args = argparse.Namespace(
        api_url="http://api.local", flink_url="http://flink.local", kafka="localhost:19092"
    )
    scale_experiment.resource_sample(args)
    assert requests[0].headers["Authorization"] == "Bearer test-read-token"
    assert "Authorization" not in requests[1].headers


def test_fault_journal_failure_still_cleans_created_resource(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from unittest.mock import Mock

    from scripts.research import cluster_experiment

    case = make_plan(2026)[0]
    doc = manifest(case, "boutique")
    command = Mock(return_value="")
    monkeypatch.setattr(cluster_experiment, "command", command)
    monkeypatch.setattr(
        cluster_experiment, "append", Mock(side_effect=[OSError("disk full"), None])
    )
    with (
        pytest.raises(OSError, match="disk full"),
        cluster_experiment.managed_fault(["kubectl"], doc, case, tmp_path),
    ):
        pytest.fail("body cannot run after journal failure")
    assert command.call_args_list[0].args[0][1] == "create"
    assert command.call_args_list[1].args[0][1] == "delete"
    command.reset_mock()
    command.side_effect = RuntimeError("already exists")
    with (
        pytest.raises(RuntimeError, match="already exists"),
        cluster_experiment.managed_fault(["kubectl"], doc, case, tmp_path),
    ):
        pytest.fail("body cannot run when create failed")
    assert command.call_count == 1  # Must not delete an object that was not created by this call.
