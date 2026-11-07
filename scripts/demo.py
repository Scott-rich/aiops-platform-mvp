"""Offline fixture replay: verifies Python logic, DOES NOT run Kafka or Flink."""
import argparse
import json
import os
from pathlib import Path
import tempfile
from app.contracts import now_ms
from app.diagnosis import diagnose, markdown
from app.storage import Store
from flink.rules import evaluate_window


def fixture(start=None):
    start = start or now_ms() - 60_000
    events = []
    for i in range(40):
        trace = f"{i+1:032x}"
        failed = i >= 15
        spans = {"gateway": f"{i*4+1:016x}", "order": f"{i*4+2:016x}",
                 "inventory": f"{i*4+3:016x}", "payment": f"{i*4+4:016x}"}
        for service in ("gateway", "order", "inventory", "payment"):
            abnormal = failed and service != "inventory"
            latency = (1500 if service == "payment" else 1580) if abnormal else 35
            status = (503 if service == "payment" else 502) if abnormal else 200
            common = {"schema_version": 1, "service": service, "instance_id": service+"-fixture",
                      "trace_id": trace, "event_time": start + i*500}
            downstream = {"gateway": ["order"], "order": ["inventory", "payment"], "payment": [], "inventory": []}[service]
            parent = {"gateway": None, "order": spans["gateway"], "inventory": spans["order"], "payment": spans["order"]}[service]
            for kind in ("request", "span", "log"):
                event = {**common, "event_id": f"fixture-{i}-{service}-{kind}", "event_type": kind,
                         "span_id": spans[service]}
                if kind in ("request", "span"):
                    event.update(latency_ms=latency, status_code=status)
                if kind == "span":
                    event.update(parent_span_id=parent, downstream=downstream)
                if kind == "log":
                    event.update(level="ERROR" if abnormal else "INFO",
                                 message=("service temporarily unavailable" if service == "payment" else "dependency payment returned HTTP 503") if abnormal else "request completed")
                events.append(event)
    return events, start, start+30_000


def replay(output="examples"):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    # Never call an external model from this fixture demonstration.
    original = os.environ.pop("LLM_BASE_URL", None)
    try:
        with tempfile.TemporaryDirectory(prefix="aiops-replay-") as tmp:
            store = Store(tmp)
            events, start, end = fixture()
            for event in events:
                store.ingest(event, {"transport": "offline-fixture", "fixture": "payment-observation-v1"})
            alerts = evaluate_window(events, "gateway", start, end)
            alert = next(a for a in alerts if a["rule_id"] == "high_error_rate")
            store.alert(alert, {"transport": "offline-rule-evaluation", "engine": "pure-python-shared-rules"})
            report = diagnose(store, alert["alert_id"])
            (output/"sample-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            (output/"sample-report.md").write_text(markdown(report), encoding="utf-8")
            print(json.dumps({"mode": "offline-fixture; Kafka/Flink not executed", "counts": store.stats(),
                              "candidate_service": report["analysis"]["hypotheses"][0]["service"],
                              "files": [str(output/"sample-report.json"), str(output/"sample-report.md")]}, ensure_ascii=False))
    finally:
        if original is not None:
            os.environ["LLM_BASE_URL"] = original


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="examples")
    replay(parser.parse_args().output)
