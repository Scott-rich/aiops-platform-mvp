"""Exercise the actual local UI/API/service processes through a full fault cycle."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import httpx
from app.contracts import digest
from app.rca_integrity import verify_attachment


class VerificationError(RuntimeError):
    """An observed acceptance condition failed (also enforced under python -O)."""


def require_checks(checks):
    failed = [name for name, passed in checks.items() if passed is not True]
    if failed:
        raise VerificationError("failed or unexecuted checks: "+", ".join(failed))


def report_integrity(report, api_verification):
    """Recompute integrity and check both kinds of citations against this snapshot."""
    checks = {"api_snapshot_hash_valid": api_verification.get("snapshot_sha256_valid") is True,
              "snapshot_hash_valid": False, "supporting_references_valid": False,
              "against_references_valid": False}
    try:
        snapshot = report["snapshot"]
        checks["snapshot_hash_valid"] = digest(snapshot) == report["audit"]["snapshot_sha256"]
        ids = {row["event"]["event_id"] for row in snapshot["records"]}
        hypotheses = report["analysis"]["hypotheses"]
        for field, check_name in (("supporting_evidence_ids", "supporting_references_valid"),
                                  ("against_evidence_ids", "against_references_valid")):
            valid_presence = bool(hypotheses) or (hypotheses == [] and report['analysis'].get('diagnosis_status') == 'unknown')
            checks[check_name] = valid_presence and all(
                isinstance(hypothesis.get(field), list)
                and all(isinstance(ref, str) and ref in ids for ref in hypothesis[field])
                for hypothesis in hypotheses)
        if 'rca' in report:
            checks.update(verify_attachment(report))
            checks['api_rca_checks_valid'] = all(api_verification.get(key) is True for key in verify_attachment(report))
    except (KeyError, TypeError, ValueError, AttributeError):
        # A malformed report must produce a failed check rather than disappear.
        pass
    return checks


def telemetry_healthy(services):
    states = services.get("services", {})
    return isinstance(states, dict) and {"gateway", "order", "inventory", "payment"} <= states.keys() and all(
        isinstance(state, dict) and type(state.get("telemetry_failed")) is int
        and state["telemetry_failed"] == 0 for state in states.values())


def services_ready(states):
    required = {"gateway", "order", "inventory", "payment"}
    return isinstance(states, dict) and required <= states.keys() and all(
        isinstance(states[name], dict) and states[name].get("status") == "ok" for name in required)


def run(args):
    run_clock = time.monotonic()
    root = Path(__file__).resolve().parent.parent
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    data_dir = tempfile.mkdtemp(prefix="aiops-v2-verification-")
    env = os.environ.copy()
    env["LLM_BASE_URL"] = ""
    env["LLM_MODEL"] = ""
    env["LLM_API_KEY"] = ""
    base = f"http://127.0.0.1:{args.port}"
    child, log = None, None
    frames, milestones = [], []
    reports, integrity_details = [], []
    durations = {"healthy": 35, "fault": 30, "recovery": 45}
    checks = {name: None for name in ("ui_http_200", "ui_content_valid", "one_availability_incident",
              "incident_resolved", "one_open_record", "one_recovery_record",
              "preliminary_enriched_resolved", "all_hashes_and_references_valid", "telemetry_failures_zero",
              "rca_frozen_and_verified", "rca_candidate_and_affected_roles", "rca_topology_api")}
    result = {"started_at": int(time.time()*1000), "duration_seconds": sum(durations.values()),
              "planned_duration_seconds": sum(durations.values()),
              "mode": "actual local TCP/HTTP + optimized Python incident/report pipeline; no Kafka/Flink/LLM",
              "status": "running", "completed": False, "checks": checks,
              "milestones": milestones, "frames": frames, "integrity_details": integrity_details,
              "data_dir": data_dir, "launcher_log": str(Path(data_dir)/"launcher.log")}
    phase, injected, cleared = "launch", False, False
    try:
        log = open(Path(data_dir)/"launcher.log", "w")
        child = subprocess.Popen([sys.executable, "-m", "scripts.local_platform", "--port", str(args.port),
                                 "--base-port", str(args.base_port), "--data-dir", data_dir],
                                 cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        phase = "startup"
        with httpx.Client(timeout=10, trust_env=False) as client:
            for _ in range(150):
                try:
                    response = client.get(base+"/health")
                    states = client.get(base+"/services").json()["services"]
                    if response.status_code == 200 and services_ready(states):
                        break
                except (httpx.HTTPError, KeyError):
                    pass
                if child.poll() is not None:
                    raise RuntimeError("launcher exited; inspect "+data_dir)
                time.sleep(0.2)
            else:
                raise RuntimeError("startup timeout; inspect "+data_dir)
            page = client.get(base+"/")
            checks.update(ui_http_200=page.status_code == 200, ui_content_valid="智能运维" in page.text)
            require_checks({name: checks[name] for name in ("ui_http_200", "ui_content_valid")})
            clock, epoch = time.monotonic(), int(time.time()*1000)
            result["started_at"] = epoch
            phase = "fault_cycle"
            print(json.dumps({"started": True, "duration_seconds": sum(durations.values()), "mode": "optimized-local-http; no Kafka/Flink/LLM"}), flush=True)
            seen_incidents, seen_reports, seen_notifications = set(), set(), set()
            next_frame = 0
            while time.monotonic()-clock < sum(durations.values()):
                t = time.monotonic()-clock
                if t >= 35 and not injected:
                    client.post(base+"/faults/payment", json={"kind": "error", "duration_seconds": 40, "error_rate": 1}).raise_for_status()
                    milestones.append({"t": round(t, 2), "kind": "inject"})
                    injected = True
                if t >= 65 and not cleared:
                    client.post(base+"/faults/payment", json={"kind": "none"}).raise_for_status()
                    milestones.append({"t": round(t, 2), "kind": "clear"})
                    cleared = True
                incidents = client.get(base+"/incidents").json()
                for incident in incidents:
                    iid = incident["incident_id"]
                    if iid not in seen_incidents:
                        milestones.append({"t": round(t, 2), "kind": "incident", "incident_id": iid})
                        seen_incidents.add(iid)
                    for version in client.get(base+f"/incidents/{iid}/reports").json():
                        if version["report_id"] not in seen_reports:
                            milestones.append({"t": round(t, 2), "kind": "report", "stage": version["stage"], "version": version["version"], "report_id": version["report_id"]})
                            seen_reports.add(version["report_id"])
                notifications = client.get(base+"/notifications").json()
                for note in notifications:
                    if note["notification_id"] not in seen_notifications:
                        milestones.append({"t": round(t, 2), "kind": note["kind"], "incident_id": note["incident_id"]})
                        seen_notifications.add(note["notification_id"])
                if t >= next_frame:
                    health = client.get(base+"/health").json()
                    frame = {"t": round(t, 2), "phase": "recovery" if cleared else "fault" if injected else "healthy",
                             "storage": health["storage"], "incidents": incidents}
                    frames.append(frame)
                    next_frame += 5
                    if int(t//5) % 3 == 0:
                        print(json.dumps({"elapsed_s": round(t, 1), "phase": frame["phase"], "storage": frame["storage"],
                                          "incident_states": [i["status"] for i in incidents]}), flush=True)
                time.sleep(0.5)
            incidents = [client.get(base+"/incidents/"+i["incident_id"]).json() for i in client.get(base+"/incidents").json()]
            raw_alerts = client.get(base+"/alerts?limit=500").json()
            phase = "integrity"
            for incident in incidents:
                for summary in client.get(base+f"/incidents/{incident['incident_id']}/reports").json():
                    report = client.get(base+"/report-versions/"+summary["report_id"]).json()
                    verify = client.get(base+f"/report-versions/{summary['report_id']}/verify").json()
                    integrity_details.append({"report_id": report.get("report_id"),
                                              "checks": report_integrity(report, verify)})
                    reports.append(report)
            notifications = client.get(base+"/notifications").json()
            health = client.get(base+"/health").json()
            services = client.get(base+"/services").json()
            availability = [i for i in incidents if i["family"] == "availability"]
            checks.update({"one_availability_incident": len(availability) == 1,
                      "incident_resolved": len(availability) == 1 and availability[0]["status"] == "resolved",
                      "one_open_record": len([n for n in notifications if n["kind"] == "opened" and n["incident_id"] in {i['incident_id'] for i in availability}]) == 1,
                      "one_recovery_record": len([n for n in notifications if n["kind"] == "resolved" and n["incident_id"] in {i['incident_id'] for i in availability}]) == 1,
                      "preliminary_enriched_resolved": {"preliminary", "enriched", "resolved"} <= {r["stage"] for r in reports},
                      "all_hashes_and_references_valid": bool(integrity_details) and all(
                          all(item["checks"].values()) for item in integrity_details),
                      "telemetry_failures_zero": telemetry_healthy(services)})
            checks['rca_frozen_and_verified'] = bool(reports) and all(
                'rca' in report and all(verify_attachment(report).values()) for report in reports)
            enriched = [report for report in reports if report.get('stage') in {'enriched', 'resolved'}]
            checks['rca_candidate_and_affected_roles'] = bool(enriched) and all(
                (report.get('rca', {}).get('candidates') or [{}])[0].get('service') == 'payment'
                and {'gateway', 'order'} <= set(report['rca']['affected_services']) for report in enriched)
            if availability:
                topology = client.get(base + '/incidents/' + availability[0]['incident_id'] + '/topology')
                topology.raise_for_status()
                result['rca_topology'] = topology.json()
                edges = {(edge['caller'], edge['callee']) for edge in topology.json()['graph']['edges']
                         if edge['basis'] == 'parent_span'}
                checks['rca_topology_api'] = {('gateway', 'order'), ('order', 'payment')} <= edges
            result.update({"storage": health["storage"], "incidents": incidents,
                      "notifications": notifications, "milestones": milestones, "frames": frames,
                      "raw_alerts": raw_alerts, "report_summaries": [{k: r[k] for k in ("report_id", "incident_id", "version", "stage", "created_at")} for r in reports],
                      "services": services})
            phase = "acceptance"
            require_checks(checks)
            if reports:
                latest = reports[-1]
                response = client.get(base+f"/report-versions/{latest['report_id']}/markdown")
                response.raise_for_status()
                (output/"optimized-report.md").write_text(response.text, encoding="utf-8")
            result.update(status="passed", completed=True)
            return result
    except BaseException as exc:
        result.update(status="failed", completed=False, failed_phase=phase,
                      error={"type": type(exc).__name__, "message": str(exc) if isinstance(exc, VerificationError)
                             else "verification failed during "+phase})
        raise
    finally:
        cleanup = {"fault_clear_attempted": False, "launcher_stopped": child is None, "forced_kill": False}
        try:
            if injected and not cleared:
                cleanup["fault_clear_attempted"] = True
                try:
                    with httpx.Client(timeout=2, trust_env=False) as client:
                        client.post(base+"/faults/payment", json={"kind": "none"}).raise_for_status()
                    cleanup["fault_cleared"] = True
                except Exception as exc:
                    cleanup.update(fault_cleared=False, fault_clear_error=type(exc).__name__)
            if child is not None:
                child.terminate()
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    cleanup["forced_kill"] = True
                    child.kill()
                    child.wait(timeout=5)
                cleanup["launcher_stopped"] = True
        except Exception as exc:
            cleanup["error_type"] = type(exc).__name__
            result.update(status="failed", completed=False)
        finally:
            if log is not None:
                log.close()
            result.update(cleanup=cleanup, finished_at=datetime.now(timezone.utc).isoformat(),
                          elapsed_seconds=round(time.monotonic()-run_clock, 3))
            (output/"optimized-report-versions.json").write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
            (output/"optimized-run.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps({"completed": result["completed"], "status": result["status"], "checks": checks,
                              "result_path": str(output/"optimized-run.json")}, ensure_ascii=False), flush=True)
        if not cleanup["launcher_stopped"] and sys.exc_info()[0] is None:
            raise VerificationError("launcher cleanup failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="examples")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument("--base-port", type=int, default=18181)
    run(parser.parse_args())
