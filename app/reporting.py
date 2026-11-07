"""Progressive incident reports with immutable snapshots and one model attempt."""

import contextlib
import json
import os
import uuid
from typing import Any

from . import diagnosis
from .contracts import canonical, digest, now_ms
from .evidence import decode_cursor, encode_cursor, evidence_audit, page_size
from .rca import RCA_VERSION, analyze
from .retrieval import retrieve


class ReportManager:
    def __init__(self, store: Any) -> None:
        self.store = store
        store.ensure_schema(
            """
                CREATE TABLE IF NOT EXISTS report_versions (
                  report_id TEXT PRIMARY KEY, incident_id TEXT NOT NULL, version INTEGER NOT NULL,
                  stage TEXT NOT NULL, created_at INTEGER NOT NULL, state_hash TEXT NOT NULL,
                  payload TEXT NOT NULL, UNIQUE(incident_id,version));
                CREATE TABLE IF NOT EXISTS incident_model_attempts (
                  incident_id TEXT PRIMARY KEY, attempted_at INTEGER NOT NULL,
                  status TEXT NOT NULL, metadata TEXT NOT NULL);
            """,
            {"report_versions", "incident_model_attempts"},
            "reporting-v1",
        )

    def get(self, report_id: Any) -> Any:
        with self.store.db() as db:
            row = db.execute(
                "SELECT payload FROM report_versions WHERE report_id=?", (report_id,)
            ).fetchone()
        if not row:
            raise KeyError(report_id)
        return json.loads(row[0])

    def latest(self, incident_id: Any) -> Any:
        with self.store.db() as db:
            row = db.execute(
                "SELECT payload FROM report_versions WHERE incident_id=? ORDER BY version DESC LIMIT 1",
                (incident_id,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def list_versions(self, incident_id: Any, limit: Any = 500) -> Any:
        # Retain the old list response and ascending order, bounded to latest 500.
        page_size(limit)
        with self.store.db() as db:
            rows = db.execute(
                self._summary_sql() + " WHERE incident_id=? ORDER BY version DESC LIMIT ?",
                (incident_id, limit),
            ).fetchall()
        return [json.loads(row[1]) for row in reversed(rows)]

    @staticmethod
    def _summary_sql() -> Any:
        return """SELECT version,json_object('report_id',report_id,'incident_id',incident_id,
            'version',version,'stage',stage,'created_at',created_at,
            'analysis',json_extract(payload,'$.analysis'),
            'audit',json_object('analysis_mode',json_extract(payload,'$.audit.analysis_mode'),
                'prompt_version',json_extract(payload,'$.audit.prompt_version'),
                'snapshot_sha256',json_extract(payload,'$.audit.snapshot_sha256'),
                'model_failure',json_extract(payload,'$.audit.model_failure'),
                'full_evidence_set_id',json_extract(payload,'$.audit.full_evidence_set_id'),
                'evidence_completeness',COALESCE(json_extract(payload,'$.audit.evidence_completeness'),'legacy_unknown')),
            'evidence_count',json_array_length(payload,'$.snapshot.records')) FROM report_versions"""

    def history(self, incident_id: Any, size: Any = 100, cursor: Any = None) -> Any:
        page_size(size)
        scope = {"incident_id": incident_id, "kind": "report-history-v1"}
        after = decode_cursor(cursor, scope)
        with self.store.db() as db:
            rows = db.execute(
                self._summary_sql() + " WHERE incident_id=? AND version>? ORDER BY version LIMIT ?",
                (incident_id, after, size + 1),
            ).fetchall()
        return {
            "reports": [json.loads(row[1]) for row in rows[:size]],
            "next_cursor": encode_cursor(scope, rows[size - 1][0]) if len(rows) > size else None,
            "order": "version ascending; later appended versions may appear on subsequent pages",
        }

    def compare(self, before_id: Any, after_id: Any, limit: Any = 100) -> Any:
        page_size(limit)
        before, after = self.get(before_id), self.get(after_id)
        if before["incident_id"] != after["incident_id"]:
            raise ValueError("reports must belong to the same incident")
        left = before["audit"].get("full_evidence_set_id")
        right = after["audit"].get("full_evidence_set_id")
        changes = {}
        if left and right:
            with self.store.db() as db:
                for name, source, target, predicate in (
                    ("added", right, left, "b.event_id IS NULL"),
                    ("removed", left, right, "b.event_id IS NULL"),
                    (
                        "changed",
                        right,
                        left,
                        "b.event_id IS NOT NULL AND a.record_sha256<>b.record_sha256",
                    ),
                ):
                    query = (
                        " FROM evidence_records a LEFT JOIN evidence_records b ON b.set_id=? AND b.event_id=a.event_id WHERE a.set_id=? AND "
                        + predicate
                    )
                    count = db.execute("SELECT COUNT(*)" + query, (target, source)).fetchone()[0]
                    ids = [
                        r[0]
                        for r in db.execute(
                            "SELECT a.event_id" + query + " ORDER BY a.ordinal LIMIT ?",
                            (target, source, limit),
                        )
                    ]
                    changes[name] = {"count": count, "event_ids": ids, "truncated": count > limit}
        return {
            "before_report_id": before_id,
            "after_report_id": after_id,
            "before_version": before["version"],
            "after_version": after["version"],
            "full_comparison_available": bool(left and right),
            "evidence_changes": changes,
            "analysis_changed": digest(before["analysis"]) != digest(after["analysis"]),
            "before_evidence": before["audit"].get("evidence_completeness", "legacy_unknown"),
            "after_evidence": after["audit"].get("evidence_completeness", "legacy_unknown"),
            "note": "counts cover full stored sets; displayed IDs are bounded; legacy full evidence is unknown",
        }

    def _base(self, incident: Any) -> Any:
        alert_id = incident.get("first_alert_id") or next(iter(incident.get("alert_ids", [])), None)
        if not alert_id:
            raise ValueError("incident has no representative alert")
        alert, source = self.store.get_alert(alert_id)
        return alert, source

    def _snapshot(self, incident: Any, alert: Any) -> Any:
        query = dict(alert)
        # Evidence bounds are rooted in the original fault window, not report time.
        query["window_start"] = min(
            alert["window_start"], incident.get("first_event_time") or alert["window_start"]
        )
        query["window_end"] = max(
            alert["window_end"],
            incident.get("latest_alert_end") or alert["window_end"],
            incident.get("last_event_time") or alert["window_end"],
        )
        snapshot = self.store.evidence(query)
        facts, missing = diagnosis.evidence_observations(snapshot)
        snapshot["coverage"]["trace_coverage"] = facts["trace_coverage"]
        return snapshot, facts, missing

    def ensure_initial(self, incident: Any, now: Any = None) -> Any:
        existing = self.latest(incident["incident_id"])
        if existing:
            return existing
        return self._create(incident, "preliminary", now or now_ms(), allow_model=False)

    def refresh(self, incident: Any, now: Any = None, force: Any = False) -> Any:
        now = now or now_ms()
        latest = self.latest(incident["incident_id"])
        if not latest:
            return self.ensure_initial(incident, now)
        if latest["stage"] == "resolved":
            return None
        age = now - latest["created_at"]
        if incident.get("status") == "resolved":
            return self._create(incident, "resolved", now, allow_model=False)
        if age < int(os.getenv("REPORT_ENRICH_MIN_MS", "3000")) and not force:
            return None
        alert, _ = self._base(incident)
        snapshot, facts, _ = self._snapshot(incident, alert)
        ready = facts["trace_coverage"]["complete_anomalous_trace_count"] > 0
        if not facts["trace_coverage"]["trace_count"]:
            ready = sum(r["event"]["event_type"] == "host" for r in snapshot["records"]) >= 3
        deadline = age >= int(os.getenv("REPORT_ENRICH_DEADLINE_MS", "15000"))
        if latest["stage"] != "preliminary" and not force:
            previous_coverage = latest["observations"]["trace_coverage"]
            previously_missing = sum(
                previous_coverage.get(key, 0)
                for key in (
                    "missing_parent_span_count",
                    "missing_downstream_span_count",
                    "missing_request_span_count",
                    "missing_request_log_count",
                )
            )
            now_missing = sum(
                facts["trace_coverage"].get(key, 0)
                for key in (
                    "missing_parent_span_count",
                    "missing_downstream_span_count",
                    "missing_request_span_count",
                    "missing_request_log_count",
                )
            )
            changed_services = sorted(incident.get("services", [])) != sorted(
                latest["incident"].get("services", [])
            )
            changed_candidate = incident.get("candidate_service") != latest["incident"].get(
                "candidate_service"
            )
            changed_evidence = snapshot.get("full_evidence", {}).get("set_id") != latest[
                "snapshot"
            ].get("full_evidence", {}).get("set_id")
            if not (
                changed_services
                or changed_candidate
                or changed_evidence
                or (previously_missing > 0 and now_missing < previously_missing)
            ):
                return None
        if not (ready or deadline or force):
            return None
        return self._create(incident, "enriched", now, allow_model=True, prepared=(snapshot, facts))

    def _create(
        self, incident: Any, stage: Any, now: Any, allow_model: Any = False, prepared: Any = None
    ) -> Any:
        alert, source = self._base(incident)
        if prepared:
            snapshot, observations = prepared
            _, missing = diagnosis.evidence_observations(snapshot)
        else:
            snapshot, observations, missing = self._snapshot(incident, alert)
        retrieval_alert = dict(alert)
        if "fast" in alert["rule_id"] and "error" in alert["rule_id"]:
            retrieval_alert["rule_id"] = "high_error_rate"
        cases = retrieve(retrieval_alert, snapshot)
        related = []
        for alert_id in incident.get("alert_ids", [])[:20]:
            with contextlib.suppress(KeyError):
                related.append(self.store.get_alert(alert_id)[0])
        payload, selected = diagnosis.build_context(
            alert,
            snapshot,
            cases,
            observations,
            missing,
            incident=incident,
            related_alerts=related or [alert],
            related_alert_total=len(incident.get("alert_ids", [])) or 1,
            prompt_version=diagnosis.PROMPT_VERSION,
        )
        analysis = diagnosis.heuristic(alert, snapshot)
        metadata: dict[str, Any]
        mode, metadata, failure, failure_code, budget_audit = (
            "heuristic",
            {"model_status": "not_requested"},
            None,
            None,
            None,
        )
        previous = self.latest(incident["incident_id"])
        if previous and previous["audit"]["analysis_mode"] == "llm-assisted":
            try:
                from .model_grounding import ground_analysis, grounding_metadata, reusable_grounded

                analysis, grounding = ground_analysis(
                    previous["analysis"], [r["event"] for r in snapshot["records"]], alert
                )
                mode = "llm-assisted"
                metadata = {
                    **previous["audit"].get("model_metadata", {}),
                    **grounding_metadata(grounding),
                    "reused_from_report_id": previous["report_id"],
                }
                if reusable_grounded(
                    previous["analysis"], previous["audit"].get("model_metadata", {})
                ):
                    analysis = previous["analysis"]
                    metadata = {
                        **previous["audit"]["model_metadata"],
                        "reused_from_report_id": previous["report_id"],
                        "reuse_revalidated": True,
                    }
                budget_audit = previous["audit"].get("model_budget")
            except ValueError:
                metadata = {"model_status": "previous_interpretation_not_reusable"}
        if allow_model and selected:
            outcome = diagnosis.model_diagnosis(
                self.store,
                "incident:" + incident["incident_id"],
                payload,
                selected,
                incident["incident_id"],
            )
            failure, failure_code, budget_audit = (
                outcome["failure"],
                outcome["failure_code"],
                outcome["budget"],
            )
            if outcome["analysis"] is not None:
                analysis, mode, metadata = outcome["analysis"], "llm-assisted", outcome["metadata"]
            elif mode != "llm-assisted":
                metadata = outcome["metadata"]
        rca_result = analyze(
            snapshot,
            alert=alert,
            historical_cases=cases,
            model_analysis=analysis if mode == "llm-assisted" else None,
            model_metadata=metadata,
        )
        if mode == "heuristic":
            analysis = diagnosis.heuristic(alert, snapshot, rca_result)
        if stage == "resolved":
            analysis = dict(analysis)
            analysis["summary"] = "故障事件已达到连续健康确认条件。" + analysis["summary"]
        state_hash = digest(
            {
                "stage": stage,
                "status": incident.get("status"),
                "snapshot": snapshot,
                "services": incident.get("services"),
                "candidate_service": incident.get("candidate_service"),
                "rca_version": RCA_VERSION,
                "rca_sha256": digest(rca_result),
            }
        )
        if previous and previous["audit"].get("state_sha256") == state_hash:
            return previous
        selected_traces = sorted({event["trace_id"] for event in selected if event.get("trace_id")})
        report = {
            "report_id": str(uuid.uuid4()),
            "created_at": now,
            "incident_id": incident["incident_id"],
            "version": 0,
            "stage": stage,
            "incident": incident,
            "alert": alert,
            "analysis": analysis,
            "observations": observations,
            "missing_evidence": missing,
            "snapshot": snapshot,
            "historical_cases": cases,
            "rca": rca_result,
            "audit": {
                "analysis_mode": mode,
                "prompt_version": diagnosis.PROMPT_VERSION,
                "snapshot_sha256": digest(snapshot),
                "state_sha256": state_hash,
                "alert_source": source,
                "rca_version": RCA_VERSION,
                "rca_sha256": digest(rca_result),
                "input_sha256": digest({"system": diagnosis.SYSTEM, "payload": payload}),
                "model_metadata": metadata,
                "model_failure": failure,
                "model_failure_code": failure_code,
                "model_budget": budget_audit,
                "evidence_set_id": payload["evidence_set"]["id"],
                "model_context_selection": payload["selection"],
                "prompt": {"system": diagnosis.SYSTEM, "payload": payload},
                "selected_trace_ids": selected_traces,
                "selected_record_count": len(selected),
                "context_selection": payload["selection"]["strategy"],
                "prompt_scope": "logical_context; actual transmitted request and hash are in model_metadata",
                "rule_version": alert.get("rule_version", "window-rules-v1"),
                "case_corpus_sha256": digest(cases),
                "case_sources": sorted(
                    {case["case"].get("source", "unspecified") for case in cases}
                ),
                "limitations": [
                    "候选服务和内部根因不同；仍需依赖、资源与变更证据验证",
                    "完整观测链不证明跨主题数据全部到齐",
                    "confidence为判断等级，不是校准概率",
                    "报告版本不可变，补证生成新版本",
                ],
            },
        }
        report["audit"].update(evidence_audit(report))
        with self.store.db() as db:
            db.execute("BEGIN IMMEDIATE")
            last = db.execute(
                "SELECT version,state_hash,payload FROM report_versions WHERE incident_id=? ORDER BY version DESC LIMIT 1",
                (incident["incident_id"],),
            ).fetchone()
            if last and last[1] == state_hash:
                return json.loads(last[2])
            report["version"] = (last[0] + 1) if last else 1
            db.execute(
                "INSERT INTO report_versions VALUES(?,?,?,?,?,?,?)",
                (
                    report["report_id"],
                    incident["incident_id"],
                    report["version"],
                    stage,
                    now,
                    state_hash,
                    canonical(report),
                ),
            )
        return report
