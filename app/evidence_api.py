"""Read-only evidence/history endpoints. Retention remains a local CLI."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from .evidence import EvidenceRepository, decode_cursor, encode_cursor, filters_sql
from .reporting import ReportManager
from .storage import Store

router = APIRouter()


def checked(operation: Any) -> Any:
    try:
        return operation()
    except KeyError:
        raise HTTPException(404, "report or evidence set not found") from None
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


def evidence_page(store: Any, report: Any, size: Any, cursor: Any, filters: Any) -> Any:
    set_id = report["audit"].get("full_evidence_set_id")
    if set_id:
        return {
            "report_id": report["report_id"],
            **EvidenceRepository(store).page(set_id, size, cursor, **filters),
        }
    # Do not reconstruct an old immutable snapshot using today's live events.
    filters = {k: v for k, v in filters.items() if v is not None}
    filters_sql(filters)
    scope = {"legacy_report": report["report_id"], "filters": filters}
    after = decode_cursor(cursor, scope)
    matches = []
    for ordinal, record in enumerate(report["snapshot"]["records"], 1):
        e = record["event"]
        if any(e.get(k) != v for k, v in filters.items() if k not in ("start", "end")):
            continue
        if e["event_time"] < filters.get("start", 0) or e["event_time"] > filters.get(
            "end", 2**63 - 1
        ):
            continue
        matches.append({"ordinal": ordinal, **record})
    remaining = [r for r in matches if r["ordinal"] > after]
    return {
        "report_id": report["report_id"],
        "set_id": None,
        "total_count": None,
        "filtered_count": len(matches),
        "records": remaining[:size],
        "stored_complete": None,
        "completeness": "legacy_unknown; only saved analysis snapshot is available",
        "next_cursor": encode_cursor(scope, remaining[size - 1]["ordinal"])
        if len(remaining) > size
        else None,
    }


def verify_report_evidence(store: Any, report: Any) -> Any:
    set_id = report["audit"].get("full_evidence_set_id")
    if not set_id:
        return {"full_evidence_valid": None, "full_evidence_status": "legacy_unknown"}
    try:
        repository = EvidenceRepository(store)
        result = repository.verify(set_id)
        metadata = repository.metadata(set_id)
        valid = result["valid"] and metadata == report["snapshot"].get("full_evidence")
        # Verify that the saved analysis sample is actually part of this set.
        from .contracts import digest

        with store.db() as db:
            for record in report["snapshot"]["records"]:
                row = db.execute(
                    "SELECT record_sha256 FROM evidence_records WHERE set_id=? AND event_id=?",
                    (set_id, record["event"]["event_id"]),
                ).fetchone()
                valid = valid and row is not None and row[0] == digest(record)
        from .evidence import evidence_audit

        expected = evidence_audit(report)
        valid = valid and all(report["audit"].get(k) == v for k, v in expected.items())
        return {"full_evidence_valid": bool(valid), "full_evidence_verification": result}
    except (KeyError, ValueError, TypeError):
        return {"full_evidence_valid": False, "full_evidence_status": "missing_or_invalid"}


@router.get("/evidence-sets/{set_id}")
def metadata(set_id: str) -> Any:
    return checked(lambda: EvidenceRepository(Store()).metadata(set_id))


@router.get("/evidence-sets/{set_id}/verify")
def verify_set(set_id: str) -> Any:
    return checked(lambda: EvidenceRepository(Store()).verify(set_id))


@router.get("/evidence-sets/{set_id}/records")
def records(
    set_id: str,
    page_size: int = Query(200, ge=1, le=500),
    cursor: str | None = Query(None, max_length=512),
    service: str | None = None,
    event_type: str | None = None,
    event_id: str | None = None,
    trace_id: str | None = None,
    start: int | None = None,
    end: int | None = None,
) -> Any:
    return checked(
        lambda: EvidenceRepository(Store()).page(
            set_id,
            page_size,
            cursor,
            service=service,
            event_type=event_type,
            event_id=event_id,
            trace_id=trace_id,
            start=start,
            end=end,
        )
    )


@router.get("/report-versions/{report_id}/evidence")
@router.get("/reports/{report_id}/evidence")
def report_evidence(
    report_id: str,
    page_size: int = Query(200, ge=1, le=500),
    cursor: str | None = Query(None, max_length=512),
    service: str | None = None,
    event_type: str | None = None,
    event_id: str | None = None,
    trace_id: str | None = None,
    start: int | None = None,
    end: int | None = None,
) -> Any:
    def operation() -> Any:
        store = Store()
        try:
            report = ReportManager(store).get(report_id)
        except KeyError:
            report = store.get_report(report_id)
        return evidence_page(
            store,
            report,
            page_size,
            cursor,
            dict(
                service=service,
                event_type=event_type,
                event_id=event_id,
                trace_id=trace_id,
                start=start,
                end=end,
            ),
        )

    return checked(operation)


@router.get("/incidents/{incident_id}/evidence")
def incident_evidence(
    incident_id: str,
    page_size: int = Query(200, ge=1, le=500),
    service: str | None = None,
    event_type: str | None = None,
    event_id: str | None = None,
    trace_id: str | None = None,
    start: int | None = None,
    end: int | None = None,
) -> Any:
    # Bootstrap the latest version; continue using the returned report_id or
    # set_id route so that new reports cannot change pages under the cursor.
    def operation() -> Any:
        store = Store()
        report = ReportManager(store).latest(incident_id)
        if not report:
            raise KeyError(incident_id)
        return evidence_page(
            store,
            report,
            page_size,
            None,
            dict(
                service=service,
                event_type=event_type,
                event_id=event_id,
                trace_id=trace_id,
                start=start,
                end=end,
            ),
        )

    return checked(operation)


@router.get("/incidents/{incident_id}/report-history")
def history(
    incident_id: str,
    page_size: int = Query(100, ge=1, le=500),
    cursor: str | None = Query(None, max_length=512),
) -> Any:
    return checked(lambda: ReportManager(Store()).history(incident_id, page_size, cursor))


@router.get("/report-versions/{before_id}/compare/{after_id}")
def compare(before_id: str, after_id: str, sample_limit: int = Query(100, ge=1, le=500)) -> Any:
    return checked(lambda: ReportManager(Store()).compare(before_id, after_id, sample_limit))
