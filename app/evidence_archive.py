"""Streaming archives and conservative, explicitly applied raw-event retention.

Frozen evidence sets and reports are never deleted by this module. Every delete
has both a verified JSONL archive and an SQLite online backup made beforehand.
"""

import gzip
import hashlib
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

from .contracts import canonical, digest, now_ms, validate_event
from .evidence import (
    HASH_ALGORITHM,
    EvidenceRepository,
    deadline,
    online_backup,
    page_size,
    set_identity,
)

log = logging.getLogger(__name__)
MAX_ARCHIVE_LINE = 2_000_000


def write_archive(path: Any, records: Any, metadata: Any) -> Any:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".partial")
    count = 0
    sha = hashlib.sha256()
    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as output:
            output.write(
                canonical(
                    {"kind": "header", "format": "aiops-evidence-jsonl-v1", "metadata": metadata}
                )
                + "\n"
            )
            for record in records:
                encoded = canonical({"kind": "record", "record": record})
                if len(encoded.encode("utf-8")) > MAX_ARCHIVE_LINE - 1:
                    raise ValueError("archive record exceeds line byte limit")
                output.write(encoded + "\n")
                count += 1
                sha.update(bytes.fromhex(digest(record)))
            output.write(
                canonical(
                    {
                        "kind": "footer",
                        "count": count,
                        "records_sha256": sha.hexdigest(),
                        "hash_algorithm": HASH_ALGORITHM,
                    }
                )
                + "\n"
            )
        verified = verify_archive(temporary)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        # Exclusive atomic publication: never overwrite another archive.
        os.link(temporary, path)
        return {**verified, "path": str(path)}
    finally:
        temporary.unlink(missing_ok=True)


def read_archive(path: Any) -> Any:
    with gzip.open(path, "rb") as source:
        while True:
            raw = source.readline(MAX_ARCHIVE_LINE + 1)
            if not raw:
                break
            if len(raw) > MAX_ARCHIVE_LINE or not raw.endswith(b"\n"):
                raise ValueError("invalid or oversized archive line")
            yield json.loads(raw)


def consume_archive(path: Any, on_record: Any = None) -> Any:
    reader = read_archive(path)
    header = next(reader, None)
    if (
        not isinstance(header, dict)
        or header.get("kind") != "header"
        or header.get("format") != "aiops-evidence-jsonl-v1"
    ):
        raise ValueError("invalid archive header")
    sha = hashlib.sha256()
    count = 0
    footer = None
    previous = None
    for item in reader:
        if not isinstance(item, dict) or footer is not None:
            raise ValueError("invalid archive structure")
        if item.get("kind") == "footer":
            footer = item
            continue
        if item.get("kind") != "record":
            raise ValueError("invalid archive record")
        record = item.get("record")
        if not isinstance(record, dict) or set(record) != {"event", "source", "ingested_at"}:
            raise ValueError("invalid record envelope")
        event = validate_event(record["event"])
        if not isinstance(record["source"], dict) or type(record["ingested_at"]) is not int:
            raise ValueError("invalid record provenance")
        key = (event["event_time"], event["event_id"])
        if previous is not None and key <= previous:
            raise ValueError("archive records are duplicated or unordered")
        previous = key
        count += 1
        sha.update(bytes.fromhex(digest(record)))
        if on_record:
            on_record(record)
    if (
        not footer
        or footer.get("count") != count
        or footer.get("records_sha256") != sha.hexdigest()
        or footer.get("hash_algorithm") != HASH_ALGORITHM
    ):
        raise ValueError("archive incomplete or hash mismatch")
    metadata = header.get("metadata", {})
    if "records_sha256" in metadata and (
        metadata["records_sha256"] != sha.hexdigest() or metadata.get("total_count") != count
    ):
        raise ValueError("archive differs from frozen set manifest")
    if "set_id" in metadata and set_identity(metadata) != metadata["set_id"]:
        raise ValueError("archive manifest identity mismatch")
    return {"valid": True, "count": count, "records_sha256": sha.hexdigest(), "metadata": metadata}


def verify_archive(path: Any) -> Any:
    return consume_archive(path)


def export_set(store: Any, set_id: Any, path: Any) -> Any:
    repo = EvidenceRepository(store)
    # Read metadata and records under the same lock. Verification failures leave
    # no published archive, so retention cannot rely on a partial file.
    with store.db():
        metadata = repo.metadata(set_id)
        if not repo.verify(set_id)["valid"]:
            raise ValueError("evidence set integrity check failed")
        result = write_archive(
            path, (json.loads(r[1]) for r in repo.iter_records(set_id)), metadata
        )
    log.info("evidence_archive_created set_id=%s records=%d", set_id, result["count"])
    return result


def restore_events(store: Any, path: Any) -> Any:
    # Revalidate while inserting inside one transaction to avoid a verify/use
    # race; any footer failure rolls back all inserts. Existing IDs never change.
    with store.db() as db:
        backup = online_backup(store, db, "before-evidence-restore")
    inserted = 0
    with store.atomic() as db, deadline(db) as check:

        def insert(record: Any) -> Any:
            nonlocal inserted
            check()
            e = record["event"]
            payload = canonical(e)
            source = canonical(record["source"])
            old = db.execute(
                "SELECT payload,source,ingested_at FROM events WHERE event_id=?", (e["event_id"],)
            ).fetchone()
            if old:
                if tuple(old) != (payload, source, record["ingested_at"]):
                    raise ValueError(
                        "archive event_id conflicts with existing content or provenance"
                    )
                return
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?,?,?,?)",
                (
                    e["event_id"],
                    e["event_time"],
                    e["service"],
                    e.get("trace_id", ""),
                    e["event_type"],
                    payload,
                    source,
                    record["ingested_at"],
                ),
            )
            inserted += 1

        result = consume_archive(path, insert)
    log.info("evidence_archive_restored inserted=%d", inserted)
    return {**result, "inserted": inserted, "backup": str(backup)}


def retention_plan(store: Any, before: Any, size: Any = 500, now: Any = None) -> Any:
    page_size(size)
    retention_days = int(os.getenv("EVIDENCE_RETENTION_DAYS", "30"))
    if not 7 <= retention_days <= 3650:
        raise ValueError("EVIDENCE_RETENTION_DAYS must be in 7..3650")
    now = now_ms() if now is None else now
    if type(before) is not int or before < 0 or before > now - retention_days * 86400000:
        raise ValueError("cutoff violates EVIDENCE_RETENTION_DAYS minimum age")
    effective: int | float = before
    protections = []
    with store.db() as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "incidents" in tables:
            for row in db.execute("SELECT payload FROM incidents WHERE status<>'resolved'"):
                incident = json.loads(row[0])
                start = incident.get("first_anomaly_at", incident.get("first_event_time"))
                # The first anomalous sample may be later than the alert's
                # window. Preserve healthy comparison samples in that window.
                first_alert = incident.get("first_alert_id") or next(
                    iter(incident.get("alert_ids", [])), None
                )
                alert_row = db.execute(
                    "SELECT payload FROM alerts WHERE alert_id=?", (first_alert,)
                ).fetchone()
                if alert_row:
                    window_start = json.loads(alert_row[0])["window_start"]
                    start = (
                        min(start, window_start) if isinstance(start, int | float) else window_start
                    )
                # Missing legacy bounds: protect everything, never guess.
                effective = min(
                    effective, max(0, start - 5000) if isinstance(start, int | float) else 0
                )
                if "active_incidents" not in protections:
                    protections.append("active_incidents")
        reported_incident = ""
        if {"incident_alerts", "report_versions"} <= tables:
            reported_incident = " AND NOT EXISTS (SELECT 1 FROM incident_alerts ia JOIN report_versions rv ON ia.incident_id=rv.incident_id WHERE ia.alert_id=a.alert_id)"
        for row in db.execute(
            "SELECT a.payload FROM alerts a WHERE NOT EXISTS (SELECT 1 FROM reports r WHERE r.alert_id=a.alert_id)"
            + reported_incident
        ):
            effective = min(effective, max(0, json.loads(row[0])["window_start"] - 5000))
            if "unreported_alerts" not in protections:
                protections.append("unreported_alerts")
        count = db.execute(
            "SELECT COUNT(*) FROM events WHERE event_time<? AND ingested_at<?", (effective, before)
        ).fetchone()[0]
    return {
        "requested_before": before,
        "effective_before": effective,
        "retention_days": retention_days,
        "eligible_count": count,
        "batch_count": min(count, size),
        "batch_size": size,
        "protections": protections,
        "preserves": ["frozen_evidence_sets", "reports", "alerts", "incidents", "backups"],
    }


def prune_events(
    store: Any, before: Any, output_dir: Any, size: Any = 500, apply: Any = False, now: Any = None
) -> Any:
    # Hold the application access lock across plan, backup, archive and delete;
    # cooperating workers cannot race a newly opened incident into this cutoff.
    with store.access_lock():
        plan = retention_plan(store, before, size, now)
        if not apply or not plan["batch_count"]:
            return {**plan, "applied": False, "deleted": 0}
        with store.db() as db:
            backup = online_backup(store, db, "before-evidence-prune")
        with store.atomic() as db, deadline(db) as check:
            rows = db.execute(
                "SELECT payload,source,ingested_at FROM events WHERE event_time<? AND ingested_at<? ORDER BY event_time,event_id LIMIT ?",
                (plan["effective_before"], before, size),
            )
            # Only one bounded batch is retained so deletion IDs exactly match
            # the verified archive, even if event timestamps collide.
            records = [
                {"event": json.loads(r[0]), "source": json.loads(r[1]), "ingested_at": r[2]}
                for r in rows
            ]
            path = Path(output_dir) / ("raw-events-" + uuid.uuid4().hex + ".jsonl.gz")
            archive = write_archive(
                path, iter(records), {"kind": "raw-event-retention", "plan": plan}
            )
            check()
            db.executemany(
                "DELETE FROM events WHERE event_id=?", [(r["event"]["event_id"],) for r in records]
            )
    log.info("raw_events_pruned records=%d backup=%s archive=%s", len(records), backup, path)
    return {
        **plan,
        "applied": True,
        "deleted": len(records),
        "backup": str(backup),
        "archive": archive,
    }
