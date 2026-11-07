"""Immutable full evidence sets; bounded pages and deterministic content hashes.

The analysis snapshot remains bounded. Completeness here means *persisted at
capture time and matching this query*, never a Kafka cross-topic watermark.
"""

import base64
import hashlib
import json
import logging
import math
import os
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from typing import Any

from .contracts import canonical, digest, now_ms

log = logging.getLogger(__name__)
HASH_ALGORITHM = "sha256-concat-record-sha256-v1"
MAX_PAGE_SIZE = 500
SCHEMA_OBJECTS = {
    ("table", "evidence_sets"),
    ("table", "evidence_records"),
    ("index", "evidence_filter_idx"),
}


def online_backup(store: Any, db: Any, label: Any) -> Any:
    folder = store.root / "backups"
    folder.mkdir(exist_ok=True)
    path = folder / (label + "-" + uuid.uuid4().hex + ".sqlite3")
    temporary = path.with_suffix(".partial")
    deadline = time.monotonic() + 30

    def progress(*_: Any) -> Any:
        if time.monotonic() > deadline:
            raise TimeoutError("database backup deadline exceeded")

    try:
        with closing(sqlite3.connect(temporary)) as target:
            db.backup(target, pages=256, progress=progress, sleep=0.05)
            if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("database backup integrity check failed")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception as exc:
        log.error("evidence_backup_failed error_type=%s", type(exc).__name__)
        raise
    finally:
        temporary.unlink(missing_ok=True)
    return path


def schema_current(db: Any, objects: Any = None) -> Any:
    objects = (
        objects
        if objects is not None
        else {(row[0], row[1]) for row in db.execute("SELECT type,name FROM sqlite_master")}
    )
    return (
        ("table", "platform_migrations") in objects
        and objects >= SCHEMA_OBJECTS
        and db.execute("SELECT 1 FROM platform_migrations WHERE version='phase8-v1'").fetchone()
        is not None
    )


def initialize_schema(store: Any, existed: Any, backup: Any = None) -> Any:
    with store.db() as db:
        if schema_current(db):
            return
        if existed and backup is None:
            backup = online_backup(store, db, "before-phase8")
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE IF NOT EXISTS evidence_sets (
            set_id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, metadata TEXT NOT NULL)""")
        db.execute("""CREATE TABLE IF NOT EXISTS evidence_records (
            set_id TEXT NOT NULL, ordinal INTEGER NOT NULL, event_id TEXT NOT NULL,
            event_time INTEGER NOT NULL, service TEXT NOT NULL, event_type TEXT NOT NULL,
            trace_id TEXT NOT NULL, record TEXT NOT NULL, record_sha256 TEXT NOT NULL,
            PRIMARY KEY(set_id,ordinal), UNIQUE(set_id,event_id))""")
        db.execute(
            "CREATE INDEX IF NOT EXISTS evidence_filter_idx ON evidence_records(set_id,service,event_type,event_time)"
        )
        db.execute(
            "INSERT OR REPLACE INTO platform_migrations VALUES(?,?,?)",
            ("phase8-v1", now_ms(), str(backup) if backup else None),
        )
        log.info("phase8_additive_schema_initialized backup_created=%s", backup is not None)


@contextmanager
def deadline(db: Any) -> Any:
    seconds = float(os.getenv("EVIDENCE_OPERATION_TIMEOUT_SECONDS", "30"))
    if not math.isfinite(seconds) or not 1 <= seconds <= 120:
        raise ValueError("EVIDENCE_OPERATION_TIMEOUT_SECONDS must be finite in 1..120")
    end = time.monotonic() + seconds
    db.set_progress_handler(lambda: int(time.monotonic() > end), 1000)

    def check() -> Any:
        if time.monotonic() > end:
            raise sqlite3.OperationalError("evidence operation deadline exceeded")

    try:
        yield check
    except sqlite3.Error:
        log.exception("evidence_database_operation_failed")
        raise
    finally:
        db.set_progress_handler(None, 0)


def encode_cursor(scope: Any, position: Any) -> Any:
    return (
        base64.urlsafe_b64encode(canonical({"scope": digest(scope), "after": position}).encode())
        .decode()
        .rstrip("=")
    )


def decode_cursor(cursor: Any, scope: Any) -> Any:
    if cursor is None:
        return 0
    try:
        if not isinstance(cursor, str) or len(cursor) > 512:
            raise ValueError()
        obj = json.loads(
            base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        )
        if set(obj) != {"scope", "after"} or obj["scope"] != digest(scope):
            raise ValueError()
        if type(obj["after"]) is not int or not 0 <= obj["after"] <= 2**63 - 1:
            raise ValueError()
        return obj["after"]
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ValueError("invalid cursor or cursor does not match query") from exc


def page_size(value: Any) -> Any:
    if type(value) is not int or not 1 <= value <= MAX_PAGE_SIZE:
        raise ValueError("page_size must be in 1..500")
    return value


def filters_sql(filters: Any, prefix: Any = "") -> Any:
    if set(filters) - {"service", "event_type", "event_id", "trace_id", "start", "end"}:
        raise ValueError("unknown evidence filter")
    clauses: list[str] = []
    args: list[str | int] = []
    for key in ("service", "event_type", "event_id", "trace_id"):
        value = filters.get(key)
        if value is not None:
            if not isinstance(value, str) or not 1 <= len(value) <= 512:
                raise ValueError("invalid " + key)
            if key == "event_type" and value not in ("request", "host", "span", "log"):
                raise ValueError("invalid event_type")
            clauses.append(prefix + key + "=?")
            args.append(value)
    for key, operator in (("start", ">="), ("end", "<=")):
        value = filters.get(key)
        if value is not None:
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ValueError("invalid " + key)
            clauses.append(prefix + "event_time" + operator + "?")
            args.append(value)
    if (
        filters.get("start") is not None
        and filters.get("end") is not None
        and filters["start"] > filters["end"]
    ):
        raise ValueError("start must not exceed end")
    return (" AND " + " AND ".join(clauses) if clauses else ""), args


def set_identity(metadata: Any) -> Any:
    return "full-evidence-" + digest(
        {
            k: metadata[k]
            for k in (
                "query",
                "total_count",
                "records_sha256",
                "hash_algorithm",
                "service_counts",
                "type_counts",
            )
        }
    )


class EvidenceRepository:
    def __init__(self, store: Any) -> None:
        self.store = store

    def capture(self, alert: Any, limit: Any = 1800) -> Any:
        if type(limit) is not int or not 1 <= limit <= 1800:
            raise ValueError("analysis evidence limit must be in 1..1800")
        start, end = alert["window_start"] - 5000, alert["window_end"] + 5000
        query = {
            "start": start,
            "end": end,
            "service": alert["service"],
            "relation": "same-service OR same-trace",
        }
        temporary_id = "pending-" + uuid.uuid4().hex
        with self.store.atomic() as db, deadline(db) as check:
            total_range = db.execute(
                "SELECT COUNT(*) FROM events WHERE event_time BETWEEN ? AND ?", (start, end)
            ).fetchone()[0]
            rows = db.execute(
                """SELECT payload,source,ingested_at FROM events
                WHERE event_time BETWEEN ? AND ? AND (service=? OR
                    trace_id IN (SELECT trace_id FROM events WHERE service=?
                        AND event_time BETWEEN ? AND ? AND trace_id<>''))
                ORDER BY event_time,event_id""",
                (start, end, alert["service"], alert["service"], start, end),
            )
            stream_hash = hashlib.sha256()
            count, observed = 0, None
            # Counts are aggregated in SQL after the streaming copy, not an
            # unbounded Python dictionary of service names.
            while True:
                batch = rows.fetchmany(200)
                if not batch:
                    break
                check()
                inserts = []
                for row in batch:
                    record = {
                        "event": json.loads(row[0]),
                        "source": json.loads(row[1]),
                        "ingested_at": row[2],
                    }
                    event = record["event"]
                    count += 1
                    sha = digest(record)
                    stream_hash.update(bytes.fromhex(sha))
                    inserts.append(
                        (
                            temporary_id,
                            count,
                            event["event_id"],
                            event["event_time"],
                            event["service"],
                            event["event_type"],
                            event.get("trace_id", ""),
                            canonical(record),
                            sha,
                        )
                    )
                    if event["service"] == alert["service"] and event["event_type"] in (
                        "request",
                        "host",
                    ):
                        observed = max(observed or event["event_time"], event["event_time"])
                db.executemany("INSERT INTO evidence_records VALUES(?,?,?,?,?,?,?,?,?)", inserts)

            # Stats are lists with a hard bound; exact totals remain available
            # via filtered page counts even in high-cardinality environments.
            def counts(column: Any) -> Any:
                result = db.execute(
                    "SELECT "
                    + column
                    + ",COUNT(*) FROM evidence_records WHERE set_id=? GROUP BY "
                    + column
                    + " ORDER BY "
                    + column
                    + " LIMIT 501",
                    (temporary_id,),
                ).fetchall()
                return {
                    "values": {r[0]: r[1] for r in result[:500]},
                    "truncated": len(result) > 500,
                }

            metadata = {
                "query": query,
                "total_count": count,
                "records_sha256": stream_hash.hexdigest(),
                "hash_algorithm": HASH_ALGORITHM,
                "service_counts": counts("service"),
                "type_counts": counts("event_type"),
                "stored_complete": True,
                "completeness_scope": "persisted matching events at capture; no cross-topic watermark",
            }
            set_id = set_identity(metadata)
            metadata["set_id"] = set_id
            if db.execute("SELECT 1 FROM evidence_sets WHERE set_id=?", (set_id,)).fetchone():
                db.execute("DELETE FROM evidence_records WHERE set_id=?", (temporary_id,))
            else:
                db.execute(
                    "UPDATE evidence_records SET set_id=? WHERE set_id=?", (set_id, temporary_id)
                )
                db.execute(
                    "INSERT INTO evidence_sets VALUES(?,?,?)",
                    (set_id, now_ms(), canonical(metadata)),
                )
            selected = db.execute(
                """SELECT record FROM evidence_records WHERE set_id=? ORDER BY
                (COALESCE(json_extract(record,'$.event.status_code'),0)>=500 OR COALESCE(json_extract(record,'$.event.level'),'')='ERROR') DESC,
                (event_type='span') DESC,event_time,event_id LIMIT ?""",
                (set_id, limit),
            ).fetchall()
            selected = sorted(
                (json.loads(r[0]) for r in selected),
                key=lambda r: (r["event"]["event_time"], r["event"]["event_id"]),
            )
            check()
        log.info("evidence_captured set_id=%s total=%d selected=%d", set_id, count, len(selected))
        return {
            "query": query,
            "full_evidence": metadata,
            "coverage": {
                "range_candidate_count": total_range,
                "scanned": total_range,
                "related": count,
                "selected": len(selected),
                "truncated": count > limit,
                "stored_complete": True,
                "latest_service_metric_time": observed,
                "metric_time_reached_window_end": observed is not None
                and observed >= alert["window_end"],
                "all_topics_complete": "unknown; independent Kafka topics have no shared watermark",
            },
            "records": selected,
        }

    def metadata(self, set_id: Any) -> Any:
        with self.store.db() as db:
            row = db.execute(
                "SELECT metadata FROM evidence_sets WHERE set_id=?", (set_id,)
            ).fetchone()
        if row is None:
            raise KeyError(set_id)
        return json.loads(row[0])

    def page(self, set_id: Any, size: Any = 200, cursor: Any = None, **filters: Any) -> Any:
        page_size(size)
        metadata = self.metadata(set_id)
        filters = {k: v for k, v in filters.items() if v is not None}
        sql, args = filters_sql(filters)
        scope = {"set_id": set_id, "filters": filters}
        after = decode_cursor(cursor, scope)
        with self.store.db() as db, deadline(db):
            total = db.execute(
                "SELECT COUNT(*) FROM evidence_records WHERE set_id=?" + sql, [set_id] + args
            ).fetchone()[0]
            rows = db.execute(
                "SELECT ordinal,record,record_sha256 FROM evidence_records WHERE set_id=? AND ordinal>?"
                + sql
                + " ORDER BY ordinal LIMIT ?",
                [set_id, after] + args + [size + 1],
            ).fetchall()
        result = rows[:size]
        return {
            "set_id": set_id,
            "total_count": metadata["total_count"],
            "filtered_count": total,
            "records": [
                {"ordinal": r[0], "record_sha256": r[2], **json.loads(r[1])} for r in result
            ],
            "next_cursor": encode_cursor(scope, result[-1][0]) if len(rows) > size else None,
            "stored_complete": metadata["stored_complete"],
        }

    def iter_records(self, set_id: Any) -> Any:
        # One read transaction protects an export/verification from concurrent
        # modifications; yield at most one record, SQLite fetches in batches.
        with self.store.db() as db, deadline(db) as check:
            rows = db.execute(
                "SELECT ordinal,record,record_sha256,event_id,event_time,service,event_type,trace_id FROM evidence_records WHERE set_id=? ORDER BY ordinal",
                (set_id,),
            )
            while True:
                batch = rows.fetchmany(200)
                if not batch:
                    break
                check()
                yield from batch

    def verify(self, set_id: Any) -> Any:
        metadata = self.metadata(set_id)
        count, valid = 0, True
        stream_hash = hashlib.sha256()
        previous_key = None
        for row in self.iter_records(set_id):
            count += 1
            try:
                record = json.loads(row[1])
                event = record["event"]
                sha = digest(record)
                stream_hash.update(bytes.fromhex(sha))
                key = (event["event_time"], event["event_id"])
                valid = (
                    valid
                    and row[0] == count
                    and sha == row[2]
                    and (previous_key is None or key > previous_key)
                )
                valid = valid and tuple(row[3:]) == (
                    event["event_id"],
                    event["event_time"],
                    event["service"],
                    event["event_type"],
                    event.get("trace_id", ""),
                )
                previous_key = key
            except (ValueError, KeyError, TypeError, AttributeError):
                valid = False
        valid = (
            valid
            and count == metadata["total_count"]
            and stream_hash.hexdigest() == metadata["records_sha256"]
            and set_identity(metadata) == set_id
        )
        return {
            "set_id": set_id,
            "valid": valid,
            "verified_count": count,
            "expected_count": metadata["total_count"],
            "records_sha256": stream_hash.hexdigest(),
            "note": "local consistency check, not an external signature or delivery watermark",
        }


def evidence_audit(report: Any) -> Any:
    snapshot = report["snapshot"]
    full = snapshot.get("full_evidence")
    available = {r["event"]["event_id"] for r in snapshot["records"]}
    refs: set[str] = set()

    def walk(value: Any) -> Any:
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {
                    "evidence_ids",
                    "supporting_evidence_ids",
                    "against_evidence_ids",
                    "own_anomalous_evidence_ids",
                    "child_event_ids",
                } and isinstance(child, list):
                    refs.update(r for r in child if isinstance(r, str))
                elif key in {
                    "caller_event_id",
                    "callee_event_id",
                    "parent_event_id",
                } and isinstance(child, str):
                    refs.add(child)
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(report.get("analysis", {}))
    walk(report.get("rca", {}))
    if refs - available:
        raise ValueError("report cites evidence outside its analysis snapshot")
    return {
        "full_evidence_set_id": full["set_id"] if full else None,
        "evidence_completeness": "analysis_sampled"
        if full and snapshot["coverage"]["truncated"]
        else "stored_query_complete"
        if full
        else "legacy_unknown",
        "full_evidence_count": full["total_count"] if full else None,
        "analysis_evidence_count": len(available),
        "analysis_truncated": snapshot["coverage"].get("truncated"),
        "referenced_evidence_ids": sorted(refs),
        "referenced_evidence_count": len(refs),
    }
