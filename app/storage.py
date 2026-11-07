import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from .contracts import canonical, now_ms, validate_alert, validate_event
from .runtime_support import require_supported_runtime

log = logging.getLogger(__name__)

BASE_SCHEMA_OBJECTS = {
    ("table", "events"),
    ("table", "alerts"),
    ("table", "reports"),
    ("table", "deadletters"),
    ("index", "event_time_idx"),
    ("index", "trace_idx"),
    ("index", "service_kind_time_idx"),
}
PHASE7_SCHEMA_OBJECTS = {
    ("table", "pipeline_fast_state"),
    ("table", "deadletter_keys"),
    ("table", "platform_migrations"),
}


class Store:
    def __init__(self, data_dir: Any = None) -> None:
        require_supported_runtime()  # Before mkdir, lock files, SQLite, or migration.
        self._transaction = threading.local()
        self.timeout_s = float(os.getenv("DB_TIMEOUT_SECONDS", "3"))
        if not math.isfinite(self.timeout_s) or not 0.05 <= self.timeout_s <= 30:
            raise ValueError("DB_TIMEOUT_SECONDS must be finite in 0.05..30")
        self.root = Path(data_dir or os.getenv("DATA_DIR", "data"))
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "aiops.sqlite3"
        # A separate lock inode never touches SQLite's own POSIX database locks.
        # Serialize our connections across threads/processes, including close and
        # checkpoint. Keep WAL and existing data; do not repair damaged files.
        self._access_lock = FileLock(
            str(self.path.resolve()) + ".access.lock",
            timeout=30,
            is_singleton=True,
            preserve_lock_file=True,
            fallback_to_soft=False,
        )
        # Hold the same reentrant process/thread lock through inspection, backup,
        # and all initialization stages. Concurrent Store constructors must not
        # race the existence check or publish redundant migration backups.
        from .evidence import initialize_schema

        with self.access_lock():
            existed = self.path.exists()
            self._initialization_backups: dict[str, Any] = {}
            with self.db() as db:
                if existed:
                    self._backup_before_initialization(db)
                db.executescript("""
              PRAGMA journal_mode=WAL;
              CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, event_time INTEGER NOT NULL,
                service TEXT NOT NULL, trace_id TEXT NOT NULL,
                event_type TEXT NOT NULL, payload TEXT NOT NULL,
                source TEXT NOT NULL, ingested_at INTEGER NOT NULL);
              CREATE INDEX IF NOT EXISTS event_time_idx ON events(event_time);
              CREATE INDEX IF NOT EXISTS trace_idx ON events(trace_id);
              CREATE INDEX IF NOT EXISTS service_kind_time_idx ON events(service,event_type,event_time);
              CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                received_at INTEGER NOT NULL, source TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY, alert_id TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL, payload TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS deadletters (
                id INTEGER PRIMARY KEY AUTOINCREMENT, received_at INTEGER,
                reason TEXT NOT NULL, raw TEXT NOT NULL, source TEXT NOT NULL);
                """)

            self._phase7_schema(existed)
            initialize_schema(self, existed, backup=self._initialization_backups.get("phase8"))

    @staticmethod
    def _phase7_current(db: Any, objects: Any = None) -> Any:
        objects = (
            objects
            if objects is not None
            else {(r[0], r[1]) for r in db.execute("SELECT type,name FROM sqlite_master")}
        )
        return (
            objects >= PHASE7_SCHEMA_OBJECTS
            and db.execute("SELECT 1 FROM platform_migrations WHERE version='phase7-v1'").fetchone()
            is not None
        )

    def _backup_before_initialization(self, db: Any) -> Any:
        """Publish a consistent backup before *any* schema or journal change.

        Modern databases with the required objects and WAL mode only incur
        read-only inspection. A partial legacy schema is never modified if
        backup or integrity verification fails. Phase-specific names remain
        compatible with existing migration audit and restore tooling.
        """
        from .evidence import online_backup, schema_current

        objects = {(r[0], r[1]) for r in db.execute("SELECT type,name FROM sqlite_master")}
        if not self._phase7_current(db, objects):
            stage = "phase7"
        elif not schema_current(db, objects):
            stage = "phase8"
        elif (
            not objects >= BASE_SCHEMA_OBJECTS
            or db.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal"
        ):
            stage = "initialize"
        else:
            return
        backup = online_backup(self, db, "before-" + stage)
        self._initialization_backups[stage] = backup
        log.info("database_pre_initialization_backup_completed stage=%s", stage)

    def _phase7_schema(self, existed: Any) -> Any:
        with self.db() as db:
            if self._phase7_current(db):
                return
            backup = self._initialization_backups.get("phase7")
            if existed and backup is None:
                from .evidence import online_backup

                backup = online_backup(self, db, "before-phase7")
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "CREATE TABLE IF NOT EXISTS pipeline_fast_state (state_key TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at INTEGER NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS deadletter_keys (delivery_key TEXT PRIMARY KEY, deadletter_id INTEGER NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS platform_migrations (version TEXT PRIMARY KEY, applied_at INTEGER NOT NULL, backup_path TEXT)"
            )
            db.execute(
                "INSERT OR REPLACE INTO platform_migrations VALUES(?,?,?)",
                ("phase7-v1", now_ms(), str(backup) if backup else None),
            )
            log.info("phase7_additive_schema_initialized backup_created=%s", backup is not None)

    def ensure_schema(self, script: Any, required: Any, label: Any) -> Any:
        """Apply a trusted additive DDL script once, after a restorable backup.

        Callers supply static application DDL and required table/index names;
        this method is not a public SQL endpoint. Names are only compared with
        sqlite_master, never interpolated into SQL. The backup and schema
        change share one access lock, preventing constructor races. Do not
        initialize schemas inside an existing data transaction.
        """
        if not isinstance(script, str) or not script.strip() or len(script.encode()) > 65536:
            raise ValueError("schema script must be nonempty bounded application DDL")
        if (
            not isinstance(required, set | frozenset | list | tuple)
            or not 1 <= len(required) <= 100
            or any(
                not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name)
                for name in required
            )
        ):
            raise ValueError("required schema objects must be bounded table/index identifiers")
        if not isinstance(label, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,63}", label):
            raise ValueError("schema label must be a safe fixed identifier")
        required = frozenset(required)
        with self.db() as db:
            present = {
                r[0]
                for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
                )
            }
            if required <= present:
                return None
            if db.in_transaction:
                raise sqlite3.OperationalError(
                    "schema initialization cannot interrupt an active transaction"
                )
            from .evidence import online_backup

            backup = online_backup(self, db, "before-" + label)
            # executescript would commit an existing transaction, hence the
            # check above. Our BEGIN keeps this entire additive change atomic.
            db.executescript("BEGIN IMMEDIATE;\n" + script)
            present = {
                r[0]
                for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
                )
            }
            if not required <= present:
                raise sqlite3.OperationalError("schema script did not create all required objects")
            db.execute(
                "INSERT OR REPLACE INTO platform_migrations VALUES(?,?,?)",
                ("schema:" + label, now_ms(), str(backup)),
            )
            log.info("additive_schema_initialized label=%s backup_created=True", label)
            return backup

    @contextmanager
    def atomic(self) -> Any:
        """One connection/transaction for nested calls on this Store and thread."""
        if getattr(self._transaction, "connection", None) is not None:
            yield self._transaction.connection
            return
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._transaction.connection = db
            try:
                yield db
            finally:
                del self._transaction.connection

    @contextmanager
    def access_lock(self) -> Any:
        try:
            self._access_lock.acquire(timeout=min(self.timeout_s, self._access_lock.timeout))
        except (FileLockTimeout, OSError) as error:
            log.error("database_access_lock_failed error_type=%s", type(error).__name__)
            raise sqlite3.OperationalError("database access lock unavailable") from error
        try:
            yield
        finally:
            self._access_lock.release()

    @contextmanager
    def db(self) -> Any:
        active = getattr(self._transaction, "connection", None)
        if active is not None:
            yield active
            return
        with self.access_lock():
            db = sqlite3.connect(self.path, timeout=self.timeout_s)
            db.row_factory = sqlite3.Row
            try:
                with db:
                    yield db
            finally:
                db.close()

    def ingest(self, event: Any, source: Any = None) -> Any:
        self.ingest_many([(event, source)])

    def ingest_many(self, entries: Any) -> Any:
        """Persist first payload per ID; return only newly accepted entries.

        Validate the entire batch before writing anything. Replays of the same
        canonical event are harmless even when transport coordinates differ.
        Conflicting payloads are retained in deadletters in the same transaction
        as event writes, and never enter downstream detectors.
        """
        rows = []
        received = now_ms()
        for event, source in entries:
            validate_event(event)
            row = (
                event["event_id"],
                event["event_time"],
                event["service"],
                event.get("trace_id", ""),
                event["event_type"],
                canonical(event),
                canonical(source or {"transport": "local"}),
                received,
            )
            rows.append(((event, source), row))
        accepted, conflicts = [], []
        with self.db() as db:
            for entry, row in rows:
                inserted = db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?)", row)
                if inserted.rowcount == 1:
                    accepted.append(entry)
                    continue
                persisted = db.execute(
                    "SELECT payload FROM events WHERE event_id=?", (row[0],)
                ).fetchone()
                if persisted is None:
                    raise RuntimeError("event insert was ignored without an existing event_id")
                if persisted[0] != row[5]:
                    self._deadletter(
                        db,
                        received,
                        "event_id_payload_conflict:first-persisted-wins:" + row[0],
                        row[5],
                        json.loads(row[6]),
                        truncate=False,
                    )
                    conflicts.append(row[0])
        for event_id in conflicts:
            log.warning("Conflicting event payload retained in deadletters; event_id=%s", event_id)
        return accepted

    def alert(self, alert: Any, source: Any = None) -> Any:
        validate_alert(alert)
        with self.db() as db:
            db.execute(
                "INSERT OR IGNORE INTO alerts VALUES(?,?,?,?)",
                (
                    alert["alert_id"],
                    canonical(alert),
                    now_ms(),
                    canonical(source or {"transport": "local"}),
                ),
            )

    def deadletter(self, reason: Any, raw: Any, source: Any) -> Any:
        with self.db() as db:
            self._deadletter(db, now_ms(), reason, raw, source)

    def _deadletter(
        self, db: Any, received: Any, reason: Any, raw: Any, source: Any, truncate: Any = True
    ) -> Any:
        key = None
        if (
            isinstance(source, dict)
            and source.get("transport") == "kafka"
            and isinstance(source.get("topic"), str)
            and type(source.get("partition")) is int
            and type(source.get("offset")) is int
        ):
            # Coordinates plus content distinguish malformed test records and topic
            # recreation; a replay of the same record has the same durable key.
            key = hashlib.sha256(
                canonical(
                    [source["topic"], source["partition"], source["offset"], str(raw), str(reason)]
                ).encode()
            ).hexdigest()
            if db.execute("SELECT 1 FROM deadletter_keys WHERE delivery_key=?", (key,)).fetchone():
                return
        inserted = db.execute(
            "INSERT INTO deadletters(received_at,reason,raw,source) VALUES(?,?,?,?)",
            (
                received,
                str(reason)[:2000],
                str(raw)[:100_000] if truncate else str(raw),
                canonical(source),
            ),
        )
        if key:
            db.execute("INSERT INTO deadletter_keys VALUES(?,?)", (key, inserted.lastrowid))

    def get_alert(self, alert_id: Any) -> Any:
        with self.db() as db:
            row = db.execute("SELECT * FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
        if row is None:
            raise KeyError(alert_id)
        return json.loads(row["payload"]), json.loads(row["source"])

    def list_alerts(self, limit: Any = 100) -> Any:
        with self.db() as db:
            rows = db.execute(
                "SELECT payload FROM alerts ORDER BY received_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def pending(self, min_age_ms: Any = 8000) -> Any:
        with self.db() as db:
            rows = db.execute(
                """SELECT a.alert_id FROM alerts a LEFT JOIN reports r
                ON a.alert_id=r.alert_id WHERE r.report_id IS NULL AND a.received_at<=?
                ORDER BY a.received_at LIMIT 10""",
                (now_ms() - min_age_ms,),
            ).fetchall()
        return [r[0] for r in rows]

    def evidence(self, alert: Any, limit: Any = 1800) -> Any:
        from .evidence import EvidenceRepository

        return EvidenceRepository(self).capture(alert, limit)

    def report_for_alert(self, alert_id: Any) -> Any:
        with self.db() as db:
            row = db.execute("SELECT payload FROM reports WHERE alert_id=?", (alert_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def save_report(self, report: Any) -> Any:
        # Unique alert_id is the idempotent write boundary; original snapshot stays immutable.
        with self.db() as db:
            db.execute(
                "INSERT OR IGNORE INTO reports VALUES(?,?,?,?)",
                (
                    report["report_id"],
                    report["alert"]["alert_id"],
                    report["created_at"],
                    canonical(report),
                ),
            )
        return self.report_for_alert(report["alert"]["alert_id"])

    def get_report(self, report_id: Any) -> Any:
        with self.db() as db:
            row = db.execute(
                "SELECT payload FROM reports WHERE report_id=?", (report_id,)
            ).fetchone()
        if row is None:
            raise KeyError(report_id)
        return json.loads(row[0])

    def list_reports(self, limit: Any = 50) -> Any:
        with self.db() as db:
            rows = db.execute(
                """SELECT json_object('report_id',report_id,'created_at',created_at,
                'alert',json_extract(payload,'$.alert'),'analysis',json_extract(payload,'$.analysis'),
                'audit',json_object('analysis_mode',json_extract(payload,'$.audit.analysis_mode'),
                    'prompt_version',json_extract(payload,'$.audit.prompt_version'),
                    'snapshot_sha256',json_extract(payload,'$.audit.snapshot_sha256'),
                    'model_failure',json_extract(payload,'$.audit.model_failure'),
                    'full_evidence_set_id',json_extract(payload,'$.audit.full_evidence_set_id'),
                    'evidence_completeness',COALESCE(json_extract(payload,'$.audit.evidence_completeness'),'legacy_unknown')))
                FROM reports ORDER BY created_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def stats(self) -> Any:
        with self.db() as db:
            existing = {
                row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            return {
                name: db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                for name in (
                    "events",
                    "alerts",
                    "reports",
                    "deadletters",
                    "incidents",
                    "report_versions",
                    "incident_notifications",
                )
                if name in existing
            }
