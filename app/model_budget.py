"""SQLite-backed per-incident HTTP budgets, claims and append-only attempt audit."""

import json
import logging
import sqlite3
import time
import uuid
from contextlib import ExitStack, closing
from typing import Any

from .contracts import canonical, now_ms
from .llm_config import ModelError, redact

log = logging.getLogger(__name__)
SCHEMA = """
CREATE TABLE IF NOT EXISTS model_jobs (
 scope_id TEXT PRIMARY KEY, legacy_id TEXT, owner TEXT NOT NULL, claimed_at INTEGER NOT NULL,
 lease_until INTEGER NOT NULL, status TEXT NOT NULL, max_calls INTEGER NOT NULL,
 calls_reserved INTEGER NOT NULL, recoveries INTEGER NOT NULL, metadata TEXT NOT NULL, result TEXT);
CREATE TABLE IF NOT EXISTS model_calls (
 scope_id TEXT NOT NULL, ordinal INTEGER NOT NULL, started_at INTEGER NOT NULL,
 finished_at INTEGER, outcome TEXT NOT NULL, metadata TEXT NOT NULL,
 PRIMARY KEY(scope_id,ordinal));
CREATE TABLE IF NOT EXISTS model_rate_limits (
 bucket TEXT PRIMARY KEY, window_start INTEGER NOT NULL, calls INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS incident_model_attempts (
 incident_id TEXT PRIMARY KEY, attempted_at INTEGER NOT NULL, status TEXT NOT NULL, metadata TEXT NOT NULL);
"""


class ModelBudget:
    def __init__(self, store: Any) -> None:
        self.store = store
        with store.db() as db:
            present = {
                r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        if not {"model_jobs", "model_calls", "model_rate_limits"} <= present:
            # Additive schema only. An online backup must finish before CREATE.
            backup = (
                store.root / "backups" / ("before-model-budget-v2-" + uuid.uuid4().hex + ".sqlite3")
            )
            backup.parent.mkdir(parents=True, exist_ok=True)
            temporary = backup.with_suffix(".partial")
            deadline = time.monotonic() + 10

            def progress(*_: Any) -> Any:
                if time.monotonic() > deadline:
                    raise TimeoutError("schema backup deadline exceeded")

            try:
                # ExitStack closes both connections, including on partial open failure.
                with store.access_lock(), ExitStack() as connections:
                    source = connections.enter_context(
                        closing(sqlite3.connect(store.path, timeout=5))
                    )
                    target = connections.enter_context(closing(sqlite3.connect(temporary)))
                    source.backup(target, pages=256, progress=progress, sleep=0.05)
                temporary.replace(backup)
                with store.db() as db:
                    db.executescript(SCHEMA)
            finally:
                temporary.unlink(missing_ok=True)
            log.info("Model audit additive schema initialized after SQLite backup")

    def claim(
        self, scope_id: Any, config: Any, context_metadata: Any, incident_id: Any = None
    ) -> Any:
        owner, now = uuid.uuid4().hex, now_ms()
        lease_until = now + int(config.total_timeout_s * 1000) + 5000
        with self.store.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM model_jobs WHERE scope_id=?", (scope_id,)).fetchone()
            if row:
                if row["status"] != "running" or row["lease_until"] >= now:
                    return None
                # A request might have reached the provider before a crash. Never
                # spend another call to guess its outcome, even with budget left.
                if row["calls_reserved"] or row["recoveries"] >= 1:
                    metadata = {
                        **json.loads(row["metadata"]),
                        "failure_code": "expired_claim_outcome_unknown",
                        "calls_reserved": row["calls_reserved"],
                    }
                    db.execute(
                        "UPDATE model_jobs SET status='indeterminate',metadata=? WHERE scope_id=?",
                        (canonical(metadata), scope_id),
                    )
                    if row["legacy_id"]:
                        db.execute(
                            "UPDATE incident_model_attempts SET status='failed',metadata=? WHERE incident_id=?",
                            (canonical(metadata), row["legacy_id"]),
                        )
                    return None
                db.execute(
                    "UPDATE model_jobs SET owner=?,claimed_at=?,lease_until=?,recoveries=recoveries+1 WHERE scope_id=?",
                    (owner, now, lease_until, scope_id),
                )
            else:
                if incident_id:
                    prior = db.execute(
                        "SELECT 1 FROM incident_model_attempts WHERE incident_id=?", (incident_id,)
                    ).fetchone()
                    if prior:
                        return None  # Conservatively honor pre-upgrade claims.
                    db.execute(
                        "INSERT INTO incident_model_attempts VALUES(?,?,?,?)",
                        (incident_id, now, "claimed", "{}"),
                    )
                db.execute(
                    "INSERT INTO model_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        scope_id,
                        incident_id,
                        owner,
                        now,
                        lease_until,
                        "running",
                        config.max_calls_per_incident,
                        0,
                        0,
                        canonical(redact(context_metadata)),
                        None,
                    ),
                )
        return ModelSession(self, scope_id, owner, config)

    def cached(self, scope_id: Any) -> Any:
        with self.store.db() as db:
            row = db.execute(
                "SELECT result FROM model_jobs WHERE scope_id=? AND status='succeeded'", (scope_id,)
            ).fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def audit(self, scope_id: Any) -> Any:
        with self.store.db() as db:
            row = db.execute(
                "SELECT scope_id,status,max_calls,calls_reserved,recoveries,metadata FROM model_jobs WHERE scope_id=?",
                (scope_id,),
            ).fetchone()
            calls = db.execute(
                "SELECT ordinal,started_at,finished_at,outcome,metadata FROM model_calls WHERE scope_id=? ORDER BY ordinal",
                (scope_id,),
            ).fetchall()
        if not row:
            return {"scope_id": scope_id, "status": "not_claimed", "calls_reserved": 0}
        return {
            **{
                k: row[k]
                for k in ("scope_id", "status", "max_calls", "calls_reserved", "recoveries")
            },
            "metadata": json.loads(row["metadata"]),
            "attempts": [
                {
                    **{k: c[k] for k in ("ordinal", "started_at", "finished_at", "outcome")},
                    "metadata": json.loads(c["metadata"]),
                }
                for c in calls
            ],
        }


class ModelSession:
    def __init__(self, budget: Any, scope_id: Any, owner: Any, config: Any) -> None:
        self.budget, self.scope_id, self.owner, self.config = budget, scope_id, owner, config

    def reserve(self) -> Any:
        now = now_ms()
        with self.budget.store.db() as db:
            db.execute("BEGIN IMMEDIATE")
            job = db.execute(
                "SELECT * FROM model_jobs WHERE scope_id=?", (self.scope_id,)
            ).fetchone()
            if (
                not job
                or job["owner"] != self.owner
                or job["status"] != "running"
                or job["lease_until"] < now
            ):
                raise ModelError("model_claim_lost")
            if job["calls_reserved"] >= job["max_calls"]:
                raise ModelError("incident_call_budget_exhausted")
            window = now // 60_000 * 60_000
            rate = db.execute(
                "SELECT window_start,calls FROM model_rate_limits WHERE bucket='global' "
            ).fetchone()
            count = rate[1] if rate and rate[0] == window else 0
            if count >= self.config.requests_per_minute:
                raise ModelError("local_rate_limited")
            ordinal = job["calls_reserved"] + 1
            db.execute(
                "INSERT INTO model_rate_limits VALUES('global',?,?) ON CONFLICT(bucket) DO UPDATE SET window_start=excluded.window_start,calls=excluded.calls",
                (window, count + 1),
            )
            db.execute(
                "UPDATE model_jobs SET calls_reserved=? WHERE scope_id=?", (ordinal, self.scope_id)
            )
            db.execute(
                "INSERT INTO model_calls VALUES(?,?,?,?,?,?)",
                (self.scope_id, ordinal, now, None, "started", "{}"),
            )
        return ordinal

    def finish_attempt(self, ordinal: Any, outcome: Any, metadata: Any) -> Any:
        with self.budget.store.db() as db:
            db.execute(
                "UPDATE model_calls SET finished_at=?,outcome=?,metadata=? WHERE scope_id=? AND ordinal=?",
                (now_ms(), outcome, canonical(redact(metadata)), self.scope_id, ordinal),
            )

    def complete(self, analysis: Any, metadata: Any, failure_code: Any = None) -> Any:
        safe = redact(metadata)
        with self.budget.store.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT legacy_id,owner,status,metadata FROM model_jobs WHERE scope_id=?",
                (self.scope_id,),
            ).fetchone()
            if not row or row["owner"] != self.owner or row["status"] != "running":
                raise ModelError("model_claim_lost")
            result = (
                canonical(redact({"analysis": analysis, "metadata": safe}))
                if analysis is not None
                else None
            )
            state = "succeeded" if analysis is not None else "failed"
            job_metadata = {
                **json.loads(row["metadata"]),
                "failure_code": failure_code,
                "model": self.config.model,
            }
            db.execute(
                "UPDATE model_jobs SET status=?,metadata=?,result=? WHERE scope_id=?",
                (state, canonical(redact(job_metadata)), result, self.scope_id),
            )
            if row["legacy_id"]:
                db.execute(
                    "UPDATE incident_model_attempts SET status=?,metadata=? WHERE incident_id=?",
                    (
                        state,
                        canonical(
                            redact({"failure_code": failure_code, "model": self.config.model})
                        ),
                        row["legacy_id"],
                    ),
                )
