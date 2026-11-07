"""Shared incident/report orchestration for Kafka worker and local HTTP mode."""

import json
import logging
import os
import threading
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from .contracts import canonical, now_ms
from .fast_detection import FastErrorDetector
from .incidents import IncidentManager
from .reporting import ReportManager

log = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, store: Any) -> None:
        self.store = store
        self.incidents = IncidentManager(store)
        self.reports = ReportManager(store)
        self.fast = FastErrorDetector()
        self.fast_state_limit = int(os.getenv("FAST_STATE_MAX_KEYS", "512"))
        if not 1 <= self.fast_state_limit <= 4096:
            raise ValueError("FAST_STATE_MAX_KEYS must be 1..4096")
        self._ingest_lock = threading.RLock()
        self._tick_lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="incident-report")
        self.future: Future[Any] | None = None
        self._refresh_after: dict[str, Any] = {}

    def ingest_batch(self, entries: Any, alerts: Any = ()) -> Any:
        """Persist telemetry before analysis and before caller commits transport offsets."""
        changed = []
        with self._ingest_lock, self.store.atomic() as db:
            accepted = self.store.ingest_many(entries)
            detector = FastErrorDetector()
            touched = set()
            generated = []
            for event, source in accepted:
                if event.get("event_type") != "request":
                    continue
                key = (event["service"], event["instance_id"])
                state_key = canonical(key)
                if key not in touched:
                    row = db.execute(
                        "SELECT payload FROM pipeline_fast_state WHERE state_key=?", (state_key,)
                    ).fetchone()
                    if row:
                        state = json.loads(row[0])
                        state["seen"] = {e["event_id"] for e in state["errors"]}
                        state["recent"] = deque(state["seen"])
                        detector.states[key] = state
                    touched.add(key)
                for alert in detector.observe(event):
                    generated.append(
                        (
                            alert,
                            {
                                "transport": "fast-detector",
                                "input_source": source,
                                "engine": "python-consecutive-errors-v1",
                            },
                        )
                    )
            for key in touched:
                state = detector.states[key]
                persisted = {k: state[k] for k in ("last_time", "errors", "fired")}
                db.execute(
                    "INSERT INTO pipeline_fast_state VALUES(?,?,?) ON CONFLICT(state_key) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
                    (canonical(key), canonical(persisted), now_ms()),
                )
            removed = db.execute(
                "DELETE FROM pipeline_fast_state WHERE state_key IN (SELECT state_key FROM pipeline_fast_state ORDER BY updated_at DESC,state_key LIMIT -1 OFFSET ?)",
                (self.fast_state_limit,),
            ).rowcount
            if removed:
                log.warning(
                    "fast_detector_state_evicted count=%d limit=%d", removed, self.fast_state_limit
                )
            for alert, source in [*generated, *alerts]:
                changed.append(self.incidents.ingest_alert(alert, source))
        # Memory is diagnostic only; authoritative state committed in SQLite.
        self.fast = FastErrorDetector()
        return changed

    def accept_alert(self, alert: Any, source: Any = None) -> Any:
        with self._ingest_lock, self.store.atomic():
            return self.incidents.ingest_alert(alert, source)

    def tick(self, now: Any = None) -> Any:
        current = now if now is not None else now_ms()
        if not self._tick_lock.acquire(blocking=False):
            return
        try:
            with self._ingest_lock:
                self.incidents.tick(now=current)
            with self.store.db() as db:
                rows = db.execute("""SELECT payload FROM incidents WHERE status!='resolved'
                    OR incident_id NOT IN (SELECT incident_id FROM report_versions WHERE stage='resolved')
                    ORDER BY updated_at DESC""").fetchall()
            incidents = [json.loads(row[0]) for row in rows]
            active_ids = {i["incident_id"] for i in incidents}
            self._refresh_after = {k: v for k, v in self._refresh_after.items() if k in active_ids}
            # A slow enrichment/model call must not delay a different initial report.
            # This path never calls a model and only freezes bounded local evidence.
            for incident in sorted(incidents, key=lambda i: i.get("opened_at", 0)):
                if self.reports.latest(incident["incident_id"]) is None:
                    self.reports.ensure_initial(incident)
            if self.future is not None:
                if not self.future.done():
                    return
                try:
                    self.future.result()
                except Exception:
                    log.exception("Report job failed; will retry with bounded polling")
                self.future = None
            # Oldest dispatch first prevents busy/newer incidents starving others.
            for incident in sorted(
                incidents,
                key=lambda i: (
                    i["status"] != "resolved",
                    self._refresh_after.get(i["incident_id"], 0),
                    i.get("opened_at", 0),
                ),
            ):
                iid = incident["incident_id"]
                if self._refresh_after.get(iid, 0) > current:
                    continue
                latest = self.reports.latest(iid)
                if (
                    latest
                    and latest.get("stage") == "resolved"
                    and incident["status"] == "resolved"
                ):
                    continue
                self._refresh_after[iid] = current + 1000
                self.future = self.pool.submit(self.reports.refresh, incident)
                return
        finally:
            self._tick_lock.release()

    def close(self) -> Any:
        self.pool.shutdown(wait=True)
