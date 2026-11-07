"""Wall-clock local demonstration of the same window rules; not a Flink engine."""

import json
import logging
from typing import Any

from flink.adaptive import DetectionConfig, evaluate_adaptive, fresh_state
from flink.aggregate import WindowAccumulator, summarize_accumulator
from flink.rules import evaluate_window

from .contracts import now_ms
from .detection_state import DetectionStateFile

log = logging.getLogger(__name__)


class LocalWindowDetector:
    def __init__(self, pipeline: Any, start_time: Any = None, config: Any = None) -> None:
        self.pipeline = pipeline
        current = now_ms() if start_time is None else start_time
        self.next_end = (current // 10_000 + 1) * 10_000
        self.config = config or DetectionConfig.from_env()
        self.state_file = (
            DetectionStateFile(pipeline.store.root, self.config)
            if self.config.mode == "dynamic"
            else None
        )
        if self.state_file:
            with self.state_file.lock:
                self.state_file.load(self.next_end)  # fail on corruption/config changes at startup
        log.info("window_detector mode=%s config_id=%s", self.config.mode, self.config.fingerprint)

    def advance(self) -> Any:
        if self.state_file:
            return self._advance_dynamic()
        store = self.pipeline.store
        with store.db() as db:
            latest = db.execute(
                "SELECT MAX(event_time) FROM events WHERE event_type IN ('request','host')"
            ).fetchone()[0]
        if latest is None:
            return []
        watermark = latest - 5000
        emitted = []
        while self.next_end <= watermark:
            with store.db() as db:
                rows = db.execute(
                    "SELECT payload FROM events WHERE event_type IN ('request','host') AND event_time>=? AND event_time<?",
                    (self.next_end - 30_000, self.next_end),
                ).fetchall()
            events = [json.loads(row[0]) for row in rows]
            for service in sorted({e["service"] for e in events}):
                for alert in evaluate_window(
                    events, service, self.next_end - 30_000, self.next_end
                ):
                    self.pipeline.accept_alert(
                        alert,
                        {
                            "transport": "local-rule-evaluator",
                            "engine": "python-shared-window-rules-not-flink",
                        },
                    )
                    emitted.append(alert)
            self.next_end += 10_000
        return emitted

    def _advance_dynamic(self) -> Any:
        # Serialize callers across processes; persist pending alerts before delivery.
        # A crash after delivery may replay the same stable alert ID, never relearn.
        state_file = self.state_file
        if state_file is None:
            raise RuntimeError("dynamic detector state is unavailable")
        with state_file.lock:
            state = state_file.load(self.next_end)
            emitted = []

            def drain() -> Any:
                for alert in state["pending"]:
                    self.pipeline.accept_alert(
                        alert,
                        {
                            "transport": "local-rule-evaluator",
                            "engine": "python-adaptive-not-flink",
                        },
                    )
                    emitted.append(alert)
                if state["pending"]:
                    state["pending"] = []
                    state_file.save(state)

            drain()
            with self.pipeline.store.db() as db:
                latest = db.execute(
                    "SELECT MAX(event_time) FROM events WHERE event_type IN ('request','host')"
                ).fetchone()[0]
            if latest is None:
                return emitted
            # Bound catch-up work per tick; the durable cursor resumes on next call.
            for _ in range(100):
                end = state["next_end"]
                if end > latest - 5000:
                    break
                with self.pipeline.store.db() as db:
                    services = [
                        row[0]
                        for row in db.execute(
                            "SELECT DISTINCT service FROM events WHERE event_type IN ('request','host') AND event_time>=? AND event_time<? ORDER BY service",
                            (end - 30000, end),
                        )
                    ]
                for service in services:
                    acc = WindowAccumulator()
                    with self.pipeline.store.db() as db:
                        rows = db.execute(
                            "SELECT payload FROM events WHERE service=? AND event_type IN ('request','host') AND event_time>=? AND event_time<? ORDER BY event_time,event_id",
                            (service, end - 30000, end),
                        )
                        for row in rows:
                            acc.add_or_quarantine(json.loads(row[0]))
                    baselines = state["services"]
                    if service not in baselines and len(baselines) >= self.config.max_services:
                        oldest = min(baselines, key=lambda key: (baselines[key]["last_end"], key))
                        del baselines[oldest]
                        log.warning("baseline_capacity_eviction service=%s", oldest)
                    baseline = baselines.setdefault(service, fresh_state(self.config))
                    result = evaluate_adaptive(
                        summarize_accumulator(acc), service, end - 30000, end, self.config, baseline
                    )
                    state["pending"].extend(result["alerts"])
                state["next_end"] = end + 10000
                state_file.save(state)
                drain()
            self.next_end = state["next_end"]
            return emitted
