"""Deterministic labelled inputs. Labels stay outside production event payloads."""
import json

SCENARIOS = {
    "http_5xx": "payment returns 503; clear restores 200",
    "latency": "payment response increases by at least 180ms; clear restores responsiveness",
    "timeout": "actual HTTP client read timeout; clear cancels outstanding delay",
    "unavailable": "owned payment process stopped; connection refused; restart restores requests",
    "network_delay": "real TCP relay delays order→payment; relay clear restores traffic",
    "cpu": "opt-in bounded child consumes measurable CPU and exits",
    "memory": "opt-in bounded child touches 16MiB and exits",
    "cascade": "payment 503 propagates order/gateway 502 with same trace and alert evidence",
    "messages": "duplicates deduplicate, conflicts quarantine, out-of-order persists, invalid payloads reject",
    "backlog": "real Kafka consumer pause builds lag; resume drains all owned-topic records",
}


def message_cases(epoch_ms, prefix="phase5"):
    base = {"schema_version": 1, "event_type": "request", "service": "phase5-input",
            "instance_id": "phase5-1", "trace_id": "", "latency_ms": 20, "status_code": 200}
    normal = [{**base, "event_id": f"{prefix}-{i}", "event_time": epoch_ms-1000+i*100} for i in range(5)]
    ordered = [normal[i] for i in (0, 4, 2, 1, 3)]
    return {"valid": ordered, "duplicates": [normal[0], normal[2]],
            "conflict": {**normal[0], "latency_ms": 1000},
            "invalid": [b'{broken', b'null', b'[]', b'\xff',
                        json.dumps({**normal[0], "latency_ms": -1}).encode(),
                        json.dumps({**normal[0], "event_time": epoch_ms+120000}).encode()],
            "expected_unique": 5}
