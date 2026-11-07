"""Pure-Python parsing and deduplication policy, independent of PyFlink runtime."""
import base64
import hashlib
import json
import time
from wire_contract import canonical, validate_metric

ORIGIN_FIELD = "__kafka_origin"
MAX_DLQ_BYTES = 128 * 1024
PREFIX_BYTES = 4096


def parse_metric_envelope(value):
    envelope = json.loads(value)
    if not isinstance(envelope, dict) or envelope.get("_aiops_kafka_envelope") != 1:
        raise ValueError("missing Kafka source envelope")
    origin = envelope.get("origin")
    if not isinstance(origin, dict):
        raise ValueError("missing Kafka source origin")
    if envelope.get("encoding_error"):
        raise ValueError("invalid_utf8")
    event = json.loads(envelope["raw"])
    validate_metric(event)
    if ORIGIN_FIELD in event:
        raise ValueError("reserved source metadata field")
    return {**event, ORIGIN_FIELD: origin}


def event_fingerprint(event):
    return hashlib.sha256(canonical({k: v for k, v in event.items() if k != ORIGIN_FIELD}).encode()).hexdigest()


def deadletter(reason, *, value=None, event=None, previous=None):
    result = {"schema_version": 1, "event_type": "deadletter", "source": "flink-detector",
              "reason": reason, "observed_at": int(time.time() * 1000)}
    if event is not None:
        result["event"] = {k: v for k, v in event.items() if k != ORIGIN_FIELD}
        result["origin"] = event.get(ORIGIN_FIELD)
    if value is not None:
        try:
            envelope = json.loads(value)
        except (ValueError, TypeError):
            envelope = {}
        if isinstance(envelope, dict) and envelope.get("_aiops_kafka_envelope") == 1:
            result["raw"] = envelope.get("raw")
            result["origin"] = envelope.get("origin")
            for key in ("encoding_error", "raw_base64"):
                if key in envelope:
                    result[key] = envelope[key]
        else:
            result["raw"] = value
    if previous is not None:
        result["first_seen"] = previous
    return bounded_deadletter_json(result)


def _json_bytes(value):
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except UnicodeError:
        return json.dumps(value, ensure_ascii=True, allow_nan=False).encode("ascii")


def _prefix(value, maximum=PREFIX_BYTES):
    # Cut encoded bytes first, then discard only an incomplete final code point.
    return str(value).encode("utf-8", errors="replace")[:maximum].decode("utf-8", errors="ignore")


def _origin_summary(origin):
    if not isinstance(origin, dict):
        return None
    result = {}
    for key in ("topic", "partition", "offset", "timestamp"):
        value = origin.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and -(2**63) <= value < 2**63:
            result[key] = value
        elif value is not None:
            result[key] = _prefix(value, 1024 if key == "topic" else 128)
    return result


def bounded_deadletter_json(result):
    """Enforce the final encoded Kafka value limit, across every payload field.

    Oversized records keep explicit partial prefixes, byte lengths and hashes.
    The original Kafka coordinates identify where the full input can be fetched
    while it remains inside Kafka retention. This does not archive the full input.
    """
    original = _json_bytes(result)
    if len(original) <= MAX_DLQ_BYTES:
        return original.decode("utf-8")
    bounded = {"schema_version": 1, "event_type": "deadletter", "source": "flink-detector",
               "reason": _prefix(result.get("reason", "oversized_deadletter"), 1024),
               "observed_at": result.get("observed_at") if type(result.get("observed_at")) is int else int(time.time() * 1000),
               "origin": _origin_summary(result.get("origin")), "truncated": True,
               "truncation_reason": "deadletter_exceeds_byte_limit", "max_serialized_bytes": MAX_DLQ_BYTES,
               "original_serialized_bytes": len(original), "original_payload_sha256": hashlib.sha256(original).hexdigest(),
               "raw_recovery": "fetch original Kafka topic/partition/offset before source retention expires"}
    raw = result.get("raw")
    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8", errors="surrogatepass")
        bounded.update(raw_prefix=_prefix(raw), raw_byte_length=len(raw_bytes), raw_sha256=hashlib.sha256(raw_bytes).hexdigest())
    encoded = result.get("raw_base64")
    if isinstance(encoded, str):
        bounded.update(raw_base64_prefix=_prefix(encoded), raw_base64_byte_length=len(encoded.encode("ascii", errors="replace")))
        try:
            raw_bytes = base64.b64decode(encoded, validate=True)
            bounded.update(raw_byte_length=len(raw_bytes), raw_sha256=hashlib.sha256(raw_bytes).hexdigest())
        except (ValueError, TypeError):
            bounded["raw_base64_invalid"] = True
    if "encoding_error" in result:
        bounded["encoding_error"] = _prefix(result["encoding_error"], 256)
    event = result.get("event")
    if event is not None:
        event_bytes = _json_bytes(event)
        bounded.update(event_byte_length=len(event_bytes), event_sha256=hashlib.sha256(event_bytes).hexdigest(),
                       event_prefix=_prefix(event_bytes.decode("utf-8")))
        if isinstance(event, dict):
            bounded["event_summary"] = {key: _prefix(event[key], 256) for key in
                ("event_id", "service", "instance_id", "event_type", "event_time") if key in event}
    previous = result.get("first_seen")
    if isinstance(previous, dict):
        bounded["first_seen"] = {"fingerprint": _prefix(previous.get("fingerprint", ""), 128),
                                  "origin": _origin_summary(previous.get("origin"))}
    serialized = _json_bytes(bounded)
    if len(serialized) > MAX_DLQ_BYTES:
        # The final guard is independent of which original field grew. Preserve
        # only fixed-size metadata if any future summary additions exceed the cap.
        bounded = {key: bounded[key] for key in ("schema_version", "event_type", "source", "reason", "origin",
                    "truncated", "truncation_reason", "max_serialized_bytes", "original_serialized_bytes",
                    "original_payload_sha256", "raw_recovery")}
        serialized = _json_bytes(bounded)
    if len(serialized) > MAX_DLQ_BYTES:
        raise ValueError("internal deadletter metadata exceeds configured byte limit")
    return serialized.decode("utf-8")
