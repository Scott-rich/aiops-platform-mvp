"""Transparent metadata + token overlap retrieval; not vector RAG."""

import json
import logging
import re
from pathlib import Path
from typing import Any

from .contracts import canonical, digest
from .llm_config import redact

log = logging.getLogger(__name__)


def load_case(path: Any) -> Any:
    if path.stat().st_size > 262144:
        raise ValueError("case_file_too_large")
    case = json.loads(path.read_bytes())
    if (
        not isinstance(case, dict)
        or not isinstance(case.get("case_id"), str)
        or not 1 <= len(case["case_id"]) <= 256
    ):
        raise ValueError("case_identity_invalid")
    version = case.get("version")
    if not (
        (type(version) is int and version > 0)
        or (isinstance(version, str) and 1 <= len(version) <= 128)
    ):
        raise ValueError("case_version_invalid")
    for key in ("services", "rule_ids", "keywords"):
        if not isinstance(case.get(key, []), list) or not all(
            isinstance(v, str) for v in case.get(key, [])
        ):
            raise ValueError("case_metadata_invalid")
    canonical(case).encode("utf-8")
    return case


def retrieve(alert: Any, snapshot: Any, case_dir: Any = None, top_k: Any = 3) -> Any:
    root = Path(case_dir) if case_dir else Path(__file__).resolve().parent.parent / "cases"
    text = " ".join(str(r["event"].get("message", "")) for r in snapshot["records"])
    tokens = set(re.findall(r"[a-z_]{3,}", text.lower()))
    services = {r["event"]["service"] for r in snapshot["records"]} | {alert["service"]}
    ranked = []
    for path in sorted(root.glob("*.json")):
        try:
            case = load_case(path)
        except (ValueError, TypeError, OSError, RecursionError) as error:
            log.warning(
                "case_skipped file=%s error_type=%s", redact(path.name), type(error).__name__
            )
            continue
        score = 3 * bool(set(case.get("services", [])) & services)
        score += 5 * (alert["rule_id"] in case.get("rule_ids", []))
        score += len(set(case.get("keywords", [])) & tokens)
        if score:
            ranked.append(
                {
                    "case": case,
                    "case_sha256": digest(case),
                    "retrieval_score": score,
                    "retrieval_method": "metadata-token-overlap-v1",
                }
            )
    return sorted(ranked, key=lambda c: (-c["retrieval_score"], c["case"]["case_id"]))[:top_k]
