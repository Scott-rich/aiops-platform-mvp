"""Versioned diagnosis contract; old stored reports remain readable."""

from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
)


def require_text_content(value: Any) -> Any:
    # JSON punctuation masquerading as prose is not a useful diagnostic field.
    # This is only a structural sanity check, not a claim of semantic grounding.
    if not any(character.isalnum() for character in value):
        raise ValueError("diagnostic text has no substantive characters")
    return value


Text = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=3000),
    AfterValidator(require_text_content),
]
Identifier = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=256)]
Steps = Annotated[list[Text], Field(min_length=1, max_length=12)]
References = Annotated[list[Identifier], Field(max_length=48)]


class Hypothesis(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    cause: Text
    service: Identifier
    confidence: Literal["low", "medium", "high"]
    confidence_basis: Text
    supporting_evidence_ids: Annotated[list[Identifier], Field(min_length=1, max_length=48)]
    against_evidence_ids: References
    uncertainty: Steps
    verification_steps: Steps
    remediation_steps: Steps


class Analysis(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    analysis_schema_version: Literal[2]
    diagnosis_status: Literal["candidates", "unknown"]
    summary: Text
    involved_services: Annotated[list[Identifier], Field(max_length=48)]
    hypotheses: Annotated[list[Hypothesis], Field(max_length=5)]
    uncertainties: Steps
    verification_steps: Steps
    recommendations: Steps


def json_schema() -> Any:
    return Analysis.model_json_schema()


def validate_analysis(analysis: Any, records: Any, *, alert: Any = None) -> Any:
    try:
        result = Analysis.model_validate(analysis).model_dump()
    except ValidationError as exc:
        # Never copy provider input/error repr into persisted failure messages.
        raise ValueError("diagnosis_schema_invalid") from exc
    candidates = result["hypotheses"]
    if (result["diagnosis_status"] == "unknown") != (len(candidates) == 0):
        raise ValueError("diagnosis_status_hypotheses_mismatch")
    by_id = {e["event_id"]: e for e in records}
    services = {e["service"] for e in records}
    involved = result["involved_services"]
    if len(involved) != len(set(involved)) or not set(involved) <= services:
        raise ValueError("unobserved_involved_service")
    for h in candidates:
        if h["service"] not in services or h["service"] not in involved:
            raise ValueError("unobserved_candidate_service")
        support, against = h["supporting_evidence_ids"], h["against_evidence_ids"]
        if not set(support + against) <= by_id.keys():
            raise ValueError("unknown_evidence_reference")
        if (
            len(set(support)) != len(support)
            or len(set(against)) != len(against)
            or set(support) & set(against)
        ):
            raise ValueError("contradictory_or_duplicate_evidence_reference")
        if not any(by_id[ref]["service"] == h["service"] for ref in support):
            raise ValueError("candidate_has_no_own_service_evidence")
        from .model_grounding import eligible_support

        if not any(
            by_id[ref]["service"] == h["service"] and eligible_support(by_id[ref], records, alert)
            for ref in support
        ):
            raise ValueError("candidate_has_no_observed_anomaly")
    return result
