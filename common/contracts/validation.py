"""Structured validation outcome contracts.

Describes the result of evaluating the Validation Engine's rules against a
canonical InternalEvent. This is a platform-wide contract (in common/) so
that:

- the Validation Engine (processing/) can produce it,
- the Warehouse (warehouse/) can persist it as structured JSONB in
  Quarantine,
- no application module needs to parse unstructured error strings.

Distinct from ``common.models.metadata.ValidationResult``, which is a
StrEnum (passed/failed) attached to the per-event lifecycle metadata: this
contract is the structured, multi-failure rule outcome used by the trust
boundary. The enum and the models serve different purposes and coexist by
design.

Deliberately free of database-specific fields (no column names) and
transport-specific fields (no Kafka offsets): validation explains data
quality, not infrastructure context.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

__all__ = ["ValidationFailure", "ValidationOutcome"]


class ValidationFailure(BaseModel):
    """One concrete reason an event failed a validation rule.

    Attributes:
        rule: Identifier of the rule that produced the failure
            (e.g. ``price_validity``).
        code: Stable machine-readable reason code within that rule
            (e.g. ``non_positive``). Codes are part of the contract:
            downstream consumers and stored Quarantine records rely on
            them being stable identifiers, not prose.
        message: Human-readable explanation of the failure.
    """

    rule: str = Field(description="Identifier of the validation rule that failed")
    code: str = Field(description="Stable machine-readable failure code within the rule")
    message: str = Field(description="Human-readable explanation of the failure")

    model_config = {"frozen": True}


class ValidationOutcome(BaseModel):
    """Structured result of validating one InternalEvent.

    Attributes:
        valid: True when every evaluated rule passed.
        failures: All failures across all evaluated rules. Empty exactly
            when ``valid`` is True. Multiple failures are preserved so
            Quarantine can explain every problem with an event, not just
            the first.
    """

    valid: bool = Field(description="True when every evaluated rule passed")
    failures: list[ValidationFailure] = Field(
        default_factory=list,
        description="All rule failures; empty exactly when valid is True",
    )

    model_config = {"frozen": True}
