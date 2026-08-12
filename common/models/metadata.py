from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class EventSource(StrEnum):
    coinbase = "coinbase"
    unknown = "unknown"


class EventStage(StrEnum):
    ingested = "ingested"
    standardized = "standardized"
    validated = "validated"
    processed = "processed"
    quarantined = "quarantined"
    rejected = "rejected"


class ValidationResult(StrEnum):
    passed = "passed"
    failed = "failed"


class EventMetadata(BaseModel):
    event_id: str = Field(description="Unique identifier for the event")
    source: EventSource = Field(description="Origin of the event")
    stage: EventStage = Field(description="Current processing stage")
    received_at: datetime = Field(description="Timestamp when the platform received the event")
    validated_at: datetime | None = Field(
        default=None,
        description="Timestamp when validation was performed",
    )
    processed_at: datetime | None = Field(
        default=None,
        description="Timestamp when processing completed",
    )
    validation_result: ValidationResult | None = Field(
        default=None,
        description="Outcome of validation",
    )
    error_message: str | None = Field(
        default=None,
        description="Error detail if the event was rejected",
    )
