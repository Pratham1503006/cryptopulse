from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class EventType(StrEnum):
    trade = "trade"
    ticker = "ticker"
    orderbook = "orderbook"
    unknown = "unknown"


class InternalEvent(BaseModel):
    event_id: str = Field(description="Unique identifier for the event")
    source: str = Field(description="Exchange or provider that originated the event")
    event_type: EventType = Field(description="Type of market event")
    received_at: datetime = Field(
        description="Timestamp when the platform received the event"
    )
    payload: dict[str, Any] = Field(
        description="Exchange-agnostic event payload"
    )

    model_config = {"frozen": True}


class TrustedEvent(BaseModel):
    event_id: str = Field(description="Unique identifier for the event")
    source: str = Field(description="Exchange or provider that originated the event")
    event_type: EventType = Field(description="Type of market event")
    received_at: datetime = Field(
        description="Timestamp when the platform received the event"
    )
    validated_at: datetime = Field(
        description="Timestamp when the event passed validation"
    )
    payload: dict[str, Any] = Field(
        description="Exchange-agnostic event payload"
    )

    model_config = {"frozen": True}


class BusinessInformation(BaseModel):
    metric_id: str = Field(description="Unique identifier for the metric")
    metric_name: str = Field(description="Name of the business metric")
    source: str = Field(description="Exchange or provider that originated the data")
    computed_at: datetime = Field(
        description="Timestamp when the metric was computed"
    )
    window_start: datetime = Field(description="Start of the aggregation window")
    window_end: datetime = Field(description="End of the aggregation window")
    value: float = Field(description="Computed metric value")
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context about the computation",
    )

    model_config = {"frozen": True}
