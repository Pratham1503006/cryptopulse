from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class EventType(StrEnum):
    trade = "trade"
    ticker = "ticker"
    orderbook = "orderbook"
    unknown = "unknown"


class TradeSide(StrEnum):
    buy = "buy"
    sell = "sell"


class InternalEvent(BaseModel):
    """Represents a successfully received and standardised event.

    This is the canonical representation used throughout CryptoPulse.
    Corresponds to the Bronze lifecycle state.
    """

    event_id: str = Field(description="Unique identifier for the event")
    source: str = Field(description="Exchange or provider that originated the event")
    event_type: EventType = Field(description="Type of market event")
    received_at: datetime = Field(description="Timestamp when the platform received the event")
    symbol: str = Field(description="Standardised trading pair symbol (e.g., BTC-USD)")
    price: Decimal = Field(description="Trade price as decimal for precision")
    quantity: Decimal = Field(description="Trade quantity as decimal for precision")
    side: TradeSide = Field(description="Trade side (buy or sell)")
    trade_id: str = Field(description="Exchange-specific trade identifier")
    trade_timestamp: datetime = Field(
        description="Timestamp when the trade occurred on the exchange"
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional exchange-specific data preserved for debugging",
    )

    model_config = {"frozen": True}


class TrustedEvent(BaseModel):
    """Represents an InternalEvent that has passed validation.

    This is the Silver lifecycle state.
    Every event stored here has passed the platform's quality checks.
    """

    event_id: str = Field(description="Unique identifier for the event")
    source: str = Field(description="Exchange or provider that originated the event")
    event_type: EventType = Field(description="Type of market event")
    received_at: datetime = Field(description="Timestamp when the platform received the event")
    validated_at: datetime = Field(description="Timestamp when the event passed validation")
    symbol: str = Field(description="Standardised trading pair symbol (e.g., BTC-USD)")
    price: Decimal = Field(description="Trade price as decimal for precision")
    quantity: Decimal = Field(description="Trade quantity as decimal for precision")
    side: TradeSide = Field(description="Trade side (buy or sell)")
    trade_id: str = Field(description="Exchange-specific trade identifier")
    trade_timestamp: datetime = Field(
        description="Timestamp when the trade occurred on the exchange"
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional exchange-specific data preserved for debugging",
    )

    model_config = {"frozen": True}


class BusinessInformation(BaseModel):
    """Represents analytical information derived from TrustedEvent(s).

    This is the Gold lifecycle state.
    Represents business-ready information rather than individual events.
    """

    metric_id: str = Field(description="Unique identifier for the metric")
    metric_name: str = Field(description="Name of the business metric")
    source: str = Field(description="Exchange or provider that originated the data")
    computed_at: datetime = Field(description="Timestamp when the metric was computed")
    window_start: datetime = Field(description="Start of the aggregation window")
    window_end: datetime = Field(description="End of the aggregation window")
    value: float = Field(description="Computed metric value")
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context about the computation",
    )

    model_config = {"frozen": True}
