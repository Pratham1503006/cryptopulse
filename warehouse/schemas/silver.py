"""Silver layer database schema and row mapping.

Owns the mapping between the canonical ``TrustedEvent`` contract and the
PostgreSQL representation of the Silver layer. This is the only place that
knows how a TrustedEvent is physically represented in PostgreSQL, and it
contains no business logic.

Storage decisions (see warehouse/migrations/sql/002_create_silver_events.sql):

- ``price`` and ``quantity`` use ``NUMERIC`` so exact Decimal values survive
  the round-trip without float conversion (same decision as Bronze).
- ``event_type`` and ``side`` are stored as ``TEXT`` holding the StrEnum's
  canonical string value, CHECK-constrained to the contract's members
  (same representation as Bronze and the wire codec).
- ``received_at``, ``trade_timestamp``, and ``validated_at`` use
  ``TIMESTAMPTZ`` so timezone information is preserved; asyncpg returns
  timezone-aware datetimes.
- ``payload`` uses ``JSONB`` for the event's additional exchange-specific data.
- The primary key is ``event_id`` for technical row identity only: a
  repeated processing attempt for the same canonical event is idempotent
  (ON CONFLICT DO NOTHING). It is NOT business duplicate detection.

This module contains no SQL execution; repositories own execution.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from common.contracts import EventType, TradeSide, TrustedEvent
from common.exceptions import StorageError

__all__ = [
    "SILVER_COLUMNS",
    "SILVER_SCHEMA",
    "SILVER_TABLE",
    "SilverRowError",
    "record_to_trusted_event",
    "trusted_event_to_record",
]


SILVER_SCHEMA = "silver"
SILVER_TABLE = "events"

# Columns of the Silver events table, in canonical column order.
# Kept in one place so INSERT and SELECT always agree on the layout.
SILVER_COLUMNS: tuple[str, ...] = (
    "event_id",
    "source",
    "event_type",
    "received_at",
    "validated_at",
    "symbol",
    "price",
    "quantity",
    "side",
    "trade_id",
    "trade_timestamp",
    "payload",
)


class SilverRowError(StorageError):
    """Raised when a Silver row cannot be mapped to or from the canonical contract."""


def trusted_event_to_record(event: TrustedEvent) -> dict[str, Any]:
    """Map a canonical TrustedEvent to a Silver row record.

    The returned mapping's keys match the Silver table columns so it can be
    passed directly to asyncpg parameterised statements.
    """
    return {
        "event_id": event.event_id,
        "source": event.source,
        # StrEnum members are str subclasses: ``value`` is their canonical text.
        "event_type": event.event_type.value,
        "received_at": event.received_at,
        "validated_at": event.validated_at,
        "symbol": event.symbol,
        "price": event.price,
        "quantity": event.quantity,
        "side": event.side.value,
        "trade_id": event.trade_id,
        "trade_timestamp": event.trade_timestamp,
        "payload": event.payload,
    }


def record_to_trusted_event(row: Any) -> TrustedEvent:
    """Map a Silver table row back to the canonical TrustedEvent.

    Reconstructs the exact contract values: NUMERIC columns arrive from
    PostgreSQL as Decimal, TIMESTAMPTZ columns arrive timezone-aware, and
    enum columns are validated against the canonical StrEnum types.

    Raises:
        SilverRowError: If the row cannot be represented as a TrustedEvent.
    """
    try:
        return TrustedEvent(
            event_id=_as_str(row, "event_id"),
            source=_as_str(row, "source"),
            event_type=EventType(_as_str(row, "event_type")),
            received_at=_as_datetime(row, "received_at"),
            validated_at=_as_datetime(row, "validated_at"),
            symbol=_as_str(row, "symbol"),
            price=_as_decimal(row, "price"),
            quantity=_as_decimal(row, "quantity"),
            side=TradeSide(_as_str(row, "side")),
            trade_id=_as_str(row, "trade_id"),
            trade_timestamp=_as_datetime(row, "trade_timestamp"),
            payload=_as_payload(row, "payload"),
        )
    except SilverRowError:
        raise
    except (ValueError, TypeError, KeyError) as exc:
        raise SilverRowError(
            "silver row does not conform to the TrustedEvent contract",
            detail=str(exc),
        ) from exc


def _as_str(row: Any, key: str) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise SilverRowError(f"column '{key}' is not text", detail=repr(value))
    return value


def _as_datetime(row: Any, key: str) -> datetime:
    value = row[key]
    if isinstance(value, datetime):
        return value
    raise SilverRowError(f"column '{key}' is not a timestamp", detail=repr(value))


def _as_decimal(row: Any, key: str) -> Decimal:
    value = row[key]
    if isinstance(value, Decimal):
        return value
    raise SilverRowError(f"column '{key}' is not exact numeric", detail=repr(value))


def _as_payload(row: Any, key: str) -> dict[str, Any]:
    value = row[key]
    if isinstance(value, dict):
        return value
    raise SilverRowError(f"column '{key}' is not a JSON object", detail=repr(value))
