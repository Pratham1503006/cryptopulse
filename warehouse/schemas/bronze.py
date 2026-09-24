"""Bronze layer database schema and row mapping.

Owns the mapping between the canonical ``InternalEvent`` contract and the
PostgreSQL representation of the Bronze layer. This is the only place that
knows how an InternalEvent is physically represented in PostgreSQL, and it
contains no business logic.

Storage decisions (see warehouse/migrations/sql/001_create_bronze_events.sql):

- ``price`` and ``quantity`` use ``NUMERIC`` so exact Decimal values survive
  the round-trip without float conversion.
- ``event_type`` and ``side`` are stored as ``TEXT`` holding the StrEnum's
  canonical string value, which is the same representation used on the wire.
- ``received_at`` and ``trade_timestamp`` use ``TIMESTAMPTZ`` so timezone
  information is preserved; asyncpg returns timezone-aware datetimes.
- ``payload`` uses ``JSONB`` for the event's additional exchange-specific data.
- The primary key is ``event_id`` for technical row identity only. It is NOT
  business duplicate detection: Bronze preserves every successfully received
  event, and duplicate detection belongs to the Validation Engine.

This module contains no SQL execution; repositories own execution.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from common.contracts import EventType, InternalEvent, TradeSide
from common.exceptions import StorageError

__all__ = [
    "BRONZE_COLUMNS",
    "BRONZE_SCHEMA",
    "BRONZE_TABLE",
    "BronzeRowError",
    "internal_event_to_record",
    "record_to_internal_event",
]


BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "events"

# Columns of the Bronze events table, in canonical column order.
# Kept in one place so INSERT and SELECT always agree on the layout.
BRONZE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "source",
    "event_type",
    "received_at",
    "symbol",
    "price",
    "quantity",
    "side",
    "trade_id",
    "trade_timestamp",
    "payload",
)


class BronzeRowError(StorageError):
    """Raised when a Bronze row cannot be mapped to or from the canonical contract."""


def internal_event_to_record(event: InternalEvent) -> dict[str, Any]:
    """Map a canonical InternalEvent to a Bronze row record.

    The returned mapping's keys match the Bronze table columns so it can be
    passed directly to asyncpg parameterised statements.

    Args:
        event: The InternalEvent to map.

    Returns:
        Column-name to value mapping for the Bronze table.
    """
    return {
        "event_id": event.event_id,
        "source": event.source,
        # StrEnum members are str subclasses: ``value`` is their canonical text.
        "event_type": event.event_type.value,
        "received_at": event.received_at,
        "symbol": event.symbol,
        "price": event.price,
        "quantity": event.quantity,
        "side": event.side.value,
        "trade_id": event.trade_id,
        "trade_timestamp": event.trade_timestamp,
        "payload": event.payload,
    }


def record_to_internal_event(row: Any) -> InternalEvent:
    """Map a Bronze table row back to the canonical InternalEvent.

    Reconstructs the exact contract values: NUMERIC columns arrive from
    PostgreSQL as Decimal, TIMESTAMPTZ columns arrive timezone-aware, and
    enum columns are validated against the canonical StrEnum types.

    Args:
        row: An asyncpg Record or mapping keyed by the Bronze column names.

    Returns:
        The reconstructed InternalEvent.

    Raises:
        BronzeRowError: If the row cannot be represented as an InternalEvent.
    """
    try:
        return InternalEvent(
            event_id=_as_str(row, "event_id"),
            source=_as_str(row, "source"),
            event_type=EventType(_as_str(row, "event_type")),
            received_at=_as_datetime(row, "received_at"),
            symbol=_as_str(row, "symbol"),
            price=_as_decimal(row, "price"),
            quantity=_as_decimal(row, "quantity"),
            side=TradeSide(_as_str(row, "side")),
            trade_id=_as_str(row, "trade_id"),
            trade_timestamp=_as_datetime(row, "trade_timestamp"),
            payload=_as_payload(row, "payload"),
        )
    except BronzeRowError:
        raise
    except (ValueError, TypeError, KeyError) as exc:
        raise BronzeRowError(
            "bronze row does not conform to the InternalEvent contract",
            detail=str(exc),
        ) from exc


def _as_str(row: Any, key: str) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise BronzeRowError(f"column '{key}' is not text", detail=repr(value))
    return value


def _as_datetime(row: Any, key: str) -> datetime:
    value = row[key]
    if isinstance(value, datetime):
        return value
    raise BronzeRowError(f"column '{key}' is not a timestamp", detail=repr(value))


def _as_decimal(row: Any, key: str) -> Decimal:
    value = row[key]
    if isinstance(value, Decimal):
        return value
    raise BronzeRowError(f"column '{key}' is not exact numeric", detail=repr(value))


def _as_payload(row: Any, key: str) -> dict[str, Any]:
    value = row[key]
    if isinstance(value, dict):
        return value
    raise BronzeRowError(f"column '{key}' is not a JSON object", detail=repr(value))
