"""Quarantine layer database schema and row mapping.

Owns the mapping between the quarantine record contract and the PostgreSQL
representation of the Quarantine layer. This is the only place that knows
how a rejected InternalEvent is physically represented in PostgreSQL, and
it contains no business logic.

A quarantine record preserves a rejected InternalEvent together with the
structured validation failures that caused the rejection and the quarantine
timestamp, giving investigators everything needed to understand the
rejection without touching Bronze.

Storage decisions (see warehouse/migrations/sql/003_create_quarantine_events.sql):

- Business fields mirror Bronze: exact NUMERIC for price/quantity,
  TIMESTAMPTZ for timestamps, TEXT + CHECK for the canonical enums, JSONB
  for the original payload.
- ``failures`` is JSONB: the full structured ValidationFailure list, so the
  rejection reason survives as data, not prose.
- ``quarantined_at`` records when the platform rejected the event.
- Technical identity is (event_id, quarantined_at): each processing
  attempt that rejects an event appends a distinct quarantine record, so
  repeated processing attempts remain visible for investigation. Business
  duplicate detection is NOT performed here.

This module contains no SQL execution; repositories own execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from common.contracts import EventType, InternalEvent, TradeSide, ValidationFailure
from common.exceptions import StorageError

__all__ = [
    "QUARANTINE_COLUMNS",
    "QUARANTINE_SCHEMA",
    "QUARANTINE_TABLE",
    "QuarantineRecord",
    "QuarantineRowError",
    "record_to_quarantine_record",
    "quarantine_record_to_record",
]


QUARANTINE_SCHEMA = "quarantine"
QUARANTINE_TABLE = "events"

# Columns of the Quarantine events table, in canonical column order.
QUARANTINE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "source",
    "event_type",
    "received_at",
    "quarantined_at",
    "symbol",
    "price",
    "quantity",
    "side",
    "trade_id",
    "trade_timestamp",
    "payload",
    "failures",
)


class QuarantineRowError(StorageError):
    """Raised when a Quarantine row cannot be mapped to or from the contract."""


@dataclass(frozen=True)
class QuarantineRecord:
    """A rejected InternalEvent plus its structured rejection context.

    The persistence shape of the Quarantine layer: the canonical
    InternalEvent contract, the platform ValidationFailure contract, and
    quarantine bookkeeping composed into one immutable carrier.
    """

    event: InternalEvent
    quarantined_at: datetime
    failures: list[ValidationFailure]


def quarantine_record_to_record(record: QuarantineRecord) -> dict[str, Any]:
    """Map a QuarantineRecord to a Quarantine table row.

    The returned mapping's keys match the Quarantine table columns so it
    can be passed directly to asyncpg parameterised statements.
    """
    event = record.event
    return {
        "event_id": event.event_id,
        "source": event.source,
        "event_type": event.event_type.value,
        "received_at": event.received_at,
        "quarantined_at": record.quarantined_at,
        "symbol": event.symbol,
        "price": event.price,
        "quantity": event.quantity,
        "side": event.side.value,
        "trade_id": event.trade_id,
        "trade_timestamp": event.trade_timestamp,
        "payload": event.payload,
        "failures": [failure.model_dump() for failure in record.failures],
    }


def record_to_quarantine_record(row: Any) -> QuarantineRecord:
    """Map a Quarantine table row back to a QuarantineRecord.

    Reconstructs the exact contract values: NUMERIC columns arrive from
    PostgreSQL as Decimal, TIMESTAMPTZ columns arrive timezone-aware, enum
    columns are validated against the canonical StrEnum types, and the
    JSONB failures column is validated into ValidationFailure models.

    Raises:
        QuarantineRowError: If the row cannot be represented.
    """
    try:
        event = InternalEvent(
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
        quarantined_at = _as_datetime(row, "quarantined_at")
        raw_failures = row["failures"]
        if not isinstance(raw_failures, list):
            raise QuarantineRowError(
                "column 'failures' is not a JSON array",
                detail=repr(raw_failures),
            )
        failures = [ValidationFailure.model_validate(item) for item in raw_failures]
        return QuarantineRecord(
            event=event,
            quarantined_at=quarantined_at,
            failures=failures,
        )
    except QuarantineRowError:
        raise
    except (ValueError, TypeError, KeyError) as exc:
        raise QuarantineRowError(
            "quarantine row does not conform to the canonical contracts",
            detail=str(exc),
        ) from exc


def _as_str(row: Any, key: str) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise QuarantineRowError(f"column '{key}' is not text", detail=repr(value))
    return value


def _as_datetime(row: Any, key: str) -> datetime:
    value = row[key]
    if isinstance(value, datetime):
        return value
    raise QuarantineRowError(f"column '{key}' is not a timestamp", detail=repr(value))


def _as_decimal(row: Any, key: str) -> Decimal:
    value = row[key]
    if isinstance(value, Decimal):
        return value
    raise QuarantineRowError(f"column '{key}' is not exact numeric", detail=repr(value))


def _as_payload(row: Any, key: str) -> dict[str, Any]:
    value = row[key]
    if isinstance(value, dict):
        return value
    raise QuarantineRowError(f"column '{key}' is not a JSON object", detail=repr(value))
