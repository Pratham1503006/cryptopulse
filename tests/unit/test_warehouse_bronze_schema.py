"""Unit tests for Bronze schema mapping.

Verifies the row-to-contract mapping in isolation (no PostgreSQL needed):
exact Decimal preservation, timezone-aware timestamps, enum reconstruction,
payload handling, and rejection of malformed rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from common.contracts import EventType, InternalEvent, TradeSide
from warehouse.schemas import (
    internal_event_to_record,
    record_to_internal_event,
)

PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")


def build_event() -> InternalEvent:
    return InternalEvent(
        event_id="ut-1",
        source="coinbase",
        event_type=EventType.trade,
        received_at=datetime(2026, 9, 24, 12, 30, 45, 123456, tzinfo=UTC),
        symbol="BTC-USD",
        price=PRECISE_PRICE,
        quantity=PRECISE_QUANTITY,
        side=TradeSide.buy,
        trade_id="trade-xyz-789",
        trade_timestamp=datetime(2026, 9, 24, 12, 30, 0, tzinfo=UTC),
        payload={"venue": "coinbase-pro", "sequence": 99},
    )


def test_record_contains_all_bronze_columns() -> None:
    record = internal_event_to_record(build_event())
    assert set(record) == {
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
    }


def test_enums_are_stored_as_canonical_text_values() -> None:
    record = internal_event_to_record(build_event())
    assert record["event_type"] == "trade"
    assert record["side"] == "buy"
    assert isinstance(record["event_type"], str)
    assert isinstance(record["side"], str)


def test_round_trip_preserves_exact_values() -> None:
    event = build_event()
    reconstructed = record_to_internal_event(internal_event_to_record(event))
    assert reconstructed == event
    assert reconstructed.price == PRECISE_PRICE
    assert reconstructed.quantity == PRECISE_QUANTITY
    assert reconstructed.price.as_tuple() == PRECISE_PRICE.as_tuple()


def test_round_trip_preserves_timezone_awareness() -> None:
    event = build_event()
    reconstructed = record_to_internal_event(internal_event_to_record(event))
    assert reconstructed.received_at.tzinfo is not None
    assert reconstructed.trade_timestamp.tzinfo is not None
    assert reconstructed.received_at.utcoffset() == event.received_at.utcoffset()


def test_round_trip_preserves_empty_payload_default() -> None:
    event = build_event()
    empty = event.model_copy(update={"payload": {}})
    reconstructed = record_to_internal_event(internal_event_to_record(empty))
    assert reconstructed.payload == {}


def test_malformed_row_raises_storage_error() -> None:
    from common.exceptions import StorageError

    with pytest.raises(StorageError):
        record_to_internal_event({"event_id": None})


def test_non_numeric_price_raises_storage_error() -> None:
    from common.exceptions import StorageError

    record = internal_event_to_record(build_event())
    record["price"] = 123.45  # float, not Decimal
    with pytest.raises(StorageError):
        record_to_internal_event(record)


def test_unknown_enum_value_raises_storage_error() -> None:
    from common.exceptions import StorageError

    record = internal_event_to_record(build_event())
    record["side"] = "unknown-side"
    with pytest.raises(StorageError):
        record_to_internal_event(record)
