"""Unit tests for the development event preparer."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from common.contracts import EventType, InternalEvent, TradeSide
from producer.preparation.base import PreparationError
from producer.preparation.dev import DevEventPreparer


def build_raw_event(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "type": "trade",
        "symbol": "BTC-USD",
        "price": "64001.12345678901234567890",
        "quantity": "0.000123456789012345678901234567890",
        "side": "buy",
        "trade_id": "trade-001",
        "trade_time_ms": 1787739000000,  # 2026-08-26T10:10:00Z
        "venue": "dev-exchange",
    }
    raw.update(overrides)
    return raw


def test_prepare_translates_raw_event_into_internal_event() -> None:
    preparer = DevEventPreparer()
    event = preparer.prepare(build_raw_event())

    assert isinstance(event, InternalEvent)
    assert event.source == "dev"
    assert event.event_type is EventType.trade
    assert event.symbol == "BTC-USD"
    assert event.price == Decimal("64001.12345678901234567890")
    assert event.quantity == Decimal("0.000123456789012345678901234567890")
    assert event.side is TradeSide.buy
    assert event.trade_id == "trade-001"


def test_prepare_converts_epoch_ms_to_timezone_aware_utc() -> None:
    preparer = DevEventPreparer()
    event = preparer.prepare(build_raw_event())

    assert event.trade_timestamp == datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)
    assert event.trade_timestamp.tzinfo is not None


def test_decimal_precision_is_exact_not_float() -> None:
    preparer = DevEventPreparer()
    raw = build_raw_event(price="0.1", quantity="0.2")
    event = preparer.prepare(raw)

    assert event.price == Decimal("0.1")
    assert event.price.as_tuple() == Decimal("0.1").as_tuple()
    assert not isinstance(event.price, float)


def test_event_id_is_deterministic_for_identical_raw_input() -> None:
    preparer = DevEventPreparer()
    raw = build_raw_event()

    first = preparer.prepare(raw)
    second = preparer.prepare(dict(raw))

    assert first.event_id == second.event_id
    assert first.event_id.startswith("dev:trade-001:")


def test_same_trade_id_different_time_yields_distinct_event_ids() -> None:
    """Identity is (source, trade_id, trade_time_ms): distinct received events
    sharing a business trade_id map to distinct canonical identities, so
    Bronze preserves both. Business duplicate detection belongs to the
    Validation Engine, not to preparation.
    """
    preparer = DevEventPreparer()
    first = preparer.prepare(build_raw_event(trade_id="trade-001"))
    second = preparer.prepare(build_raw_event(trade_id="trade-001", trade_time_ms=1787739002000))

    assert first.trade_id == second.trade_id
    assert first.event_id != second.event_id


def test_identical_raw_input_maps_to_identical_event_id() -> None:
    """A redelivered raw event (byte-identical) maps to the same identity,
    which makes the Bronze append idempotent for Kafka redelivery."""
    preparer = DevEventPreparer()
    raw = build_raw_event(trade_id="trade-001")

    first = preparer.prepare(raw)
    second = preparer.prepare(build_raw_event(trade_id="trade-001"))

    assert first.event_id == second.event_id


def test_payload_preserves_raw_event_verbatim() -> None:
    preparer = DevEventPreparer()
    raw = build_raw_event()
    event = preparer.prepare(raw)

    assert event.payload == raw
    # Mutating the original afterwards must not corrupt the stored payload.
    raw["price"] = "mutated"
    assert event.payload["price"] == "64001.12345678901234567890"


def test_missing_required_field_raises_preparation_error() -> None:
    preparer = DevEventPreparer()
    raw = build_raw_event()
    del raw["price"]

    with pytest.raises(PreparationError) as excinfo:
        preparer.prepare(raw)
    assert excinfo.value.raw_event is raw


def test_non_numeric_price_raises_preparation_error() -> None:
    preparer = DevEventPreparer()

    with pytest.raises(PreparationError, match="Decimal"):
        preparer.prepare(build_raw_event(price="not-a-number"))


def test_unknown_side_raises_preparation_error() -> None:
    preparer = DevEventPreparer()

    with pytest.raises(PreparationError, match="side"):
        preparer.prepare(build_raw_event(side="hold"))


def test_unknown_type_raises_preparation_error() -> None:
    preparer = DevEventPreparer()

    with pytest.raises(PreparationError, match="type"):
        preparer.prepare(build_raw_event(type="liquidation"))


def test_non_integer_timestamp_raises_preparation_error() -> None:
    preparer = DevEventPreparer()

    with pytest.raises(PreparationError, match="trade_time_ms"):
        preparer.prepare(build_raw_event(trade_time_ms="1787739000000"))


def test_can_prepare_defaults_to_true() -> None:
    preparer = DevEventPreparer()

    assert preparer.can_prepare(build_raw_event()) is True
