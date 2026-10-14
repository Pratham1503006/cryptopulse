"""Unit tests for the Coinbase event preparer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from common.contracts import EventType, InternalEvent, TradeSide
from producer.preparation.base import PreparationError
from producer.preparation.coinbase import CoinbaseEventPreparer

FIXED_NOW = datetime(2026, 8, 26, 10, 11, 0, tzinfo=UTC)


def build_match(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "type": "match",
        "trade_id": 12345,
        "product_id": "BTC-USD",
        "price": "64001.12345678901234567890",
        "size": "0.000123456789012345678901234567890",
        "side": "buy",
        "time": "2026-08-26T10:10:00.000000Z",
    }
    raw.update(overrides)
    return raw


def build_preparer() -> CoinbaseEventPreparer:
    return CoinbaseEventPreparer(clock=lambda: FIXED_NOW)


def test_prepare_translates_match_into_internal_event() -> None:
    event = build_preparer().prepare(build_match())

    assert isinstance(event, InternalEvent)
    assert event.source == "coinbase"
    assert event.event_type is EventType.trade
    assert event.symbol == "BTC-USD"
    assert event.price == Decimal("64001.12345678901234567890")
    assert event.quantity == Decimal("0.000123456789012345678901234567890")
    assert event.side is TradeSide.buy
    assert event.trade_id == "12345"
    assert event.received_at == FIXED_NOW


def test_prepare_maps_size_to_quantity_and_product_id_to_symbol() -> None:
    event = build_preparer().prepare(build_match(product_id="ETH-USD", size="2.5"))

    assert event.symbol == "ETH-USD"
    assert event.quantity == Decimal("2.5")
    # Coinbase's raw field names are preserved in the payload, not the contract.
    assert event.payload["size"] == "2.5"
    assert event.payload["product_id"] == "ETH-USD"


def test_integer_trade_id_is_stored_as_canonical_string() -> None:
    event = build_preparer().prepare(build_match(trade_id=987654321))

    assert event.trade_id == "987654321"
    assert isinstance(event.trade_id, str)


def test_time_is_parsed_to_timezone_aware_utc() -> None:
    event = build_preparer().prepare(build_match(time="2026-08-26T10:10:00.000000Z"))

    assert event.trade_timestamp == datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)
    assert event.trade_timestamp.tzinfo is not None
    assert event.trade_timestamp.utcoffset() == timedelta(0)


def test_non_utc_offset_is_converted_to_utc() -> None:
    event = build_preparer().prepare(build_match(time="2026-08-26T12:10:00+02:00"))

    assert event.trade_timestamp == datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(PreparationError, match="timezone-aware"):
        build_preparer().prepare(build_match(time="2026-08-26T10:10:00"))


def test_decimal_precision_is_exact_not_float() -> None:
    event = build_preparer().prepare(build_match(price="0.1", size="0.2"))

    assert event.price == Decimal("0.1")
    assert event.price.as_tuple() == Decimal("0.1").as_tuple()
    assert not isinstance(event.price, float)


def test_event_id_is_deterministic_per_trade() -> None:
    preparer = build_preparer()

    first = preparer.prepare(build_match(trade_id=555))
    second = preparer.prepare(build_match(trade_id=555))

    assert first.event_id == second.event_id == "coinbase:555"


def test_payload_preserves_raw_event_verbatim() -> None:
    raw = build_match()
    event = build_preparer().prepare(raw)

    assert event.payload == raw
    raw["price"] = "mutated"
    assert event.payload["price"] == "64001.12345678901234567890"


def test_missing_required_field_raises_preparation_error() -> None:
    raw = build_match()
    del raw["price"]

    with pytest.raises(PreparationError) as excinfo:
        build_preparer().prepare(raw)
    assert excinfo.value.raw_event is raw


def test_non_numeric_price_raises_preparation_error() -> None:
    with pytest.raises(PreparationError, match="Decimal"):
        build_preparer().prepare(build_match(price="not-a-number"))


def test_unknown_side_raises_preparation_error() -> None:
    with pytest.raises(PreparationError, match="side"):
        build_preparer().prepare(build_match(side="hold"))


def test_non_match_type_raises_preparation_error() -> None:
    with pytest.raises(PreparationError, match="type"):
        build_preparer().prepare(build_match(type="heartbeat"))


def test_non_string_time_raises_preparation_error() -> None:
    with pytest.raises(PreparationError, match="ISO-8601"):
        build_preparer().prepare(build_match(time=1787739000000))


def test_last_match_is_accepted() -> None:
    event = build_preparer().prepare(build_match(type="last_match"))

    assert event.event_type is EventType.trade


def test_can_prepare_recognises_match_messages_only() -> None:
    preparer = build_preparer()

    assert preparer.can_prepare(build_match()) is True
    assert preparer.can_prepare(build_match(type="last_match")) is True
    assert preparer.can_prepare({"type": "heartbeat"}) is False
    assert preparer.can_prepare({"type": "subscriptions"}) is False
    assert preparer.can_prepare({"type": "error", "message": "boom"}) is False
    assert preparer.can_prepare({"type": "match"}) is False  # no product_id
