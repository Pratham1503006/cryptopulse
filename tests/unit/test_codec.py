"""Verify the InternalEvent wire-format codec.

Covers deterministic serialization, Decimal precision, enum values,
timestamps, payload preservation, and round-trip fidelity.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from common.contracts import EventType, InternalEvent, TradeSide
from messaging import InternalEventCodec, SerializationError


def build_event(**overrides: Any) -> InternalEvent:
    defaults: dict[str, Any] = {
        "event_id": "evt-001",
        "source": "coinbase",
        "event_type": EventType.trade,
        "received_at": datetime(2026, 8, 19, 10, 0, 0, tzinfo=UTC),
        "symbol": "BTC-USD",
        "price": Decimal("64001.12345678901234567890"),
        "quantity": Decimal("0.00012345"),
        "side": TradeSide.buy,
        "trade_id": "trade-001",
        "trade_timestamp": datetime(2026, 8, 19, 9, 59, 59, tzinfo=UTC),
        "payload": {"venue": "coinbase-pro", "sequence": 42},
    }
    defaults.update(overrides)
    return InternalEvent(**defaults)


def test_encode_decode_round_trip() -> None:
    codec = InternalEventCodec()
    event = build_event()

    decoded = codec.decode(codec.encode(event))

    assert decoded == event


def test_serialization_is_deterministic() -> None:
    codec = InternalEventCodec()
    event = build_event()

    first = codec.encode(event)
    second = codec.encode(event)

    assert first == second


def test_decimal_precision_is_preserved() -> None:
    codec = InternalEventCodec()
    event = build_event(price=Decimal("64001.12345678901234567890"))

    decoded = codec.decode(codec.encode(event))

    assert decoded.price == Decimal("64001.12345678901234567890")
    assert isinstance(decoded.price, Decimal)


def test_enum_values_are_preserved() -> None:
    codec = InternalEventCodec()
    event = build_event(event_type=EventType.orderbook, side=TradeSide.sell)

    decoded = codec.decode(codec.encode(event))

    assert decoded.event_type is EventType.orderbook
    assert decoded.side is TradeSide.sell


def test_timestamps_are_preserved() -> None:
    codec = InternalEventCodec()
    received = datetime(2026, 8, 19, 10, 0, 0, tzinfo=UTC)
    traded = datetime(2026, 8, 19, 9, 59, 59, tzinfo=timezone(timedelta(hours=5)))
    event = build_event(received_at=received, trade_timestamp=traded)

    decoded = codec.decode(codec.encode(event))

    assert decoded.received_at == received
    assert decoded.trade_timestamp == traded


def test_payload_dict_is_preserved() -> None:
    codec = InternalEventCodec()
    payload = {"venue": "coinbase-pro", "nested": {"key": [1, 2, 3]}}
    event = build_event(payload=payload)

    decoded = codec.decode(codec.encode(event))

    assert decoded.payload == payload


def test_encode_rejects_non_json_payload_values() -> None:
    codec = InternalEventCodec()
    event = build_event(payload={"bad": object()})

    with pytest.raises(SerializationError):
        codec.encode(event)


def test_decode_rejects_invalid_payload() -> None:
    codec = InternalEventCodec()

    with pytest.raises(SerializationError):
        codec.decode(b'{"not": "an event"}')


def test_decode_rejects_non_json_bytes() -> None:
    codec = InternalEventCodec()

    with pytest.raises(SerializationError):
        codec.decode(b"this is not json")
