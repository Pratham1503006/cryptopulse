"""Kafka integration test.

Proves the full InternalEvent round-trip across the event-streaming
backbone: create -> encode -> publish -> consume -> decode -> verify.

Requires a running Kafka broker (see docker-compose.yml). Runs only when
explicitly selected:

    pytest -m integration
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from common.contracts import EventType, InternalEvent, TradeSide
from messaging import KafkaConsumer, KafkaPublisher, KafkaSettings

pytestmark = pytest.mark.integration


def build_event(event_id: str) -> InternalEvent:
    return InternalEvent(
        event_id=event_id,
        source="coinbase",
        event_type=EventType.trade,
        received_at=datetime(2026, 8, 19, 10, 0, 0, tzinfo=UTC),
        symbol="BTC-USD",
        price=Decimal("64001.12345678901234567890"),
        quantity=Decimal("0.00012345"),
        side=TradeSide.sell,
        trade_id="trade-abc-123",
        trade_timestamp=datetime(2026, 8, 19, 9, 59, 59, tzinfo=UTC) - timedelta(seconds=1),
        payload={"venue": "coinbase-pro", "sequence": 12345},
    )


@pytest.mark.asyncio
async def test_internal_event_round_trip_via_kafka() -> None:
    settings = KafkaSettings()
    event = build_event(event_id=f"it-{uuid4().hex}")

    publisher = KafkaPublisher(settings)
    await publisher.start()
    try:
        await publisher.publish(event)
    finally:
        await publisher.close()

    consumer = KafkaConsumer(settings, group_id=f"it-{uuid4().hex}")
    await consumer.start()
    stream = consumer.stream()
    try:
        async with asyncio.timeout(30):
            async for received in stream:
                if received.event_id != event.event_id:
                    continue
                assert received == event
                assert received.price == Decimal("64001.12345678901234567890")
                assert received.quantity == Decimal("0.00012345")
                assert received.side is TradeSide.sell
                assert received.event_type is EventType.trade
                assert received.received_at == event.received_at
                assert received.trade_timestamp == event.trade_timestamp
                assert received.payload == event.payload
                break
            else:
                pytest.fail("published event was never consumed within timeout")
    finally:
        await stream.aclose()
        await consumer.close()
