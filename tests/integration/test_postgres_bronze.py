"""PostgreSQL integration test for the Bronze repository.

Proves the exact milestone flow:

    InternalEvent -> BronzeRepository -> PostgreSQL
                  -> BronzeRepository -> InternalEvent

Requires a running PostgreSQL instance (see docker-compose.yml). Runs only
when explicitly selected, matching the existing integration convention:

    pytest -m integration
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from common.config import PostgresSettings
from common.contracts import EventType, InternalEvent, TradeSide
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import PostgresBronzeRepository

pytestmark = pytest.mark.integration

# Deliberately over-precise values: 20 decimal places exposes any float
# conversion (float64 has ~15-17 significant decimal digits).
PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")


def build_event(event_id: str) -> InternalEvent:
    return InternalEvent(
        event_id=event_id,
        source="coinbase",
        event_type=EventType.trade,
        received_at=datetime(2026, 9, 24, 12, 30, 45, 123456, tzinfo=UTC),
        symbol="BTC-USD",
        price=PRECISE_PRICE,
        quantity=PRECISE_QUANTITY,
        side=TradeSide.sell,
        trade_id="trade-abc-123",
        trade_timestamp=datetime(2026, 9, 24, 12, 29, 59, tzinfo=UTC) - timedelta(seconds=1),
        payload={"venue": "coinbase-pro", "sequence": 12345, "extra": {"nested": True}},
    )


@pytest.mark.asyncio
async def test_bronze_round_trip_preserves_exact_internal_event() -> None:
    settings = PostgresSettings()
    pool = DatabasePool(settings)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        event = build_event(event_id=f"it-{uuid4().hex}")

        # 1. Persist through the BronzeRepository abstraction.
        await repository.append(event)

        # 2. Read back through the repository and reconstruct.
        replayed = await repository.replay(offset=0, limit=100)
        fetched = await repository.get(event.event_id)

        # replay returns a batch; locate this test's event.
        matches = [e for e in replayed if e.event_id == event.event_id]
        assert matches, "appended event not found in replay"
        from_replay = matches[0]
        assert fetched is not None
        from_get = fetched

        # 3. Exact equality against the canonical contract.
        assert from_get == event
        assert from_replay == event

        # 4. Field-by-field verification.
        assert from_get.symbol == "BTC-USD"
        assert from_get.price == PRECISE_PRICE
        assert from_get.quantity == PRECISE_QUANTITY
        assert from_get.side is TradeSide.sell
        assert from_get.event_type is EventType.trade
        assert from_get.trade_id == "trade-abc-123"
        assert from_get.trade_timestamp == event.trade_timestamp
        assert from_get.received_at == event.received_at
        assert from_get.payload == event.payload

        # 5. Decimal precision is exact, not float-approximated.
        assert from_get.price.as_tuple() == PRECISE_PRICE.as_tuple()
        assert isinstance(from_get.price, Decimal)
        assert isinstance(from_get.quantity, Decimal)

        # 6. Timestamps remain timezone-aware with the original offset.
        assert from_get.received_at.tzinfo is not None
        assert from_get.trade_timestamp.tzinfo is not None
        assert from_get.received_at.utcoffset() == timedelta(0)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_bronze_preserves_distinct_events_with_same_trade_id() -> None:
    """Bronze preserves received events; duplicate detection is NOT its job."""
    settings = PostgresSettings()
    pool = DatabasePool(settings)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        suffix = uuid4().hex
        first = build_event(event_id=f"it-dup1-{suffix}")
        second = build_event(event_id=f"it-dup2-{suffix}")
        # Same trade_id, same payload: a duplicate from the business point
        # of view, but a distinct received event from Bronze's point of view.
        second = second.model_copy(update={"event_id": f"it-dup2-{suffix}"})

        await repository.append(first)
        await repository.append(second)

        got_first = await repository.get(first.event_id)
        got_second = await repository.get(second.event_id)
        assert got_first is not None
        assert got_second is not None
        assert got_first.trade_id == got_second.trade_id == "trade-abc-123"
        assert got_first.event_id != got_second.event_id
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_bronze_redelivery_of_same_event_is_idempotent() -> None:
    """Re-appending an already-preserved event never corrupts the row."""
    settings = PostgresSettings()
    pool = DatabasePool(settings)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        event = build_event(event_id=f"it-redeliver-{uuid4().hex}")
        await repository.append(event)
        await repository.append(event)  # redelivery of the same event

        fetched = await repository.get(event.event_id)
        assert fetched == event
    finally:
        await pool.close()
