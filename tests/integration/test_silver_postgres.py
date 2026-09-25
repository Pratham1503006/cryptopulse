"""PostgreSQL integration test for the Silver repository.

Proves the trust-milestone flow for valid events:

    TrustedEvent -> SilverRepository -> PostgreSQL
                 -> SilverRepository -> TrustedEvent

Requires a running PostgreSQL instance (see docker-compose.yml). Runs only
when explicitly selected, matching the existing integration convention:

    pytest -m integration
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from common.config import PostgresSettings
from common.contracts import EventType, TradeSide, TrustedEvent
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import PostgresSilverRepository

pytestmark = pytest.mark.integration

# Deliberately over-precise values: 20 decimal places exposes any float
# conversion (float64 has ~15-17 significant decimal digits).
PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")


def build_trusted(event_id: str) -> TrustedEvent:
    return TrustedEvent(
        event_id=event_id,
        source="dev",
        event_type=EventType.trade,
        received_at=datetime(2026, 9, 26, 12, 30, 45, 123456, tzinfo=UTC),
        validated_at=datetime(2026, 9, 26, 12, 30, 46, tzinfo=UTC),
        symbol="BTC-USD",
        price=PRECISE_PRICE,
        quantity=PRECISE_QUANTITY,
        side=TradeSide.sell,
        trade_id="trade-abc-123",
        trade_timestamp=datetime(2026, 9, 26, 12, 30, 0, tzinfo=UTC),
        payload={"venue": "dev-exchange", "sequence": 12345, "extra": {"nested": True}},
    )


@pytest.mark.asyncio
async def test_silver_round_trip_preserves_exact_trusted_event() -> None:
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresSilverRepository(pool.pool)

        event = build_trusted(event_id=f"it-silver-{uuid4().hex}")

        await repository.append(event)

        fetched = await repository.get(event.event_id)
        assert fetched is not None
        assert fetched == event

        read_batch = await repository.read(offset=0, limit=100)
        matches = [trusted for trusted in read_batch if trusted.event_id == event.event_id]
        assert matches and matches[0] == event

        # Field-by-field: exact Decimal precision, not float-approximated.
        assert fetched.price == PRECISE_PRICE
        assert fetched.price.as_tuple() == PRECISE_PRICE.as_tuple()
        assert fetched.quantity == PRECISE_QUANTITY
        assert isinstance(fetched.price, Decimal)
        assert isinstance(fetched.quantity, Decimal)

        # Timezone-aware timestamps.
        assert fetched.received_at.tzinfo is not None
        assert fetched.validated_at.tzinfo is not None
        assert fetched.trade_timestamp.tzinfo is not None
        assert fetched.validated_at == event.validated_at
        assert fetched.received_at.utcoffset() == event.received_at.utcoffset()

        # Payload survives as structured JSON.
        assert fetched.payload == event.payload

        # Canonical enum representation.
        assert fetched.side is TradeSide.sell
        assert fetched.event_type is EventType.trade
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_silver_preserves_distinct_events_with_same_trade_id() -> None:
    """Same trade_id is NOT a unique key: distinct trusted events coexist."""
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresSilverRepository(pool.pool)

        suffix = uuid4().hex
        first = build_trusted(event_id=f"it-silver-a-{suffix}")
        second = build_trusted(event_id=f"it-silver-b-{suffix}")

        await repository.append(first)
        await repository.append(second)

        got_first = await repository.get(first.event_id)
        got_second = await repository.get(second.event_id)
        assert got_first is not None and got_second is not None
        assert got_first.trade_id == got_second.trade_id == "trade-abc-123"
        assert got_first.event_id != got_second.event_id
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_silver_reprocessing_same_event_is_idempotent() -> None:
    """Re-appending a canonical event leaves the original row untouched."""
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresSilverRepository(pool.pool)

        event = build_trusted(event_id=f"it-silver-r-{uuid4().hex}")
        await repository.append(event)
        await repository.append(event)  # repeated processing attempt

        fetched = await repository.get(event.event_id)
        assert fetched == event
    finally:
        await pool.close()
