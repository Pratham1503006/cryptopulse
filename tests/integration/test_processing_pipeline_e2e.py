"""End-to-end ProcessingPipeline integration test.

Proves the composed consumer pipeline against real PostgreSQL, without
Kafka (Kafka is stubbed, so the run ingests nothing and does not touch
offsets):

    (empty consumer) -> BronzeIngestionService -> Bronze
    Bronze -> TrustRoutingService -> Silver / Quarantine
    Silver -> GoldProcessingService -> Gold

and the invariants that matter across the whole path:

- a valid trade is routed to Silver and then aggregated into Gold,
- an invalid event is quarantined, never aggregated,
- Bronze is left intact after both paths.

Events are constructed by the test itself under a unique symbol; existing
database contents are never used as fixtures. Runs only when explicitly
selected:

    pytest -m integration
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from common.config import PostgresSettings
from common.contracts import EventType, InternalEvent, TradeSide
from messaging.consumer import ConsumedEvent as _ConsumedEvent
from messaging.consumer import Consumer
from processing import (
    BronzeIngestionService,
    GoldProcessingService,
    GoldSettings,
    ProcessingPipeline,
    TrustRoutingService,
    ValidationEngine,
)
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import (
    PostgresBronzeRepository,
    PostgresGoldRepository,
    PostgresQuarantineRepository,
    PostgresSilverRepository,
)

pytestmark = pytest.mark.integration

WINDOW_START = datetime(2026, 10, 1, 9, 0, 0, tzinfo=UTC)


class EmptyConsumer(Consumer):
    """A Kafka-free consumer double: no messages, no acks."""

    async def start(self) -> None:
        return None

    async def stream(self) -> AsyncGenerator[InternalEvent, None]:
        raise NotImplementedError("not used in this test")
        yield None  # pragma: no cover - makes this an async generator

    def stream_events(self) -> AsyncGenerator[_ConsumedEvent, None]:
        return self._empty()

    async def _empty(self) -> AsyncGenerator[_ConsumedEvent, None]:
        events: list[_ConsumedEvent] = []
        for consumed in events:
            yield consumed

    async def close(self) -> None:
        return None


def build_event(
    *,
    event_id: str,
    symbol: str,
    price: str,
    quantity: str,
    side: TradeSide,
    offset_seconds: int,
) -> InternalEvent:
    return InternalEvent(
        event_id=event_id,
        source="integration",
        event_type=EventType.trade,
        received_at=WINDOW_START,
        symbol=symbol,
        price=Decimal(price),
        quantity=Decimal(quantity),
        side=side,
        trade_id=f"trade-{event_id}",
        trade_timestamp=WINDOW_START + timedelta(seconds=offset_seconds),
        payload={},
    )


@pytest.mark.asyncio
async def test_pipeline_routes_bronze_to_silver_and_gold() -> None:
    tag = uuid4().hex
    symbol = f"IT-{tag[:8]}"

    valid_a = build_event(
        event_id=f"it-pipe-a-{tag}",
        symbol=symbol,
        price="100",
        quantity="2",
        side=TradeSide.buy,
        offset_seconds=1,
    )
    valid_b = build_event(
        event_id=f"it-pipe-b-{tag}",
        symbol=symbol,
        price="110",
        quantity="1",
        side=TradeSide.sell,
        offset_seconds=20,
    )
    invalid = build_event(
        event_id=f"it-pipe-x-{tag}",
        symbol=symbol,
        price="0",
        quantity="-1",
        side=TradeSide.buy,
        offset_seconds=30,
    )

    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()

        bronze = PostgresBronzeRepository(pool.pool)
        silver = PostgresSilverRepository(pool.pool)
        gold = PostgresGoldRepository(pool.pool)

        # Seed Bronze (received, unvalidated) as the producer would.
        for event in (valid_a, valid_b, invalid):
            await bronze.append(event)

        pipeline = ProcessingPipeline(
            ingestion=BronzeIngestionService(consumer=EmptyConsumer(), repository=bronze),
            trust=TrustRoutingService(
                bronze=bronze,
                silver=silver,
                quarantine=PostgresQuarantineRepository(pool.pool),
                validator=ValidationEngine(),
            ),
            gold=GoldProcessingService(
                silver=silver,
                gold=gold,
                settings=GoldSettings(window_seconds=300),
                clock=lambda: datetime(2026, 10, 1, 9, 30, 0, tzinfo=UTC),
            ),
            # Large window so a shared database never truncates the page.
            batch_size=1_000_000,
        )

        await pipeline.start()
        try:
            result = await pipeline.run_once()
        finally:
            await pipeline.close()

        # Kafka was stubbed: nothing ingested, but the pass still routed and
        # aggregated what Bronze/Silver already held.
        assert result.ingested == 0
        assert result.validated >= 2
        assert result.quarantined >= 1
        assert result.gold_windows >= 1

        # Valid events reached Silver; the invalid one did not.
        assert await silver.get(valid_a.event_id) is not None
        assert await silver.get(valid_b.event_id) is not None
        assert await silver.get(invalid.event_id) is None

        # The invalid event is preserved in Quarantine.
        records = await PostgresQuarantineRepository(pool.pool).get_records(invalid.event_id)
        assert len(records) == 1

        # Gold aggregated exactly the two valid trades under the unique symbol.
        window_id = f"{symbol}:{WINDOW_START.isoformat()}"
        window = await gold.get(window_id)
        assert window is not None
        assert window.trade_count == 2
        assert window.open == Decimal("100")
        assert window.close == Decimal("110")
        assert window.high == Decimal("110")
        assert window.low == Decimal("100")
        assert window.volume == Decimal("3")
        assert window.buy_volume == Decimal("2")
        assert window.sell_volume == Decimal("1")
        # (100*2 + 110*1) / 3 = 310/3 = 103.33333333 (ROUND_HALF_UP).
        assert window.vwap == Decimal("103.33333333")

        # Bronze is the immutable record of what was received: untouched.
        assert await bronze.get(valid_a.event_id) == valid_a
        assert await bronze.get(invalid.event_id) == invalid
    finally:
        await pool.close()
