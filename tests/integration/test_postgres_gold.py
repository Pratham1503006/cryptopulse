"""PostgreSQL integration test for the Gold processing path.

Proves the exact milestone flow end to end:

    TrustedEvent -> SilverRepository -> PostgreSQL
                 -> GoldProcessingService -> GoldRepository -> PostgreSQL
                 -> GoldRepository -> MarketWindow

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
from common.contracts import EventType, TradeSide, TrustedEvent
from processing.config import GoldSettings
from processing.gold.service import GoldProcessingService
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import (
    PostgresGoldRepository,
    PostgresSilverRepository,
)

pytestmark = pytest.mark.integration

WINDOW_START = datetime(2026, 9, 24, 12, 30, 0, tzinfo=UTC)


def unique_symbol() -> str:
    """A fresh symbol so a shared database never mixes windows across runs."""
    return f"IT-{uuid4().hex[:8]}"


def build_trusted_event(
    *,
    symbol: str,
    event_id: str,
    price: str,
    quantity: str,
    side: TradeSide = TradeSide.buy,
    offset_seconds: int = 0,
) -> TrustedEvent:
    return TrustedEvent(
        event_id=event_id,
        source="integration",
        event_type=EventType.trade,
        received_at=WINDOW_START,
        validated_at=WINDOW_START,
        symbol=symbol,
        price=Decimal(price),
        quantity=Decimal(quantity),
        side=side,
        trade_id=f"trade-{event_id}",
        trade_timestamp=WINDOW_START + timedelta(seconds=offset_seconds),
        payload={},
    )


@pytest.mark.asyncio
async def test_gold_pipeline_from_silver_to_gold_round_trip() -> None:
    settings = PostgresSettings()
    pool = DatabasePool(settings)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        silver = PostgresSilverRepository(pool.pool)
        gold = PostgresGoldRepository(pool.pool)

        symbol = unique_symbol()
        suffix = uuid4().hex
        events = [
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-gold-a-{suffix}",
                price="100",
                quantity="2",
                offset_seconds=1,
            ),
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-gold-b-{suffix}",
                price="110",
                quantity="1",
                side=TradeSide.sell,
                offset_seconds=20,
            ),
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-gold-c-{suffix}",
                price="90",
                quantity="3",
                offset_seconds=40,
            ),
        ]
        for event in events:
            await silver.append(event)

        service = GoldProcessingService(
            silver=silver,
            gold=gold,
            settings=GoldSettings(window_seconds=300),
            clock=lambda: datetime(2026, 9, 24, 12, 40, 0, tzinfo=UTC),
        )

        written = await service.process_batch()
        assert written >= 1

        # Fetch the window we just wrote by its stable identity.
        window_id = f"{symbol}:{WINDOW_START.isoformat()}"
        window = await gold.get(window_id)
        assert window is not None

        # Aggregation is correct and exact.
        assert window.open == Decimal("100")
        assert window.close == Decimal("90")
        assert window.high == Decimal("110")
        assert window.low == Decimal("90")
        assert window.trade_count == 3
        assert window.volume == Decimal("6")
        assert window.buy_volume == Decimal("5")
        assert window.sell_volume == Decimal("1")
        # (100*2 + 110*1 + 90*3) / 6 = 580/6 = 96.66666667 (ROUND_HALF_UP).
        assert window.vwap == Decimal("96.66666667")
        assert isinstance(window.vwap, Decimal)

        # Timestamps remain timezone-aware.
        assert window.window_start.tzinfo is not None
        assert window.computed_at.tzinfo is not None
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_gold_upsert_is_idempotent_and_updates_late_arrivals() -> None:
    settings = PostgresSettings()
    pool = DatabasePool(settings)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        silver = PostgresSilverRepository(pool.pool)
        gold = PostgresGoldRepository(pool.pool)

        symbol = unique_symbol()
        suffix = uuid4().hex
        await silver.append(
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-late-a-{suffix}",
                price="100",
                quantity="1",
            )
        )
        service = GoldProcessingService(
            silver=silver,
            gold=gold,
            settings=GoldSettings(window_seconds=300),
            clock=lambda: datetime(2026, 9, 24, 12, 40, 0, tzinfo=UTC),
        )
        await service.process_batch()

        window_id = f"{symbol}:{WINDOW_START.isoformat()}"
        before = await gold.get(window_id)
        assert before is not None
        assert before.trade_count == 1

        # A late trusted event lands in the same window.
        await silver.append(
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-late-b-{suffix}",
                price="200",
                quantity="1",
                offset_seconds=30,
            )
        )
        await service.process_batch()

        after = await gold.get(window_id)
        assert after is not None
        assert after.trade_count == 2
        assert after.high == Decimal("200")
        assert after.close == Decimal("200")
        # Same row, recomputed in place: no duplicate window.
        assert after.window_id == before.window_id
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_gold_read_surfaces_symbols_and_latest_windows() -> None:
    settings = PostgresSettings()
    pool = DatabasePool(settings)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        silver = PostgresSilverRepository(pool.pool)
        gold = PostgresGoldRepository(pool.pool)

        symbol = unique_symbol()
        suffix = uuid4().hex

        # Two windows for the same symbol (second starts 300s later).
        await silver.append(
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-sym-a-{suffix}",
                price="100",
                quantity="1",
                offset_seconds=0,
            )
        )
        await silver.append(
            build_trusted_event(
                symbol=symbol,
                event_id=f"it-sym-b-{suffix}",
                price="100",
                quantity="1",
                offset_seconds=400,
            )
        )

        service = GoldProcessingService(
            silver=silver,
            gold=gold,
            settings=GoldSettings(window_seconds=300),
            clock=lambda: datetime(2026, 9, 24, 13, 0, 0, tzinfo=UTC),
        )
        await service.process_batch()

        windows = await gold.read(symbol=symbol)
        assert len(windows) == 2
        # Deterministic order: newest window first.
        assert windows[0].window_start > windows[1].window_start

        latest = await gold.latest_by_symbol()
        assert any(w.symbol == symbol for w in latest)

        symbols = await gold.list_symbols()
        assert symbol in symbols
    finally:
        await pool.close()
