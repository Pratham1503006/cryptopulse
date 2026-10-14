"""Unit tests for the Gold processing engine.

Uses in-memory Silver/Gold repositories to prove, without PostgreSQL:

- trusted events are aggregated into the correct windows and symbols,
- the aggregation is reproducible and idempotent (upsert per window),
- late-arriving trusted events recompute the affected window,
- finality follows the configured allowed lateness,
- Gold is derived only from Silver.

These tests need no running infrastructure.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from common.contracts import (
    EventType,
    MarketWindow,
    TradeSide,
    TrustedEvent,
)
from common.exceptions import StorageError
from processing.config import GoldSettings
from processing.gold.service import GoldProcessingService
from warehouse.repositories.base import GoldRepository, SilverRepository

WINDOW_START = datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)


def build_event(
    *,
    event_id: str,
    price: str,
    quantity: str,
    side: TradeSide = TradeSide.buy,
    offset_seconds: int = 0,
    symbol: str = "BTC-USD",
) -> TrustedEvent:
    return TrustedEvent(
        event_id=event_id,
        source="dev",
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


class InMemorySilver(SilverRepository):
    def __init__(self, events: Sequence[TrustedEvent]) -> None:
        self.events = list(events)

    async def append(self, event: TrustedEvent) -> None:
        self.events.append(event)

    async def read(self, offset: int = 0, limit: int = 100) -> list[TrustedEvent]:
        return self.events[offset : offset + limit]

    async def read_window(
        self,
        symbol: str,
        window_start: datetime,
        window_end: datetime,
        limit: int = 1_000_000,
    ) -> list[TrustedEvent]:
        matches = [
            event
            for event in self.events
            if event.symbol == symbol and window_start <= event.trade_timestamp < window_end
        ]
        matches.sort(key=lambda event: (event.trade_timestamp, event.event_id))
        return matches[:limit]


class InMemoryGold(GoldRepository):
    def __init__(self) -> None:
        self.windows: dict[str, MarketWindow] = {}
        self.upsert_calls = 0

    async def upsert(self, window: MarketWindow) -> None:
        self.upsert_calls += 1
        self.windows[window.window_id] = window

    async def read(
        self,
        symbol: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[MarketWindow]:
        windows = [
            window for window in self.windows.values() if symbol is None or window.symbol == symbol
        ]
        windows.sort(key=lambda window: window.window_start, reverse=True)
        return windows[offset : offset + limit]


def build_service(
    events: Sequence[TrustedEvent],
    *,
    now: datetime | None = None,
    settings: GoldSettings | None = None,
) -> tuple[GoldProcessingService, InMemoryGold]:
    gold = InMemoryGold()
    clock = (lambda: now) if now is not None else None
    service = GoldProcessingService(
        silver=InMemorySilver(events),
        gold=gold,
        settings=settings,
        clock=clock,
    )
    return service, gold


@pytest.mark.asyncio
async def test_batch_aggregates_events_into_a_window() -> None:
    events = [
        build_event(event_id="a", price="100", quantity="2", offset_seconds=1),
        build_event(event_id="b", price="110", quantity="1", offset_seconds=20),
        build_event(event_id="c", price="90", quantity="3", offset_seconds=40),
    ]
    service, gold = build_service(events)

    written = await service.process_batch()

    assert written == 1
    (window,) = gold.windows.values()
    assert window.symbol == "BTC-USD"
    assert window.open == Decimal("100")
    assert window.close == Decimal("90")
    assert window.high == Decimal("110")
    assert window.low == Decimal("90")
    assert window.trade_count == 3
    assert window.volume == Decimal("6")


@pytest.mark.asyncio
async def test_multiple_symbols_and_windows_are_separated() -> None:
    events = [
        build_event(event_id="btc", price="100", quantity="1"),
        build_event(event_id="eth", price="50", quantity="1", symbol="ETH-USD"),
        build_event(event_id="btc2", price="101", quantity="1", offset_seconds=120),
    ]
    service, gold = build_service(events)

    written = await service.process_batch()

    # BTC-USD window 0, BTC-USD window 1 (t+120s), ETH-USD window 0.
    assert written == 3
    assert len(gold.windows) == 3
    assert {w.symbol for w in gold.windows.values()} == {"BTC-USD", "ETH-USD"}


@pytest.mark.asyncio
async def test_reprocessing_is_idempotent_per_window() -> None:
    events = [build_event(event_id="a", price="100", quantity="1")]
    # Fixed clock so computed_at is deterministic across the two runs.
    fixed_now = datetime(2026, 8, 26, 10, 14, 0, tzinfo=UTC)
    service, gold = build_service(events, now=fixed_now)

    await service.process_batch()
    first = dict(gold.windows)
    await service.process_batch()

    # Same window_id overwritten in place: no duplicate rows, same values.
    assert gold.windows == first
    assert len(gold.windows) == 1


@pytest.mark.asyncio
async def test_recompute_window_includes_late_arrival() -> None:
    silver_events = [build_event(event_id="early", price="100", quantity="1")]
    silver = InMemorySilver(silver_events)
    gold = InMemoryGold()
    service = GoldProcessingService(silver=silver, gold=gold)

    await service.recompute_window("BTC-USD", WINDOW_START)
    (before,) = gold.windows.values()
    assert before.trade_count == 1

    # A late trusted event within the same window arrives afterwards.
    silver.events.append(build_event(event_id="late", price="200", quantity="1", offset_seconds=30))
    await service.recompute_window("BTC-USD", WINDOW_START)

    (after,) = gold.windows.values()
    assert after.trade_count == 2
    assert after.high == Decimal("200")
    assert after.close == Decimal("200")
    assert after.window_id == before.window_id  # same row, recomputed


@pytest.mark.asyncio
async def test_finality_follows_allowed_lateness() -> None:
    events = [build_event(event_id="a", price="100", quantity="1")]
    # Window is [10:10:00, 10:11:00). Lateness 120s -> final at 10:13:00.
    service, gold = build_service(
        events,
        now=datetime(2026, 8, 26, 10, 12, 0, tzinfo=UTC),
        settings=GoldSettings(allowed_lateness_seconds=120),
    )
    await service.process_batch()
    (provisional,) = gold.windows.values()
    assert provisional.is_final is False

    final_service, final_gold = build_service(
        events,
        now=datetime(2026, 8, 26, 10, 14, 0, tzinfo=UTC),
        settings=GoldSettings(allowed_lateness_seconds=120),
    )
    await final_service.process_batch()
    (finalized,) = final_gold.windows.values()
    assert finalized.is_final is True


@pytest.mark.asyncio
async def test_empty_silver_writes_nothing() -> None:
    service, gold = build_service([])
    assert await service.process_batch() == 0
    assert gold.windows == {}


@pytest.mark.asyncio
async def test_configured_window_size_is_respected() -> None:
    events = [
        build_event(event_id="a", price="100", quantity="1", offset_seconds=0),
        build_event(event_id="b", price="101", quantity="1", offset_seconds=30),
    ]
    # 30s windows put the two events in distinct windows.
    service, gold = build_service(events, settings=GoldSettings(window_seconds=30))
    written = await service.process_batch()
    assert written == 2
    assert len(gold.windows) == 2


@pytest.mark.asyncio
async def test_storage_failure_propagates() -> None:
    class FailingGold(InMemoryGold):
        async def upsert(self, window: MarketWindow) -> None:
            raise StorageError("gold unavailable", detail="unit-test failure")

    service = GoldProcessingService(
        silver=InMemorySilver([build_event(event_id="a", price="1", quantity="1")]),
        gold=FailingGold(),
    )
    with pytest.raises(StorageError, match="gold unavailable"):
        await service.process_batch()


@pytest.mark.asyncio
async def test_process_batch_rejects_invalid_arguments() -> None:
    service, _gold = build_service([])
    with pytest.raises(ValueError, match="offset"):
        await service.process_batch(offset=-1)
    with pytest.raises(ValueError, match="limit"):
        await service.process_batch(limit=0)
