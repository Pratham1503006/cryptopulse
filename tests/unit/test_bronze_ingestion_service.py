"""Unit tests for the Bronze ingestion service ack semantics.

Uses an in-memory consumer/repository to prove, without Kafka or
PostgreSQL, that:

- a message is acknowledged only AFTER its Bronze append commits,
- a failed Bronze append is never acknowledged and propagates,
- a failed ack propagates after the event is persisted.

These tests need no running infrastructure.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from common.contracts import EventType, InternalEvent, TradeSide
from common.exceptions import StorageError
from messaging import ConsumedEvent, ConsumeError
from messaging.consumer import Consumer
from processing.bronze_ingestion import BronzeIngestionService
from warehouse.repositories.base import BronzeRepository


class FakeConsumer(Consumer):
    """In-memory Consumer implementation for ack-semantics tests."""

    def __init__(self, items: list[ConsumedEvent]) -> None:
        self.stream_items = items
        self.acked: list[ConsumedEvent] = []
        self.started = False
        self.closed = False
        self.ack_error: Exception | None = None

    async def start(self) -> None:
        self.started = True

    async def stream(self) -> AsyncGenerator[InternalEvent, None]:
        """Unused in these tests; present to satisfy the Consumer ABC."""
        raise NotImplementedError("not used in these tests")
        yield None  # pragma: no cover - makes this function an async generator

    def stream_events(self) -> AsyncGenerator[ConsumedEvent, None]:
        return self._stream_events_gen()

    async def _stream_events_gen(self) -> AsyncGenerator[ConsumedEvent, None]:
        for item in self.stream_items:
            yield item

    async def acknowledge(self, consumed: ConsumedEvent) -> None:
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.append(consumed)

    async def close(self) -> None:
        self.closed = True


class FakeBronzeRepository(BronzeRepository):
    """In-memory BronzeRepository implementation for ack-semantics tests."""

    def __init__(self, fail_event_ids: set[str] | None = None) -> None:
        self.appended: list[InternalEvent] = []
        self.fail_event_ids = fail_event_ids or set()

    async def append(self, event: InternalEvent) -> None:
        if event.event_id in self.fail_event_ids:
            raise StorageError("simulated bronze failure", detail="unit-test failure")
        self.appended.append(event)

    async def replay(self, offset: int = 0, limit: int = 100) -> list[InternalEvent]:
        return list(self.appended)


def build_event(event_id: str) -> InternalEvent:
    return InternalEvent(
        event_id=event_id,
        source="dev",
        event_type=EventType.trade,
        received_at=datetime(2026, 9, 1, tzinfo=UTC),
        symbol="BTC-USD",
        price=Decimal("100.5"),
        quantity=Decimal("1"),
        side=TradeSide.buy,
        trade_id="trade-1",
        trade_timestamp=datetime(2026, 9, 1, tzinfo=UTC),
        payload={"venue": "dev"},
    )


def build_consumed(event_id: str, offset: int) -> ConsumedEvent:
    return ConsumedEvent(
        event=build_event(event_id),
        topic="cryptopulse.market.events",
        partition=0,
        offset=offset,
    )


@pytest.mark.asyncio
async def test_ack_happens_only_after_bronze_append_commits() -> None:
    """Append-then-ack ordering, verified by recording the interleaving."""
    order: list[str] = []

    class RecordingRepository(FakeBronzeRepository):
        async def append(self, event: InternalEvent) -> None:
            order.append(f"append:{event.event_id}")
            await super().append(event)

    class RecordingConsumer(FakeConsumer):
        async def acknowledge(self, consumed: ConsumedEvent) -> None:
            order.append(f"ack:{consumed.event.event_id}")
            await super().acknowledge(consumed)

    consumed = [build_consumed("e1", 0), build_consumed("e2", 1)]
    consumer = RecordingConsumer(consumed)
    repository = RecordingRepository()

    service = BronzeIngestionService(consumer=consumer, repository=repository)
    await service.start()
    persisted = await service.ingest(max_events=2)
    await service.close()

    assert persisted == 2
    assert order == ["append:e1", "ack:e1", "append:e2", "ack:e2"]
    assert [e.event_id for e in repository.appended] == ["e1", "e2"]
    assert [c.event.event_id for c in consumer.acked] == ["e1", "e2"]


@pytest.mark.asyncio
async def test_failed_bronze_write_is_never_acknowledged_and_raises() -> None:
    consumed = [build_consumed("bad", 0), build_consumed("good", 1)]
    consumer = FakeConsumer(consumed)
    repository = FakeBronzeRepository(fail_event_ids={"bad"})

    service = BronzeIngestionService(consumer=consumer, repository=repository)
    await service.start()

    with pytest.raises(StorageError, match="simulated bronze failure"):
        await service.ingest(max_events=2)

    await service.close()

    # The failed event was NOT persisted and NOT acknowledged; the good
    # event after it was never reached (the run stops on failure).
    assert repository.appended == []
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_failed_ack_after_successful_append_raises() -> None:
    consumed = [build_consumed("e1", 0)]
    consumer = FakeConsumer(consumed)
    consumer.ack_error = ConsumeError("simulated commit failure")
    repository = FakeBronzeRepository()

    service = BronzeIngestionService(consumer=consumer, repository=repository)
    await service.start()

    with pytest.raises(ConsumeError, match="simulated commit failure"):
        await service.ingest(max_events=1)

    await service.close()

    # Event is persisted (never lost) but not acknowledged, so it will be
    # redelivered: at-least-once semantics.
    assert [e.event_id for e in repository.appended] == ["e1"]
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_ingest_respects_max_events_bound() -> None:
    consumed = [build_consumed(f"e{i}", i) for i in range(5)]
    consumer = FakeConsumer(consumed)
    repository = FakeBronzeRepository()

    service = BronzeIngestionService(consumer=consumer, repository=repository)
    await service.start()
    persisted = await service.ingest(max_events=3)
    await service.close()

    assert persisted == 3
    assert len(consumer.acked) == 3


@pytest.mark.asyncio
async def test_ingest_rejects_non_positive_max_events() -> None:
    consumer = FakeConsumer([])
    repository = FakeBronzeRepository()
    service = BronzeIngestionService(consumer=consumer, repository=repository)

    with pytest.raises(ValueError, match="max_events"):
        await service.ingest(max_events=0)


def test_consumed_event_offsets_build_commit_map() -> None:
    consumed = build_consumed("e1", offset=41)

    offsets = consumed._offsets()
    assert len(offsets) == 1
    (offset_and_metadata,) = offsets.values()
    assert offset_and_metadata.offset == 42  # next offset to consume
