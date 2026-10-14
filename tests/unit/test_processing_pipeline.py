"""Unit tests for the end-to-end processing pipeline.

Uses in-memory stage doubles to prove, without Kafka or PostgreSQL:

- the pipeline sequences ingestion -> trust -> gold and passes the batch
  size to every stage,
- it aggregates each stage's counts into a single PipelineResult,
- trust and gold still run when a pass ingests nothing (Bronze/Silver may
  already hold unprocessed data),
- start/close delegate to the ingestion stage (the only broker owner),
- any stage failure propagates and stops the run (no swallowing),
- the batch size must be positive.

These tests need no running infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import pytest

from common.exceptions import StorageError
from messaging import ConsumeError
from processing.bronze_ingestion import BronzeIngestionService
from processing.gold.service import GoldProcessingService
from processing.pipeline import ProcessingPipeline
from processing.validation.service import TrustRoutingService


@dataclass
class _TrustResult:
    validated: list[object] = field(default_factory=list)
    quarantined: list[object] = field(default_factory=list)


class FakeIngestion:
    """Records calls and returns a fixed count (or raises a configured error)."""

    def __init__(
        self,
        *,
        count: int = 0,
        error: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.count = count
        self.error = error
        self.order = order if order is not None else []
        self.started = 0
        self.closed = 0
        self.max_events_calls: list[int] = []

    async def start(self) -> None:
        self.started += 1

    async def close(self) -> None:
        self.closed += 1

    async def ingest(self, max_events: int) -> int:
        self.order.append("ingest")
        self.max_events_calls.append(max_events)
        if self.error is not None:
            raise self.error
        return self.count


class FakeTrust:
    """Records calls and returns a configured routing result (or raises)."""

    def __init__(
        self,
        *,
        validated: int = 0,
        quarantined: int = 0,
        error: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.validated = validated
        self.quarantined = quarantined
        self.error = error
        self.order = order if order is not None else []
        self.limit_calls: list[int] = []

    async def process_batch(self, limit: int = 100) -> _TrustResult:
        self.order.append("trust")
        self.limit_calls.append(limit)
        if self.error is not None:
            raise self.error
        return _TrustResult(
            validated=[object() for _ in range(self.validated)],
            quarantined=[object() for _ in range(self.quarantined)],
        )


class FakeGold:
    """Records calls and returns a fixed window count (or raises)."""

    def __init__(
        self,
        *,
        written: int = 0,
        error: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.written = written
        self.error = error
        self.order = order if order is not None else []
        self.limit_calls: list[int | None] = []

    async def process_batch(self, offset: int = 0, limit: int | None = None) -> int:
        self.order.append("gold")
        self.limit_calls.append(limit)
        if self.error is not None:
            raise self.error
        return self.written


def make_pipeline(
    ingestion: FakeIngestion,
    trust: FakeTrust,
    gold: FakeGold,
    *,
    batch_size: int = 100,
) -> ProcessingPipeline:
    """Build a pipeline from doubles.

    The stage parameters are typed as the concrete services (the pipeline is
    a composition root), so the doubles are cast here; only the methods the
    pipeline actually calls are exercised.
    """
    return ProcessingPipeline(
        ingestion=cast(BronzeIngestionService, ingestion),
        trust=cast(TrustRoutingService, trust),
        gold=cast(GoldProcessingService, gold),
        batch_size=batch_size,
    )


@pytest.mark.asyncio
async def test_run_once_aggregates_counts_and_passes_batch_size() -> None:
    ingestion = FakeIngestion(count=3)
    trust = FakeTrust(validated=2, quarantined=1)
    gold = FakeGold(written=4)
    pipeline = make_pipeline(ingestion, trust, gold, batch_size=50)

    result = await pipeline.run_once()

    assert result.ingested == 3
    assert result.validated == 2
    assert result.quarantined == 1
    assert result.gold_windows == 4
    assert ingestion.max_events_calls == [50]
    assert trust.limit_calls == [50]
    assert gold.limit_calls == [50]


@pytest.mark.asyncio
async def test_stages_run_in_order() -> None:
    order: list[str] = []
    pipeline = make_pipeline(
        FakeIngestion(order=order),
        FakeTrust(order=order),
        FakeGold(order=order),
    )

    await pipeline.run_once()

    assert order == ["ingest", "trust", "gold"]


@pytest.mark.asyncio
async def test_trust_and_gold_run_when_nothing_ingested() -> None:
    ingestion = FakeIngestion(count=0)
    trust = FakeTrust()
    gold = FakeGold()
    pipeline = make_pipeline(ingestion, trust, gold)

    result = await pipeline.run_once()

    assert result.ingested == 0
    assert trust.limit_calls == [100]
    assert gold.limit_calls == [100]


@pytest.mark.asyncio
async def test_start_and_close_delegate_to_ingestion() -> None:
    ingestion = FakeIngestion()
    pipeline = make_pipeline(ingestion, FakeTrust(), FakeGold())

    await pipeline.start()
    await pipeline.close()

    assert ingestion.started == 1
    assert ingestion.closed == 1


def test_batch_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        make_pipeline(FakeIngestion(), FakeTrust(), FakeGold(), batch_size=0)


@pytest.mark.asyncio
async def test_ingestion_failure_propagates_and_skips_later_stages() -> None:
    order: list[str] = []
    ingestion = FakeIngestion(error=StorageError("bronze down", detail="unit"), order=order)
    trust = FakeTrust(order=order)
    gold = FakeGold(order=order)
    pipeline = make_pipeline(ingestion, trust, gold)

    with pytest.raises(StorageError, match="bronze down"):
        await pipeline.run_once()

    assert order == ["ingest"]
    assert trust.limit_calls == []
    assert gold.limit_calls == []


@pytest.mark.asyncio
async def test_trust_failure_propagates_and_skips_gold() -> None:
    order: list[str] = []
    trust = FakeTrust(error=StorageError("silver down", detail="unit"), order=order)
    gold = FakeGold(order=order)
    pipeline = make_pipeline(FakeIngestion(order=order), trust, gold)

    with pytest.raises(StorageError, match="silver down"):
        await pipeline.run_once()

    assert order == ["ingest", "trust"]
    assert gold.limit_calls == []


@pytest.mark.asyncio
async def test_gold_failure_propagates() -> None:
    gold = FakeGold(error=StorageError("gold down", detail="unit"))
    pipeline = make_pipeline(FakeIngestion(), FakeTrust(), gold)

    with pytest.raises(StorageError, match="gold down"):
        await pipeline.run_once()


@pytest.mark.asyncio
async def test_consume_error_propagates() -> None:
    ingestion = FakeIngestion(error=ConsumeError("commit failed"))
    pipeline = make_pipeline(ingestion, FakeTrust(), FakeGold())

    with pytest.raises(ConsumeError, match="commit failed"):
        await pipeline.run_once()


@pytest.mark.asyncio
async def test_default_batch_size_is_one_hundred() -> None:
    ingestion = FakeIngestion()
    trust = FakeTrust()
    gold = FakeGold()
    pipeline = make_pipeline(ingestion, trust, gold)

    await pipeline.run_once()

    assert ingestion.max_events_calls == [100]
