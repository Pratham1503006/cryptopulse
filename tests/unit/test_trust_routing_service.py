"""Unit tests for the TrustRoutingService.

Uses in-memory repository fakes to prove routing and failure semantics
without PostgreSQL: valid events go to Silver as TrustedEvents, invalid
events go to Quarantine with structured failures, Bronze is untouched, and
infrastructure failures propagate instead of being misclassified as
validation outcomes.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from common.contracts import (
    EventType,
    InternalEvent,
    TradeSide,
    TrustedEvent,
    ValidationFailure,
)
from common.exceptions import StorageError
from processing.validation import TrustRoutingService, ValidationEngine
from warehouse.repositories.base import BronzeRepository, QuarantineRepository, SilverRepository
from warehouse.schemas.quarantine import QuarantineRecord


def build_event(**overrides: Any) -> InternalEvent:
    defaults: dict[str, Any] = {
        "event_id": "evt-1",
        "source": "dev",
        "event_type": EventType.trade,
        "received_at": datetime(2026, 8, 26, 10, 10, 5, tzinfo=UTC),
        "symbol": "BTC-USD",
        "price": Decimal("100.5"),
        "quantity": Decimal("1"),
        "side": TradeSide.buy,
        "trade_id": "trade-1",
        "trade_timestamp": datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC),
        "payload": {"venue": "dev-exchange"},
    }
    defaults.update(overrides)
    return InternalEvent(**defaults)


class InMemoryBronze(BronzeRepository):
    def __init__(self, events: Sequence[InternalEvent]) -> None:
        self.events = list(events)

    async def append(self, event: InternalEvent) -> None:
        raise AssertionError("the trust service must never write to Bronze")

    async def replay(self, offset: int = 0, limit: int = 100) -> list[InternalEvent]:
        return self.events[offset : offset + limit]


class InMemorySilver(SilverRepository):
    def __init__(self) -> None:
        self.appended: list[TrustedEvent] = []
        self.fail_once = False

    async def append(self, event: TrustedEvent) -> None:
        if self.fail_once:
            self.fail_once = False
            raise StorageError("silver unavailable", detail="unit-test failure")
        self.appended.append(event)

    async def read(self, offset: int = 0, limit: int = 100) -> list[TrustedEvent]:
        return self.appended[offset : offset + limit]


class InMemoryQuarantine(QuarantineRepository):
    def __init__(self) -> None:
        self.records: list[QuarantineRecord] = []

    async def append(self, event: InternalEvent, reason: str) -> None:
        raise AssertionError("the trust service must use append_record, not append")

    async def append_record(
        self,
        event: InternalEvent,
        quarantined_at: datetime,
        failures: list[ValidationFailure],
    ) -> None:
        self.records.append(
            QuarantineRecord(
                event=event,
                quarantined_at=quarantined_at,
                failures=failures,
            )
        )

    async def read_rejected(self, offset: int = 0, limit: int = 100) -> list[InternalEvent]:
        return [record.event for record in self.records[offset : offset + limit]]


def build_service(
    events: Sequence[InternalEvent],
    silver: InMemorySilver | None = None,
    quarantine: InMemoryQuarantine | None = None,
) -> tuple[TrustRoutingService, InMemorySilver, InMemoryQuarantine]:
    silver = silver if silver is not None else InMemorySilver()
    quarantine = quarantine if quarantine is not None else InMemoryQuarantine()
    service = TrustRoutingService(
        bronze=InMemoryBronze(events),
        silver=silver,
        quarantine=quarantine,
        validator=ValidationEngine(),
    )
    return service, silver, quarantine


@pytest.mark.asyncio
async def test_valid_event_goes_to_silver_as_trusted_event() -> None:
    event = build_event()
    service, silver, quarantine = build_service([event])

    result = await service.process_batch()

    assert len(result.validated) == 1
    assert result.quarantined == []
    assert len(silver.appended) == 1
    trusted = silver.appended[0]
    assert isinstance(trusted, TrustedEvent)
    assert trusted.event_id == event.event_id
    assert trusted.validated_at.tzinfo is not None
    assert quarantine.records == []


@pytest.mark.asyncio
async def test_invalid_event_goes_to_quarantine_with_structured_failures() -> None:
    event = build_event(price=Decimal("0"), quantity=Decimal("-1"))
    service, silver, quarantine = build_service([event])

    result = await service.process_batch()

    assert result.validated == []
    assert len(result.quarantined) == 1
    assert silver.appended == []
    record = quarantine.records[0]
    assert record.event == event
    assert record.quarantined_at.tzinfo is not None
    rules = {failure.rule for failure in record.failures}
    assert rules == {"price_validity", "quantity_validity"}


@pytest.mark.asyncio
async def test_mixed_batch_routes_each_event_appropriately() -> None:
    valid = build_event(event_id="evt-ok")
    invalid = build_event(event_id="evt-bad", symbol="  ")
    service, silver, quarantine = build_service([valid, invalid])

    result = await service.process_batch()

    assert [event.event_id for event in result.validated] == ["evt-ok"]
    assert [event.event_id for event in result.quarantined] == ["evt-bad"]
    assert len(silver.appended) == 1
    assert len(quarantine.records) == 1
    # Structured outcome retained for the quarantined event.
    assert result.outcomes["evt-bad"].valid is False


@pytest.mark.asyncio
async def test_events_sharing_trade_id_are_all_validated() -> None:
    """Same trade_id is NOT a duplicate signal: both events are trusted."""
    first = build_event(event_id="evt-a", trade_id="shared")
    second = build_event(event_id="evt-b", trade_id="shared", price=Decimal("2"))
    service, silver, _quarantine = build_service([first, second])

    result = await service.process_batch()

    assert len(result.validated) == 2
    assert len(silver.appended) == 2


@pytest.mark.asyncio
async def test_bronze_is_never_modified() -> None:
    event = build_event()
    bronze = InMemoryBronze([event])
    service = TrustRoutingService(
        bronze=bronze,
        silver=InMemorySilver(),
        quarantine=InMemoryQuarantine(),
        validator=ValidationEngine(),
    )

    await service.process_batch()

    assert bronze.events == [event]


@pytest.mark.asyncio
async def test_silver_infrastructure_failure_propagates() -> None:
    event = build_event()
    silver = InMemorySilver()
    silver.fail_once = True
    service, _silver, quarantine = build_service([event], silver=silver)

    with pytest.raises(StorageError, match="silver unavailable"):
        await service.process_batch()

    # Not misclassified as a validation outcome.
    assert quarantine.records == []


@pytest.mark.asyncio
async def test_quarantine_infrastructure_failure_propagates() -> None:
    class FailingQuarantine(InMemoryQuarantine):
        async def append_record(
            self,
            event: InternalEvent,
            quarantined_at: datetime,
            failures: list[ValidationFailure],
        ) -> None:
            raise StorageError("quarantine unavailable", detail="unit-test failure")

    event = build_event(price=Decimal("0"))
    quarantine = FailingQuarantine()
    service, silver, _ = build_service([event], quarantine=quarantine)

    with pytest.raises(StorageError, match="quarantine unavailable"):
        await service.process_batch()

    assert silver.appended == []


@pytest.mark.asyncio
async def test_reprocessing_valid_event_is_idempotent_in_silver() -> None:
    event = build_event()
    service, silver, _ = build_service([event])

    first = await service.process_batch()
    second = await service.process_batch()

    assert first.validated == second.validated == [event]
    # In-memory fake accepts both; the PostgreSQL ON CONFLICT behavior is
    # proven in the integration suite.
    assert [trusted.event_id for trusted in silver.appended] == [event.event_id] * 2


@pytest.mark.asyncio
async def test_reprocessing_invalid_event_appends_distinct_records() -> None:
    event = build_event(price=Decimal("0"))
    service, _silver, quarantine = build_service([event])

    await service.process_batch()
    await service.process_batch()

    assert len(quarantine.records) == 2
    assert all(record.event == event for record in quarantine.records)


@pytest.mark.asyncio
async def test_process_event_routes_single_event() -> None:
    valid = build_event(event_id="evt-ok")
    invalid = build_event(event_id="evt-bad", quantity=Decimal("0"))
    service, silver, quarantine = build_service([])

    outcome_valid = await service.process_event(valid)
    outcome_invalid = await service.process_event(invalid)

    assert outcome_valid.valid is True
    assert outcome_invalid.valid is False
    assert [trusted.event_id for trusted in silver.appended] == ["evt-ok"]
    assert [record.event.event_id for record in quarantine.records] == ["evt-bad"]


@pytest.mark.asyncio
async def test_process_batch_rejects_non_positive_limit() -> None:
    service, _silver, _quarantine = build_service([])
    with pytest.raises(ValueError, match="limit"):
        await service.process_batch(limit=0)
