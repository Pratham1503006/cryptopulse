"""PostgreSQL integration test for the Quarantine repository.

Proves that rejected events survive with their structured validation
failure information, and that repeated rejection attempts remain visible.

Requires a running PostgreSQL instance (see docker-compose.yml). Runs only
when explicitly selected:

    pytest -m integration
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from common.config import PostgresSettings
from common.contracts import EventType, InternalEvent, TradeSide, ValidationFailure
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import PostgresQuarantineRepository

pytestmark = pytest.mark.integration


def build_invalid_event(event_id: str) -> InternalEvent:
    return InternalEvent(
        event_id=event_id,
        source="dev",
        event_type=EventType.trade,
        received_at=datetime(2026, 9, 26, 12, 30, 45, tzinfo=UTC),
        symbol="BTC-USD",
        price=Decimal("0"),  # invalid: non-positive price
        quantity=Decimal("-1"),  # invalid: non-positive quantity
        side=TradeSide.buy,
        trade_id="trade-bad-1",
        trade_timestamp=datetime(2026, 9, 26, 12, 30, 0, tzinfo=UTC),
        payload={"venue": "dev-exchange"},
    )


def build_failures() -> list[ValidationFailure]:
    return [
        ValidationFailure(
            rule="price_validity",
            code="non_positive",
            message="price must be positive, got 0",
        ),
        ValidationFailure(
            rule="quantity_validity",
            code="non_positive",
            message="quantity must be positive, got -1",
        ),
    ]


@pytest.mark.asyncio
async def test_quarantine_preserves_event_and_structured_failures() -> None:
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresQuarantineRepository(pool.pool)

        event = build_invalid_event(f"it-quar-{uuid4().hex}")
        quarantined_at = datetime(2026, 9, 26, 12, 31, 0, tzinfo=UTC)

        await repository.append_record(
            event=event,
            quarantined_at=quarantined_at,
            failures=build_failures(),
        )

        records = await repository.get_records(event.event_id)
        assert len(records) == 1
        record = records[0]

        # Original InternalEvent fields survive exactly.
        assert record.event == event
        assert record.event.price == Decimal("0")
        assert record.event.symbol == "BTC-USD"
        assert record.event.payload == event.payload

        # Quarantine timestamp preserved and timezone-aware.
        assert record.quarantined_at == quarantined_at
        assert record.quarantined_at.tzinfo is not None

        # Structured failure information survives as data.
        assert record.failures == build_failures()
        assert [failure.rule for failure in record.failures] == [
            "price_validity",
            "quantity_validity",
        ]

        # read_rejected reconstructs the InternalEvent.
        rejected = await repository.read_rejected(offset=0, limit=100)
        assert event in rejected
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_quarantine_keeps_each_rejection_attempt_visible() -> None:
    """Repeated processing attempts append distinct, inspectable records."""
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresQuarantineRepository(pool.pool)

        event = build_invalid_event(f"it-quar-r-{uuid4().hex}")
        first_at = datetime(2026, 9, 26, 12, 31, 0, tzinfo=UTC)
        second_at = datetime(2026, 9, 26, 12, 32, 0, tzinfo=UTC)

        await repository.append_record(
            event=event, quarantined_at=first_at, failures=build_failures()
        )
        await repository.append_record(
            event=event, quarantined_at=second_at, failures=build_failures()
        )

        records = await repository.get_records(event.event_id)
        assert len(records) == 2
        assert [record.quarantined_at for record in records] == [first_at, second_at]
        assert all(record.event == event for record in records)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_quarantine_text_reason_append_is_preserved() -> None:
    """The ABC's text-reason append is stored as structured failure data."""
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresQuarantineRepository(pool.pool)

        event = build_invalid_event(f"it-quar-t-{uuid4().hex}")
        await repository.append(event, reason="failed validation engine")

        records = await repository.get_records(event.event_id)
        assert len(records) == 1
        assert records[0].failures[0].rule == "quarantine"
        assert records[0].failures[0].code == "rejection_reason"
        assert records[0].failures[0].message == "failed validation engine"
    finally:
        await pool.close()
