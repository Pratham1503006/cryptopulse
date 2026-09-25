"""End-to-end trust boundary integration test (Milestone 7).

Proves the complete flow against real PostgreSQL:

    Bronze (seeded with deterministic InternalEvents)
        -> ValidationEngine
        ├── valid   -> TrustedEvent -> Silver
        └── invalid -> Quarantine (+ structured failures)

and the architectural invariants:

- Bronze remains intact after both paths,
- exact Decimal values / timezone-aware timestamps survive Silver,
- failure information survives Quarantine,
- same trade_id does NOT cause rejection,
- technical event_id stays distinct from business duplicate semantics,
- reprocessing the same technical event behaves deterministically.

Events are constructed by the tests themselves; existing database contents
are never used as fixtures. Runs only when explicitly selected:

    pytest -m integration
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from common.config import PostgresSettings
from common.contracts import EventType, InternalEvent, TradeSide, TrustedEvent
from processing.validation import TrustRoutingService, ValidationEngine
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import (
    PostgresBronzeRepository,
    PostgresQuarantineRepository,
    PostgresSilverRepository,
)

pytestmark = pytest.mark.integration

PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")
TRADE_TS = datetime(2026, 9, 26, 12, 30, 0, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 9, 26, 12, 30, 45, tzinfo=UTC)


def build_event(event_id: str, **overrides: Any) -> InternalEvent:
    defaults: dict[str, Any] = {
        "event_id": event_id,
        "source": "dev",
        "event_type": EventType.trade,
        "received_at": RECEIVED_AT,
        "symbol": "BTC-USD",
        "price": PRECISE_PRICE,
        "quantity": PRECISE_QUANTITY,
        "side": TradeSide.buy,
        "trade_id": "trade-e2e",
        "trade_timestamp": TRADE_TS,
        "payload": {"venue": "dev-exchange", "sequence": 7},
    }
    defaults.update(overrides)
    return InternalEvent(**defaults)


@pytest.mark.asyncio
async def test_bronze_validation_silver_quarantine_end_to_end() -> None:
    tag = uuid4().hex[:8]
    valid = build_event(f"it-tb-valid-{tag}")
    same_trade_different_event = build_event(
        f"it-tb-valid2-{tag}",
        price=Decimal("64100.00"),
        trade_timestamp=datetime(2026, 9, 26, 12, 31, 0, tzinfo=UTC),
    )
    invalid = build_event(
        f"it-tb-invalid-{tag}",
        price=Decimal("0"),
        quantity=Decimal("-1"),
    )

    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        bronze = PostgresBronzeRepository(pool.pool)
        silver = PostgresSilverRepository(pool.pool)
        quarantine = PostgresQuarantineRepository(pool.pool)

        # Seed Bronze with the deterministic events (received, unvalidated).
        for event in (valid, same_trade_different_event, invalid):
            await bronze.append(event)

        bronze_before = await bronze.replay(offset=0, limit=1000)

        # Run the trust boundary over the whole batch.
        service = TrustRoutingService(
            bronze=bronze,
            silver=silver,
            quarantine=quarantine,
            validator=ValidationEngine(),
        )
        result = await service.process_batch(limit=1000)

        # Routing: two valid, one invalid (foreign events from other tests
        # may also be routed; scope assertions to this run's events).
        routed_ids = {event.event_id for event in result.validated + result.quarantined}
        assert {valid.event_id, same_trade_different_event.event_id, invalid.event_id} <= routed_ids
        assert valid in result.validated
        assert same_trade_different_event in result.validated
        assert invalid in result.quarantined

        # --- Silver path: valid InternalEvent became the canonical
        # TrustedEvent with only the contract-defined transformation. ----
        trusted = await silver.get(valid.event_id)
        assert trusted is not None
        assert isinstance(trusted, TrustedEvent)
        assert trusted.price == PRECISE_PRICE
        assert trusted.price.as_tuple() == PRECISE_PRICE.as_tuple()  # exact Decimal
        assert trusted.quantity.as_tuple() == PRECISE_QUANTITY.as_tuple()
        assert trusted.trade_timestamp == TRADE_TS
        assert trusted.received_at == RECEIVED_AT
        assert trusted.trade_timestamp.tzinfo is not None  # timezone-aware
        assert trusted.received_at.tzinfo is not None
        assert trusted.validated_at.tzinfo is not None
        assert trusted.payload == valid.payload  # payload survives
        assert trusted.trade_id == "trade-e2e"

        # Same trade_id pair: both events are trusted, NOT treated as
        # duplicates.
        trusted_second = await silver.get(same_trade_different_event.event_id)
        assert trusted_second is not None
        assert trusted_second.trade_id == trusted.trade_id
        assert trusted_second.event_id != trusted.event_id
        assert trusted_second.price != trusted.price

        # --- Quarantine path: rejected event preserved with structured
        # failures. -------------------------------------------------------
        records = await quarantine.get_records(invalid.event_id)
        assert len(records) == 1
        record = records[0]
        assert record.event == invalid  # full original event preserved
        failure_rules = {failure.rule for failure in record.failures}
        assert failure_rules == {"price_validity", "quantity_validity"}
        assert record.quarantined_at.tzinfo is not None

        # --- Bronze remains intact after both paths. ----------------------
        bronze_after = await bronze.replay(offset=0, limit=1000)
        assert bronze_after == bronze_before
        from_bronze = await bronze.get(invalid.event_id)
        assert from_bronze == invalid  # rejected events stay in Bronze
        assert await bronze.get(valid.event_id) == valid

        # --- Reprocessing the same technical event is deterministic. ------
        result_again = await service.process_batch(limit=1000)
        assert invalid in result_again.quarantined
        records_after = await quarantine.get_records(invalid.event_id)
        assert len(records_after) == 2  # each attempt visible in quarantine
        assert all(r.event == invalid for r in records_after)
        assert await silver.get(valid.event_id) == trusted  # unchanged row
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_technical_identity_is_distinct_from_business_duplicates() -> None:
    """event_id drives identity; shared trade_id never causes rejection."""
    tag = uuid4().hex[:8]
    # Two events with identical business content but distinct technical
    # identity: both are valid and both are preserved in Silver.
    first = build_event(f"it-tb-id1-{tag}")
    second = build_event(f"it-tb-id2-{tag}")

    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        silver = PostgresSilverRepository(pool.pool)
        service = TrustRoutingService(
            bronze=PostgresBronzeRepository(pool.pool),
            silver=silver,
            quarantine=PostgresQuarantineRepository(pool.pool),
            validator=ValidationEngine(),
        )

        outcome1 = await service.process_event(first)
        outcome2 = await service.process_event(second)

        assert outcome1.valid and outcome2.valid
        got_first = await silver.get(first.event_id)
        got_second = await silver.get(second.event_id)
        assert got_first is not None and got_second is not None
        assert got_first.trade_id == got_second.trade_id
        assert got_first.event_id != got_second.event_id
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_validation_failure_does_not_crash_processing() -> None:
    """Invalid events are outcomes, not exceptions: the batch continues."""
    tag = uuid4().hex[:8]
    events = [
        build_event(f"it-tb-c1-{tag}"),
        build_event(f"it-tb-c2-{tag}", symbol="  "),  # invalid
        build_event(f"it-tb-c3-{tag}", quantity=Decimal("0")),  # invalid
        build_event(f"it-tb-c4-{tag}"),
    ]

    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        service = TrustRoutingService(
            bronze=PostgresBronzeRepository(pool.pool),
            silver=PostgresSilverRepository(pool.pool),
            quarantine=PostgresQuarantineRepository(pool.pool),
            validator=ValidationEngine(),
        )

        for event in events:
            await service.process_event(event)

        # No exception raised: all four events were routed by outcome.
        for valid_id in (events[0].event_id, events[3].event_id):
            assert await PostgresSilverRepository(pool.pool).get(valid_id) is not None
        for invalid in (events[1], events[2]):
            records = await PostgresQuarantineRepository(pool.pool).get_records(invalid.event_id)
            assert len(records) == 1
    finally:
        await pool.close()
