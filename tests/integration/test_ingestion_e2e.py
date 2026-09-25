"""End-to-end ingestion integration test (Milestone 6).

Proves the full ingestion vertical slice against real infrastructure:

    DevExchangeConnector (deterministic raw events)
        -> DevEventPreparer (InternalEvent)
        -> KafkaPublisher (Kafka)
        -> KafkaConsumer (InternalEvent)
        -> PostgresBronzeRepository (PostgreSQL)
        -> read back and verify exact equality

Requires running Kafka and PostgreSQL (see docker-compose.yml). Runs only
when explicitly selected, matching the existing integration convention:

    pytest -m integration

Design for safe redelivery (at-least-once): the Bronze append is
idempotent per canonical event identity (``event_id``), so Kafka
redelivery of an already-preserved event never corrupts the landing zone.
This is delivery-level idempotency, NOT business duplicate detection:
distinct events sharing a trade_id are preserved as distinct rows.

Note on determinism: the canonical ``market-events`` topic is shared
across runs, so every raw event carries a per-run unique trade_id. Event
identity is canonical (source + trade_id + trade timestamp), so each run's
events are distinct from any backlog while remaining internally
deterministic and reproducible within the run.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from common.config import PostgresSettings
from common.contracts import EventType, InternalEvent, TradeSide
from messaging import KafkaConsumer, KafkaPublisher, KafkaSettings
from producer.connectors import ConnectionState
from producer.connectors.dev import DevExchangeConnector
from producer.pipeline import IngestionPipeline
from producer.preparation.dev import DevEventPreparer
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import PostgresBronzeRepository

pytestmark = pytest.mark.integration

# Over-precise values: 20+ decimal places exposes any float conversion.
PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")

# Fixed epoch-milliseconds base: 2026-08-26T10:10:00Z.
_BASE_MS = 1787739000000


def build_raw_events(run_tag: str) -> tuple[dict[str, Any], ...]:
    """Deterministic raw events for one E2E run.

    The second event deliberately shares trade_id ``trade-001-{run_tag}``
    with the first but differs in price and timestamp: distinct canonical
    identity, same business trade id. Both must survive Bronze.
    """
    return (
        {
            "type": "trade",
            "symbol": "BTC-USD",
            "price": str(PRECISE_PRICE),
            "quantity": str(PRECISE_QUANTITY),
            "side": "buy",
            "trade_id": f"trade-001-{run_tag}",
            "trade_time_ms": _BASE_MS,  # 2026-08-26T10:10:00Z
            "venue": "dev-exchange",
        },
        {
            "type": "trade",
            "symbol": "BTC-USD",
            "price": "64002.00",
            "quantity": "0.5",
            "side": "buy",
            "trade_id": f"trade-001-{run_tag}",  # same trade_id as event 1
            "trade_time_ms": _BASE_MS + 2000,  # 2026-08-26T10:10:02Z
            "venue": "dev-exchange",
        },
        {
            "type": "trade",
            "symbol": "ETH-USD",
            "price": "3141.59265358979323846",
            "quantity": "10.0",
            "side": "sell",
            "trade_id": f"trade-003-{run_tag}",
            "trade_time_ms": _BASE_MS + 3000,  # 2026-08-26T10:10:03Z
            "venue": "dev-exchange",
        },
    )


@pytest.mark.asyncio
async def test_ingestion_vertical_slice_end_to_end() -> None:
    """Connector -> Preparation -> Kafka -> Bronze -> read back == original."""
    kafka_settings = KafkaSettings()
    run_tag = uuid4().hex[:8]
    # Fixed clock so preparation is fully deterministic: re-preparing the
    # same raw events yields identical InternalEvents.
    fixed_now = datetime(2026, 8, 19, 10, 10, 5, tzinfo=UTC)
    preparer = DevEventPreparer(clock=lambda: fixed_now)

    # --- 1/2. Infrastructure is up; apply migrations. --------------------
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        # --- 3. Deterministic raw events via the development connector. ---
        connector = DevExchangeConnector(events=build_raw_events(run_tag))
        assert connector.source == "dev"

        # --- 4. Prepare raw dicts into InternalEvents. ---------------------
        expected: list[InternalEvent] = []
        await connector.connect()
        try:
            async for raw in connector.stream():
                expected.append(preparer.prepare(raw))
        finally:
            await connector.disconnect()
        assert connector.connection_state is ConnectionState.disconnected
        assert len(expected) == 3

        # Distinct canonical identity despite identical trade_id.
        assert expected[0].trade_id == expected[1].trade_id
        assert expected[0].event_id != expected[1].event_id

        # --- 5. Publish to Kafka through the messaging abstraction. -------
        publisher = KafkaPublisher(kafka_settings)
        await publisher.start()
        try:
            for event in expected:
                await publisher.publish(event)
        finally:
            await publisher.close()

        # --- 6. Consume and persist into Bronze (ack after persist). ------
        expected_ids = {event.event_id for event in expected}
        consumer = KafkaConsumer(kafka_settings, group_id=f"it-e2e-{run_tag}")
        await consumer.start()
        try:
            stream = consumer.stream_events()
            persisted: list[InternalEvent] = []
            try:
                async with asyncio.timeout(60):
                    async for consumed in stream:
                        # Topic backlog from earlier runs is skipped without
                        # acknowledgement (it belongs to other runs).
                        if consumed.event.event_id not in expected_ids:
                            continue
                        await repository.append(consumed.event)  # Bronze first
                        await consumer.acknowledge(consumed)  # then commit
                        persisted.append(consumed.event)
                        if len(persisted) == len(expected):
                            break
            finally:
                await stream.aclose()
        finally:
            await consumer.close()
        assert len(persisted) == 3

        # --- 7/8. Read back from Bronze and verify canonical equality. ----
        for original in expected:
            from_bronze = await repository.get(original.event_id)
            assert from_bronze is not None, f"missing {original.event_id} in Bronze"
            assert from_bronze == original

        # --- Field-by-field verification on the first event. ---------------
        verified = await repository.get(expected[0].event_id)
        assert verified is not None
        assert verified.event_id == expected[0].event_id
        assert verified.symbol == "BTC-USD"
        assert verified.price == PRECISE_PRICE
        assert verified.quantity == PRECISE_QUANTITY
        assert verified.side is TradeSide.buy
        assert verified.trade_id == f"trade-001-{run_tag}"
        assert verified.event_type is EventType.trade
        assert verified.source == "dev"
        assert verified.received_at == fixed_now
        assert verified.trade_timestamp == datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)
        assert verified.payload["venue"] == "dev-exchange"
        assert verified.payload["price"] == str(PRECISE_PRICE)

        # Decimal precision is exact, not float-approximated.
        assert verified.price.as_tuple() == PRECISE_PRICE.as_tuple()
        assert isinstance(verified.price, Decimal)
        assert isinstance(verified.quantity, Decimal)

        # Timestamps remain timezone-aware.
        assert verified.received_at.tzinfo is not None
        assert verified.trade_timestamp.tzinfo is not None

        # --- The same-trade_id pair both survives in Bronze. ---------------
        first = await repository.get(expected[0].event_id)
        second = await repository.get(expected[1].event_id)
        assert first is not None and second is not None
        assert first.trade_id == second.trade_id
        assert first.event_id != second.event_id
        assert first.price != second.price
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_ingestion_pipeline_publishes_prepared_events() -> None:
    """The IngestionPipeline wires connector -> preparer -> publisher."""
    kafka_settings = KafkaSettings()
    run_tag = uuid4().hex[:8]
    fixed_now = datetime(2026, 8, 19, 10, 10, 5, tzinfo=UTC)
    preparer = DevEventPreparer(clock=lambda: fixed_now)

    # Expected events, prepared independently for later verification.
    expected = [preparer.prepare(raw) for raw in build_raw_events(run_tag)]

    publisher = KafkaPublisher(kafka_settings)
    await publisher.start()
    try:
        connector = DevExchangeConnector(events=build_raw_events(run_tag))
        pipeline = IngestionPipeline(
            connector=connector,
            preparer=preparer,
            publisher=publisher,
        )
        published = await pipeline.run()
        assert published == len(expected)
        assert connector.connection_state is ConnectionState.disconnected
    finally:
        await publisher.close()

    # Consume the pipeline's events and persist them: the flow that the
    # production entry points run.
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        expected_ids = {event.event_id for event in expected}
        consumer = KafkaConsumer(kafka_settings, group_id=f"it-pipe-{run_tag}")
        await consumer.start()
        try:
            stream = consumer.stream_events()
            persisted = 0
            try:
                async with asyncio.timeout(60):
                    async for consumed in stream:
                        if consumed.event.event_id not in expected_ids:
                            continue
                        await repository.append(consumed.event)
                        await consumer.acknowledge(consumed)
                        persisted += 1
                        if persisted == len(expected):
                            break
            finally:
                await stream.aclose()
        finally:
            await consumer.close()
        assert persisted == len(expected)

        for original in expected:
            from_bronze = await repository.get(original.event_id)
            assert from_bronze == original
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_redelivered_event_is_persisted_idempotently() -> None:
    """Kafka redelivery of a preserved event never corrupts Bronze."""
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        preparer = DevEventPreparer()
        event = preparer.prepare(build_raw_events(uuid4().hex[:8])[0])

        # First delivery: append.
        await repository.append(event)
        # Redelivery of the same event (e.g. offset not committed before a
        # restart): the idempotent append leaves the original row intact.
        await repository.append(event)

        fetched = await repository.get(event.event_id)
        assert fetched == event
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_bronze_write_failure_does_not_falsely_acknowledge() -> None:
    """DB failure propagates and the message is not marked as processed.

    Uses the real KafkaConsumer and BronzeIngestionService against the real
    broker: the Bronze write fails (repository backed by a failing pool),
    the StorageError surfaces, and acknowledge() is never reached. A retry
    with a healthy repository then proves the message is redelivered
    (at-least-once), not lost and not falsely committed.
    """
    from common.exceptions import StorageError
    from processing.bronze_ingestion import BronzeIngestionService

    kafka_settings = KafkaSettings()
    run_tag = uuid4().hex[:8]

    # Publish one deterministic event from the ingestion path.
    preparer = DevEventPreparer()
    raw_events = build_raw_events(run_tag)
    publisher = KafkaPublisher(kafka_settings)
    await publisher.start()
    try:
        connector = DevExchangeConnector(events=raw_events)
        pipeline = IngestionPipeline(
            connector=connector,
            preparer=preparer,
            publisher=publisher,
        )
        assert await pipeline.run() == len(raw_events)
    finally:
        await publisher.close()

    class _ExplodingPool:
        """Stands in for the asyncpg pool; every acquire fails."""

        def acquire(self) -> Any:
            raise StorageError("simulated bronze outage", detail="integration-test failure")

    failing_repository = PostgresBronzeRepository(_ExplodingPool())

    consumer = KafkaConsumer(kafka_settings, group_id=f"it-redeliver-{run_tag}")
    service = BronzeIngestionService(consumer=consumer, repository=failing_repository)
    await service.start()
    try:
        # The first consumed message (topic backlog or this run's event)
        # fails the Bronze write; the service must raise instead of
        # acknowledging the offset.
        with pytest.raises(StorageError, match="simulated bronze outage"):
            async with asyncio.timeout(60):
                await service.ingest(max_events=1)
    finally:
        await service.close()

    # Retry semantics: with a healthy repository and the SAME consumer
    # group, the unacknowledged message is redelivered and can be
    # persisted successfully. This proves the failed message was NOT
    # falsely acknowledged as processed.
    pool = DatabasePool(PostgresSettings())
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()
        repository = PostgresBronzeRepository(pool.pool)

        retry_consumer = KafkaConsumer(
            kafka_settings,
            group_id=f"it-redeliver-{run_tag}",  # same group: resumes at the
            # last committed offset; nothing was committed, so earliest
            # reset applies for this fresh group.
        )
        retry_service = BronzeIngestionService(consumer=retry_consumer, repository=repository)
        await retry_service.start()
        try:
            stream = retry_consumer.stream_events()
            redelivered: InternalEvent | None = None
            try:
                async with asyncio.timeout(60):
                    async for consumed in stream:
                        await repository.append(consumed.event)
                        await retry_consumer.acknowledge(consumed)
                        redelivered = consumed.event
                        break
            finally:
                await stream.aclose()

            assert redelivered is not None
            from_bronze = await repository.get(redelivered.event_id)
            assert from_bronze == redelivered
        finally:
            await retry_service.close()
    finally:
        await pool.close()
