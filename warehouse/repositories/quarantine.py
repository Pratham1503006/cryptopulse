"""PostgreSQL implementation of the Quarantine repository.

Persists rejected InternalEvents to the Quarantine table and reconstructs
them exactly, together with the structured validation failures that caused
the rejection. This is the concrete Warehouse implementation of the
``QuarantineRepository`` abstraction defined in
``warehouse/repositories/base.py``.

Responsibilities (deliberately narrow):

- preserve rejected events with their structured failure context,
- read rejected events in deterministic order,
- fetch quarantine records for one technical event identity.

This repository does NOT:

- validate events (Validation Engine, in processing/),
- delete or modify Bronze rows (rejected events stay in Bronze),
- detect business duplicates (not defined by the architecture yet),
- know anything about Kafka, the producer, or the Validation Engine.

Identity semantics: each processing attempt that rejects an event appends a
DISTINCT quarantine record (primary key ``(event_id, quarantined_at)``), so
repeated processing attempts remain visible for investigation. This is
deliberately different from Bronze/Silver, whose technical identity is
idempotent per event_id: for quarantine, visibility of every rejection
attempt is the point.

Transactions are deliberate: each operation commits on success and rolls
back on failure. Raw asyncpg exceptions are wrapped in the platform
exception hierarchy so callers never depend on driver types.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import asyncpg

from common.contracts import InternalEvent, ValidationFailure
from common.exceptions import StorageError
from common.utils import get_logger
from warehouse.repositories.base import QuarantineRepository
from warehouse.schemas.quarantine import (
    QUARANTINE_COLUMNS,
    QuarantineRecord,
    quarantine_record_to_record,
    record_to_quarantine_record,
)

logger = get_logger(__name__)

__all__ = ["PostgresQuarantineRepository"]

_INSERT_SQL = "INSERT INTO quarantine.events ({columns}) VALUES ({placeholders})".format(
    columns=", ".join(QUARANTINE_COLUMNS),
    placeholders=", ".join(f"${index}" for index in range(1, len(QUARANTINE_COLUMNS) + 1)),
)

_READ_REJECTED_SQL = (
    "SELECT {columns} FROM quarantine.events "
    "ORDER BY quarantined_at, event_id OFFSET $1 LIMIT $2".format(
        columns=", ".join(QUARANTINE_COLUMNS),
    )
)

_SELECT_RECORDS_SQL = (
    "SELECT {columns} FROM quarantine.events WHERE event_id = $1 "
    "ORDER BY quarantined_at LIMIT $2".format(
        columns=", ".join(QUARANTINE_COLUMNS),
    )
)


class PostgresQuarantineRepository(QuarantineRepository):
    """Quarantine repository backed by PostgreSQL (asyncpg).

    Args:
        pool: An asyncpg connection pool. The repository never creates or
            caches connections of its own; pool lifecycle belongs to the
            composition root (see ``warehouse.database.DatabasePool``).
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def append(self, event: InternalEvent, reason: str) -> None:
        """Preserve one rejected InternalEvent with its failure context.

        The abstract ``reason: str`` is carried as a structured
        ValidationFailure (rule=``quarantine``, code=``rejection_reason``)
        merged ahead of any detailed failures supplied via
        :meth:`append_record`, preserving the ABC's text-reason contract
        while keeping rejection context structured.

        Args:
            event: The rejected InternalEvent.
            reason: Short human-readable rejection reason.

        Raises:
            StorageError: If the event cannot be persisted.
        """
        await self.append_record(
            event=event,
            quarantined_at=datetime.now(UTC),
            failures=[
                ValidationFailure(
                    rule="quarantine",
                    code="rejection_reason",
                    message=reason,
                )
            ],
        )

    async def append_record(
        self,
        event: InternalEvent,
        quarantined_at: datetime,
        failures: list[ValidationFailure],
    ) -> None:
        """Preserve one rejected InternalEvent with structured failures.

        Args:
            event: The rejected InternalEvent.
            quarantined_at: Rejection timestamp (timezone-aware).
            failures: Structured validation failures explaining the
                rejection; preserved verbatim as JSONB.

        Raises:
            StorageError: If the event cannot be persisted.
        """
        record = QuarantineRecord(
            event=event,
            quarantined_at=quarantined_at,
            failures=failures,
        )
        row = quarantine_record_to_record(record)
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        _INSERT_SQL,
                        *row.values(),
                    )
        except asyncpg.PostgresError as exc:
            logger.error(
                "quarantine_append_failed",
                event_id=event.event_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to persist rejected InternalEvent to Quarantine",
                detail=str(exc),
            ) from exc
        logger.debug(
            "quarantine_event_appended",
            event_id=event.event_id,
            failures=len(failures),
        )

    async def read_rejected(self, offset: int = 0, limit: int = 100) -> Sequence[InternalEvent]:
        """Read rejected events back in deterministic order.

        Args:
            offset: Number of rows to skip (>= 0).
            limit: Maximum number of rows to return (>= 1).

        Returns:
            Reconstructed InternalEvents ordered by quarantine time then
            event id.

        Raises:
            ValueError: If offset or limit are negative.
            StorageError: If rows cannot be read or reconstructed.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        try:
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(_READ_REJECTED_SQL, offset, limit)
        except asyncpg.PostgresError as exc:
            logger.error("quarantine_read_rejected_failed", error=str(exc))
            raise StorageError(
                "failed to read rejected events from Quarantine",
                detail=str(exc),
            ) from exc
        events: list[InternalEvent] = []
        for row in rows:
            try:
                events.append(record_to_quarantine_record(row).event)
            except StorageError as exc:
                logger.error("quarantine_row_mapping_failed", detail=exc.detail)
                raise
        return events

    async def get_records(
        self,
        event_id: str,
        limit: int = 100,
    ) -> Sequence[QuarantineRecord]:
        """Fetch quarantine records for one technical event identity.

        Args:
            event_id: The event's canonical identifier.
            limit: Maximum number of records to return (>= 1).

        Returns:
            QuarantineRecords ordered by quarantine time; multiple records
            exist when the event was rejected by multiple processing
            attempts.

        Raises:
            StorageError: If rows cannot be read or reconstructed.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        try:
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(_SELECT_RECORDS_SQL, event_id, limit)
        except asyncpg.PostgresError as exc:
            logger.error(
                "quarantine_get_records_failed",
                event_id=event_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to read quarantine records",
                detail=str(exc),
            ) from exc
        records: list[QuarantineRecord] = []
        for row in rows:
            try:
                records.append(record_to_quarantine_record(row))
            except StorageError as exc:
                logger.error("quarantine_row_mapping_failed", detail=exc.detail)
                raise
        return records
