"""PostgreSQL implementation of the Bronze repository.

Persists canonical ``InternalEvent`` instances to the Bronze table and
reconstructs them exactly. This is the concrete Warehouse implementation of
the ``BronzeRepository`` abstraction defined in
``warehouse/repositories/base.py``.

Responsibilities (deliberately narrow):

- persist received InternalEvents (Bronze = successfully received events
  before validation),
- replay preserved events in deterministic order,
- fetch a single preserved event by its technical row identity.

This repository does NOT:

- validate events (Validation Engine, later milestone),
- detect business duplicates (Validation Engine, later milestone),
- own Silver/Gold/Quarantine behavior,
- know anything about Kafka, the producer, or processing.

Transactions are deliberate: each operation commits on success and rolls
back on failure. Raw asyncpg exceptions are wrapped in the platform
exception hierarchy so callers never depend on driver types.
"""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg

from common.contracts import InternalEvent
from common.exceptions import StorageError
from common.utils import get_logger
from warehouse.repositories.base import BronzeRepository
from warehouse.schemas.bronze import (
    BRONZE_COLUMNS,
    internal_event_to_record,
    record_to_internal_event,
)

logger = get_logger(__name__)

__all__ = ["PostgresBronzeRepository"]

_INSERT_SQL = (
    "INSERT INTO bronze.events ({columns}) VALUES ({placeholders}) "
    "ON CONFLICT (event_id) DO NOTHING".format(
        columns=", ".join(BRONZE_COLUMNS),
        placeholders=", ".join(f"${index}" for index in range(1, len(BRONZE_COLUMNS) + 1)),
    )
)

_SELECT_BY_EVENT_ID_SQL = "SELECT {columns} FROM bronze.events WHERE event_id = $1".format(
    columns=", ".join(BRONZE_COLUMNS),
)

_REPLAY_SQL = (
    "SELECT {columns} FROM bronze.events ORDER BY received_at, event_id OFFSET $1 LIMIT $2".format(
        columns=", ".join(BRONZE_COLUMNS),
    )
)


class PostgresBronzeRepository(BronzeRepository):
    """Bronze repository backed by PostgreSQL (asyncpg).

    Args:
        pool: An asyncpg connection pool. The repository never creates or
            caches connections of its own; pool lifecycle belongs to the
            composition root (see ``warehouse.database.DatabasePool``).
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def append(self, event: InternalEvent) -> None:
        """Persist one InternalEvent to the Bronze table.

        Idempotent per ``event_id`` (technical row identity): re-appending
        the same event leaves the original row untouched, so redelivery of
        an already-preserved event never corrupts the landing zone. This is
        delivery-level idempotency, NOT business duplicate detection;
        distinct events sharing a ``trade_id`` are preserved as distinct
        rows (duplicate detection belongs to the Validation Engine).

        Args:
            event: The InternalEvent to preserve.

        Raises:
            StorageError: If the event cannot be persisted.
        """
        record = internal_event_to_record(event)
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        _INSERT_SQL,
                        *record.values(),
                    )
        except asyncpg.PostgresError as exc:
            logger.error(
                "bronze_append_failed",
                event_id=event.event_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to persist InternalEvent to Bronze",
                detail=str(exc),
            ) from exc
        logger.debug("bronze_event_appended", event_id=event.event_id)

    async def replay(self, offset: int = 0, limit: int = 100) -> Sequence[InternalEvent]:
        """Read preserved events back in deterministic order.

        Args:
            offset: Number of rows to skip (>= 0).
            limit: Maximum number of rows to return (>= 1).

        Returns:
            Reconstructed InternalEvents ordered by received time then
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
                rows = await connection.fetch(_REPLAY_SQL, offset, limit)
        except asyncpg.PostgresError as exc:
            logger.error("bronze_replay_failed", error=str(exc))
            raise StorageError(
                "failed to read events from Bronze",
                detail=str(exc),
            ) from exc
        return _map_rows(rows, context="replay")

    async def get(self, event_id: str) -> InternalEvent | None:
        """Fetch one preserved event by its technical row identity.

        Args:
            event_id: The event's canonical identifier (primary key).

        Returns:
            The reconstructed InternalEvent, or None if no row exists.

        Raises:
            StorageError: If the row cannot be read or reconstructed.
        """
        try:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(_SELECT_BY_EVENT_ID_SQL, event_id)
        except asyncpg.PostgresError as exc:
            logger.error(
                "bronze_get_failed",
                event_id=event_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to read event from Bronze",
                detail=str(exc),
            ) from exc
        if row is None:
            return None
        return _map_rows([row], context=f"get:{event_id}")[0]


def _map_rows(rows: Sequence[asyncpg.Record], context: str) -> list[InternalEvent]:
    """Reconstruct InternalEvents from Bronze rows.

    Mapping failures surface as StorageError with row context so callers
    see a single, predictable exception type at the repository boundary.
    """
    events: list[InternalEvent] = []
    for row in rows:
        try:
            events.append(record_to_internal_event(row))
        except StorageError as exc:
            logger.error("bronze_row_mapping_failed", context=context, detail=exc.detail)
            raise
    return events
