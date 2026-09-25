"""PostgreSQL implementation of the Silver repository.

Persists canonical ``TrustedEvent`` instances to the Silver table and
reconstructs them exactly. This is the concrete Warehouse implementation of
the ``SilverRepository`` abstraction defined in
``warehouse/repositories/base.py``.

Responsibilities (deliberately narrow):

- persist trusted events (Silver = events that passed validation),
- read trusted events in deterministic order,
- fetch a single trusted event by its technical row identity.

This repository does NOT:

- validate events (Validation Engine, in processing/),
- detect business duplicates (not defined by the architecture yet),
- own Bronze/Quarantine/Gold behavior,
- know anything about Kafka, the producer, or the Validation Engine.

Transactions are deliberate: each operation commits on success and rolls
back on failure. Raw asyncpg exceptions are wrapped in the platform
exception hierarchy so callers never depend on driver types.
"""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg

from common.contracts import TrustedEvent
from common.exceptions import StorageError
from common.utils import get_logger
from warehouse.repositories.base import SilverRepository
from warehouse.schemas.silver import (
    SILVER_COLUMNS,
    record_to_trusted_event,
    trusted_event_to_record,
)

logger = get_logger(__name__)

__all__ = ["PostgresSilverRepository"]

_INSERT_SQL = (
    "INSERT INTO silver.events ({columns}) VALUES ({placeholders}) "
    "ON CONFLICT (event_id) DO NOTHING".format(
        columns=", ".join(SILVER_COLUMNS),
        placeholders=", ".join(f"${index}" for index in range(1, len(SILVER_COLUMNS) + 1)),
    )
)

_SELECT_BY_EVENT_ID_SQL = "SELECT {columns} FROM silver.events WHERE event_id = $1".format(
    columns=", ".join(SILVER_COLUMNS),
)

_READ_SQL = (
    "SELECT {columns} FROM silver.events ORDER BY validated_at, event_id OFFSET $1 LIMIT $2".format(
        columns=", ".join(SILVER_COLUMNS),
    )
)


class PostgresSilverRepository(SilverRepository):
    """Silver repository backed by PostgreSQL (asyncpg).

    Args:
        pool: An asyncpg connection pool. The repository never creates or
            caches connections of its own; pool lifecycle belongs to the
            composition root (see ``warehouse.database.DatabasePool``).
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def append(self, event: TrustedEvent) -> None:
        """Persist one TrustedEvent to the Silver table.

        Idempotent per ``event_id`` (technical row identity): re-appending
        the same canonical event leaves the original row untouched, so a
        repeated processing attempt never corrupts the trusted layer. This
        is delivery-level idempotency, NOT business duplicate detection;
        distinct events sharing a ``trade_id`` are preserved as distinct
        rows.

        Args:
            event: The TrustedEvent to persist.

        Raises:
            StorageError: If the event cannot be persisted.
        """
        record = trusted_event_to_record(event)
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        _INSERT_SQL,
                        *record.values(),
                    )
        except asyncpg.PostgresError as exc:
            logger.error(
                "silver_append_failed",
                event_id=event.event_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to persist TrustedEvent to Silver",
                detail=str(exc),
            ) from exc
        logger.debug("silver_event_appended", event_id=event.event_id)

    async def read(self, offset: int = 0, limit: int = 100) -> Sequence[TrustedEvent]:
        """Read trusted events back in deterministic order.

        Args:
            offset: Number of rows to skip (>= 0).
            limit: Maximum number of rows to return (>= 1).

        Returns:
            Reconstructed TrustedEvents ordered by validation time then
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
                rows = await connection.fetch(_READ_SQL, offset, limit)
        except asyncpg.PostgresError as exc:
            logger.error("silver_read_failed", error=str(exc))
            raise StorageError(
                "failed to read events from Silver",
                detail=str(exc),
            ) from exc
        return _map_rows(rows, context="read")

    async def get(self, event_id: str) -> TrustedEvent | None:
        """Fetch one trusted event by its technical row identity.

        Args:
            event_id: The event's canonical identifier (primary key).

        Returns:
            The reconstructed TrustedEvent, or None if no row exists.

        Raises:
            StorageError: If the row cannot be read or reconstructed.
        """
        try:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(_SELECT_BY_EVENT_ID_SQL, event_id)
        except asyncpg.PostgresError as exc:
            logger.error(
                "silver_get_failed",
                event_id=event_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to read event from Silver",
                detail=str(exc),
            ) from exc
        if row is None:
            return None
        return _map_rows([row], context=f"get:{event_id}")[0]


def _map_rows(rows: Sequence[asyncpg.Record], context: str) -> list[TrustedEvent]:
    """Reconstruct TrustedEvents from Silver rows.

    Mapping failures surface as StorageError with row context so callers
    see a single, predictable exception type at the repository boundary.
    """
    events: list[TrustedEvent] = []
    for row in rows:
        try:
            events.append(record_to_trusted_event(row))
        except StorageError as exc:
            logger.error("silver_row_mapping_failed", context=context, detail=exc.detail)
            raise
    return events
