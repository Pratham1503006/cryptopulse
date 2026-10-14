"""PostgreSQL implementation of the Gold repository.

Persists canonical ``MarketWindow`` aggregates to the Gold table and
reconstructs them exactly. This is the concrete Warehouse implementation of
the ``GoldRepository`` abstraction defined in ``warehouse/repositories/base.py``.

Responsibilities (deliberately narrow):

- persist market-window aggregates (Gold = business information derived from
  trusted Silver events),
- read aggregates back in deterministic order,
- fetch a single window, the latest window per symbol, and the symbol list.

This repository does NOT:

- aggregate events (business rules live in the processing engine),
- read Bronze or Quarantine (Gold is derived only from Silver),
- validate events (Validation Engine, in processing/),
- know anything about Kafka, the producer, or Spark.

Writing is an idempotent upsert keyed by ``window_id``: recomputing a window
(same input) or recomputing after late arrivals replaces the row in place,
so replay and recovery never duplicate or corrupt Gold.

Transactions are deliberate: each operation commits on success and rolls
back on failure. Raw asyncpg exceptions are wrapped in the platform
exception hierarchy so callers never depend on driver types.
"""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg

from common.contracts import MarketWindow
from common.exceptions import StorageError
from common.utils import get_logger
from warehouse.repositories.base import GoldRepository
from warehouse.schemas.gold import (
    GOLD_COLUMNS,
    market_window_to_record,
    record_to_market_window,
)

logger = get_logger(__name__)

__all__ = ["PostgresGoldRepository"]

# Every column except the primary key is refreshed on conflict so a
# recomputed window always reflects the latest trusted data.
_UPDATABLE_COLUMNS = tuple(column for column in GOLD_COLUMNS if column != "window_id")

_INSERT_SQL = (
    "INSERT INTO gold.market_windows ({columns}) VALUES ({placeholders}) "
    "ON CONFLICT (window_id) DO UPDATE SET {updates}".format(
        columns=", ".join(GOLD_COLUMNS),
        placeholders=", ".join(f"${index}" for index in range(1, len(GOLD_COLUMNS) + 1)),
        updates=", ".join(f"{column} = EXCLUDED.{column}" for column in _UPDATABLE_COLUMNS),
    )
)

_SELECT_BY_WINDOW_ID_SQL = "SELECT {columns} FROM gold.market_windows WHERE window_id = $1".format(
    columns=", ".join(GOLD_COLUMNS),
)

_SELECT_ALL_SQL = (
    "SELECT {columns} FROM gold.market_windows "
    "ORDER BY window_start DESC, symbol OFFSET $1 LIMIT $2".format(
        columns=", ".join(GOLD_COLUMNS),
    )
)

_SELECT_BY_SYMBOL_SQL = (
    "SELECT {columns} FROM gold.market_windows WHERE symbol = $1 "
    "ORDER BY window_start DESC OFFSET $2 LIMIT $3".format(
        columns=", ".join(GOLD_COLUMNS),
    )
)

# DISTINCT ON is PostgreSQL's idiomatic "latest row per group": one row per
# symbol, the one with the greatest window_start.
_SELECT_LATEST_BY_SYMBOL_SQL = (
    "SELECT {columns} FROM ("
    "  SELECT DISTINCT ON (symbol) {columns} FROM gold.market_windows "
    "  ORDER BY symbol, window_start DESC"
    ") AS latest ORDER BY window_start DESC LIMIT $1".format(
        columns=", ".join(GOLD_COLUMNS),
    )
)

_SELECT_SYMBOLS_SQL = "SELECT DISTINCT symbol FROM gold.market_windows ORDER BY symbol"


class PostgresGoldRepository(GoldRepository):
    """Gold repository backed by PostgreSQL (asyncpg).

    Args:
        pool: An asyncpg connection pool. The repository never creates or
            caches connections of its own; pool lifecycle belongs to the
            composition root (see ``warehouse.database.DatabasePool``).
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def upsert(self, window: MarketWindow) -> None:
        """Persist (insert or replace) one MarketWindow aggregate.

        Idempotent per ``window_id``: a recomputed window updates the same
        row. Safe under replay, restart, and late-arrival recomputation.

        Args:
            window: The aggregate to persist.

        Raises:
            StorageError: If the aggregate cannot be persisted.
        """
        record = market_window_to_record(window)
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(_INSERT_SQL, *record.values())
        except asyncpg.PostgresError as exc:
            logger.error(
                "gold_upsert_failed",
                window_id=window.window_id,
                error=str(exc),
            )
            raise StorageError(
                "failed to persist MarketWindow to Gold",
                detail=str(exc),
            ) from exc
        logger.debug("gold_window_upserted", window_id=window.window_id)

    async def read(
        self,
        symbol: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Sequence[MarketWindow]:
        """Read market windows in deterministic order (newest first).

        Args:
            symbol: Optional symbol filter.
            offset: Number of rows to skip (>= 0).
            limit: Maximum number of rows to return (>= 1).

        Returns:
            Reconstructed MarketWindows ordered by window_start descending.

        Raises:
            ValueError: If offset or limit are invalid.
            StorageError: If rows cannot be read or reconstructed.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        try:
            async with self._pool.acquire() as connection:
                if symbol is None:
                    rows = await connection.fetch(_SELECT_ALL_SQL, offset, limit)
                else:
                    rows = await connection.fetch(_SELECT_BY_SYMBOL_SQL, symbol, offset, limit)
        except asyncpg.PostgresError as exc:
            logger.error("gold_read_failed", symbol=symbol, error=str(exc))
            raise StorageError(
                "failed to read MarketWindows from Gold",
                detail=str(exc),
            ) from exc
        return _map_rows(rows, context="read")

    async def get(self, window_id: str) -> MarketWindow | None:
        """Fetch one window by its stable technical identity."""
        try:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(_SELECT_BY_WINDOW_ID_SQL, window_id)
        except asyncpg.PostgresError as exc:
            logger.error("gold_get_failed", window_id=window_id, error=str(exc))
            raise StorageError(
                "failed to read MarketWindow from Gold",
                detail=str(exc),
            ) from exc
        if row is None:
            return None
        return _map_rows([row], context=f"get:{window_id}")[0]

    async def latest_by_symbol(self, limit: int = 100) -> Sequence[MarketWindow]:
        """Return the most recent window per symbol (one row per symbol)."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        try:
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(_SELECT_LATEST_BY_SYMBOL_SQL, limit)
        except asyncpg.PostgresError as exc:
            logger.error("gold_latest_by_symbol_failed", error=str(exc))
            raise StorageError(
                "failed to read latest MarketWindows from Gold",
                detail=str(exc),
            ) from exc
        return _map_rows(rows, context="latest_by_symbol")

    async def list_symbols(self) -> Sequence[str]:
        """Return the distinct symbols present in Gold."""
        try:
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(_SELECT_SYMBOLS_SQL)
        except asyncpg.PostgresError as exc:
            logger.error("gold_list_symbols_failed", error=str(exc))
            raise StorageError(
                "failed to list Gold symbols",
                detail=str(exc),
            ) from exc
        return [row["symbol"] for row in rows]


def _map_rows(rows: Sequence[asyncpg.Record], context: str) -> list[MarketWindow]:
    """Reconstruct MarketWindows from Gold rows.

    Mapping failures surface as StorageError with row context so callers see
    a single, predictable exception type at the repository boundary.
    """
    windows: list[MarketWindow] = []
    for row in rows:
        try:
            windows.append(record_to_market_window(row))
        except StorageError as exc:
            logger.error("gold_row_mapping_failed", context=context, detail=exc.detail)
            raise
    return windows
