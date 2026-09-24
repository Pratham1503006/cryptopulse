"""PostgreSQL connection pool management.

Provides a small asyncpg pool wrapper with explicit lifecycle control
(``start`` / ``close``) so database connectivity is always injected into
repositories rather than created as hidden global state.

The pool itself is created by the caller (composition root) and handed to
repositories via constructor injection. This module deliberately knows
nothing about Bronze, Silver, Gold, or Quarantine.
"""

from __future__ import annotations

import json
from typing import Any

from asyncpg import Pool, create_pool

from common.config import PostgresSettings
from common.utils import get_logger
from warehouse.exceptions import PoolNotStartedError

logger = get_logger(__name__)

__all__ = ["DatabasePool"]


def _init_connection(connection: Any) -> Any:
    """Configure driver-level codecs on each new connection.

    asyncpg has no default codec for ``jsonb``: without this registration
    it exchanges JSONB columns as raw strings, forcing every caller to
    serialize by hand. Registering ``json.dumps``/``json.loads`` here means
    the rest of the warehouse works with plain ``dict`` payloads and the
    driver detail never leaks past this module.
    """
    return connection.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


class DatabasePool:
    """Owns the asyncpg connection pool lifecycle.

    Example:
        pool = DatabasePool(settings)
        await pool.start()
        try:
            repository = PostgresBronzeRepository(pool)
            ...
        finally:
            await pool.close()
    """

    def __init__(self, settings: PostgresSettings) -> None:
        """Create a pool holder from PostgreSQL settings.

        Args:
            settings: Environment-driven PostgreSQL configuration.
        """
        self._settings = settings
        self._pool: Pool | None = None

    @property
    def settings(self) -> PostgresSettings:
        """The PostgreSQL configuration this pool was built from."""
        return self._settings

    @property
    def pool(self) -> Pool:
        """The underlying asyncpg pool.

        Raises:
            PoolNotStartedError: If :meth:`start` has not completed.
        """
        if self._pool is None:
            raise PoolNotStartedError("database pool has not been started")
        return self._pool

    async def start(self) -> None:
        """Establish the connection pool (idempotent)."""
        if self._pool is not None:
            return
        self._pool = await create_pool(
            host=self._settings.host,
            port=self._settings.port,
            database=self._settings.database,
            user=self._settings.username,
            password=self._settings.password,
            min_size=self._settings.min_connections,
            max_size=self._settings.max_connections,
            init=_init_connection,
        )
        logger.info(
            "database_pool_started",
            host=self._settings.host,
            port=self._settings.port,
            database=self._settings.database,
        )

    async def close(self) -> None:
        """Close the connection pool and release resources (idempotent)."""
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None
        logger.info("database_pool_stopped")
