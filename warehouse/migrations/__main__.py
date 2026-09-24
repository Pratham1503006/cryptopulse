"""Run warehouse migrations from the command line.

Usage:
    python -m warehouse.migrations
"""

from __future__ import annotations

import asyncio
import sys

from common.config import PostgresSettings
from common.utils import configure_logging, get_logger
from warehouse.database import DatabasePool
from warehouse.exceptions import WarehouseError
from warehouse.migrations.runner import MigrationRunner

logger = get_logger(__name__)


def main() -> int:
    """Apply all pending warehouse migrations and report the outcome."""
    configure_logging()
    settings = PostgresSettings()
    return asyncio.run(_run(settings))


async def _run(settings: PostgresSettings) -> int:
    pool = DatabasePool(settings)
    try:
        await pool.start()
    except Exception as exc:
        logger.error(
            "migration_runner_pool_failed",
            host=settings.host,
            port=settings.port,
            database=settings.database,
            error=str(exc),
        )
        return 1

    try:
        applied = await MigrationRunner(pool.pool).apply_migrations()
    except WarehouseError as exc:
        logger.error(
            "migration_runner_failed",
            error=exc.message,
            detail=exc.detail,
        )
        return 1
    finally:
        await pool.close()

    logger.info("migration_runner_complete", applied=applied)
    return 0


if __name__ == "__main__":
    sys.exit(main())
