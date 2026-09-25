"""Processing module entry point.

Runs the Bronze ingestion service, the consumer side of the ingestion
vertical slice:

    Kafka -> KafkaConsumer -> InternalEvent -> PostgresBronzeRepository

Usage:
    python -m processing.main
"""

from __future__ import annotations

import asyncio
import sys

from common.config import load_settings
from common.utils import configure_logging, get_logger
from messaging import KafkaConsumer, KafkaSettings
from processing.bronze_ingestion import BronzeIngestionService
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import PostgresBronzeRepository

logger = get_logger(__name__)

# Bounded batch per run: the vertical slice processes a fixed window of
# events and exits cleanly. Continuous consumption arrives with the
# Validation Engine milestone.
_BATCH_SIZE = 100


async def run_bronze_ingestion() -> int:
    """Consume InternalEvents from Kafka and persist them into Bronze.

    Applies pending migrations, starts the database pool and Kafka
    consumer, processes one bounded batch, then releases all resources in
    ``finally`` so shutdown is clean on success and failure alike.

    Returns:
        The number of events persisted into Bronze.
    """
    configure_logging()
    app_settings = load_settings()

    pool = DatabasePool(app_settings.postgres)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()

        consumer = KafkaConsumer(KafkaSettings())
        repository = PostgresBronzeRepository(pool.pool)
        service = BronzeIngestionService(consumer=consumer, repository=repository)

        await service.start()
        try:
            persisted = await service.ingest(max_events=_BATCH_SIZE)
        finally:
            await service.close()
    finally:
        await pool.close()

    logger.info("bronze_ingestion_complete", persisted=persisted)
    return persisted


def main() -> None:
    """Start the Processing module (Bronze ingestion)."""
    try:
        persisted = asyncio.run(run_bronze_ingestion())
    except Exception as exc:  # pragma: no cover - defensive entry-point guard
        logger.error("bronze_ingestion_run_failed", error=str(exc))
        sys.exit(1)
    logger.info("processing_exited", persisted=persisted)


if __name__ == "__main__":
    main()
