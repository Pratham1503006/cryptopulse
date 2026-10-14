"""Processing module entry point.

Runs the consumer side of the platform end to end in one bounded pass:

    Kafka -> Bronze -> Validation -> Silver/Quarantine -> Gold

Concretely:

    KafkaConsumer -> BronzeIngestionService   (Kafka -> Bronze)
                  -> TrustRoutingService       (Bronze -> Silver/Quarantine)
                  -> GoldProcessingService     (Silver -> Gold)

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
from processing.config import GoldSettings
from processing.gold.service import GoldProcessingService
from processing.pipeline import PipelineResult, ProcessingPipeline
from processing.validation import TrustRoutingService, ValidationEngine
from warehouse.database import DatabasePool
from warehouse.migrations import MigrationRunner
from warehouse.repositories import (
    PostgresBronzeRepository,
    PostgresGoldRepository,
    PostgresQuarantineRepository,
    PostgresSilverRepository,
)

logger = get_logger(__name__)

# Bounded batch per run: the run processes a fixed window of events and exits
# cleanly. Continuous consumption (with durable watermarks) is future work.
_BATCH_SIZE = 100


async def run_processing() -> PipelineResult:
    """Consume, validate, route, and aggregate one bounded batch of events.

    Applies pending migrations, starts the database pool and Kafka consumer,
    runs one bounded pipeline pass, then releases all resources in ``finally``
    so shutdown is clean on success and failure alike.

    Returns:
        The :class:`PipelineResult` for the run.
    """
    configure_logging()
    app_settings = load_settings()

    pool = DatabasePool(app_settings.postgres)
    await pool.start()
    try:
        await MigrationRunner(pool.pool).apply_migrations()

        bronze = PostgresBronzeRepository(pool.pool)
        silver = PostgresSilverRepository(pool.pool)

        ingestion = BronzeIngestionService(
            consumer=KafkaConsumer(KafkaSettings()),
            repository=bronze,
        )
        trust = TrustRoutingService(
            bronze=bronze,
            silver=silver,
            quarantine=PostgresQuarantineRepository(pool.pool),
            validator=ValidationEngine(),
        )
        gold = GoldProcessingService(
            silver=silver,
            gold=PostgresGoldRepository(pool.pool),
            settings=GoldSettings(),
        )

        pipeline = ProcessingPipeline(
            ingestion=ingestion,
            trust=trust,
            gold=gold,
            batch_size=_BATCH_SIZE,
        )
        await pipeline.start()
        try:
            result = await pipeline.run_once()
        finally:
            await pipeline.close()
    finally:
        await pool.close()

    logger.info(
        "processing_run_complete",
        ingested=result.ingested,
        validated=result.validated,
        quarantined=result.quarantined,
        gold_windows=result.gold_windows,
    )
    return result


def main() -> None:
    """Start the Processing module (full Kafka -> Bronze -> Silver -> Gold run)."""
    try:
        result = asyncio.run(run_processing())
    except Exception as exc:  # pragma: no cover - defensive entry-point guard
        logger.error("processing_run_failed", error=str(exc))
        sys.exit(1)
    logger.info("processing_exited", ingested=result.ingested)


if __name__ == "__main__":
    main()
