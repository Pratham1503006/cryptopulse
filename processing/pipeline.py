"""End-to-end processing pipeline.

Composes the three processing stages into one bounded run so the consumer
side of the platform can be driven by a single entry point:

    Kafka -> Bronze          (BronzeIngestionService)
    Bronze -> Silver/Quarantine (TrustRoutingService + ValidationEngine)
    Silver -> Gold           (GoldProcessingService)

Responsibilities:

- sequence the stages and report a single result,
- own nothing the stages already own: no decoding, validation rules,
  aggregation, or SQL live here.

Deliberate boundaries:

- It reuses the existing service boundaries unchanged; it is a composition
  root, not a fourth implementation of their logic.
- It runs a BOUNDED pass (``batch_size`` events per stage) rather than a
  continuous loop. Continuous consumption with durable watermarks is future
  work; the stages already process deterministically, so repeated bounded
  passes are safe (Bronze append, Silver append and Gold upsert are all
  idempotent per technical identity).

Failure semantics:

- Any stage failure (``StorageError``, ``ConsumeError``, ...) propagates and
  stops the run. Failures are never swallowed or misclassified.
- A batch that ingests zero events still runs the trust and gold stages,
  because Bronze/Silver may already hold unprocessed data from an earlier
  run (for example after a restart).
"""

from __future__ import annotations

from dataclasses import dataclass

from common.utils import get_logger
from processing.bronze_ingestion import BronzeIngestionService
from processing.gold.service import GoldProcessingService
from processing.validation.service import TrustRoutingService

logger = get_logger(__name__)

__all__ = ["ProcessingPipeline", "PipelineResult"]


@dataclass(frozen=True)
class PipelineResult:
    """Summary of one bounded pipeline pass.

    Attributes:
        ingested: Events persisted from Kafka into Bronze.
        validated: Bronze events routed to Silver as trusted.
        quarantined: Bronze events routed to Quarantine as rejected.
        gold_windows: Gold market windows written from Silver.
    """

    ingested: int
    validated: int
    quarantined: int
    gold_windows: int


class ProcessingPipeline:
    """Runs the Bronze -> Silver -> Gold processing stages once.

    Args:
        ingestion: Bronze ingestion stage (Kafka -> Bronze).
        trust: Trust routing stage (Bronze -> Silver/Quarantine).
        gold: Gold processing stage (Silver -> Gold).
        batch_size: Maximum events processed per stage in one pass (>= 1).

    Raises:
        ValueError: If ``batch_size`` < 1.
    """

    def __init__(
        self,
        ingestion: BronzeIngestionService,
        trust: TrustRoutingService,
        gold: GoldProcessingService,
        *,
        batch_size: int = 100,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._ingestion = ingestion
        self._trust = trust
        self._gold = gold
        self._batch_size = batch_size

    async def start(self) -> None:
        """Establish the broker connection owned by the ingestion stage."""
        await self._ingestion.start()

    async def close(self) -> None:
        """Release the broker connection (idempotent)."""
        await self._ingestion.close()

    async def run_once(self) -> PipelineResult:
        """Run one bounded pass through all three stages.

        Returns:
            A :class:`PipelineResult` describing what each stage produced.

        Raises:
            StorageError: If any warehouse operation fails.
            ConsumeError: If the broker stream or offset commit fails.
        """
        ingested = await self._ingestion.ingest(max_events=self._batch_size)
        trust_result = await self._trust.process_batch(limit=self._batch_size)
        gold_windows = await self._gold.process_batch(limit=self._batch_size)

        result = PipelineResult(
            ingested=ingested,
            validated=len(trust_result.validated),
            quarantined=len(trust_result.quarantined),
            gold_windows=gold_windows,
        )
        logger.info(
            "processing_pipeline_complete",
            ingested=result.ingested,
            validated=result.validated,
            quarantined=result.quarantined,
            gold_windows=result.gold_windows,
        )
        return result
