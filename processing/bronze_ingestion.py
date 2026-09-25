"""Bronze ingestion service.

The smallest consumer-side application: it connects the event-streaming
backbone to Bronze persistence.

    Kafka -> InternalEvent -> Bronze

This is the application boundary that links ``messaging/`` and
``warehouse/``. It deliberately performs NO validation (the Validation
Engine belongs to processing/validation) and no business duplicate
detection.

Offset / acknowledgement semantics (at-least-once):

    Kafka message
        -> decode
        -> Bronze append (database transaction commits)
        -> message considered successfully processed
        -> Kafka offset acknowledged

A message is acknowledged ONLY after the Bronze append has committed. If
the database write fails, the error propagates to the caller and the
offset is never acknowledged, so the message is redelivered on restart.
Kafka redelivery of an already-preserved event is safe because the Bronze
append is idempotent per canonical event identity (``event_id``): the
original row is left untouched. This is delivery-level idempotency, NOT
business duplicate detection.
"""

from __future__ import annotations

from common.exceptions import StorageError
from common.utils import get_logger
from messaging import ConsumeError
from messaging.consumer import Consumer
from warehouse.repositories.base import BronzeRepository

logger = get_logger(__name__)

__all__ = ["BronzeIngestionService"]


class BronzeIngestionService:
    """Persists InternalEvents from the broker into Bronze.

    Depends only on the messaging ``Consumer`` and warehouse
    ``BronzeRepository`` abstractions.

    Args:
        consumer: The messaging consumer abstraction.
        repository: The Bronze persistence abstraction.

    Example:
        await service.start()
        try:
            preserved = await service.ingest(max_events=10)
        finally:
            await service.close()
    """

    def __init__(self, consumer: Consumer, repository: BronzeRepository) -> None:
        self._consumer = consumer
        self._repository = repository

    async def start(self) -> None:
        """Establish the broker connection."""
        await self._consumer.start()

    async def close(self) -> None:
        """Release the broker connection (idempotent)."""
        await self._consumer.close()

    async def ingest(self, max_events: int) -> int:
        """Persist up to ``max_events`` events into Bronze.

        Each message is acknowledged only after its Bronze append commits.
        Failures propagate:

        - ``StorageError``: the Bronze write failed; the offset is not
          acknowledged, so the message will be redelivered (safe retry).
        - ``ConsumeError``: offset commit failed after a successful Bronze
          write; the event is persisted but may be redelivered
          (at-least-once, never lost).

        Args:
            max_events: Maximum number of events to process in this call
                (>= 1). The call returns as soon as the count is reached.

        Returns:
            The number of events successfully persisted and acknowledged.
        """
        if max_events < 1:
            raise ValueError("max_events must be >= 1")
        persisted = 0
        stream = self._consumer.stream_events()
        try:
            async for consumed in stream:
                event = consumed.event
                try:
                    await self._repository.append(event)
                except StorageError as exc:
                    # Bronze write failed: deliberately NOT acknowledging.
                    logger.error(
                        "bronze_ingest_append_failed",
                        event_id=event.event_id,
                        offset=consumed.offset,
                        error=exc.message,
                        detail=exc.detail,
                    )
                    raise
                try:
                    await self._consumer.acknowledge(consumed)
                except ConsumeError as exc:
                    # Event IS persisted but the offset commit failed: the
                    # message may be redelivered. Report the failure.
                    logger.error(
                        "bronze_ingest_ack_failed",
                        event_id=event.event_id,
                        offset=consumed.offset,
                        error=exc.message,
                        detail=exc.detail,
                    )
                    raise
                persisted += 1
                logger.debug(
                    "bronze_event_persisted",
                    event_id=event.event_id,
                    offset=consumed.offset,
                )
                if persisted >= max_events:
                    return persisted
        finally:
            await stream.aclose()
        return persisted
