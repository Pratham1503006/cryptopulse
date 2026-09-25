"""Producer ingestion pipeline.

The smallest orchestration that turns the Exchange Connector boundary into
published InternalEvents:

    ExchangeConnector (raw dicts)
        -> EventPreparer (InternalEvent)
        -> Publisher (Kafka transport)

Responsibilities:
- connect to the connector and receive raw events,
- hand each raw event to preparation,
- publish successfully prepared InternalEvents,
- stop cleanly (connector disconnected) on normal completion or shutdown.

Deliberate boundaries:

- The producer depends only on common/ and messaging/ abstractions
  (``ExchangeConnector``, ``EventPreparer``, ``Publisher``); it never
  imports warehouse/ — Kafka-to-Bronze wiring belongs to the consuming
  application side.
- It performs no validation and no storage.
- A raw event that cannot be prepared is logged with its error and NOT
  published; the failure is visible but does not halt ingestion.
- A publish failure (``PublishError``) is fatal to the run: the pipeline
  stops rather than silently dropping events. The connector is
  disconnected in ``finally`` so resources are always released.
"""

from __future__ import annotations

from common.utils import get_logger
from messaging.publisher import Publisher
from producer.connectors.base import ExchangeConnector
from producer.preparation.base import EventPreparer, PreparationError

logger = get_logger(__name__)

__all__ = ["IngestionPipeline"]


class IngestionPipeline:
    """Streams raw events from a connector, prepares and publishes them.

    Args:
        connector: The ingestion source boundary (raw events in).
        preparer: The translation boundary (raw dict -> InternalEvent).
        publisher: The transport boundary (InternalEvent -> broker).
    """

    def __init__(
        self,
        connector: ExchangeConnector,
        preparer: EventPreparer,
        publisher: Publisher,
    ) -> None:
        self._connector = connector
        self._preparer = preparer
        self._publisher = publisher

    async def run(self) -> int:
        """Run one ingestion session to completion.

        Connects to the connector, streams raw events, prepares and
        publishes each one, then always disconnects the connector.

        Returns:
            The number of events successfully published.

        Raises:
            PublishError: If publishing an event fails. The run stops so
                the failure is visible rather than silently dropping data.
        """
        published = 0
        await self._connector.connect()
        try:
            async for raw_event in self._connector.stream():
                if not self._preparer.can_prepare(raw_event):
                    logger.warning(
                        "ingestion_event_not_recognised",
                        source=self._preparer.source,
                        keys=sorted(raw_event),
                    )
                    continue
                try:
                    event = self._preparer.prepare(raw_event)
                except PreparationError as exc:
                    logger.warning(
                        "ingestion_preparation_failed",
                        source=self._preparer.source,
                        error=exc.args[0] if exc.args else str(exc),
                        detail=exc.detail,
                    )
                    continue
                await self._publisher.publish(event)
                published += 1
                logger.debug(
                    "ingestion_event_published",
                    event_id=event.event_id,
                    symbol=event.symbol,
                )
        finally:
            await self._connector.disconnect()
        logger.info(
            "ingestion_run_completed",
            published=published,
            source=self._connector.source,
        )
        return published
