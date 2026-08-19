"""Kafka publisher abstraction.

Defines the boundary for publishing InternalEvent instances onto the
event-streaming backbone. This is the only Kafka surface the Producer
module needs: publishing is serialization plus transport, with no
validation and no business logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from aiokafka import AIOKafkaProducer

from common.contracts import InternalEvent
from common.utils import get_logger
from messaging.codec import InternalEventCodec
from messaging.config import KafkaSettings
from messaging.exceptions import PublishError

logger = get_logger(__name__)

__all__ = ["Publisher", "KafkaPublisher", "PublishError"]


class Publisher(ABC):
    """Boundary for publishing InternalEvent instances to the broker."""

    @abstractmethod
    async def start(self) -> None:
        """Establish the underlying transport connection."""

    @abstractmethod
    async def publish(self, event: InternalEvent) -> None:
        """Publish a single InternalEvent to the broker.

        Serialization happens here. The caller does not need to know the
        wire format or any Kafka client details.

        Args:
            event: The InternalEvent to publish.

        Raises:
            PublishError: If the event cannot be delivered.
        """

    @abstractmethod
    async def close(self) -> None:
        """Cleanly shut down the publisher and release resources."""


class KafkaPublisher(Publisher):
    """Publishes InternalEvent instances to a Kafka topic.

    Wraps the aiokafka producer so callers depend only on InternalEvent
    and the Publisher interface.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        codec: InternalEventCodec | None = None,
    ) -> None:
        self._settings = settings
        self._codec = codec if codec is not None else InternalEventCodec()
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            retry_backoff_ms=self._settings.producer_retry_backoff_ms,
        )
        await producer.start()
        self._producer = producer
        logger.info(
            "kafka_publisher_started",
            topic=self._settings.market_events_topic,
            bootstrap_servers=self._settings.bootstrap_servers,
        )

    async def publish(self, event: InternalEvent) -> None:
        if self._producer is None:
            raise PublishError("publisher has not been started")
        payload = self._codec.encode(event)
        try:
            await self._producer.send_and_wait(
                self._settings.market_events_topic,
                payload,
            )
        except Exception as exc:
            raise PublishError(
                "failed to publish InternalEvent",
                detail=str(exc),
            ) from exc
        logger.debug(
            "kafka_event_published",
            topic=self._settings.market_events_topic,
            event_id=event.event_id,
        )

    async def close(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None
        logger.info("kafka_publisher_stopped")
