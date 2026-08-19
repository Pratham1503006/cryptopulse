"""Kafka consumer abstraction.

Defines the minimal surface the future Processing module needs to consume
InternalEvent instances from the event-streaming backbone.

Consumer behavior is intentionally minimal: it streams decoded
InternalEvent objects. Offset management (checkpointing, rebalancing,
manual commits) is the responsibility of the consuming application and is
not implemented here.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator

from aiokafka import AIOKafkaConsumer

from common.contracts import InternalEvent
from common.utils import get_logger
from messaging.codec import InternalEventCodec
from messaging.config import KafkaSettings
from messaging.exceptions import ConsumeError, SerializationError

logger = get_logger(__name__)

__all__ = ["Consumer", "KafkaConsumer", "ConsumeError"]


class Consumer(ABC):
    """Boundary for consuming InternalEvent instances from the broker."""

    @abstractmethod
    async def start(self) -> None:
        """Establish the underlying transport connection."""

    @abstractmethod
    def stream(self) -> AsyncGenerator[InternalEvent, None]:
        """Yield decoded InternalEvent instances as they arrive."""

    @abstractmethod
    async def close(self) -> None:
        """Cleanly shut down the consumer and release resources."""


class KafkaConsumer(Consumer):
    """Consumes InternalEvent instances from a Kafka topic.

    Wraps the aiokafka consumer so callers depend only on InternalEvent
    and the Consumer interface. Messages that cannot be deserialized are
    logged and skipped so a single bad message does not halt the stream.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        codec: InternalEventCodec | None = None,
        *,
        group_id: str | None = None,
    ) -> None:
        self._settings = settings
        self._codec = codec if codec is not None else InternalEventCodec()
        self._group_id = group_id if group_id is not None else settings.consumer_group
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if self._consumer is not None:
            return
        consumer = AIOKafkaConsumer(
            self._settings.market_events_topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset=self._settings.auto_offset_reset,
            enable_auto_commit=self._settings.enable_auto_commit,
            session_timeout_ms=self._settings.session_timeout_ms,
        )
        await consumer.start()
        self._consumer = consumer
        logger.info(
            "kafka_consumer_started",
            topic=self._settings.market_events_topic,
            group_id=self._group_id,
            bootstrap_servers=self._settings.bootstrap_servers,
        )

    async def stream(self) -> AsyncGenerator[InternalEvent, None]:
        if self._consumer is None:
            raise ConsumeError("consumer has not been started")
        async for message in self._consumer:
            if not isinstance(message.value, bytes):
                logger.warning(
                    "kafka_message_unreadable",
                    reason="unexpected payload type",
                    offset=message.offset,
                    value_type=type(message.value).__name__,
                )
                continue
            try:
                yield self._codec.decode(message.value)
            except SerializationError as exc:
                logger.warning(
                    "kafka_message_unreadable",
                    reason="deserialization failed",
                    offset=message.offset,
                    detail=exc.message,
                )

    async def close(self) -> None:
        if self._consumer is None:
            return
        await self._consumer.stop()
        self._consumer = None
        logger.info("kafka_consumer_stopped", group_id=self._group_id)
