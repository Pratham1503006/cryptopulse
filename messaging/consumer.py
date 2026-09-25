"""Kafka consumer abstraction.

Defines the minimal surface applications need to consume InternalEvent
instances from the event-streaming backbone.

Two consumption surfaces are provided:

- :class:`Consumer` / :meth:`KafkaConsumer.stream` yield decoded
  InternalEvent objects with no offset management at all. Offset
  management (checkpointing, rebalancing, manual commits) is the
  responsibility of the consuming application.

- :meth:`KafkaConsumer.stream_events` yields
  :class:`ConsumedEvent` envelopes, pairing each decoded InternalEvent
  with the broker metadata needed for explicit offset control, plus an
  :meth:`ConsumedEvent.acknowledge` coroutine. Applications must call
  ``acknowledge()`` only after the event has been durably processed
  (e.g. persisted); a message that fails processing is simply never
  acknowledged and will be redelivered on restart (at-least-once
  semantics).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata

from common.contracts import InternalEvent
from common.utils import get_logger
from messaging.codec import InternalEventCodec
from messaging.config import KafkaSettings
from messaging.exceptions import ConsumeError, SerializationError

logger = get_logger(__name__)

__all__ = ["Consumer", "ConsumedEvent", "KafkaConsumer", "ConsumeError"]


@dataclass(frozen=True)
class ConsumedEvent:
    """A decoded InternalEvent plus its broker position.

    ``acknowledge()`` commits the event's offset (offset + 1) so the
    message is never delivered to this consumer group again. Applications
    must only call it after the event has been durably processed; a
    message that fails processing is simply never acknowledged and will
    be redelivered (at-least-once semantics).
    """

    event: InternalEvent
    topic: str
    partition: int
    offset: int

    def _offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Build the aiokafka commit map for this message's position."""
        return {
            TopicPartition(self.topic, self.partition): OffsetAndMetadata(
                self.offset + 1,
                "",
            ),
        }


class Consumer(ABC):
    """Boundary for consuming InternalEvent instances from the broker."""

    @abstractmethod
    async def start(self) -> None:
        """Establish the underlying transport connection."""

    @abstractmethod
    def stream(self) -> AsyncGenerator[InternalEvent, None]:
        """Yield decoded InternalEvent instances as they arrive."""

    def stream_events(self) -> AsyncGenerator[ConsumedEvent, None]:
        """Yield ConsumedEvent envelopes with explicit offset control.

        Optional capability with a default no-offset-control
        implementation so existing/alternative implementations remain
        backward-compatible. Applications that need ack-after-persist
        semantics use this surface; the base :meth:`stream` remains the
        minimal fire-and-forget consumption surface.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support stream_events()")

    async def acknowledge(self, consumed: ConsumedEvent) -> None:
        """Commit the offset of a successfully processed message.

        Optional capability with a default failing implementation so
        existing implementations remain backward-compatible. Only called
        by applications after the message has been durably processed;
        failure means the offset stays uncommitted and the message is
        redelivered (at-least-once semantics).

        Raises:
            ConsumeError: If offset commit fails or is unsupported.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support acknowledge()")

    @abstractmethod
    async def close(self) -> None:
        """Cleanly shut down the consumer and release resources."""


class KafkaConsumer(Consumer):
    """Consumes InternalEvent instances from a Kafka topic.

    Wraps the aiokafka consumer so callers depend only on InternalEvent
    and the Consumer interface. Messages that cannot be deserialized are
    logged and skipped so a single bad message does not halt the stream.

    Offset management is manual (``enable_auto_commit`` is taken from
    :class:`KafkaSettings`, which defaults to False): offsets are only
    committed when the application explicitly acknowledges a message.
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
        """Yield decoded InternalEvents; offsets are not touched."""
        if self._consumer is None:
            raise ConsumeError("consumer has not been started")
        async for message in self._consumer:
            event = self._decode_message(message)
            if event is not None:
                yield event

    async def stream_events(self) -> AsyncGenerator[ConsumedEvent, None]:
        """Yield ConsumedEvent envelopes with explicit offset control.

        Undecodable messages are logged and skipped: their offsets are
        NOT advanced, so a group restart reads them again and the failure
        remains visible in the logs rather than silently committed.
        """
        if self._consumer is None:
            raise ConsumeError("consumer has not been started")
        async for message in self._consumer:
            event = self._decode_message(message)
            if event is None:
                continue
            yield ConsumedEvent(
                event=event,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )

    async def acknowledge(self, consumed: ConsumedEvent) -> None:
        """Commit the offset of a successfully processed message.

        Raises:
            ConsumeError: If the consumer has not been started or the
                broker rejects the commit. A failed commit leaves the
                offset uncommitted, so the message is redelivered on
                restart (at-least-once semantics).
        """
        if self._consumer is None:
            raise ConsumeError("consumer has not been started")
        offsets = consumed._offsets()
        try:
            await self._consumer.commit(offsets)
        except Exception as exc:
            raise ConsumeError(
                "failed to commit Kafka offsets",
                detail=str(exc),
            ) from exc
        logger.debug(
            "kafka_offset_committed",
            topic=consumed.topic,
            partition=consumed.partition,
            offset=consumed.offset,
            event_id=consumed.event.event_id,
        )

    async def close(self) -> None:
        if self._consumer is None:
            return
        await self._consumer.stop()
        self._consumer = None
        logger.info("kafka_consumer_stopped", group_id=self._group_id)

    def _decode_message(self, message: ConsumerRecord[Any, Any]) -> InternalEvent | None:
        """Decode one broker message, returning None for unreadable payloads."""
        if not isinstance(message.value, bytes):
            logger.warning(
                "kafka_message_unreadable",
                reason="unexpected payload type",
                offset=message.offset,
                value_type=type(message.value).__name__,
            )
            return None
        try:
            return self._codec.decode(message.value)
        except SerializationError as exc:
            logger.warning(
                "kafka_message_unreadable",
                reason="deserialization failed",
                offset=message.offset,
                detail=exc.message,
            )
            return None
