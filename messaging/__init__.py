"""messaging module - event-streaming backbone.

Implements the transport/buffering layer between Producer and Processing.
Kafka is used purely as transport: it carries standardised InternalEvent
payloads and buffers them for downstream consumption.

This module deliberately contains NO:
- validation logic
- business transformations
- warehouse awareness
- knowledge of Bronze/Silver/Gold layers

It depends only on common/ (for the canonical contracts and exceptions).
Both producer/ and processing/ depend on this module.
"""

from messaging.codec import InternalEventCodec as InternalEventCodec
from messaging.codec import SerializationError as SerializationError
from messaging.config import KafkaSettings as KafkaSettings
from messaging.consumer import ConsumedEvent as ConsumedEvent
from messaging.consumer import ConsumeError as ConsumeError
from messaging.consumer import Consumer as Consumer
from messaging.consumer import KafkaConsumer as KafkaConsumer
from messaging.exceptions import MessagingError as MessagingError
from messaging.publisher import KafkaPublisher as KafkaPublisher
from messaging.publisher import Publisher as Publisher
from messaging.publisher import PublishError as PublishError

__all__ = [
    # Configuration
    "KafkaSettings",
    # Codec
    "InternalEventCodec",
    "SerializationError",
    # Publisher
    "Publisher",
    "KafkaPublisher",
    "PublishError",
    # Consumer
    "Consumer",
    "KafkaConsumer",
    "ConsumedEvent",
    "ConsumeError",
    # Exceptions
    "MessagingError",
]
