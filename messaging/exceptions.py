"""Messaging-specific exceptions.

Transport-level errors raised by the messaging module. These wrap
low-level client failures so application modules never depend on the
Kafka client's exception types.
"""

from __future__ import annotations

from common.exceptions import CryptoPulseError

__all__ = ["MessagingError", "SerializationError", "PublishError", "ConsumeError"]


class MessagingError(CryptoPulseError):
    """Base exception for all messaging errors."""


class SerializationError(MessagingError):
    """Raised when an InternalEvent cannot be serialized or deserialized."""


class PublishError(MessagingError):
    """Raised when publishing an InternalEvent to the broker fails."""


class ConsumeError(MessagingError):
    """Raised when consuming from the broker fails."""
