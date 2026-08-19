"""InternalEvent wire-format codec.

Defines how the canonical InternalEvent contract is serialized to bytes
for the event-streaming backbone and deserialized back into InternalEvent.

Wire format: JSON (UTF-8). Serialization is deterministic: the same event
always produces the same bytes for a given schema version.

Typed fields round-trip exactly:
- Decimal (price, quantity) serialized as exact decimal strings
- StrEnum values (event_type, side) serialized as their string values
- datetime fields serialized as ISO-8601 with offset

The ``payload`` dict is preserved as JSON-compatible data. Values inside
it that are not natively JSON (e.g. Decimal, datetime, enum) are carried
in their JSON representation (string / ISO-8601 / value) and are NOT
reconstituted on deserialization; arbitrary non-JSON objects are rejected.

The codec is the only component that knows how InternalEvent is
represented on the wire. It contains no validation and no business logic.
"""

from __future__ import annotations

from common.contracts import InternalEvent
from messaging.exceptions import SerializationError

__all__ = ["InternalEventCodec", "SerializationError"]


class InternalEventCodec:
    """Encodes InternalEvent to deterministic JSON bytes and decodes them back."""

    def encode(self, event: InternalEvent) -> bytes:
        """Serialize an InternalEvent into a deterministic byte payload.

        Args:
            event: The InternalEvent to serialize.

        Returns:
            UTF-8 encoded JSON bytes.

        Raises:
            SerializationError: If the event cannot be represented on the wire.
        """
        try:
            return event.model_dump_json().encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise SerializationError(
                "failed to encode InternalEvent",
                detail=str(exc),
            ) from exc

    def decode(self, payload: bytes) -> InternalEvent:
        """Deserialize a byte payload back into an InternalEvent.

        Args:
            payload: UTF-8 encoded JSON bytes produced by :meth:`encode`.

        Returns:
            The reconstructed InternalEvent.

        Raises:
            SerializationError: If the payload cannot be decoded.
        """
        try:
            return InternalEvent.model_validate_json(payload)
        except (TypeError, ValueError) as exc:
            raise SerializationError(
                "failed to decode InternalEvent payload",
                detail=str(exc),
            ) from exc
