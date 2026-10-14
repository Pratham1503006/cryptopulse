"""Coinbase event preparation.

Translates the raw match payloads produced by
:class:`producer.connectors.coinbase.CoinbaseExchangeConnector` into canonical
InternalEvent instances.

Responsibility is strictly:

    raw Coinbase payload -> InternalEvent

It performs only mechanical translation:

- field mapping (``product_id`` -> symbol, ``size`` -> quantity),
- type conversion (``Decimal`` for price/quantity, ISO-8601 -> aware UTC),
- ``TradeSide`` / ``EventType`` conversion,
- payload preservation (the raw dict is preserved verbatim).

It deliberately does NOT decide trust or apply quality rules; those belong to
the Validation Engine.

Coinbase ``matches`` payloads look like::

    {
      "type": "match",
      "trade_id": 10,
      "product_id": "BTC-USD",
      "price": "100.00",
      "size": "0.001",
      "side": "buy",
      "time": "2026-08-26T10:10:00.000000Z"
    }

``trade_id`` is an integer on Coinbase; it is stored as its canonical string
form so the canonical identity matches the rest of the platform.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from common.contracts import EventType, InternalEvent, TradeSide
from common.utils import get_logger
from producer.preparation.base import EventPreparer, PreparationError

logger = get_logger(__name__)

__all__ = ["CoinbaseEventPreparer"]

# Message types that carry a trade on the Coinbase matches channel.
_MATCH_TYPES = frozenset({"match", "last_match"})


def _require(raw_event: dict[str, Any], key: str) -> Any:
    """Fetch a required field, raising PreparationError when absent or null."""
    value = raw_event.get(key)
    if value is None:
        raise PreparationError(
            f"raw event is missing required field '{key}'",
            raw_event=raw_event,
        )
    return value


def _to_decimal(raw_event: dict[str, Any], key: str) -> Decimal:
    """Convert a raw numeric field to Decimal exactly (no float detour)."""
    value = _require(raw_event, key)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise PreparationError(
            f"field '{key}' is not convertible to Decimal",
            raw_event=raw_event,
            detail=str(exc),
        ) from exc


def _to_utc_datetime(raw_event: dict[str, Any], key: str) -> datetime:
    """Parse an ISO-8601 field into a timezone-aware UTC datetime."""
    value = _require(raw_event, key)
    if not isinstance(value, str):
        raise PreparationError(
            f"field '{key}' is not an ISO-8601 string",
            raw_event=raw_event,
            detail=repr(value),
        )
    text = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PreparationError(
            f"field '{key}' is not a valid ISO-8601 timestamp",
            raw_event=raw_event,
            detail=str(exc),
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PreparationError(
            f"field '{key}' must be timezone-aware",
            raw_event=raw_event,
            detail=repr(value),
        )
    return parsed.astimezone(UTC)


class CoinbaseEventPreparer(EventPreparer):
    """Translates Coinbase match payloads into canonical InternalEvents.

    Args:
        clock: Optional ``received_at`` provider. Defaults to the current UTC
            time, which is the semantically correct receive timestamp; tests
            may inject a fixed clock to make preparation deterministic.

    ``event_id`` is synthesised from the deterministic Coinbase trade
    identity (``coinbase:{trade_id}``). Coinbase ``trade_id`` is globally
    unique, so a redelivered raw match maps to the same canonical identity and
    the Bronze append is idempotent for redelivery. It is technical row
    identity, not a business duplicate guarantee; business duplicate detection
    belongs to the Validation Engine.
    """

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))

    def can_prepare(self, raw_event: dict[str, Any]) -> bool:
        """Recognise a Coinbase match payload by shape, before attempting preparation."""
        return (
            isinstance(raw_event, dict)
            and raw_event.get("type") in _MATCH_TYPES
            and "product_id" in raw_event
        )

    def prepare(self, raw_event: dict[str, Any]) -> InternalEvent:
        """Translate one raw Coinbase payload into an InternalEvent.

        Raises:
            PreparationError: If the payload cannot be represented as an
                InternalEvent. The raw event is preserved on the exception
                for review, never silently discarded.
        """
        if not isinstance(raw_event, dict):
            raise PreparationError(
                "raw event must be a dictionary",
                detail=repr(raw_event),
            )

        raw_type = raw_event.get("type")
        if raw_type not in _MATCH_TYPES:
            raise PreparationError(
                "field 'type' is not a Coinbase match message",
                raw_event=raw_event,
                detail=str(raw_type),
            )

        try:
            side = TradeSide(str(_require(raw_event, "side")))
        except ValueError as exc:
            raise PreparationError(
                "field 'side' is not a known TradeSide",
                raw_event=raw_event,
                detail=str(raw_event.get("side")),
            ) from exc

        product_id = _require(raw_event, "product_id")
        if not isinstance(product_id, str) or not product_id:
            raise PreparationError(
                "field 'product_id' must be a non-empty string",
                raw_event=raw_event,
                detail=repr(product_id),
            )

        raw_trade_id = _require(raw_event, "trade_id")
        trade_id = str(raw_trade_id)
        if not trade_id:
            raise PreparationError(
                "field 'trade_id' must not be empty",
                raw_event=raw_event,
                detail=repr(raw_trade_id),
            )

        trade_timestamp = _to_utc_datetime(raw_event, "time")
        price = _to_decimal(raw_event, "price")
        quantity = _to_decimal(raw_event, "size")
        event_id = f"{self.source}:{trade_id}"

        event = InternalEvent(
            event_id=event_id,
            source=self.source,
            event_type=EventType.trade,
            received_at=self._clock(),
            symbol=product_id,
            price=price,
            quantity=quantity,
            side=side,
            trade_id=trade_id,
            trade_timestamp=trade_timestamp,
            payload=dict(raw_event),
        )
        logger.debug(
            "coinbase_event_prepared",
            event_id=event.event_id,
            symbol=event.symbol,
            trade_id=event.trade_id,
        )
        return event

    @property
    def source(self) -> str:
        return "coinbase"
