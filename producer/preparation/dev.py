"""Development/test event preparation.

Translates the deterministic raw payloads produced by
:class:`producer.connectors.dev.DevExchangeConnector` into canonical
InternalEvent instances.

Responsibility is strictly:

    raw dictionary -> InternalEvent

It performs only mechanical translation:

- field mapping
- type conversion (Decimal, datetime)
- TradeSide / EventType conversion
- payload preservation (the raw dict is preserved verbatim)

It deliberately does NOT:

- decide whether an event is trustworthy,
- apply quality rules (sanity checks, freshness, duplicates) — those
  belong to the Validation Engine,
- persist anything.

Events that cannot be represented as InternalEvent raise PreparationError
and are never published.
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

__all__ = ["DevEventPreparer"]

_MS_PER_SECOND = 1000


def _require(raw_event: dict[str, Any], key: str) -> Any:
    """Fetch a required field, raising PreparationError when absent."""
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


def _to_datetime_from_epoch_ms(raw_event: dict[str, Any], key: str) -> datetime:
    """Convert an epoch-milliseconds field to a timezone-aware UTC datetime."""
    value = _require(raw_event, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PreparationError(
            f"field '{key}' is not an epoch-milliseconds integer",
            raw_event=raw_event,
            detail=repr(value),
        )
    return datetime.fromtimestamp(value / _MS_PER_SECOND, tz=UTC)


class DevEventPreparer(EventPreparer):
    """Translates dev-exchange raw payloads into canonical InternalEvents.

    Args:
        clock: Optional ``received_at`` provider. Defaults to the current
            UTC time, which is the semantically correct receive timestamp;
            tests may inject a fixed clock to make preparation fully
            deterministic.

    ``event_id`` is synthesised from deterministic raw fields
    (``{source}:{trade_id}:{trade_time_ms}``) so identical raw input always
    maps to the same identity. It is technical row identity for Bronze, not
    a business duplicate guarantee: two raw events sharing a trade_id but
    differing in any other field produce distinct event_ids and both are
    preserved.
    """

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))

    def prepare(self, raw_event: dict[str, Any]) -> InternalEvent:
        """Translate one raw payload into an InternalEvent.

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

        raw_type = raw_event.get("type", "trade")
        try:
            event_type = EventType(str(raw_type))
        except ValueError as exc:
            raise PreparationError(
                "field 'type' is not a known EventType",
                raw_event=raw_event,
                detail=str(raw_type),
            ) from exc

        try:
            side = TradeSide(str(_require(raw_event, "side")))
        except ValueError as exc:
            raise PreparationError(
                "field 'side' is not a known TradeSide",
                raw_event=raw_event,
                detail=str(raw_event.get("side")),
            ) from exc

        trade_id = _require(raw_event, "trade_id")
        symbol = _require(raw_event, "symbol")
        trade_time_ms = _require(raw_event, "trade_time_ms")
        if not isinstance(trade_id, str) or not trade_id:
            raise PreparationError(
                "field 'trade_id' must be a non-empty string",
                raw_event=raw_event,
                detail=repr(trade_id),
            )
        if not isinstance(symbol, str) or not symbol:
            raise PreparationError(
                "field 'symbol' must be a non-empty string",
                raw_event=raw_event,
                detail=repr(symbol),
            )

        trade_timestamp = _to_datetime_from_epoch_ms(raw_event, "trade_time_ms")
        price = _to_decimal(raw_event, "price")
        quantity = _to_decimal(raw_event, "quantity")
        event_id = f"{self.source}:{trade_id}:{trade_time_ms}"

        event = InternalEvent(
            event_id=event_id,
            source=self.source,
            event_type=event_type,
            received_at=self._clock(),
            symbol=symbol,
            price=price,
            quantity=quantity,
            side=side,
            trade_id=trade_id,
            trade_timestamp=trade_timestamp,
            payload=dict(raw_event),
        )
        logger.debug(
            "dev_event_prepared",
            event_id=event.event_id,
            symbol=event.symbol,
            trade_id=event.trade_id,
        )
        return event

    @property
    def source(self) -> str:
        return "dev"
