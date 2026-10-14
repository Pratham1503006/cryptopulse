"""Deterministic time-window aggregation from trusted events to Gold.

Pure, side-effect-free business logic: given trusted (Silver) events for one
symbol and one time window, produce the canonical ``MarketWindow`` aggregate.
No database, no transport, no configuration-dependent behaviour beyond the
explicit window/lateness parameters passed in.

Determinism and correctness:

- A window is the half-open UTC interval ``[start, end)``. ``window_start``
  is inclusive; ``window_end`` is exclusive.
- Events are ordered by ``(trade_timestamp, event_id)`` before open/close are
  chosen, so the aggregate is reproducible and ties on ``trade_timestamp``
  are broken deterministically by canonical event identity.
- All financial arithmetic uses ``decimal.Decimal``. VWAP is the
  quantity-weighted average price quantized to a documented scale with
  ``ROUND_HALF_UP``; quantities are never treated as notional value.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from common.contracts import MarketWindow, TradeSide, TrustedEvent, window_id_for

__all__ = [
    "aggregate_window",
    "is_final_for",
    "window_bounds_for",
    "window_start_for",
]


def window_start_for(trade_timestamp: datetime, window_seconds: int) -> datetime:
    """Return the inclusive UTC start of the window containing a timestamp.

    Windows are aligned to the Unix epoch so they are stable across restarts
    and independent of process start time.
    """
    if window_seconds <= 0:
        raise ValueError("window_seconds must be > 0")
    if trade_timestamp.tzinfo is None or trade_timestamp.utcoffset() is None:
        raise ValueError("trade_timestamp must be timezone-aware")
    epoch_seconds = int(trade_timestamp.timestamp())
    start_epoch = epoch_seconds - (epoch_seconds % window_seconds)
    return datetime.fromtimestamp(start_epoch, tz=UTC)


def window_bounds_for(trade_timestamp: datetime, window_seconds: int) -> tuple[datetime, datetime]:
    """Return the ``(start, end)`` half-open bounds of the containing window."""
    start = window_start_for(trade_timestamp, window_seconds)
    return start, datetime.fromtimestamp(start.timestamp() + window_seconds, tz=UTC)


def is_final_for(
    window_end: datetime,
    now: datetime,
    allowed_lateness_seconds: int,
) -> bool:
    """Report whether a window is closed for late arrivals.

    A window becomes final once ``now`` has advanced at least
    ``allowed_lateness_seconds`` beyond its end. Until then it is provisional
    and may be recomputed (upserted) as further trusted events arrive.
    """
    elapsed = (now - window_end).total_seconds()
    return elapsed >= allowed_lateness_seconds


def aggregate_window(
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    events: Sequence[TrustedEvent],
    *,
    computed_at: datetime,
    is_final: bool,
    vwap_scale: int,
) -> MarketWindow:
    """Aggregate trusted events into one MarketWindow.

    Args:
        symbol: Trading pair the window covers.
        window_start: Inclusive UTC start.
        window_end: Exclusive UTC end.
        events: Trusted events known to fall inside the window. Must be
            non-empty (a window with no trades is not persisted).
        computed_at: UTC timestamp recorded on the aggregate.
        is_final: Whether the window is closed for late arrivals.
        vwap_scale: Decimal places for the quantized VWAP.

    Returns:
        The canonical MarketWindow aggregate.

    Raises:
        ValueError: If ``events`` is empty or total quantity is not positive.
    """
    if not events:
        raise ValueError("cannot aggregate an empty window")

    ordered = sorted(events, key=lambda event: (event.trade_timestamp, event.event_id))

    prices = [event.price for event in ordered]
    open_price = prices[0]
    close_price = prices[-1]
    high_price = max(prices)
    low_price = min(prices)

    total_quantity = Decimal(0)
    buy_quantity = Decimal(0)
    sell_quantity = Decimal(0)
    notional = Decimal(0)
    for event in ordered:
        quantity = event.quantity
        total_quantity += quantity
        notional += event.price * quantity
        if event.side is TradeSide.buy:
            buy_quantity += quantity
        else:
            sell_quantity += quantity

    if total_quantity <= 0:
        raise ValueError("total quantity must be positive to compute VWAP")

    quantizer = Decimal(1).scaleb(-vwap_scale)
    vwap = (notional / total_quantity).quantize(quantizer, rounding=ROUND_HALF_UP)

    return MarketWindow(
        window_id=window_id_for(symbol, window_start),
        symbol=symbol,
        window_start=window_start,
        window_end=window_end,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        trade_count=len(ordered),
        volume=total_quantity,
        buy_volume=buy_quantity,
        sell_volume=sell_quantity,
        vwap=vwap,
        computed_at=computed_at,
        is_final=is_final,
    )
