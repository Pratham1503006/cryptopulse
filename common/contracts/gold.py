"""Gold business-information contracts.

Defines the platform's first Gold data product: a fixed-size, time-windowed
market aggregate computed from trusted (Silver) trade events.

Semantics (deliberately explicit so the metric cannot be misread):

- The window covers the half-open interval ``[window_start, window_end)`` in
  UTC. ``window_start`` is inclusive, ``window_end`` is exclusive, so an
  event exactly on a boundary belongs to exactly one window.
- ``volume`` is the sum of ``quantity`` (base-asset units traded), NOT
  notional USD value. No notional / quote-currency value is computed because
  no FX conversion is applied. It is therefore never called ``usd_volume``.
- ``vwap`` is the quantity-weighted average price
  (``sum(price * quantity) / sum(quantity)``), computed with exact Decimal
  arithmetic and quantized to a documented number of decimal places. It
  equals the arithmetic mean price only when quantities are uniform.
- ``open``/``close`` are the prices of the first/last trade in deterministic
  order (``trade_timestamp`` then ``event_id``). Ties on ``trade_timestamp``
  are broken by the canonical ``event_id`` so the result is reproducible.
- ``is_final`` marks whether the window is closed for late arrivals: a window
  is final once ``window_end + allowed_lateness`` has elapsed. Provisional
  windows may be recomputed (upserted) as additional trusted events arrive.

This contract lives in common/ because it is technology-independent: the
processing engine produces it, the warehouse persists it, and analytics
consumes it, with no module depending on another's private types.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

__all__ = ["MarketWindow", "window_id_for"]


def window_id_for(symbol: str, window_start: datetime) -> str:
    """Build the stable technical identity of a market window.

    The identity is ``"{symbol}:{window_start ISO-8601}"``. It is stable
    across recomputation, so writing Gold is idempotent per window:
    reprocessing the same window updates the same record in place rather
    than appending a duplicate.
    """
    return f"{symbol}:{window_start.isoformat()}"


class MarketWindow(BaseModel):
    """OHLCV + volume aggregate for one symbol and one time window.

    This is the Gold lifecycle state: business-ready information derived
    only from trusted (Silver) events.
    """

    window_id: str = Field(description="Stable technical identity: symbol + window_start")
    symbol: str = Field(description="Standardised trading pair symbol (e.g., BTC-USD)")
    window_start: datetime = Field(description="Inclusive UTC start of the aggregation window")
    window_end: datetime = Field(description="Exclusive UTC end of the aggregation window")

    open: Decimal = Field(description="Price of the first trade in deterministic order")
    high: Decimal = Field(description="Highest trade price in the window")
    low: Decimal = Field(description="Lowest trade price in the window")
    close: Decimal = Field(description="Price of the last trade in deterministic order")

    trade_count: int = Field(ge=0, description="Number of trusted trades in the window")

    volume: Decimal = Field(
        description="Total traded quantity (base units); sum of quantities, NOT notional USD"
    )
    buy_volume: Decimal = Field(description="Traded quantity where side == buy (base units)")
    sell_volume: Decimal = Field(description="Traded quantity where side == sell (base units)")

    vwap: Decimal = Field(
        description="Quantity-weighted average price: sum(price*quantity)/sum(quantity)"
    )

    computed_at: datetime = Field(description="UTC timestamp when this aggregate was computed")
    is_final: bool = Field(
        default=False,
        description="True once window_end + allowed lateness has elapsed; provisional otherwise",
    )

    model_config = {"frozen": True}
