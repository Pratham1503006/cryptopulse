"""Gold layer database schema and row mapping.

Owns the mapping between the canonical ``MarketWindow`` contract and the
PostgreSQL representation of the Gold layer. This is the only place that
knows how Gold business information is physically represented in
PostgreSQL, and it contains no business logic: the aggregation rules live in
the processing engine, not here.

Storage decisions (see warehouse/migrations/sql/004_create_gold_market_windows.sql):

- ``open``/``high``/``low``/``close``/``volume``/``buy_volume``/
  ``sell_volume``/``vwap`` use ``NUMERIC`` so exact Decimal values survive
  the round-trip without float conversion (same decision as Bronze/Silver).
- ``window_start``/``window_end``/``computed_at`` use ``TIMESTAMPTZ`` so
  timezone information is preserved.
- ``window_id`` is the PRIMARY KEY and the stable technical identity of a
  window, so upserting a recomputed window updates the same row rather than
  appending a duplicate. ``(symbol, window_start)`` is additionally unique
  because that pair is the semantic identity of a window.
- ``is_final`` records whether the window is closed for late arrivals.

This module contains no SQL execution; repositories own execution.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from common.contracts import MarketWindow
from common.exceptions import StorageError

__all__ = [
    "GOLD_COLUMNS",
    "GOLD_SCHEMA",
    "GOLD_TABLE",
    "GoldRowError",
    "market_window_to_record",
    "record_to_market_window",
]


GOLD_SCHEMA = "gold"
GOLD_TABLE = "market_windows"

# Columns of the Gold market_windows table, in canonical column order.
# Kept in one place so INSERT/SELECT/UPDATE always agree on the layout.
GOLD_COLUMNS: tuple[str, ...] = (
    "window_id",
    "symbol",
    "window_start",
    "window_end",
    "open",
    "high",
    "low",
    "close",
    "trade_count",
    "volume",
    "buy_volume",
    "sell_volume",
    "vwap",
    "computed_at",
    "is_final",
)


class GoldRowError(StorageError):
    """Raised when a Gold row cannot be mapped to or from the MarketWindow contract."""


def market_window_to_record(window: MarketWindow) -> dict[str, Any]:
    """Map a canonical MarketWindow to a Gold row record.

    The returned mapping's keys match the Gold table columns so it can be
    passed directly to asyncpg parameterised statements.
    """
    return {
        "window_id": window.window_id,
        "symbol": window.symbol,
        "window_start": window.window_start,
        "window_end": window.window_end,
        "open": window.open,
        "high": window.high,
        "low": window.low,
        "close": window.close,
        "trade_count": window.trade_count,
        "volume": window.volume,
        "buy_volume": window.buy_volume,
        "sell_volume": window.sell_volume,
        "vwap": window.vwap,
        "computed_at": window.computed_at,
        "is_final": window.is_final,
    }


def record_to_market_window(row: Any) -> MarketWindow:
    """Map a Gold table row back to the canonical MarketWindow.

    Reconstructs the exact contract values: NUMERIC columns arrive from
    PostgreSQL as Decimal and TIMESTAMPTZ columns arrive timezone-aware.

    Raises:
        GoldRowError: If the row cannot be represented as a MarketWindow.
    """
    try:
        return MarketWindow(
            window_id=_as_str(row, "window_id"),
            symbol=_as_str(row, "symbol"),
            window_start=_as_datetime(row, "window_start"),
            window_end=_as_datetime(row, "window_end"),
            open=_as_decimal(row, "open"),
            high=_as_decimal(row, "high"),
            low=_as_decimal(row, "low"),
            close=_as_decimal(row, "close"),
            trade_count=_as_int(row, "trade_count"),
            volume=_as_decimal(row, "volume"),
            buy_volume=_as_decimal(row, "buy_volume"),
            sell_volume=_as_decimal(row, "sell_volume"),
            vwap=_as_decimal(row, "vwap"),
            computed_at=_as_datetime(row, "computed_at"),
            is_final=_as_bool(row, "is_final"),
        )
    except GoldRowError:
        raise
    except (ValueError, TypeError, KeyError) as exc:
        raise GoldRowError(
            "gold row does not conform to the MarketWindow contract",
            detail=str(exc),
        ) from exc


def _as_str(row: Any, key: str) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise GoldRowError(f"column '{key}' is not text", detail=repr(value))
    return value


def _as_datetime(row: Any, key: str) -> datetime:
    value = row[key]
    if isinstance(value, datetime):
        return value
    raise GoldRowError(f"column '{key}' is not a timestamp", detail=repr(value))


def _as_decimal(row: Any, key: str) -> Decimal:
    value = row[key]
    if isinstance(value, Decimal):
        return value
    raise GoldRowError(f"column '{key}' is not exact numeric", detail=repr(value))


def _as_int(row: Any, key: str) -> int:
    value = row[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise GoldRowError(f"column '{key}' is not an integer", detail=repr(value))
    return value


def _as_bool(row: Any, key: str) -> bool:
    value = row[key]
    if not isinstance(value, bool):
        raise GoldRowError(f"column '{key}' is not a boolean", detail=repr(value))
    return value
