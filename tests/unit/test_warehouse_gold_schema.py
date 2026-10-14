"""Unit tests for Gold schema mapping and repository guards.

Verifies the row-to-contract mapping in isolation (no PostgreSQL needed):
exact Decimal preservation, timezone-aware timestamps, boolean/int handling,
and rejection of malformed rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from common.contracts import MarketWindow
from warehouse.repositories import PostgresGoldRepository
from warehouse.schemas import market_window_to_record, record_to_market_window

WINDOW_START = datetime(2026, 9, 24, 12, 30, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 9, 24, 12, 31, 0, tzinfo=UTC)
COMPUTED_AT = datetime(2026, 9, 24, 12, 32, 0, tzinfo=UTC)


def build_window() -> MarketWindow:
    return MarketWindow(
        window_id=f"BTC-USD:{WINDOW_START.isoformat()}",
        symbol="BTC-USD",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        open=Decimal("64001.12345678901234567890"),
        high=Decimal("64500.5"),
        low=Decimal("63900.25"),
        close=Decimal("64100.75"),
        trade_count=42,
        volume=Decimal("12.345678901234567890"),
        buy_volume=Decimal("7.0"),
        sell_volume=Decimal("5.345678901234567890"),
        vwap=Decimal("64050.12345679"),
        computed_at=COMPUTED_AT,
        is_final=True,
    )


def test_record_contains_all_gold_columns() -> None:
    record = market_window_to_record(build_window())
    assert set(record) == {
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
    }


def test_round_trip_preserves_exact_values() -> None:
    window = build_window()
    reconstructed = record_to_market_window(market_window_to_record(window))
    assert reconstructed == window
    assert reconstructed.open.as_tuple() == window.open.as_tuple()
    assert reconstructed.volume.as_tuple() == window.volume.as_tuple()
    assert isinstance(reconstructed.vwap, Decimal)
    assert isinstance(reconstructed.is_final, bool)
    assert isinstance(reconstructed.trade_count, int)


def test_round_trip_preserves_timezone_awareness() -> None:
    window = build_window()
    reconstructed = record_to_market_window(market_window_to_record(window))
    assert reconstructed.window_start.tzinfo is not None
    assert reconstructed.window_end.tzinfo is not None
    assert reconstructed.computed_at.tzinfo is not None


def test_malformed_row_raises_storage_error() -> None:
    from common.exceptions import StorageError

    with pytest.raises(StorageError):
        record_to_market_window({"window_id": None})


def test_float_financial_value_raises_storage_error() -> None:
    from common.exceptions import StorageError

    record = market_window_to_record(build_window())
    record["vwap"] = 64050.12  # float, not Decimal
    with pytest.raises(StorageError):
        record_to_market_window(record)


def test_non_bool_is_final_raises_storage_error() -> None:
    from common.exceptions import StorageError

    record = market_window_to_record(build_window())
    record["is_final"] = 1  # int, not bool
    with pytest.raises(StorageError):
        record_to_market_window(record)


def test_read_rejects_negative_offset() -> None:
    class _Pool:  # never awaited; validation happens before pool use
        def acquire(self) -> None:  # pragma: no cover
            raise AssertionError("pool must not be touched for invalid arguments")

    repository = PostgresGoldRepository(_Pool())
    with pytest.raises(ValueError, match="offset"):
        import asyncio

        asyncio.run(repository.read(offset=-1))


def test_read_rejects_non_positive_limit() -> None:
    class _Pool:
        def acquire(self) -> None:  # pragma: no cover
            raise AssertionError("pool must not be touched for invalid arguments")

    repository = PostgresGoldRepository(_Pool())
    with pytest.raises(ValueError, match="limit"):
        import asyncio

        asyncio.run(repository.read(limit=0))
