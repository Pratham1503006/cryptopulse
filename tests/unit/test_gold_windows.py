"""Unit tests for deterministic Gold window aggregation.

All events are constructed in-memory; no database or transport is involved.
Proves the documented metric semantics: half-open windows, deterministic
open/close ordering with event_id tie-breaking, exact Decimal arithmetic,
quantity-based volume, and quantity-weighted VWAP.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from common.contracts import EventType, MarketWindow, TradeSide, TrustedEvent
from processing.gold.windows import (
    aggregate_window,
    is_final_for,
    window_bounds_for,
    window_start_for,
)

WINDOW_END = datetime(2026, 8, 26, 10, 11, 0, tzinfo=UTC)
WINDOW_START = datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)
COMPUTED_AT = datetime(2026, 8, 26, 10, 12, 0, tzinfo=UTC)


def build_event(
    *,
    event_id: str,
    price: str,
    quantity: str,
    side: TradeSide = TradeSide.buy,
    trade_timestamp: datetime | None = None,
    symbol: str = "BTC-USD",
) -> TrustedEvent:
    return TrustedEvent(
        event_id=event_id,
        source="dev",
        event_type=EventType.trade,
        received_at=WINDOW_START,
        validated_at=WINDOW_START,
        symbol=symbol,
        price=Decimal(price),
        quantity=Decimal(quantity),
        side=side,
        trade_id=f"trade-{event_id}",
        trade_timestamp=trade_timestamp or WINDOW_START,
        payload={},
    )


class TestWindowBounds:
    def test_default_alignment_to_epoch(self) -> None:
        ts = datetime(2026, 8, 26, 10, 10, 37, tzinfo=UTC)
        assert window_start_for(ts, 60) == datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)

    def test_bounds_are_half_open(self) -> None:
        start, end = window_bounds_for(datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC), 60)
        assert start == datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC)
        assert end == datetime(2026, 8, 26, 10, 11, 0, tzinfo=UTC)

    def test_boundary_event_belongs_to_exactly_one_window(self) -> None:
        exactly_on_boundary = datetime(2026, 8, 26, 10, 11, 0, tzinfo=UTC)
        assert window_start_for(exactly_on_boundary, 60) == datetime(
            2026, 8, 26, 10, 11, 0, tzinfo=UTC
        )

    def test_non_positive_window_seconds_rejected(self) -> None:
        with pytest.raises(ValueError, match="window_seconds"):
            window_start_for(WINDOW_START, 0)

    def test_naive_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            window_start_for(datetime(2026, 8, 26, 10, 10, 0), 60)  # noqa: DTZ001


class TestFinality:
    def test_provisional_before_lateness_elapses(self) -> None:
        now = WINDOW_END + timedelta(seconds=30)
        assert is_final_for(WINDOW_END, now, allowed_lateness_seconds=120) is False

    def test_final_once_lateness_elapses(self) -> None:
        now = WINDOW_END + timedelta(seconds=120)
        assert is_final_for(WINDOW_END, now, allowed_lateness_seconds=120) is True

    def test_zero_lateness_final_immediately_at_end(self) -> None:
        assert is_final_for(WINDOW_END, WINDOW_END, allowed_lateness_seconds=0) is True


class TestAggregation:
    def test_empty_window_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty window"):
            aggregate_window(
                "BTC-USD",
                WINDOW_START,
                WINDOW_END,
                [],
                computed_at=COMPUTED_AT,
                is_final=True,
                vwap_scale=8,
            )

    def test_ohlc_and_ordering(self) -> None:
        events = [
            build_event(
                event_id="e2",
                price="105",
                quantity="1",
                trade_timestamp=WINDOW_START + timedelta(seconds=10),
            ),
            build_event(
                event_id="e1",
                price="100",
                quantity="2",
                trade_timestamp=WINDOW_START + timedelta(seconds=5),
            ),
            build_event(
                event_id="e3",
                price="90",
                quantity="1",
                trade_timestamp=WINDOW_START + timedelta(seconds=20),
            ),
        ]
        window = aggregate_window(
            "BTC-USD",
            WINDOW_START,
            WINDOW_END,
            events,
            computed_at=COMPUTED_AT,
            is_final=True,
            vwap_scale=8,
        )
        assert isinstance(window, MarketWindow)
        assert window.open == Decimal("100")  # first by timestamp
        assert window.close == Decimal("90")  # last by timestamp
        assert window.high == Decimal("105")
        assert window.low == Decimal("90")
        assert window.trade_count == 3

    def test_tie_on_timestamp_broken_by_event_id(self) -> None:
        ts = WINDOW_START + timedelta(seconds=5)
        events = [
            build_event(event_id="evt-b", price="200", quantity="1", trade_timestamp=ts),
            build_event(event_id="evt-a", price="100", quantity="1", trade_timestamp=ts),
        ]
        window = aggregate_window(
            "BTC-USD",
            WINDOW_START,
            WINDOW_END,
            events,
            computed_at=COMPUTED_AT,
            is_final=True,
            vwap_scale=8,
        )
        # evt-a sorts before evt-b, so it defines open; evt-b defines close.
        assert window.open == Decimal("100")
        assert window.close == Decimal("200")

    def test_volume_and_buy_sell_split(self) -> None:
        events = [
            build_event(event_id="b1", price="100", quantity="2", side=TradeSide.buy),
            build_event(event_id="s1", price="100", quantity="3", side=TradeSide.sell),
        ]
        window = aggregate_window(
            "BTC-USD",
            WINDOW_START,
            WINDOW_END,
            events,
            computed_at=COMPUTED_AT,
            is_final=True,
            vwap_scale=8,
        )
        assert window.volume == Decimal("5")
        assert window.buy_volume == Decimal("2")
        assert window.sell_volume == Decimal("3")
        # buy + sell always equals total volume.
        assert window.buy_volume + window.sell_volume == window.volume

    def test_vwap_is_quantity_weighted_exact_decimal(self) -> None:
        events = [
            build_event(event_id="a", price="100", quantity="2"),
            build_event(event_id="b", price="200", quantity="1"),
        ]
        window = aggregate_window(
            "BTC-USD",
            WINDOW_START,
            WINDOW_END,
            events,
            computed_at=COMPUTED_AT,
            is_final=True,
            vwap_scale=8,
        )
        # (100*2 + 200*1) / 3 = 400/3 = 133.33333333 (ROUND_HALF_UP at scale 8)
        assert window.vwap == Decimal("133.33333333")
        assert isinstance(window.vwap, Decimal)

    def test_vwap_uses_exact_precision_not_float(self) -> None:
        events = [
            build_event(event_id="a", price="64001.12345678901234567890", quantity="1"),
        ]
        window = aggregate_window(
            "BTC-USD",
            WINDOW_START,
            WINDOW_END,
            events,
            computed_at=COMPUTED_AT,
            is_final=True,
            vwap_scale=8,
        )
        assert window.vwap == Decimal("64001.12345679")

    def test_window_identity_is_symbol_and_start(self) -> None:
        window = aggregate_window(
            "ETH-USD",
            WINDOW_START,
            WINDOW_END,
            [build_event(event_id="a", price="1", quantity="1", symbol="ETH-USD")],
            computed_at=COMPUTED_AT,
            is_final=False,
            vwap_scale=8,
        )
        assert window.window_id == f"ETH-USD:{WINDOW_START.isoformat()}"

    def test_zero_total_quantity_rejected(self) -> None:
        event = TrustedEvent.model_construct(
            **{
                **build_event(event_id="a", price="1", quantity="1").model_dump(),
                "quantity": Decimal("0"),
            }
        )
        with pytest.raises(ValueError, match="total quantity"):
            aggregate_window(
                "BTC-USD",
                WINDOW_START,
                WINDOW_END,
                [event],
                computed_at=COMPUTED_AT,
                is_final=True,
                vwap_scale=8,
            )

    def test_market_window_is_frozen(self) -> None:
        window = aggregate_window(
            "BTC-USD",
            WINDOW_START,
            WINDOW_END,
            [build_event(event_id="a", price="1", quantity="1")],
            computed_at=COMPUTED_AT,
            is_final=True,
            vwap_scale=8,
        )
        with pytest.raises(PydanticValidationError):
            setattr(window, "symbol", "other")
