"""Gold business-processing engine.

The application boundary that derives Gold business information from trusted
(Silver) events:

    Silver (TrustedEvents)
        -> window discovery (symbol + UTC window)
        -> window aggregation (pure, deterministic)
        -> Gold (MarketWindow upsert)

Responsibilities:

- read trusted events from Silver only (never Bronze, never Quarantine),
- discover the trading windows represented in that data,
- recompute each window from its full set of trusted events,
- persist the aggregate to Gold through the warehouse repository boundary.

Deliberate boundaries:

- It performs NO validation: trust was already established before Silver.
- It writes NO SQL: persistence belongs to the Gold repository.
- It contains no transport awareness (Kafka/Spark).

Recomputation model (idempotent, replay-safe):

Windows are recomputed from the complete set of trusted events for that
symbol and window, so:

- replaying processing reproduces identical aggregates (the Gold upsert is
  keyed by ``window_id`` and replaces in place),
- late-arriving trusted events are handled by recomputing the affected
  window; a window is provisional until ``window_end + allowed_lateness``
  has elapsed, after which it is marked final.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

from common.contracts import MarketWindow, TrustedEvent
from common.utils import get_logger
from processing.config import GoldSettings
from processing.gold.windows import (
    aggregate_window,
    is_final_for,
    window_start_for,
)
from warehouse.repositories.base import GoldRepository, SilverRepository

logger = get_logger(__name__)

__all__ = ["GoldProcessingService"]

# Upper bound when falling back to a page scan for a window's events on
# repositories that do not implement the targeted read_window() query.
_FALLBACK_SCAN_LIMIT = 1_000_000


class GoldProcessingService:
    """Derives Gold market windows from trusted Silver events.

    Args:
        silver: The trusted-event source abstraction (read-only usage).
        gold: The Gold persistence abstraction.
        settings: Gold windowing configuration; defaults are applied when
            omitted.
        clock: Optional UTC ``now`` provider for finality decisions. Tests
            inject a fixed clock to make ``is_final`` deterministic.
    """

    def __init__(
        self,
        silver: SilverRepository,
        gold: GoldRepository,
        settings: GoldSettings | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._silver = silver
        self._gold = gold
        self._settings = settings if settings is not None else GoldSettings()
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))

    async def process_batch(self, offset: int = 0, limit: int | None = None) -> int:
        """Discover and recompute every window represented in a Silver page.

        Candidate windows are discovered from a page of trusted events, then
        each is recomputed from its complete event set. This makes the result
        independent of pagination boundaries and correct under late arrivals.

        Args:
            offset: Number of Silver rows to skip (>= 0).
            limit: Page size; defaults to the configured batch size.

        Returns:
            The number of windows written to Gold.

        Raises:
            ValueError: If offset/limit are invalid.
            StorageError: If a repository operation fails.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        page_size = limit if limit is not None else self._settings.batch_size
        if page_size < 1:
            raise ValueError("limit must be >= 1")

        page: Sequence[TrustedEvent] = await self._silver.read(offset=offset, limit=page_size)
        if not page:
            return 0

        candidates = {
            (event.symbol, window_start_for(event.trade_timestamp, self._settings.window_seconds))
            for event in page
        }

        written = 0
        for symbol, window_start in sorted(candidates, key=lambda c: (c[1], c[0])):
            window = await self.recompute_window(symbol, window_start)
            if window is not None:
                written += 1

        logger.info(
            "gold_batch_complete",
            events_considered=len(page),
            windows_written=written,
            window_seconds=self._settings.window_seconds,
        )
        return written

    async def recompute_window(
        self,
        symbol: str,
        window_start: datetime,
    ) -> MarketWindow | None:
        """Recompute and persist one symbol/window aggregate from Silver.

        Args:
            symbol: Trading pair to aggregate.
            window_start: Inclusive UTC start of the window.

        Returns:
            The written MarketWindow, or None when the window has no trusted
            events (an empty window is not persisted).

        Raises:
            StorageError: If a repository operation fails.
        """
        window_end = window_start + timedelta(seconds=self._settings.window_seconds)
        events = await self._read_window(symbol, window_start, window_end)
        if not events:
            return None
        now = self._clock()
        window = aggregate_window(
            symbol=symbol,
            window_start=window_start,
            window_end=window_end,
            events=events,
            computed_at=now,
            is_final=is_final_for(window_end, now, self._settings.allowed_lateness_seconds),
            vwap_scale=self._settings.vwap_scale,
        )
        await self._gold.upsert(window)
        logger.debug(
            "gold_window_written",
            window_id=window.window_id,
            trade_count=window.trade_count,
            is_final=window.is_final,
        )
        return window

    async def _read_window(
        self,
        symbol: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Sequence[TrustedEvent]:
        """Read a window's trusted events, preferring the targeted query."""
        try:
            return await self._silver.read_window(symbol, window_start, window_end)
        except NotImplementedError:
            page = await self._silver.read(offset=0, limit=_FALLBACK_SCAN_LIMIT)
            return [
                event
                for event in page
                if event.symbol == symbol and window_start <= event.trade_timestamp < window_end
            ]
