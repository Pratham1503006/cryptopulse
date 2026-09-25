"""Deterministic development/test exchange connector.

A fully local implementation of the Exchange Connector boundary used to
prove the ingestion vertical slice (Milestone 6) without a real exchange
integration. It generates a finite, deterministic sequence of raw market
trade events, exactly as a real connector would receive them.

It deliberately:

- fabricates NO network protocol; it is a pure in-memory source,
- performs no translation, validation, or preparation (that is Event
  Preparation's responsibility),
- emits only raw dictionaries shaped like exchange trade payloads.

Every event it produces is identical across runs: no random values are
used, so integration tests are reproducible by construction. The event
sequence is finite: ``stream()`` yields the scripted events and then
returns, modelling a bounded ingestion session that ends cleanly.

Shutdown is cooperative: ``disconnect()`` (or exiting the iteration
context) stops the stream between events.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from common.utils import get_logger
from producer.connectors.base import ConnectionState, ExchangeConnector

logger = get_logger(__name__)

__all__ = ["DevExchangeConnector"]

# The deterministic raw payload source. Field names and value types mimic a
# real exchange trade feed (string numerics, millisecond epoch timestamps) so
# the development EventPreparer exercises genuine mapping and conversion work.
DEV_RAW_EVENTS: tuple[dict[str, Any], ...] = (
    {
        "type": "trade",
        "symbol": "BTC-USD",
        "price": "64001.12345678901234567890",
        "quantity": "0.000123456789012345678901234567890",
        "side": "buy",
        "trade_id": "trade-001",
        "trade_time_ms": 1787739000000,  # 2026-08-26T10:10:00Z
        "venue": "dev-exchange",
    },
    {
        "type": "trade",
        "symbol": "BTC-USD",
        "price": "64100.50",
        "quantity": "1.25",
        "side": "sell",
        "trade_id": "trade-002",
        "trade_time_ms": 1787739001000,  # 2026-08-26T10:10:01Z
        "venue": "dev-exchange",
    },
    {
        # Same trade_id as the first event but a distinct event: proves
        # Bronze preserves distinct received events (Kafka redelivery vs
        # business duplicate distinction is exercised in the integration
        # tests).
        "type": "trade",
        "symbol": "BTC-USD",
        "price": "64002.00",
        "quantity": "0.5",
        "side": "buy",
        "trade_id": "trade-001",
        "trade_time_ms": 1787739002000,  # 2026-08-26T10:10:02Z
        "venue": "dev-exchange",
    },
    {
        "type": "trade",
        "symbol": "ETH-USD",
        "price": "3141.59265358979323846",
        "quantity": "10.0",
        "side": "sell",
        "trade_id": "trade-003",
        "trade_time_ms": 1787739003000,  # 2026-08-26T10:10:03Z
        "venue": "dev-exchange",
    },
)


class DevExchangeConnector(ExchangeConnector):
    """Deterministic, finite, in-memory ingestion source for development.

    Args:
        events: Raw payloads to yield, in order. Defaults to the shared
            deterministic sequence. Each yielded dict is a copy, so a
            consumer mutating one event cannot corrupt subsequent ones.
        interval_seconds: Optional delay between events, useful for
            exercising asynchronous shutdown paths.
    """

    def __init__(
        self,
        events: tuple[dict[str, Any], ...] | None = None,
        *,
        interval_seconds: float = 0.0,
    ) -> None:
        self._events = events if events is not None else DEV_RAW_EVENTS
        self._interval_seconds = interval_seconds
        self._state = ConnectionState.disconnected
        self._stop_requested = False

    async def connect(self) -> None:
        """Enter the connected state (idempotent while connected)."""
        if self._state is ConnectionState.connected:
            return
        self._state = ConnectionState.connecting
        self._stop_requested = False
        self._state = ConnectionState.connected
        logger.info("dev_connector_connected", source=self.source, events=len(self._events))

    async def disconnect(self) -> None:
        """Stop streaming and return to the disconnected state."""
        self._stop_requested = True
        if self._state is ConnectionState.disconnected:
            return
        self._state = ConnectionState.disconnected
        logger.info("dev_connector_disconnected", source=self.source)

    async def reconnect(self) -> None:
        """Recover from a connection failure by re-establishing state.

        A real connector would resume the external subscription here. The
        development source simply resets its stream position and returns to
        the connected state.
        """
        logger.info("dev_connector_reconnecting", source=self.source)
        self._state = ConnectionState.reconnecting
        self._stop_requested = False
        await self.connect()

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield the scripted raw events in order, then return.

        The iterator is a generator: the connector cannot know whether the
        caller will exhaust it. Clean shutdown therefore relies on
        ``disconnect()`` being awaited (e.g. in a ``finally`` block), which
        is checked before each yield.
        """
        if self._state is not ConnectionState.connected:
            raise RuntimeError("connector is not connected; call connect() first")
        for raw in self._events:
            if self._stop_requested or self._state is not ConnectionState.connected:
                logger.info("dev_connector_stream_stopped", source=self.source)
                return
            if self._interval_seconds > 0:
                await asyncio.sleep(self._interval_seconds)
            logger.debug("dev_connector_event_emitted", source=self.source)
            yield dict(raw)
        logger.info("dev_connector_stream_completed", source=self.source, events=len(self._events))

    @property
    def is_connected(self) -> bool:
        return self._state is ConnectionState.connected

    @property
    def connection_state(self) -> ConnectionState:
        return self._state

    @property
    def source(self) -> str:
        return "dev"
