"""Live Coinbase Exchange WebSocket connector.

The production implementation of the Exchange Connector boundary. It consumes
the public Coinbase Exchange ``matches`` channel and yields raw match payloads
exactly as the exchange publishes them.

Responsibilities (deliberately narrow, matching the boundary contract):

- establish a WebSocket connection to Coinbase,
- subscribe to the configured products and the ``matches`` channel,
- yield raw match payloads unchanged,
- automatically recover from temporary connection failures with bounded,
  exponential backoff (Version 1 requirement: "automatically recover from
  temporary connection failures").

It deliberately does NOT:

- validate, transform, or normalise events (Event Preparation's job),
- decide trust (Validation Engine's job in processing/),
- persist anything,
- fabricate an ``InternalEvent`` from exchange data.

Connection control messages from the exchange (``subscriptions`` confirmation,
``heartbeat``) are not market data and are consumed internally rather than
yielded; only ``match``/``last_match`` payloads are forwarded.

Reliability model:

- ``stream()`` wraps message iteration in a retry loop. Transient failures
  (``ConnectionClosed``, ``OSError``) trigger reconnection with capped
  exponential backoff up to ``max_reconnect_attempts`` consecutive failures.
- A protocol error reported by the exchange (``type == "error"``) is fatal:
  it is surfaced as :class:`CoinbaseExchangeError` rather than retried,
  because reconnecting cannot fix a rejected subscription.
- The consecutive-failure counter resets once a message is successfully
  received, so a long-lived connection that occasionally drops never
  exhausts the retry budget.

The websocket client factory and the sleep function are injectable so the
connector can be tested deterministically without a network.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any, Protocol, TypeVar

import websockets
from websockets.exceptions import ConnectionClosed

from common.utils import get_logger
from producer.config import ExchangeSettings
from producer.connectors.base import ConnectionState, ExchangeConnector

logger = get_logger(__name__)

__all__ = ["CoinbaseExchangeConnector", "CoinbaseExchangeError"]

# Coinbase publishes trades on the matches channel under these message types.
_MATCH_TYPES = frozenset({"match", "last_match"})

# The channel we subscribe to. Only matches are treated as market data.
_MATCHES_CHANNEL = "matches"


class CoinbaseExchangeError(Exception):
    """Raised when Coinbase reports a fatal protocol-level error.

    This is distinct from a transient transport failure: reconnecting cannot
    resolve it, so it propagates out of ``stream()`` instead of being retried.
    """


_T = TypeVar("_T", covariant=True)


class _WebSocket(Protocol):
    """Minimal connection surface the connector depends on."""

    async def send(self, message: str) -> None: ...

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...


class _AsyncContextManager(Protocol[_T]):
    """Structural async context manager, kept local to avoid a typing dependency."""

    async def __aenter__(self) -> _T: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None: ...


ConnectFactory = Callable[[str], _AsyncContextManager[_WebSocket]]
SleepFunction = Callable[[float], Awaitable[None]]


def _default_connect(url: str) -> _AsyncContextManager[_WebSocket]:
    """Open a real Coinbase websocket (lazily bound so tests can inject one)."""
    return websockets.connect(url)


class CoinbaseExchangeConnector(ExchangeConnector):
    """Live Coinbase Exchange connector.

    Args:
        settings: Exchange configuration (URL, product ids, reconnect policy).
        connect: Optional websocket factory ``(url) -> async context manager``.
            Defaults to :func:`websockets.connect`. Tests inject a fake.
        sleep: Optional sleep function used for reconnect backoff. Defaults to
            :func:`asyncio.sleep`. Tests inject a no-op to avoid real delays.
        clock: Reserved for future latency metrics; not used for event fields
            (received_at is assigned during Event Preparation).
    """

    def __init__(
        self,
        settings: ExchangeSettings | None = None,
        *,
        connect: ConnectFactory | None = None,
        sleep: SleepFunction | None = None,
    ) -> None:
        self._settings = settings if settings is not None else ExchangeSettings()
        self._connect = connect if connect is not None else _default_connect
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._state = ConnectionState.disconnected
        self._stop_requested = False
        self._context: _AsyncContextManager[_WebSocket] | None = None
        self._ws: _WebSocket | None = None

    async def connect(self) -> None:
        """Open the websocket and subscribe to configured products (idempotent)."""
        if self._state is ConnectionState.connected:
            return
        self._state = ConnectionState.connecting
        self._stop_requested = False
        await self._open()
        self._state = ConnectionState.connected
        logger.info(
            "coinbase_connector_connected",
            source=self.source,
            products=list(self._settings.product_ids),
        )

    async def disconnect(self) -> None:
        """Stop streaming and close the websocket (idempotent)."""
        self._stop_requested = True
        await self._close()
        self._state = ConnectionState.disconnected
        logger.info("coinbase_connector_disconnected", source=self.source)

    async def reconnect(self) -> None:
        """Close the current connection and re-establish it with a fresh subscription."""
        logger.warning("coinbase_connector_reconnecting", source=self.source)
        self._state = ConnectionState.reconnecting
        await self._close()
        await self._open()
        self._state = ConnectionState.connected
        logger.info("coinbase_connector_reconnected", source=self.source)

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield raw Coinbase match payloads, recovering from transient failures.

        Raises:
            RuntimeError: If called before :meth:`connect`.
            CoinbaseExchangeError: If the exchange reports a fatal protocol
                error (for example a rejected subscription).
        """
        if self._state is not ConnectionState.connected:
            raise RuntimeError("connector is not connected; call connect() first")

        consecutive_failures = 0
        while not self._stop_requested and self._state is ConnectionState.connected:
            ws = self._ws
            if ws is None:
                raise RuntimeError("connector is not connected; call connect() first")
            try:
                async for message in ws:
                    consecutive_failures = 0
                    if self._stop_requested:
                        return
                    payload = self._decode(message)
                    if payload is None:
                        continue
                    message_type = payload.get("type")
                    if message_type == "error":
                        raise CoinbaseExchangeError(
                            str(payload.get("message") or payload.get("reason") or payload)
                        )
                    if message_type in _MATCH_TYPES:
                        logger.debug(
                            "coinbase_event_emitted",
                            source=self.source,
                            product_id=payload.get("product_id"),
                        )
                        yield payload
                # The async iterator ended without an exception: the server
                # closed the stream cleanly. Treat it as a transient failure.
                raise ConnectionError("coinbase websocket stream ended")
            except CoinbaseExchangeError:
                logger.error("coinbase_protocol_error", source=self.source)
                raise
            except (ConnectionClosed, OSError, ConnectionError) as exc:
                if self._stop_requested or self._state is not ConnectionState.connected:
                    return
                consecutive_failures += 1
                if consecutive_failures > self._settings.max_reconnect_attempts:
                    logger.error(
                        "coinbase_reconnect_exhausted",
                        source=self.source,
                        attempts=consecutive_failures - 1,
                        error=str(exc),
                    )
                    return
                await self._wait_before_reconnect(consecutive_failures)
                if self._stop_requested:
                    return
                await self.reconnect()

    @property
    def is_connected(self) -> bool:
        return self._state is ConnectionState.connected

    @property
    def connection_state(self) -> ConnectionState:
        return self._state

    @property
    def source(self) -> str:
        return "coinbase"

    async def _open(self) -> None:
        """Open a connection and send the matches subscription."""
        self._context = self._connect(self._settings.websocket_url)
        self._ws = await self._context.__aenter__()
        await self._ws.send(self._subscription_message())

    async def _close(self) -> None:
        """Release the current connection, ignoring teardown errors."""
        context = self._context
        self._context = None
        self._ws = None
        if context is None:
            return
        with contextlib.suppress(Exception):
            await context.__aexit__(None, None, None)

    def _subscription_message(self) -> str:
        return json.dumps(
            {
                "type": "subscribe",
                "product_ids": list(self._settings.product_ids),
                "channels": [_MATCHES_CHANNEL],
            }
        )

    async def _wait_before_reconnect(self, attempt: int) -> None:
        """Sleep with capped exponential backoff between reconnect attempts."""
        delay = min(
            self._settings.reconnect_delay_seconds * (2 ** (attempt - 1)),
            self._settings.max_reconnect_delay_seconds,
        )
        logger.warning(
            "coinbase_reconnect_backoff",
            source=self.source,
            attempt=attempt,
            delay_seconds=delay,
        )
        await self._sleep(delay)

    def _decode(self, message: str | bytes) -> dict[str, Any] | None:
        """Decode a raw websocket frame; malformed frames are logged and skipped."""
        try:
            payload = json.loads(message)
        except (ValueError, TypeError):
            logger.warning("coinbase_message_undecodable", source=self.source)
            return None
        if not isinstance(payload, dict):
            logger.warning("coinbase_message_not_object", source=self.source)
            return None
        return payload
