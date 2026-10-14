"""Unit tests for the live Coinbase connector.

A fake websocket factory is injected so connection, subscription, message
filtering, and reconnection behaviour are exercised deterministically without
touching the network. A separate test (test_coinbase_websocket_live.py) proves
the same connector against a real local websocket server.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed

from producer.config import ExchangeSettings
from producer.connectors.base import ConnectionState
from producer.connectors.coinbase import CoinbaseExchangeConnector, CoinbaseExchangeError


def match_message(trade_id: int = 1, product_id: str = "BTC-USD") -> str:
    return json.dumps(
        {
            "type": "match",
            "trade_id": trade_id,
            "product_id": product_id,
            "price": "100.00",
            "size": "0.5",
            "side": "buy",
            "time": "2026-08-26T10:10:00.000000Z",
        }
    )


Frame = str | bytes | Exception


class FakeWebSocket:
    """A scripted websocket. Items are frames (str/bytes) or exceptions to raise."""

    def __init__(self, script: list[Frame]) -> None:
        self._script = list(script)
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self

    async def __anext__(self) -> str | bytes:
        if not self._script:
            raise StopAsyncIteration
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeConnection:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self._websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self._websocket

    async def __aexit__(self, *exc: object) -> bool:
        return False


class FakeConnectFactory:
    """Returns one connection per script; exhausted calls yield an empty stream."""

    def __init__(self, scripts: list[list[Frame]]) -> None:
        self._scripts = list(scripts)
        self.connections: list[FakeWebSocket] = []

    def __call__(self, url: str) -> FakeConnection:
        script = self._scripts.pop(0) if self._scripts else []
        websocket = FakeWebSocket(script)
        self.connections.append(websocket)
        return FakeConnection(websocket)


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def build_connector(
    factory: FakeConnectFactory,
    *,
    sleep: RecordingSleep | None = None,
    **settings_overrides: Any,
) -> CoinbaseExchangeConnector:
    settings = ExchangeSettings(
        product_ids=["BTC-USD", "ETH-USD"],
        **settings_overrides,
    )
    return CoinbaseExchangeConnector(
        settings,
        connect=factory,
        sleep=sleep if sleep is not None else RecordingSleep(),
    )


def subscription_payload(websocket: FakeWebSocket) -> dict[str, Any]:
    assert websocket.sent, "connector must send a subscription on connect"
    payload = json.loads(websocket.sent[0])
    assert isinstance(payload, dict)
    return payload


@pytest.mark.asyncio
async def test_connect_sends_matches_subscription() -> None:
    factory = FakeConnectFactory([[]])
    connector = build_connector(factory)

    await connector.connect()

    payload = subscription_payload(factory.connections[0])
    assert payload["type"] == "subscribe"
    assert payload["product_ids"] == ["BTC-USD", "ETH-USD"]
    assert payload["channels"] == ["matches"]
    assert connector.is_connected
    assert connector.connection_state is ConnectionState.connected
    assert connector.source == "coinbase"
    await connector.disconnect()


@pytest.mark.asyncio
async def test_stream_requires_connected_state() -> None:
    connector = build_connector(FakeConnectFactory([[]]))

    with pytest.raises(RuntimeError, match="not connected"):
        async for _event in connector.stream():
            pass  # pragma: no cover - must raise before yielding


@pytest.mark.asyncio
async def test_stream_yields_only_match_messages() -> None:
    factory = FakeConnectFactory(
        [
            [
                json.dumps({"type": "subscriptions", "channels": ["matches"]}),
                json.dumps({"type": "heartbeat", "sequence": 1}),
                match_message(trade_id=1),
                json.dumps({"type": "last_match", "trade_id": 2, "product_id": "ETH-USD"}),
                "not-json",
                json.dumps(["not", "an", "object"]),
            ]
        ]
    )
    connector = build_connector(factory)
    await connector.connect()
    try:
        received = [event async for event in connector.stream()]
    finally:
        await connector.disconnect()

    assert [event["trade_id"] for event in received] == [1, 2]
    assert received[1]["type"] == "last_match"


@pytest.mark.asyncio
async def test_exchange_error_message_is_fatal() -> None:
    factory = FakeConnectFactory([[json.dumps({"type": "error", "message": "bad token"})]])
    connector = build_connector(factory)
    await connector.connect()
    try:
        with pytest.raises(CoinbaseExchangeError, match="bad token"):
            async for _event in connector.stream():
                pass  # pragma: no cover
    finally:
        await connector.disconnect()


@pytest.mark.asyncio
async def test_transient_failure_triggers_reconnect_and_resumes() -> None:
    factory = FakeConnectFactory(
        [
            [match_message(trade_id=1), ConnectionClosed(None, None)],
            [match_message(trade_id=2), ConnectionClosed(None, None)],
        ]
    )
    connector = build_connector(factory)
    await connector.connect()

    received: list[dict[str, Any]] = []
    async for event in connector.stream():
        received.append(event)
        if len(received) == 2:
            await connector.disconnect()
            break

    assert [event["trade_id"] for event in received] == [1, 2]
    # A fresh subscription is sent on every (re)connection.
    assert len(factory.connections) == 2
    assert subscription_payload(factory.connections[1])["type"] == "subscribe"


@pytest.mark.asyncio
async def test_reconnect_counter_resets_after_successful_message() -> None:
    # max_reconnect_attempts=1 would allow only one reconnect, but receiving a
    # message between drops resets the counter, so both reconnects happen.
    factory = FakeConnectFactory(
        [
            [match_message(trade_id=1), OSError("drop-1")],
            [match_message(trade_id=2), OSError("drop-2")],
        ]
    )
    connector = build_connector(factory, max_reconnect_attempts=1)
    await connector.connect()

    received: list[dict[str, Any]] = []
    async for event in connector.stream():
        received.append(event)
        if len(received) == 2:
            await connector.disconnect()
            break

    assert [event["trade_id"] for event in received] == [1, 2]


@pytest.mark.asyncio
async def test_reconnect_is_bounded_then_stream_ends() -> None:
    factory = FakeConnectFactory([[OSError("drop")]])
    connector = build_connector(factory, max_reconnect_attempts=1)
    await connector.connect()

    received = [event async for event in connector.stream()]

    assert received == []
    # First connection + one reconnect attempt, then the budget is exhausted.
    assert len(factory.connections) == 2


@pytest.mark.asyncio
async def test_backoff_is_exponential_and_capped() -> None:
    sleep = RecordingSleep()
    factory = FakeConnectFactory(
        [
            [OSError("drop-1")],
            [OSError("drop-2")],
            [OSError("drop-3")],
        ]
    )
    connector = build_connector(
        factory,
        sleep=sleep,
        max_reconnect_attempts=3,
        reconnect_delay_seconds=1.0,
        max_reconnect_delay_seconds=4.0,
    )
    await connector.connect()

    _ = [event async for event in connector.stream()]

    assert sleep.delays == [1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_disconnect_stops_the_stream() -> None:
    factory = FakeConnectFactory([[match_message(trade_id=1), match_message(trade_id=2)]])
    connector = build_connector(factory)
    await connector.connect()

    received: list[dict[str, Any]] = []
    async for event in connector.stream():
        received.append(event)
        await connector.disconnect()
        break

    assert len(received) == 1
    assert connector.connection_state is ConnectionState.disconnected


@pytest.mark.asyncio
async def test_reconnect_reestablishes_and_resubscribes() -> None:
    factory = FakeConnectFactory([[], []])
    connector = build_connector(factory)
    await connector.connect()

    await connector.reconnect()

    assert connector.is_connected
    assert len(factory.connections) == 2
    assert subscription_payload(factory.connections[1])["type"] == "subscribe"
    await connector.disconnect()


@pytest.mark.asyncio
async def test_connect_is_idempotent_while_connected() -> None:
    factory = FakeConnectFactory([[], []])
    connector = build_connector(factory)
    await connector.connect()
    await connector.connect()

    assert len(factory.connections) == 1
    await connector.disconnect()
