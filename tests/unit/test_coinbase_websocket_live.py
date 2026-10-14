"""End-to-end test of the Coinbase connector against a real websocket server.

Unlike the injected-fake tests, this binds a real websocket server on an
ephemeral localhost port and drives the connector's real
:func:`websockets.connect` path: TCP handshake, subscription frame, exchange
control messages, and match filtering. It is hermetic (loopback only, no
external services), so it runs as part of the default suite.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from websockets.asyncio.server import serve

from producer.config import ExchangeSettings
from producer.connectors.coinbase import CoinbaseExchangeConnector

MATCH = {
    "type": "match",
    "trade_id": 42,
    "product_id": "BTC-USD",
    "price": "64000.00",
    "size": "0.25",
    "side": "sell",
    "time": "2026-08-26T10:10:00.000000Z",
}


@pytest.mark.asyncio
async def test_connector_streams_matches_over_a_real_websocket() -> None:
    received: list[dict[str, Any]] = []

    async def handler(websocket: Any) -> None:
        async for message in websocket:
            payload = json.loads(message)
            received.append(payload)
            if payload.get("type") == "subscribe":
                await websocket.send(json.dumps({"type": "subscriptions", "channels": ["matches"]}))
                await websocket.send(json.dumps({"type": "heartbeat", "sequence": 1}))
                await websocket.send(json.dumps(MATCH))

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        connector = CoinbaseExchangeConnector(
            ExchangeSettings(
                websocket_url=f"ws://127.0.0.1:{port}",
                product_ids=["BTC-USD"],
            )
        )
        await connector.connect()
        try:
            events: list[dict[str, Any]] = []

            async def consume() -> None:
                async for event in connector.stream():
                    events.append(event)
                    if events:
                        await connector.disconnect()
                        break

            await asyncio.wait_for(consume(), timeout=10)
        finally:
            await connector.disconnect()

    # The connector sent a real matches subscription...
    assert received[0]["type"] == "subscribe"
    assert received[0]["channels"] == ["matches"]
    assert received[0]["product_ids"] == ["BTC-USD"]
    # ...and forwarded the raw match payload unchanged (no heartbeat leaked).
    assert events == [MATCH]
    assert connector.source == "coinbase"
