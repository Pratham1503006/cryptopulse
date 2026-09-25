"""Unit tests for the deterministic development exchange connector."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from producer.connectors import ConnectionState
from producer.connectors.dev import DEV_RAW_EVENTS, DevExchangeConnector


@pytest.mark.asyncio
async def test_stream_yields_all_scripted_events_in_order() -> None:
    connector = DevExchangeConnector()
    await connector.connect()
    try:
        received: list[dict[str, Any]] = [raw async for raw in connector.stream()]
    finally:
        await connector.disconnect()

    assert received == list(DEV_RAW_EVENTS)
    # Each yield is an independent copy so callers cannot mutate the source.
    assert received[0] is not DEV_RAW_EVENTS[0]


@pytest.mark.asyncio
async def test_stream_requires_connected_state() -> None:
    connector = DevExchangeConnector()

    with pytest.raises(RuntimeError, match="not connected"):
        async for _raw in connector.stream():
            pass  # pragma: no cover - stream must raise before yielding


@pytest.mark.asyncio
async def test_disconnect_stops_stream_between_events() -> None:
    connector = DevExchangeConnector()
    await connector.connect()

    received: list[dict[str, Any]] = []
    stream = connector.stream()
    async for raw in stream:
        received.append(raw)
        if len(received) == 1:
            await connector.disconnect()
            break

    assert len(received) == 1


@pytest.mark.asyncio
async def test_reconnect_restores_connected_state() -> None:
    connector = DevExchangeConnector()
    await connector.connect()
    await connector.disconnect()
    assert connector.connection_state is ConnectionState.disconnected

    await connector.reconnect()

    assert connector.is_connected
    await connector.disconnect()


@pytest.mark.asyncio
async def test_raw_events_carry_exchange_trade_fields() -> None:
    """Raw payloads look like exchange data, not InternalEvents."""
    for raw in DEV_RAW_EVENTS:
        assert set(raw) >= {
            "symbol",
            "price",
            "quantity",
            "side",
            "trade_id",
            "trade_time_ms",
            "type",
        }
        # Raw numerics stay strings/timestamps stay ints: conversion is
        # Event Preparation's responsibility, not the connector's.
        assert isinstance(raw["price"], str)
        assert isinstance(raw["quantity"], str)
        assert isinstance(raw["trade_time_ms"], int)
        assert "event_id" not in raw


@pytest.mark.asyncio
async def test_events_are_deterministic_across_instances() -> None:
    first = DevExchangeConnector()
    second = DevExchangeConnector()
    await first.connect()
    await second.connect()
    try:
        stream_a = [raw async for raw in first.stream()]
        stream_b = [raw async for raw in second.stream()]
    finally:
        await first.disconnect()
        await second.disconnect()
    assert stream_a == stream_b


@pytest.mark.asyncio
async def test_interval_connector_sleeps_between_events() -> None:
    connector = DevExchangeConnector(
        events=(
            {
                "symbol": "BTC-USD",
                "price": "1",
                "quantity": "1",
                "side": "buy",
                "trade_id": "t1",
                "trade_time_ms": 1,
                "type": "trade",
            },
        ),
        interval_seconds=0.01,
    )
    await connector.connect()
    try:
        started = asyncio.get_running_loop().time()
        received = [raw async for raw in connector.stream()]
        elapsed = asyncio.get_running_loop().time() - started
    finally:
        await connector.disconnect()
    assert len(received) == 1
    assert elapsed >= 0.01
