"""Exchange Connector base interface.

Defines the boundary for communicating with external event sources.
The Exchange Connector's only responsibility is bringing external events
into the platform reliably.

It deliberately does NOT:
- understand internal business rules
- validate events
- transform event structures
- store data
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from enum import StrEnum
from typing import Any

from common.utils import get_logger

logger = get_logger(__name__)


class ConnectionState(StrEnum):
    """Connection state of an exchange connector."""

    disconnected = "disconnected"
    connecting = "connecting"
    connected = "connected"
    reconnecting = "reconnecting"


class ExchangeConnector(ABC):
    """Abstract base class for exchange connectors.

    Responsible for:
    - Establishing connections to external event sources
    - Receiving live market events
    - Recovering from temporary connection failures
    - Forwarding received events for preparation

    Connection lifecycle:
    1. connect() - establish initial connection
    2. stream() - receive events as async iterator
    3. disconnect() - cleanly close connection
    4. reconnect() - recover from connection failures
    """

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the external event source."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Cleanly close the connection to the external event source."""
        ...

    @abstractmethod
    async def reconnect(self) -> None:
        """Recover from a connection failure.

        Should attempt to re-establish the connection and resume
        streaming. Events published while disconnected are outside
        the platform's control (known limitation of Version 1).
        """
        ...

    @abstractmethod
    def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Stream raw exchange events from the external source.

        Declared as a (non-async) method returning an async iterator so
        implementations are async generator functions; callers iterate
        with ``async for event in connector.stream()``.

        Yields raw exchange payloads exactly as received. No translation,
        validation, or preparation is performed here; translating raw
        payloads into InternalEvent is the responsibility of Event Preparation.
        """
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if the connector is currently connected."""
        ...

    @property
    @abstractmethod
    def connection_state(self) -> ConnectionState:
        """Return the current connection state."""
        ...

    @property
    @abstractmethod
    def source(self) -> str:
        """Return the exchange/provider identifier (e.g., 'coinbase')."""
        ...
