from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from common.contracts import InternalEvent


class ExchangeConnector(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    def stream(self) -> AsyncIterator[InternalEvent]: ...

    @property
    @abstractmethod
    def is_connected(self) -> bool: ...
