from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from common.contracts import BusinessInformation, InternalEvent, TrustedEvent


class BronzeRepository(ABC):
    @abstractmethod
    async def append(self, event: InternalEvent) -> None: ...

    @abstractmethod
    async def replay(
        self, offset: int = 0, limit: int = 100
    ) -> Sequence[InternalEvent]: ...


class SilverRepository(ABC):
    @abstractmethod
    async def append(self, event: TrustedEvent) -> None: ...

    @abstractmethod
    async def read(
        self, offset: int = 0, limit: int = 100
    ) -> Sequence[TrustedEvent]: ...


class GoldRepository(ABC):
    @abstractmethod
    async def write(self, info: BusinessInformation) -> None: ...

    @abstractmethod
    async def query(
        self, metric_name: str, limit: int = 100
    ) -> Sequence[BusinessInformation]: ...


class QuarantineRepository(ABC):
    @abstractmethod
    async def append(self, event: InternalEvent, reason: str) -> None: ...

    @abstractmethod
    async def read_rejected(
        self, offset: int = 0, limit: int = 100
    ) -> Sequence[InternalEvent]: ...
