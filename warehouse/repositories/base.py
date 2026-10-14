from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime

from common.contracts import (
    InternalEvent,
    MarketWindow,
    TrustedEvent,
    ValidationFailure,
)
from warehouse.schemas.quarantine import QuarantineRecord


class BronzeRepository(ABC):
    @abstractmethod
    async def append(self, event: InternalEvent) -> None: ...

    @abstractmethod
    async def replay(self, offset: int = 0, limit: int = 100) -> Sequence[InternalEvent]: ...

    async def get(self, event_id: str) -> InternalEvent | None:
        """Fetch one preserved event by its technical row identity.

        Optional capability: implementations that support targeted lookup
        override this method. The default raises NotImplementedError so
        existing implementations remain backward-compatible.

        ``event_id`` is technical row identity, not a business-duplicate
        guarantee; duplicate detection belongs to the Validation Engine.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support get()")


class SilverRepository(ABC):
    @abstractmethod
    async def append(self, event: TrustedEvent) -> None: ...

    @abstractmethod
    async def read(self, offset: int = 0, limit: int = 100) -> Sequence[TrustedEvent]: ...

    async def get(self, event_id: str) -> TrustedEvent | None:
        """Fetch one trusted event by its technical row identity.

        Optional capability: implementations that support targeted lookup
        override this method. The default raises NotImplementedError so
        existing implementations remain backward-compatible.

        ``event_id`` is technical row identity, not a business-duplicate
        guarantee; duplicate detection belongs to the Validation Engine.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support get()")

    async def read_window(
        self,
        symbol: str,
        window_start: datetime,
        window_end: datetime,
        limit: int = 1_000_000,
    ) -> Sequence[TrustedEvent]:
        """Read the trusted events belonging to one trading symbol and window.

        Used by the Gold processing engine to recompute a single window from
        trusted data only (late-arrival handling). Optional capability:
        implementations that support a targeted query override this method.
        The default raises NotImplementedError so existing implementations
        remain backward-compatible.

        The window is half-open ``[window_start, window_end)`` and ordering
        is deterministic by ``(trade_timestamp, event_id)``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support read_window()")


class GoldRepository(ABC):
    """Boundary for persisting and reading Gold business information.

    Gold is derived exclusively from trusted (Silver) events. This boundary
    stores aggregates only; it never performs aggregation itself (business
    rules live in the processing engine).
    """

    @abstractmethod
    async def upsert(self, window: MarketWindow) -> None:
        """Persist (insert or update) one market window aggregate.

        Idempotent per ``window_id``: recomputing a window replaces the
        existing aggregate in place, so replay and late-arrival recomputation
        never create duplicate rows.
        """
        ...

    @abstractmethod
    async def read(
        self,
        symbol: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Sequence[MarketWindow]:
        """Read market windows, optionally filtered by symbol."""
        ...

    async def get(self, window_id: str) -> MarketWindow | None:
        """Fetch one window by its stable technical identity.

        Optional capability: implementations that support targeted lookup
        override this method. The default raises NotImplementedError so
        existing implementations remain backward-compatible.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support get()")

    async def latest_by_symbol(self, limit: int = 100) -> Sequence[MarketWindow]:
        """Return the most recent window per symbol (one row per symbol).

        Optional capability used by analytics surfaces; the default raises
        NotImplementedError so minimal implementations remain compatible.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support latest_by_symbol()")

    async def list_symbols(self) -> Sequence[str]:
        """Return the distinct symbols present in Gold.

        Optional capability used by analytics surfaces; the default raises
        NotImplementedError so minimal implementations remain compatible.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support list_symbols()")


class QuarantineRepository(ABC):
    @abstractmethod
    async def append(self, event: InternalEvent, reason: str) -> None: ...

    async def append_record(
        self,
        event: InternalEvent,
        quarantined_at: datetime,
        failures: list[ValidationFailure],
    ) -> None:
        """Preserve a rejected event with structured failure context.

        Optional capability: implementations that can store the structured
        ValidationFailure list override this method. The default falls
        back to a single text reason so minimal implementations remain
        backward-compatible. This is the surface the trust service uses,
        because Quarantine must explain rejections as data, not prose.
        """
        reason = "; ".join(f"{f.rule}:{f.code}: {f.message}" for f in failures)
        await self.append(event, reason)

    @abstractmethod
    async def read_rejected(self, offset: int = 0, limit: int = 100) -> Sequence[InternalEvent]: ...

    async def get_records(
        self,
        event_id: str,
        limit: int = 100,
    ) -> Sequence[QuarantineRecord]:
        """Fetch quarantine records for one technical event identity.

        Optional capability: implementations that support targeted lookup
        override this method. The default raises NotImplementedError so
        existing implementations remain backward-compatible. Multiple
        records may exist for one event_id because each processing attempt
        that rejects the event appends a distinct record.

        ``event_id`` is technical row identity, not a business-duplicate
        guarantee; duplicate detection belongs to the Validation Engine.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support get_records()")
