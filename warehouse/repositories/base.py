from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime

from common.contracts import (
    BusinessInformation,
    InternalEvent,
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


class GoldRepository(ABC):
    @abstractmethod
    async def write(self, info: BusinessInformation) -> None: ...

    @abstractmethod
    async def query(self, metric_name: str, limit: int = 100) -> Sequence[BusinessInformation]: ...


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
