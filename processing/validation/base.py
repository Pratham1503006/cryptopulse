from __future__ import annotations

from abc import ABC, abstractmethod

from common.contracts import InternalEvent, TrustedEvent


class Validator(ABC):
    @abstractmethod
    def validate(self, event: InternalEvent) -> TrustedEvent | None: ...
