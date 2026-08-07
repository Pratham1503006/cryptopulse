from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from common.contracts import InternalEvent


class EventPreparer(ABC):
    @abstractmethod
    def prepare(self, raw_event: dict[str, Any]) -> InternalEvent: ...
