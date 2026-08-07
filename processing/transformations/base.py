from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from common.contracts import BusinessInformation, TrustedEvent


class Transformer(ABC):
    @abstractmethod
    def transform(self, events: Sequence[TrustedEvent]) -> BusinessInformation: ...
