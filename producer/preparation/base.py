"""Event Preparation base interface.

Defines the boundary for translating exchange-specific payloads into
the platform's canonical InternalEvent model.

Event Preparation is responsible for:
- Translating exchange-specific payloads
- Standardising field names
- Converting timestamps and data types
- Producing the platform's internal event model

It deliberately does NOT:
- decide whether an event is trustworthy
- calculate business metrics
- persist data

Events that cannot be translated remain visible for review rather than
disappearing silently.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from common.contracts import InternalEvent
from common.utils import get_logger

logger = get_logger(__name__)


class PreparationError(Exception):
    """Raised when an event cannot be translated into InternalEvent."""

    def __init__(
        self,
        message: str,
        *,
        raw_event: dict[str, Any] | None = None,
        detail: str | None = None,
    ) -> None:
        self.raw_event = raw_event
        self.detail = detail
        super().__init__(message)


class EventPreparer(ABC):
    """Abstract base class for event preparers.

    Responsible for translating exchange-specific payloads into the
    canonical InternalEvent model. Each exchange should have its own
    preparer implementation.

    The preparation flow:
    1. Receive raw exchange JSON payload
    2. Parse exchange-specific fields
    3. Map to standardised InternalEvent fields
    4. Return InternalEvent (or raise PreparationError)
    """

    @abstractmethod
    def prepare(self, raw_event: dict[str, Any]) -> InternalEvent:
        """Translate a raw exchange event into an InternalEvent.

        Args:
            raw_event: The raw JSON payload from the exchange.

        Returns:
            A standardised InternalEvent.

        Raises:
            PreparationError: If the event cannot be translated.
        """
        ...

    @property
    @abstractmethod
    def source(self) -> str:
        """Return the exchange/provider identifier (e.g., 'coinbase')."""
        ...

    def can_prepare(self, raw_event: dict[str, Any]) -> bool:
        """Report whether this preparer recognises and can translate the raw event.

        Override this method to implement exchange-specific recognition of
        raw message structure before attempting preparation. The default
        implementation returns True.

        This is a recognition check only. Data quality and trust validation
        belong exclusively to the Validation Engine in processing/.
        """
        return True
