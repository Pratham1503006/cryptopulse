"""Event Preparation subpackage.

Provides the abstract interface for translating exchange-specific payloads
into the platform's canonical InternalEvent model, and the development/test
implementation used by the ingestion vertical slice.
"""

from producer.preparation.base import EventPreparer as EventPreparer
from producer.preparation.base import PreparationError as PreparationError
from producer.preparation.dev import DevEventPreparer as DevEventPreparer

__all__ = [
    "EventPreparer",
    "PreparationError",
    "DevEventPreparer",
]
