"""Event Preparation subpackage.

Provides the abstract interface for translating exchange-specific payloads
into the platform's canonical InternalEvent model.
"""

from producer.preparation.base import EventPreparer as EventPreparer
from producer.preparation.base import PreparationError as PreparationError

__all__ = [
    "EventPreparer",
    "PreparationError",
]
