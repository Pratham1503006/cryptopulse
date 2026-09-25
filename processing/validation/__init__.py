"""Validation Engine package.

Implements the platform's trust boundary:

    InternalEvent -> (ValidationEngine) -> valid | invalid (+ structured failures)

Components:

- ``base``: ValidationRule and Validator abstractions
- ``rules``: the initial deterministic rule set
- ``engine``: rule composition producing ValidationOutcome
- ``service``: Bronze -> validation -> Silver/Quarantine routing

Validation determines trust only. It never aggregates business metrics
(Processing Engine, later milestone), never writes SQL itself (warehouse
repositories own persistence), and never touches Kafka.
"""

from processing.validation.base import ValidationRule as ValidationRule
from processing.validation.base import Validator as Validator
from processing.validation.engine import ValidationEngine as ValidationEngine
from processing.validation.rules import default_rules as default_rules
from processing.validation.service import TrustRoutingResult as TrustRoutingResult
from processing.validation.service import TrustRoutingService as TrustRoutingService

__all__ = [
    "TrustRoutingResult",
    "TrustRoutingService",
    "ValidationEngine",
    "ValidationRule",
    "Validator",
    "default_rules",
]
