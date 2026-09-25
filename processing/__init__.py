"""Processing module.

Implements the Validation Engine (trust boundary) and hosts the future
Processing Engine. The Validation Engine routes events from Bronze into
Silver (trusted) or Quarantine (rejected) via the TrustRoutingService.

This module depends on common/, messaging/, and warehouse/; the trust
service is an application boundary that connects validation decisions to
warehouse persistence. No Gold/analytics logic exists yet.
"""

from processing.bronze_ingestion import BronzeIngestionService as BronzeIngestionService
from processing.validation import TrustRoutingResult as TrustRoutingResult
from processing.validation import TrustRoutingService as TrustRoutingService
from processing.validation import ValidationEngine as ValidationEngine
from processing.validation import ValidationRule as ValidationRule
from processing.validation import Validator as Validator
from processing.validation import default_rules as default_rules

__all__ = [
    "BronzeIngestionService",
    "TrustRoutingResult",
    "TrustRoutingService",
    "ValidationEngine",
    "ValidationRule",
    "Validator",
    "default_rules",
]
