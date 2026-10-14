"""Processing module.

Implements the Validation Engine (trust boundary) and hosts the future
Processing Engine. The Validation Engine routes events from Bronze into
Silver (trusted) or Quarantine (rejected) via the TrustRoutingService.

This module depends on common/, messaging/, and warehouse/; the trust
service is an application boundary that connects validation decisions to
warehouse persistence. No Gold/analytics logic exists yet.
"""

from processing.bronze_ingestion import BronzeIngestionService as BronzeIngestionService
from processing.config import GoldSettings as GoldSettings
from processing.gold import GoldProcessingService as GoldProcessingService
from processing.gold import aggregate_window as aggregate_window
from processing.gold import window_start_for as window_start_for
from processing.validation import TrustRoutingResult as TrustRoutingResult
from processing.validation import TrustRoutingService as TrustRoutingService
from processing.validation import ValidationEngine as ValidationEngine
from processing.validation import ValidationRule as ValidationRule
from processing.validation import Validator as Validator
from processing.validation import default_rules as default_rules

__all__ = [
    "BronzeIngestionService",
    "GoldProcessingService",
    "GoldSettings",
    "TrustRoutingResult",
    "TrustRoutingService",
    "ValidationEngine",
    "ValidationRule",
    "Validator",
    "aggregate_window",
    "default_rules",
    "window_start_for",
]
