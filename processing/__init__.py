"""Processing module.

Implements the consumer side of the platform and composes it into one
pipeline:

    Kafka -> Bronze -> Validation -> Silver/Quarantine -> Gold

- BronzeIngestionService: Kafka -> Bronze persistence.
- TrustRoutingService + ValidationEngine: Bronze -> Silver (trusted) or
  Quarantine (rejected).
- GoldProcessingService: Silver -> Gold market-window aggregates.
- ProcessingPipeline: sequences the stages for a single entry point.

This module depends on common/, messaging/, and warehouse/; the services are
application boundaries that connect streaming, validation decisions, and
warehouse persistence.
"""

from processing.bronze_ingestion import BronzeIngestionService as BronzeIngestionService
from processing.config import GoldSettings as GoldSettings
from processing.gold import GoldProcessingService as GoldProcessingService
from processing.gold import aggregate_window as aggregate_window
from processing.gold import window_start_for as window_start_for
from processing.pipeline import PipelineResult as PipelineResult
from processing.pipeline import ProcessingPipeline as ProcessingPipeline
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
    "PipelineResult",
    "ProcessingPipeline",
    "TrustRoutingResult",
    "TrustRoutingService",
    "ValidationEngine",
    "ValidationRule",
    "Validator",
    "aggregate_window",
    "default_rules",
    "window_start_for",
]
