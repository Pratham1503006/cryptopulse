"""Common module - shared platform foundation.

Contains platform-wide models, contracts, configuration, shared exceptions,
and utilities used by every application module.

This module has NO dependencies on application modules (producer, processing,
warehouse, analytics, monitoring).
"""

from common.config import AppSettings as AppSettings
from common.config import GrafanaSettings as GrafanaSettings
from common.config import PostgresSettings as PostgresSettings
from common.config import PrometheusSettings as PrometheusSettings
from common.config import load_settings as load_settings
from common.contracts.events import BusinessInformation as BusinessInformation
from common.contracts.events import EventType as EventType
from common.contracts.events import InternalEvent as InternalEvent
from common.contracts.events import TradeSide as TradeSide
from common.contracts.events import TrustedEvent as TrustedEvent
from common.exceptions import ConfigurationError as ConfigurationError
from common.exceptions import ConnectionError as ConnectionError
from common.exceptions import CryptoPulseError as CryptoPulseError
from common.exceptions import ProcessingError as ProcessingError
from common.exceptions import StorageError as StorageError
from common.exceptions import ValidationError as ValidationError
from common.models.metadata import EventMetadata as EventMetadata
from common.models.metadata import EventSource as EventSource
from common.models.metadata import EventStage as EventStage
from common.models.metadata import ValidationResult as ValidationResult
from common.utils import configure_logging as configure_logging
from common.utils import get_logger as get_logger

__all__ = [
    # Configuration
    "AppSettings",
    "GrafanaSettings",
    "PostgresSettings",
    "PrometheusSettings",
    "load_settings",
    # Event models
    "BusinessInformation",
    "EventType",
    "InternalEvent",
    "TradeSide",
    "TrustedEvent",
    # Exceptions
    "ConfigurationError",
    "ConnectionError",
    "CryptoPulseError",
    "ProcessingError",
    "StorageError",
    "ValidationError",
    # Metadata models
    "EventMetadata",
    "EventSource",
    "EventStage",
    "ValidationResult",
    # Utilities
    "configure_logging",
    "get_logger",
]
