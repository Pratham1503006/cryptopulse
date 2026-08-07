"""Verify that common module components import correctly."""


def test_config_imports() -> None:
    from common.config import AppSettings, load_settings

    assert AppSettings is not None
    assert load_settings is not None


def test_contracts_imports() -> None:
    from common.contracts import BusinessInformation, InternalEvent, TrustedEvent

    assert InternalEvent is not None
    assert TrustedEvent is not None
    assert BusinessInformation is not None


def test_exceptions_imports() -> None:
    from common.exceptions import (
        ConfigurationError,
        ConnectionError,
        CryptoPulseError,
        ProcessingError,
        StorageError,
        ValidationError,
    )

    assert CryptoPulseError is not None
    assert ConfigurationError is not None
    assert ConnectionError is not None
    assert ValidationError is not None
    assert ProcessingError is not None
    assert StorageError is not None


def test_models_imports() -> None:
    from common.models import EventMetadata, EventSource, EventStage, ValidationResult

    assert EventMetadata is not None
    assert EventSource is not None
    assert EventStage is not None
    assert ValidationResult is not None


def test_utils_imports() -> None:
    from common.utils import configure_logging, get_logger

    assert configure_logging is not None
    assert get_logger is not None
