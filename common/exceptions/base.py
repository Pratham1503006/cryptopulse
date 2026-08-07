from __future__ import annotations


class CryptoPulseError(Exception):
    """Base exception for all platform errors."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        self.message = message
        self.detail = detail
        super().__init__(message)


class ConfigurationError(CryptoPulseError):
    """Raised when application configuration is invalid or missing."""


class ConnectionError(CryptoPulseError):
    """Raised when a connection to an external service fails."""


class ValidationError(CryptoPulseError):
    """Raised when an event fails validation."""


class ProcessingError(CryptoPulseError):
    """Raised when event processing fails."""


class StorageError(CryptoPulseError):
    """Raised when a storage operation fails."""
