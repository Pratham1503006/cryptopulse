"""Warehouse-specific exceptions.

Persistence-level errors raised by the warehouse module. These wrap
low-level database failures so application modules never depend on a
specific database driver's exception types.

The hierarchy reuses ``StorageError`` from common for storage-operation
failures and adds module-specific subclasses for lifecycle concerns that
storage operations do not cover (schema migration state, pool lifecycle).
"""

from __future__ import annotations

from common.exceptions import StorageError

__all__ = ["WarehouseError", "MigrationError", "PoolNotStartedError"]


class WarehouseError(StorageError):
    """Base exception for all warehouse errors."""


class MigrationError(WarehouseError):
    """Raised when applying database migrations fails."""


class PoolNotStartedError(WarehouseError):
    """Raised when a database operation is attempted before the pool is started."""
