"""warehouse.database - PostgreSQL connection management.

Owns the asyncpg connection pool lifecycle. Deliberately technology-scoped:
higher-level warehouse code depends on this package instead of importing
asyncpg directly, keeping driver usage contained.
"""

from warehouse.database.pool import DatabasePool as DatabasePool
from warehouse.exceptions import PoolNotStartedError as PoolNotStartedError

__all__ = [
    "DatabasePool",
    "PoolNotStartedError",
]
