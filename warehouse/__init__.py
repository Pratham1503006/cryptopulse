"""warehouse - physical persistence for the CryptoPulse data layers.

Implements storage for the Bronze, Silver, Gold, and Quarantine layers.
Owns the database schemas, repositories, and migrations. Contains no
business logic, no validation, and no knowledge of Kafka or the producer.

Dependency rule: this module depends only on ``common/``.
"""

from warehouse.database import DatabasePool as DatabasePool
from warehouse.exceptions import (
    MigrationError as MigrationError,
)
from warehouse.exceptions import (
    PoolNotStartedError as PoolNotStartedError,
)
from warehouse.exceptions import (
    WarehouseError as WarehouseError,
)
from warehouse.migrations import MigrationRunner as MigrationRunner
from warehouse.repositories import BronzeRepository as BronzeRepository
from warehouse.repositories import PostgresBronzeRepository as PostgresBronzeRepository
from warehouse.schemas.bronze import BronzeRowError as BronzeRowError

__all__ = [
    "BronzeRepository",
    "BronzeRowError",
    "DatabasePool",
    "MigrationError",
    "MigrationRunner",
    "PoolNotStartedError",
    "PostgresBronzeRepository",
    "WarehouseError",
]
