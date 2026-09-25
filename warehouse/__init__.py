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
from warehouse.repositories import PostgresQuarantineRepository as PostgresQuarantineRepository
from warehouse.repositories import PostgresSilverRepository as PostgresSilverRepository
from warehouse.repositories import QuarantineRepository as QuarantineRepository
from warehouse.repositories import SilverRepository as SilverRepository
from warehouse.schemas.bronze import BronzeRowError as BronzeRowError
from warehouse.schemas.quarantine import QuarantineRecord as QuarantineRecord
from warehouse.schemas.quarantine import QuarantineRowError as QuarantineRowError
from warehouse.schemas.silver import SilverRowError as SilverRowError

__all__ = [
    "BronzeRepository",
    "BronzeRowError",
    "DatabasePool",
    "MigrationError",
    "MigrationRunner",
    "PoolNotStartedError",
    "PostgresBronzeRepository",
    "PostgresQuarantineRepository",
    "PostgresSilverRepository",
    "QuarantineRecord",
    "QuarantineRepository",
    "QuarantineRowError",
    "SilverRepository",
    "SilverRowError",
    "WarehouseError",
]
