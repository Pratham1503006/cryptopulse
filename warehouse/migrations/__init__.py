"""warehouse.migrations - reproducible database schema management.

Contains plain SQL migrations (``sql/NNN_name.sql``) and the runner that
applies them idempotently. This is the repository-supported mechanism for
bringing a clean PostgreSQL instance to the canonical warehouse schema.

Command line:

    python -m warehouse.migrations
"""

from warehouse.migrations.runner import (
    MIGRATION_FILENAME_PATTERN as MIGRATION_FILENAME_PATTERN,
)
from warehouse.migrations.runner import MIGRATIONS_DIR as MIGRATIONS_DIR
from warehouse.migrations.runner import MigrationRunner as MigrationRunner

__all__ = [
    "MIGRATION_FILENAME_PATTERN",
    "MIGRATIONS_DIR",
    "MigrationRunner",
]
