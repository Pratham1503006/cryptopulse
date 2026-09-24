"""Warehouse schema migration runner.

Applies SQL migrations from ``warehouse/migrations/sql`` to PostgreSQL in
deterministic filename order. Applied migrations are recorded in
``warehouse.migration_history`` so re-running is idempotent and the applied
state is auditable.

Migrations are plain SQL files applied inside explicit transactions: either
every statement of a migration applies or none do. The runner deliberately
avoids a migration framework; a senior engineer can review the full schema
by reading the SQL files, and a clean PostgreSQL instance can be brought to
the canonical state with a single command:

    python -m warehouse.migrations

Migration files must follow the naming convention ``NNN_name.sql`` (three
digit ordinal prefix) so ordering is deterministic across platforms.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Final

import asyncpg

from common.utils import get_logger
from warehouse.exceptions import MigrationError

logger = get_logger(__name__)

__all__ = ["MigrationRunner", "MIGRATIONS_DIR", "MIGRATION_FILENAME_PATTERN"]

MIGRATIONS_DIR: Final[Path] = Path(__file__).resolve().parent / "sql"
MIGRATION_FILENAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{3}_[a-z0-9_]+$")


class MigrationRunner:
    """Applies warehouse SQL migrations idempotently, in filename order."""

    def __init__(self, pool: Any) -> None:
        """Create a runner.

        Args:
            pool: An asyncpg ``Pool`` (or any object exposing the same
                ``acquire()``/``fetch()`` protocol, e.g. the warehouse
                ``DatabasePool`` wrapper).
        """
        self._pool = pool

    async def apply_migrations(self, migrations_dir: Path | None = None) -> list[str]:
        """Apply all pending migrations and return their names in order.

        Args:
            migrations_dir: Directory containing ``NNN_name.sql`` files.
                Defaults to the packaged ``warehouse/migrations/sql``.

        Returns:
            Names of migrations applied by this call (already-applied
            migrations are skipped and not included).

        Raises:
            MigrationError: If the migrations directory is missing, a file
                does not follow the naming convention, or a migration fails
                to apply. A failed migration leaves no partial changes.
        """
        directory = migrations_dir if migrations_dir is not None else MIGRATIONS_DIR
        if not directory.is_dir():
            raise MigrationError(
                "migrations directory not found",
                detail=str(directory),
            )

        names = sorted(path.stem for path in directory.glob("*.sql"))
        for name in names:
            if not MIGRATION_FILENAME_PATTERN.match(name):
                raise MigrationError(
                    "migration file does not follow the NNN_name.sql convention",
                    detail=name,
                )

        applied = await self._fetch_applied()
        pending = [name for name in names if name not in applied]

        for name in pending:
            sql = (directory / f"{name}.sql").read_text(encoding="utf-8")
            await self._apply_one(name, sql)
            logger.info("migration_applied", migration=name)

        skipped = len(names) - len(pending)
        if skipped:
            logger.info("migrations_already_applied", count=skipped)
        return pending

    async def _fetch_applied(self) -> set[str]:
        query = "SELECT migration FROM warehouse.migration_history"
        try:
            rows = await self._pool.fetch(query)
        except (asyncpg.UndefinedTableError, asyncpg.InvalidSchemaNameError):
            # Fresh database: the history table (or its schema) does not
            # exist yet. The first migration creates both in one transaction.
            return set()
        return {row["migration"] for row in rows}

    async def _apply_one(self, name: str, sql: str) -> None:
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(sql)
        except Exception as exc:
            raise MigrationError(
                f"failed to apply migration {name}",
                detail=str(exc),
            ) from exc
