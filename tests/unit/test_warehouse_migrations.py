"""Unit tests for warehouse migration conventions and repository guards.

These tests require no PostgreSQL connection.
"""

from __future__ import annotations

import re

import pytest

from warehouse.migrations import MIGRATION_FILENAME_PATTERN, MIGRATIONS_DIR
from warehouse.repositories import PostgresBronzeRepository


def test_migration_files_follow_naming_convention() -> None:
    sql_files = list(MIGRATIONS_DIR.glob("*.sql"))
    assert sql_files, "warehouse migrations directory must contain migrations"
    for path in sql_files:
        assert MIGRATION_FILENAME_PATTERN.match(
            path.stem
        ), f"migration file does not follow NNN_name.sql convention: {path.name}"


def test_bronze_migration_creates_expected_table() -> None:
    migration_names = {path.stem for path in MIGRATIONS_DIR.glob("*.sql")}
    assert "001_create_bronze_events" in migration_names


def test_migration_sql_uses_exact_numeric_for_financial_values() -> None:
    migration = next(
        path for path in MIGRATIONS_DIR.glob("*.sql") if path.stem == "001_create_bronze_events"
    ).read_text(encoding="utf-8")
    assert "NUMERIC" in migration
    assert not re.search(r"price\s+(FLOAT|REAL|DOUBLE)", migration, re.IGNORECASE)
    assert not re.search(r"quantity\s+(FLOAT|REAL|DOUBLE)", migration, re.IGNORECASE)


def test_migration_sql_uses_timestamptz() -> None:
    migration = next(
        path for path in MIGRATIONS_DIR.glob("*.sql") if path.stem == "001_create_bronze_events"
    ).read_text(encoding="utf-8")
    assert "TIMESTAMPTZ" in migration


def test_replay_rejects_negative_offset() -> None:
    class _Pool:  # never awaited; validation happens before pool use
        def acquire(self) -> None:  # pragma: no cover
            raise AssertionError("pool must not be touched for invalid arguments")

    repository = PostgresBronzeRepository(_Pool())
    with pytest.raises(ValueError, match="offset"):
        import asyncio

        asyncio.run(repository.replay(offset=-1))


def test_replay_rejects_non_positive_limit() -> None:
    class _Pool:
        def acquire(self) -> None:  # pragma: no cover
            raise AssertionError("pool must not be touched for invalid arguments")

    repository = PostgresBronzeRepository(_Pool())
    with pytest.raises(ValueError, match="limit"):
        import asyncio

        asyncio.run(repository.replay(limit=0))
