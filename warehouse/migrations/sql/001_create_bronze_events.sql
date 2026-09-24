-- Migration: 001_create_bronze_events
-- Creates the physical Bronze table that persists the canonical InternalEvent.
--
-- The Bronze table records every InternalEvent the platform has successfully
-- received, before any validation. It is an append-only landing zone:
--
-- - price / quantity are unconstrained NUMERIC so exact Decimal values are
--   preserved at any precision/scale (no binary floating point is used for
--   financial values, and no fixed scale can silently round them).
-- - event_type / side are TEXT holding the canonical StrEnum values; the
--   same representation used by the wire codec. Values are constrained to
--   the contract's members so rows can always be reconstructed exactly.
-- - received_at / trade_timestamp are TIMESTAMPTZ so timezone information
--   is preserved and never silently dropped.
-- - payload is JSONB preserving the event's additional exchange-specific data.
-- - The primary key on event_id is technical row identity only, NOT business
--   duplicate detection. Duplicate detection belongs to the Validation Engine.
--
-- This file is applied by warehouse.migrations.runner and by the Docker
-- first-startup initialization (docker/postgres/init/init.sql).

BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.events (
    event_id        TEXT        PRIMARY KEY,
    source          TEXT        NOT NULL,
    event_type      TEXT        NOT NULL
                    CONSTRAINT bronze_event_type_allowed
                    CHECK (event_type IN ('trade', 'ticker', 'orderbook', 'unknown')),
    received_at     TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    price           NUMERIC     NOT NULL,
    quantity        NUMERIC     NOT NULL,
    side            TEXT        NOT NULL
                    CONSTRAINT bronze_side_allowed
                    CHECK (side IN ('buy', 'sell')),
    trade_id        TEXT        NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);

-- Ordered replay index supporting BronzeRepository.replay().
CREATE INDEX IF NOT EXISTS bronze_events_received_at_idx
    ON bronze.events (received_at);

-- Migration history tracking (shared by all future warehouse migrations).
-- The warehouse schema is created here (and in the Docker init script) so this
-- migration is self-sufficient on a clean PostgreSQL instance.
CREATE TABLE IF NOT EXISTS warehouse.migration_history (
    migration      TEXT        PRIMARY KEY,
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO warehouse.migration_history (migration)
VALUES ('001_create_bronze_events')
ON CONFLICT (migration) DO NOTHING;

COMMIT;
