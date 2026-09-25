-- Migration: 002_create_silver_events
-- Creates the physical Silver table that persists the canonical TrustedEvent.
--
-- Silver contains ONLY events that passed the Validation Engine: the
-- platform's trusted events, ready for downstream processing (Processing
-- Engine / Gold in a later milestone). It is populated exclusively through
-- the SilverRepository; validation itself never writes SQL.
--
-- Storage decisions mirror Bronze (001_create_bronze_events.sql):
--
-- - price / quantity are unconstrained NUMERIC so exact Decimal values are
--   preserved at any precision/scale (no binary floating point for
--   financial values, no fixed scale that could silently round them).
-- - event_type / side are TEXT holding the canonical StrEnum values, the
--   same representation used by the wire codec; CHECK-constrained to the
--   contract's members so rows can always be reconstructed exactly.
-- - received_at / trade_timestamp / validated_at are TIMESTAMPTZ so
--   timezone information is preserved and never silently dropped.
--   validated_at records when the event passed validation.
-- - payload is JSONB preserving the event's additional exchange-specific
--   data.
-- - The primary key on event_id is technical row identity only, matching
--   the canonical event's technical identity: a repeated processing
--   attempt for the same canonical event is idempotent (ON CONFLICT DO
--   NOTHING). It is NOT business duplicate detection: business duplicate
--   semantics are not defined by the architecture yet, and trade_id is
--   deliberately NOT unique.
--
-- This file is applied by warehouse.migrations.runner.

BEGIN;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.events (
    event_id        TEXT        PRIMARY KEY,
    source          TEXT        NOT NULL,
    event_type      TEXT        NOT NULL
                    CONSTRAINT silver_event_type_allowed
                    CHECK (event_type IN ('trade', 'ticker', 'orderbook', 'unknown')),
    received_at     TIMESTAMPTZ NOT NULL,
    validated_at    TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    price           NUMERIC     NOT NULL,
    quantity        NUMERIC     NOT NULL,
    side            TEXT        NOT NULL
                    CONSTRAINT silver_side_allowed
                    CHECK (side IN ('buy', 'sell')),
    trade_id        TEXT        NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);

-- Ordered read index supporting SilverRepository.read().
CREATE INDEX IF NOT EXISTS silver_events_validated_at_idx
    ON silver.events (validated_at);

COMMIT;
