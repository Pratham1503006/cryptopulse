-- Migration: 003_create_quarantine_events
-- Creates the physical Quarantine table preserving rejected InternalEvents.
--
-- Quarantine preserves every event the Validation Engine rejected, together
-- with the structured validation failures that caused the rejection, so
-- rejected events remain available for investigation and possible replay.
-- Rejecting an event never deletes it from Bronze: Quarantine is a parallel
-- record of the platform's operational history, not a replacement for it.
--
-- Storage decisions mirror Bronze (001_create_bronze_events.sql):
--
-- - price / quantity are unconstrained NUMERIC so exact Decimal values are
--   preserved at any precision/scale.
-- - event_type / side are TEXT holding the canonical StrEnum values,
--   CHECK-constrained to the contract's members.
-- - received_at / trade_timestamp / quarantined_at are TIMESTAMPTZ so
--   timezone information is preserved. quarantined_at records when the
--   platform rejected the event.
-- - payload is JSONB preserving the original exchange-specific data.
-- - failures is JSONB holding the structured ValidationFailure list
--   (rule/code/message), so the rejection reason survives as data.
-- - Technical identity is (event_id, quarantined_at): each processing
--   attempt that rejects an event appends a distinct record, so repeated
--   processing attempts remain visible. This is deliberately NOT business
--   duplicate detection, and trade_id is NOT unique.
--
-- This file is applied by warehouse.migrations.runner.

BEGIN;

CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE TABLE IF NOT EXISTS quarantine.events (
    event_id        TEXT        NOT NULL,
    source          TEXT        NOT NULL,
    event_type      TEXT        NOT NULL
                    CONSTRAINT quarantine_event_type_allowed
                    CHECK (event_type IN ('trade', 'ticker', 'orderbook', 'unknown')),
    received_at     TIMESTAMPTZ NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    price           NUMERIC     NOT NULL,
    quantity        NUMERIC     NOT NULL,
    side            TEXT        NOT NULL
                    CONSTRAINT quarantine_side_allowed
                    CHECK (side IN ('buy', 'sell')),
    trade_id        TEXT        NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    failures        JSONB       NOT NULL,

    PRIMARY KEY (event_id, quarantined_at)
);

-- Ordered read index supporting QuarantineRepository.read_rejected().
CREATE INDEX IF NOT EXISTS quarantine_events_quarantined_at_idx
    ON quarantine.events (quarantined_at);

COMMIT;
