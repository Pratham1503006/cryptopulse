-- Migration: 004_create_gold_market_windows
-- Creates the physical Gold table holding time-windowed market aggregates
-- derived ONLY from trusted (Silver) events.
--
-- Gold is business information: it does not represent individual trades. It
-- is produced by the processing engine from Silver and never from Bronze or
-- Quarantine, preserving the platform's trust boundary.
--
-- Storage decisions mirror Bronze/Silver (001, 002):
--
-- - open/high/low/close/volume/buy_volume/sell_volume/vwap use unconstrained
--   NUMERIC so exact Decimal values are preserved without rounding and no
--   binary floating point is used for financial values.
-- - window_start/window_end/computed_at are TIMESTAMPTZ so the UTC window
--   semantics are preserved.
-- - window_id is the PRIMARY KEY and the stable technical identity of a
--   window: recomputing a window upserts the same row (idempotent) instead
--   of appending a duplicate.
-- - (symbol, window_start) is UNIQUE because that pair is the semantic
--   identity of a window: one aggregate per symbol per window.
-- - is_final marks whether the window is closed for late arrivals
--   (window_end + allowed lateness has elapsed).
--
-- This file is applied by warehouse.migrations.runner.

BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.market_windows (
    window_id       TEXT        PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    open            NUMERIC     NOT NULL,
    high            NUMERIC     NOT NULL,
    low             NUMERIC     NOT NULL,
    close           NUMERIC     NOT NULL,
    trade_count     INTEGER     NOT NULL,
    volume          NUMERIC     NOT NULL,
    buy_volume      NUMERIC     NOT NULL,
    sell_volume     NUMERIC     NOT NULL,
    vwap            NUMERIC     NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL,
    is_final        BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT gold_market_windows_bounds CHECK (window_end > window_start),
    CONSTRAINT gold_market_windows_ohlc CHECK (high >= low),
    CONSTRAINT gold_market_windows_count CHECK (trade_count >= 0),
    CONSTRAINT gold_market_windows_symbol_start_key UNIQUE (symbol, window_start)
);

-- Supports "latest windows for a symbol" and "latest windows overall".
CREATE INDEX IF NOT EXISTS gold_market_windows_symbol_start_idx
    ON gold.market_windows (symbol, window_start DESC);

CREATE INDEX IF NOT EXISTS gold_market_windows_start_idx
    ON gold.market_windows (window_start DESC);

COMMIT;
