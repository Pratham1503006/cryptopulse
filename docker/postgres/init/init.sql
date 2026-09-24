-- CryptoPulse PostgreSQL initialization
-- This script runs on first container startup.
--
-- Creates the four data-layer schemas. Table creation is owned by the
-- canonical warehouse migrations (warehouse/migrations/sql/) so the schema
-- exists in exactly one authoritative form. Apply them after the
-- infrastructure is up with:
--
--     python -m warehouse.migrations
--
-- (docker-compose.yml also runs the migration runner as a one-shot init
-- service, so a fresh stack is migrated automatically.)

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS warehouse;
