-- =============================================================================
-- One-time Snowflake environment setup for the sales pipeline.
--
-- Creates the warehouse, database, RAW schema, loader role, and service user
-- used by transformer_app. Run once, as a role that can create these objects
-- (e.g. SYSADMIN + SECURITYADMIN, or ACCOUNTADMIN).
--
-- Authentication is RSA key-pair only — no password is set on the service user.
-- Generate the key pair and register the public key as described in RUN_LOCAL.md.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Warehouse — sized for a per-day bulk load, suspends quickly to limit cost
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS SALES_WH WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- -----------------------------------------------------------------------------
-- Database and raw landing schema
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SALES_DB;
CREATE SCHEMA   IF NOT EXISTS SALES_DB.RAW;

-- -----------------------------------------------------------------------------
-- Loader role
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS SALES_LOADER_ROLE;

GRANT USAGE ON WAREHOUSE SALES_WH      TO ROLE SALES_LOADER_ROLE;
GRANT USAGE ON DATABASE  SALES_DB      TO ROLE SALES_LOADER_ROLE;
GRANT USAGE ON SCHEMA    SALES_DB.RAW  TO ROLE SALES_LOADER_ROLE;

-- The loader deletes and re-inserts a business day, and runs PARSE_JSON updates,
-- so it needs DML on every table in the schema.
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA SALES_DB.RAW TO ROLE SALES_LOADER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE
  ON FUTURE TABLES IN SCHEMA SALES_DB.RAW TO ROLE SALES_LOADER_ROLE;

-- -----------------------------------------------------------------------------
-- Service user — key-pair auth, no password, no MFA to renew
--
-- After creating, register the public key body (no BEGIN/END lines):
--   ALTER USER SALES_LOADER_USER SET RSA_PUBLIC_KEY='<public key body>';
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS SALES_LOADER_USER
  DEFAULT_ROLE      = SALES_LOADER_ROLE
  DEFAULT_WAREHOUSE = SALES_WH
  TYPE              = SERVICE
  COMMENT           = 'Service account for the sales ELT transformer';

GRANT ROLE SALES_LOADER_ROLE TO USER SALES_LOADER_USER;

-- -----------------------------------------------------------------------------
-- Next: sql/003_sales_tables.sql
-- -----------------------------------------------------------------------------
