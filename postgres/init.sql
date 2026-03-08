-- =============================================================================
-- CRE Platform — PostgreSQL Initialization Script
-- Runs automatically on first container boot via docker-entrypoint-initdb.d/
-- Idempotent: CREATE SCHEMA IF NOT EXISTS is safe to run multiple times.
--
-- DYNAMIC USER: PostgreSQL runs this script AS the POSTGRES_USER defined in
-- your .env. We use current_user inside a DO block so the GRANT statements
-- always target whoever that user is — no hardcoded username anywhere.
-- Change POSTGRES_USER in .env and this script still works with zero edits.
-- =============================================================================

-- Medallion Architecture Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------------
-- Dynamic GRANT using current_user (the POSTGRES_USER from your .env)
-- PostgreSQL's GRANT does not accept current_user directly, so we use
-- a DO block with EXECUTE + format() to inject the username safely.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  app_user TEXT := current_user;
BEGIN
  -- Schema-level privileges
  EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA bronze TO %I', app_user);
  EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA silver TO %I', app_user);
  EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA gold   TO %I', app_user);

  -- Default privileges: any future tables in these schemas are also accessible
  EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO %I', app_user);
  EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO %I', app_user);
  EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES TO %I', app_user);

  RAISE NOTICE 'Schemas bronze/silver/gold created and granted to: %', app_user;
END
$$;
