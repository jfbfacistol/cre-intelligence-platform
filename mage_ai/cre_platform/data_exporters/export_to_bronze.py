"""
CRE Platform — Mage.ai Data Exporter
Block:   export_to_bronze
Layer:   Bronze (Raw Landing Zone)
Purpose: Writes the audit-enriched DataFrame into the PostgreSQL bronze schema.
         Implements the load strategy defined in schema_config.yml.

Idempotency: The default strategy is TRUNCATE + INSERT.
             Re-running the full pipeline on the same CSV always produces
             an identical Bronze table — no phantom duplicates.

Connection:  Reads ALL credentials from environment variables (set in .env).
             Zero hardcoded credentials in this file.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

if "data_exporter" not in dir():
    from mage_ai.data_preparation.decorators import data_exporter, test

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# =============================================================================
# DATABASE CONNECTION
# Credentials sourced exclusively from environment variables.
# In Docker: set via the `environment:` block in docker-compose.yml
# Locally:   set via .env file (source .env before running)
# =============================================================================

def get_connection_string() -> str:
    """
    Builds a PostgreSQL DSN from environment variables.
    Raises a clear error if any required variable is missing.
    """
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            "Check your .env file and docker-compose.yml environment block."
        )

    host     = os.environ["POSTGRES_HOST"]     # 'cre_postgres' inside Docker
    port     = os.environ["POSTGRES_PORT"]     # 5432
    db       = os.environ["POSTGRES_DB"]       # cre_db
    user     = os.environ["POSTGRES_USER"]     # cre_user
    password = os.environ["POSTGRES_PASSWORD"] # from .env

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def get_engine():
    """Returns a SQLAlchemy engine with connection pooling."""
    conn_str = get_connection_string()
    # pool_pre_ping=True: validates connections before use (handles Docker restarts)
    return create_engine(conn_str, pool_pre_ping=True)


# =============================================================================
# LOAD STRATEGY HANDLERS
# Each strategy is idempotent for its intended use case.
# =============================================================================

def truncate_and_insert(df: pd.DataFrame, schema: str, table: str, engine) -> int:
    """
    TRUNCATE the target table, then INSERT all rows.
    Idempotent: always results in exactly the rows from this pipeline run.
    Best for: daily full refreshes of source data.
    """
    with engine.begin() as conn:
        # TRUNCATE is faster than DELETE and resets any sequences
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
        logger.info(f"Truncated {schema}.{table}")

    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="append",   # Table already exists (created by DDL); just append post-truncate
        index=False,
        method="multi",       # Batch inserts — much faster than row-by-row
        chunksize=1000,
    )
    return len(df)


def append_insert(df: pd.DataFrame, schema: str, table: str, engine) -> int:
    """
    Appends rows without checking for duplicates.
    Best for: incremental loads where source guarantees no overlaps.
    """
    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(df)


def ensure_table_exists(df: pd.DataFrame, schema: str, table: str, engine) -> None:
    """
    Creates the Bronze table if it doesn't exist, using the DataFrame schema.
    All columns are TEXT or TIMESTAMPTZ — Bronze stores everything raw.
    This makes Bronze resilient to upstream type changes.
    """
    with engine.begin() as conn:
        # Check if table exists
        result = conn.execute(text(
            f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
            )
            """
        ))
        table_exists = result.scalar()

    if not table_exists:
        logger.info(f"Table {schema}.{table} not found. Creating from DataFrame schema...")
        # Let pandas create the table structure from the DataFrame
        # We use if_exists='fail' just for the CREATE step, then truncate immediately
        empty_df = df.iloc[0:0].copy()  # Schema only, no rows
        empty_df.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="fail",
            index=False,
        )
        logger.info(f"Table {schema}.{table} created successfully")
    else:
        logger.info(f"Table {schema}.{table} already exists — skipping creation")


# =============================================================================
# MAIN DATA EXPORTER BLOCK
# =============================================================================

@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs) -> None:
    """
    Exports the Bronze-ready DataFrame to PostgreSQL.

    Steps:
      1. Read target schema/table from DataFrame.attrs
      2. Establish database connection from env vars
      3. Ensure the target table exists (create if first run)
      4. Execute the configured load strategy
      5. Log row counts for monitoring

    Args:
        df: Transformer output — audit-enriched DataFrame
    """
    # -------------------------------------------------------------------------
    # Step 1: Read destination from metadata (set by transformer)
    # -------------------------------------------------------------------------
    schema        = df.attrs.get("bronze_schema", "bronze")
    table         = df.attrs.get("bronze_table",  "raw_rentals")
    load_strategy = df.attrs.get("load_strategy", "truncate_and_insert")
    source_file   = df.attrs.get("source_file",   "unknown")
    run_id        = df.attrs.get("run_id",         "unknown")

    logger.info(f"Export target: {schema}.{table}")
    logger.info(f"Load strategy: {load_strategy}")
    logger.info(f"Source file:   {source_file}")
    logger.info(f"Pipeline run:  {run_id}")
    logger.info(f"Rows to load:  {len(df):,}")

    # -------------------------------------------------------------------------
    # Step 2: Create database engine
    # -------------------------------------------------------------------------
    try:
        engine = get_engine()
        logger.info(f"Database connection established → {os.environ['POSTGRES_HOST']}")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    # -------------------------------------------------------------------------
    # Step 3: Ensure Bronze table exists
    # -------------------------------------------------------------------------
    try:
        ensure_table_exists(df, schema, table, engine)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to ensure table {schema}.{table} exists: {e}")

    # -------------------------------------------------------------------------
    # Step 4: Execute load strategy
    # -------------------------------------------------------------------------
    strategy_map = {
        "truncate_and_insert": truncate_and_insert,
        "append":              append_insert,
    }

    strategy_fn = strategy_map.get(load_strategy)
    if not strategy_fn:
        raise ValueError(
            f"Unknown load strategy '{load_strategy}'. "
            f"Valid options: {list(strategy_map.keys())}"
        )

    try:
        rows_loaded = strategy_fn(df, schema, table, engine)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database write failed for {schema}.{table}: {e}")

    # -------------------------------------------------------------------------
    # Step 5: Verify row count post-load
    # -------------------------------------------------------------------------
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"'))
        db_count = result.scalar()

    logger.info(
        f"Export COMPLETE. "
        f"Rows written: {rows_loaded:,} | "
        f"DB row count: {db_count:,} | "
        f"Table: {schema}.{table}"
    )

    if db_count != rows_loaded:
        logger.warning(
            f"Row count mismatch: wrote {rows_loaded:,} but DB reports {db_count:,}. "
            "Investigate if this was not an append strategy."
        )


# =============================================================================
# MAGE TEST BLOCK
# =============================================================================

@test
def test_output(*args, **kwargs) -> None:
    """
    Post-export validation: connects to Postgres and verifies rows exist.
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM "bronze"."raw_rentals"'))
        count = result.scalar()

    assert count > 0, (
        "bronze.raw_rentals is empty after export. "
        "Check exporter logs for errors."
    )
    logger.info(f"Exporter test PASSED. bronze.raw_rentals has {count:,} rows.")
