import os
import yaml
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter, test

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIG LOADER — reads schema_config.yml directly (not from df.attrs)
# df.attrs is wiped by Mage between blocks — never rely on it in the exporter
# =============================================================================

def load_schema_config() -> dict:
    config_path = Path("/home/src/cre_platform/cre_platform/config/schema_config.yml")
    if not config_path.exists():
        logger.warning(f"schema_config.yml not found — using defaults")
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_engine():
    host     = os.environ["POSTGRES_HOST"]
    port     = os.environ["POSTGRES_PORT"]
    db       = os.environ["POSTGRES_DB"]
    user     = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True
    )

def ensure_table_exists(df: pd.DataFrame, schema: str, table: str, engine) -> None:
    with engine.begin() as conn:
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
        logger.info(f"Table {schema}.{table} not found — creating from DataFrame schema")
        empty_df = df.iloc[0:0].copy()
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

@data_exporter
def export_data_to_postgres(df: pd.DataFrame, **kwargs) -> None:

    # -------------------------------------------------------------------------
    # Read config directly — never rely on df.attrs between Mage blocks
    # -------------------------------------------------------------------------
    config      = load_schema_config()
    dataset_cfg = config.get("datasets", {}).get("australian_rentals", {})

    schema        = dataset_cfg.get("bronze_schema", "bronze")
    table         = dataset_cfg.get("bronze_table",  "australian_rentals")  # correct default
    load_strategy = dataset_cfg.get("load_strategy", "truncate_and_insert")

    logger.info(f"Export target:  {schema}.{table}")
    logger.info(f"Load strategy:  {load_strategy}")
    logger.info(f"Rows to load:   {len(df):,}")

    # -------------------------------------------------------------------------
    # Connect to PostgreSQL
    # -------------------------------------------------------------------------
    try:
        engine = get_engine()
        logger.info(f"Connected → {os.environ['POSTGRES_HOST']}")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    # -------------------------------------------------------------------------
    # Ensure table exists (creates it on first run)
    # -------------------------------------------------------------------------
    try:
        ensure_table_exists(df, schema, table, engine)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to ensure table {schema}.{table} exists: {e}")

    # -------------------------------------------------------------------------
    # Execute load strategy — TRUNCATE + INSERT is idempotent
    # -------------------------------------------------------------------------
    if load_strategy == "truncate_and_insert":
        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
            logger.info(f"Truncated {schema}.{table}")

    try:
        df.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database write failed for {schema}.{table}: {e}")

    # -------------------------------------------------------------------------
    # Verify row count post-load
    # -------------------------------------------------------------------------
    with engine.connect() as conn:
        db_count = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        ).scalar()

    logger.info(f"Export COMPLETE. DB row count: {db_count:,} | Table: {schema}.{table}")

@test
def test_output(*args, **kwargs) -> None:
    # Read correct table name from config — never hardcode
    config      = load_schema_config()
    dataset_cfg = config.get("datasets", {}).get("australian_rentals", {})
    schema      = dataset_cfg.get("bronze_schema", "bronze")
    table       = dataset_cfg.get("bronze_table",  "australian_rentals")

    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        ).scalar()

    assert count > 0, f"{schema}.{table} is empty after export"
    logger.info(f"Exporter test PASSED. {count:,} rows in {schema}.{table}")