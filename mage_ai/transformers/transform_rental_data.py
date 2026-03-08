import re
import uuid
import yaml
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer, test

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIG LOADER — reads schema_config.yml directly (not from df.attrs)
# This is more reliable than df.attrs which Mage wipes between blocks
# =============================================================================

def load_schema_config() -> dict:
    config_path = Path("/home/src/cre_platform/cre_platform/config/schema_config.yml")
    if not config_path.exists():
        logger.warning(f"schema_config.yml not found at {config_path} — using defaults")
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def clean_html(text):
    if isinstance(text, str):
        return re.sub(r'<[^>]*>', '', text).strip()
    return text

def run_quality_checks(df: pd.DataFrame, quality_rules: list) -> pd.Series:
    warnings = pd.Series([""] * len(df), index=df.index, dtype=str)
    for rule in quality_rules:
        check = rule["check"]
        col   = rule.get("column")
        if check == "not_null" and col in df.columns:
            mask = df[col].isna()
            if mask.sum() > 0:
                msg = f"WARN:{rule['rule']}:null_{col}"
                warnings[mask] = warnings[mask].apply(
                    lambda w, m=msg: f"{w}|{m}" if w else m
                )
                logger.warning(f"{rule['rule']}: {mask.sum()} null rows in '{col}'")
        elif check == "not_empty_string" and col in df.columns:
            mask = df[col].fillna("").str.strip() == ""
            if mask.sum() > 0:
                msg = f"WARN:{rule['rule']}:empty_{col}"
                warnings[mask] = warnings[mask].apply(
                    lambda w, m=msg: f"{w}|{m}" if w else m
                )
        elif check == "min_row_count":
            if len(df) < rule.get("threshold", 100):
                raise ValueError(f"Row count {len(df)} below minimum threshold")
    return warnings

@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:

    # -------------------------------------------------------------------------
    # Load config directly — don't rely on df.attrs (Mage wipes it between blocks)
    # -------------------------------------------------------------------------
    config      = load_schema_config()
    dataset_cfg = config.get("datasets", {}).get("australian_rentals", {})
    quality_rules = dataset_cfg.get("quality_checks", [])

    # Use config values with correct defaults matching your actual table
    bronze_schema = dataset_cfg.get("bronze_schema", "bronze")
    bronze_table  = dataset_cfg.get("bronze_table",  "australian_rentals")  # correct default
    load_strategy = dataset_cfg.get("load_strategy", "truncate_and_insert")
    source_file   = dataset_cfg.get("source_file",   "australian_rental_market_2026.csv")
    run_id        = kwargs.get("execution_partition") or str(uuid.uuid4())

    # 1. Clean column names
    df.columns = df.columns.str.replace(' ', '_').str.lower().str.strip()

    # 2. Clean HTML — column by column to preserve df.attrs
    str_cols = df.select_dtypes(include=["object"]).columns.tolist()
    for col in str_cols:
        df[col] = df[col].apply(clean_html)

    # 3. Whitespace trim — column by column (avoids df = df.apply() which wipes attrs)
    for col in df.select_dtypes(include=["object"]).columns:
        if not col.startswith("_"):
            df[col] = df[col].str.strip()

    # 4. Add audit columns
    df["_ingested_at"]     = datetime.now(timezone.utc)
    df["_source_file"]     = source_file
    df["_pipeline_run_id"] = run_id

    # 5. Run quality checks — log issues, never drop rows (Bronze principle)
    df["_dq_warnings"] = run_quality_checks(df, quality_rules)
    warning_rows = (df["_dq_warnings"] != "").sum()
    logger.info(f"Quality checks done. Rows with warnings: {warning_rows:,}")

    # 6. Set attrs AFTER all operations so exporter can read them
    df.attrs["bronze_schema"] = bronze_schema
    df.attrs["bronze_table"]  = bronze_table
    df.attrs["load_strategy"] = load_strategy
    df.attrs["source_file"]   = source_file
    df.attrs["run_id"]        = run_id

    logger.info(
        f"Transformer complete. {len(df):,} rows → "
        f"{bronze_schema}.{bronze_table}"
    )
    return df

@test
def test_output(df, *args) -> None:
    for col in ["_ingested_at", "_source_file", "_pipeline_run_id", "_dq_warnings"]:
        assert col in df.columns, f"Audit column '{col}' missing"
    assert df.attrs.get("bronze_schema"), "bronze_schema missing from attrs"
    assert df.attrs.get("bronze_table"),  "bronze_table missing from attrs"
    assert len(df) > 0, "Transformer returned empty DataFrame"
    logger.info(f"Transformer test PASSED. Shape: {df.shape}")