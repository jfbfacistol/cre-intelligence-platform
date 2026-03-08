"""
CRE Platform — Mage.ai Transformer
Block:   bronze_quality_checks
Layer:   Pre-Bronze gate
Purpose: Adds audit columns, runs data quality checks defined in schema_config.yml,
         and prepares the DataFrame for exact landing in the Bronze table.

Bronze Principle: We NEVER drop rows here. Issues are logged and flagged
                  via a _dq_warnings column. Silver dbt models do the filtering.

Idempotency: Adding audit columns with current timestamps is safe to re-run —
             each run produces a fresh set of audit metadata for that run's rows.
"""

import os
import uuid
import logging
import pandas as pd
from datetime import datetime, timezone

if "transformer" not in dir():
    from mage_ai.data_preparation.decorators import transformer, test

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# =============================================================================
# QUALITY CHECK ENGINE
# Reads rules from schema_config.yml and evaluates them against the DataFrame.
# Returns a list of warning messages per row index.
# =============================================================================

def run_quality_checks(df: pd.DataFrame, quality_rules: list) -> pd.Series:
    """
    Evaluates each quality rule from the config against the DataFrame.

    Returns:
        pd.Series: A string column where each cell contains pipe-separated
                   warning messages for that row (empty string = no issues).
    """
    # Start with empty warnings for all rows
    warnings = pd.Series([""] * len(df), index=df.index, dtype=str)

    for rule in quality_rules:
        rule_name = rule["rule"]
        action    = rule.get("action", "log_warning")
        check     = rule["check"]

        # --- not_null check ---
        if check == "not_null":
            col = rule["column"]
            if col in df.columns:
                mask = df[col].isna()
                flagged_count = mask.sum()
                if flagged_count > 0:
                    msg = f"WARN:{rule_name}:null_{col}"
                    warnings[mask] = warnings[mask].apply(
                        lambda w: f"{w}|{msg}" if w else msg
                    )
                    logger.warning(f"Quality check '{rule_name}': {flagged_count} null rows in '{col}'")

        # --- not_empty_string check ---
        elif check == "not_empty_string":
            col = rule["column"]
            if col in df.columns:
                mask = df[col].fillna("").str.strip() == ""
                flagged_count = mask.sum()
                if flagged_count > 0:
                    msg = f"WARN:{rule_name}:empty_{col}"
                    warnings[mask] = warnings[mask].apply(
                        lambda w: f"{w}|{msg}" if w else msg
                    )
                    logger.warning(f"Quality check '{rule_name}': {flagged_count} empty/null rows in '{col}'")

        # --- min_row_count check (dataset-level, not row-level) ---
        elif check == "min_row_count":
            threshold = rule.get("threshold", 100)
            if len(df) < threshold:
                msg = f"Dataset row count {len(df)} is below minimum threshold {threshold}"
                if action == "raise_error":
                    raise ValueError(f"CRITICAL quality check failed — {msg}")
                else:
                    logger.warning(msg)

        else:
            logger.warning(f"Unknown quality check type '{check}' in rule '{rule_name}' — skipping")

    return warnings


# =============================================================================
# MAIN TRANSFORMER BLOCK
# =============================================================================

@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Prepares the raw DataFrame for Bronze landing.

    Steps:
      1. Retrieve metadata from DataFrame.attrs (set by the loader)
      2. Add audit columns: _ingested_at, _source_file, _pipeline_run_id
      3. Run quality checks → populate _dq_warnings column
      4. Trim whitespace from all string columns (only safe Bronze-level clean)
      5. Return Bronze-ready DataFrame

    Args:
        df: Raw DataFrame from load_rental_csv loader block

    Returns:
        pd.DataFrame: Bronze-ready DataFrame with audit + quality columns
    """
    logger.info(f"Transformer received DataFrame: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # -------------------------------------------------------------------------
    # Step 1: Retrieve metadata passed from the loader
    # -------------------------------------------------------------------------
    config       = df.attrs.get("config", {})
    source_file  = df.attrs.get("source_file", "unknown")
    dataset_cfg  = config.get("datasets", {}).get("australian_rentals", {})
    quality_rules = dataset_cfg.get("quality_checks", [])

    # Mage passes a run_id via kwargs when available; fallback to UUID
    pipeline_run_id = kwargs.get("execution_partition") or str(uuid.uuid4())

    # -------------------------------------------------------------------------
    # Step 2: Add audit columns
    # These columns prove data lineage — which file, which run, what time.
    # -------------------------------------------------------------------------
    ingested_at = datetime.now(timezone.utc)

    df["_ingested_at"]     = ingested_at
    df["_source_file"]     = source_file
    df["_pipeline_run_id"] = pipeline_run_id

    logger.info(f"Audit columns added. Run ID: {pipeline_run_id} | File: {source_file}")

    # -------------------------------------------------------------------------
    # Step 3: Run data quality checks (Bronze = log, never drop)
    # -------------------------------------------------------------------------
    if quality_rules:
        df["_dq_warnings"] = run_quality_checks(df, quality_rules)
        warning_rows = (df["_dq_warnings"] != "").sum()
        logger.info(f"Quality checks complete. Rows with warnings: {warning_rows:,} / {len(df):,}")
    else:
        df["_dq_warnings"] = ""
        logger.info("No quality rules defined in config — skipping DQ checks")

    # -------------------------------------------------------------------------
    # Step 4: Trim whitespace from all object (string) columns
    # This is the ONLY transformation allowed in Bronze.
    # We're cleaning whitespace artifacts, not business logic.
    # -------------------------------------------------------------------------
    str_columns = df.select_dtypes(include=["object"]).columns.tolist()
    # Exclude audit columns from stripping
    strip_cols = [c for c in str_columns if not c.startswith("_")]

    df[strip_cols] = df[strip_cols].apply(
        lambda col: col.str.strip() if col.dtype == object else col
    )
    logger.info(f"Whitespace trimmed on {len(strip_cols)} string columns")

    # -------------------------------------------------------------------------
    # Step 5: Preserve metadata for the exporter block
    # -------------------------------------------------------------------------
    df.attrs["source_file"]    = source_file
    df.attrs["bronze_schema"]  = dataset_cfg.get("bronze_schema", "bronze")
    df.attrs["bronze_table"]   = dataset_cfg.get("bronze_table", "raw_rentals")
    df.attrs["load_strategy"]  = dataset_cfg.get("load_strategy", "truncate_and_insert")
    df.attrs["ingested_at"]    = ingested_at.isoformat()
    df.attrs["run_id"]         = pipeline_run_id

    logger.info(
        f"Transformer complete. "
        f"Output: {df.shape[0]:,} rows × {df.shape[1]} columns → "
        f"{df.attrs['bronze_schema']}.{df.attrs['bronze_table']}"
    )

    return df


# =============================================================================
# MAGE TEST BLOCK
# =============================================================================

@test
def test_output(df: pd.DataFrame, *args) -> None:
    """Validates transformer output before export to PostgreSQL."""

    required_audit_cols = ["_ingested_at", "_source_file", "_pipeline_run_id", "_dq_warnings"]
    for col in required_audit_cols:
        assert col in df.columns, f"Audit column '{col}' missing from transformer output"

    assert df.attrs.get("bronze_schema"), "bronze_schema not set in DataFrame attrs"
    assert df.attrs.get("bronze_table"),  "bronze_table not set in DataFrame attrs"
    assert len(df) > 0,                   "Transformer returned empty DataFrame"

    # Ensure audit timestamps are not null
    assert df["_ingested_at"].notna().all(), "_ingested_at contains null values"
    assert df["_pipeline_run_id"].notna().all(), "_pipeline_run_id contains null values"

    logger.info(f"Transformer test PASSED. Final shape: {df.shape}")
