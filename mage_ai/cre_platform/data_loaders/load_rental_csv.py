"""
CRE Platform — Mage.ai Data Loader
Block:   load_rental_csv
Layer:   Ingestion (pre-Bronze)
Purpose: Reads the Australian Rental CSV from the mounted /home/src/data/
         directory using column mappings defined in schema_config.yml.

Idempotency: This block is read-only. Running it multiple times always
             produces the same DataFrame from the same source file.
"""

import os
import logging
import yaml
import pandas as pd
from pathlib import Path

# Mage decorator — marks this function as a Data Loader block
if "data_loader" not in dir():
    from mage_ai.data_preparation.decorators import data_loader, test

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# =============================================================================
# CONFIG LOADER — reads schema_config.yml once at block execution time
# =============================================================================

def load_schema_config() -> dict:
    """
    Resolves schema_config.yml relative to this file's location.
    Works whether Mage runs the block from the UI or CLI.
    """
    config_path = Path(__file__).parent.parent / "config" / "schema_config.yml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Schema config not found at {config_path}. "
            "Ensure the config/ directory is mounted correctly."
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def resolve_csv_path(dataset_cfg: dict) -> Path:
    """
    Builds the absolute path to the source CSV.
    Priority:
      1. ENV var CSV_FILENAME (set in .env for overrides)
      2. schema_config.yml source_file value
    Data directory is always /home/src/data/ (Docker mount) with a
    fallback to ./data/ for local development without Docker.
    """
    filename = os.environ.get("CSV_FILENAME") or dataset_cfg["source_file"]

    # Docker mount path (production)
    docker_path = Path("/home/src/data") / filename
    if docker_path.exists():
        return docker_path

    # Local dev fallback (running outside Docker)
    local_path = Path(__file__).parent.parent.parent / "data" / filename
    if local_path.exists():
        logger.warning(f"Docker data path not found. Using local fallback: {local_path}")
        return local_path

    raise FileNotFoundError(
        f"CSV file '{filename}' not found in /home/src/data/ or ./data/. "
        f"Ensure your CSV is placed in the ./data/ folder and the volume is mounted."
    )


# =============================================================================
# MAIN DATA LOADER BLOCK
# =============================================================================

@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Loads the Australian Rental Market CSV into a Pandas DataFrame.

    Steps:
      1. Load schema_config.yml to get column mappings
      2. Resolve the CSV file path
      3. Read only the columns defined in the config (ignore extra columns)
      4. Rename CSV columns to target snake_case names
      5. Return raw DataFrame — NO transformations here (Bronze principle)

    Returns:
        pd.DataFrame: Raw rental data with renamed columns + source metadata
                      stored in DataFrame attrs for downstream blocks.
    """
    # -------------------------------------------------------------------------
    # Step 1: Load config
    # -------------------------------------------------------------------------
    config     = load_schema_config()
    dataset_cfg = config["datasets"]["australian_rentals"]
    col_defs   = dataset_cfg["columns"]

    logger.info("Schema config loaded. Dataset: australian_rentals")
    logger.info(f"Load strategy: {dataset_cfg['load_strategy']}")

    # -------------------------------------------------------------------------
    # Step 2: Resolve CSV path
    # -------------------------------------------------------------------------
    csv_path = resolve_csv_path(dataset_cfg)
    logger.info(f"Reading CSV from: {csv_path}")

    # -------------------------------------------------------------------------
    # Step 3: Build column mapping  { csv_name → target_name }
    # -------------------------------------------------------------------------
    col_mapping = {
        col["csv_name"]: col["target_name"]
        for col in col_defs
    }
    source_columns = list(col_mapping.keys())

    # -------------------------------------------------------------------------
    # Step 4: Read CSV
    # Only load columns defined in config — resilient to extra columns in source
    # dtype=str ensures ALL columns arrive as strings (Bronze preserves raw data)
    # -------------------------------------------------------------------------
    try:
        df = pd.read_csv(
            csv_path,
            encoding=dataset_cfg.get("encoding", "utf-8"),
            sep=dataset_cfg.get("delimiter", ","),
            usecols=lambda c: c in source_columns,  # Config-driven column selection
            dtype=str,                               # Everything lands as TEXT in Bronze
            na_values=["", "N/A", "n/a", "null", "NULL", "None", "-"],
            keep_default_na=True,
        )
    except ValueError as e:
        # Some CSV columns in config may not exist — warn but don't crash
        logger.warning(f"Column selection issue: {e}. Loading all columns and filtering.")
        df = pd.read_csv(
            csv_path,
            encoding=dataset_cfg.get("encoding", "utf-8"),
            sep=dataset_cfg.get("delimiter", ","),
            dtype=str,
        )
        # Filter to only config-defined columns that actually exist
        existing = [c for c in source_columns if c in df.columns]
        missing  = [c for c in source_columns if c not in df.columns]
        if missing:
            logger.warning(f"Columns in config but not in CSV (will be NULL): {missing}")
        df = df[existing]

    # -------------------------------------------------------------------------
    # Step 5: Rename to target names (snake_case, postgres-safe)
    # -------------------------------------------------------------------------
    df.rename(columns=col_mapping, inplace=True)

    # Add any missing target columns as NaN (ensures schema consistency)
    for col in col_defs:
        if col["target_name"] not in df.columns:
            df[col["target_name"]] = pd.NA

    # -------------------------------------------------------------------------
    # Step 6: Attach metadata to DataFrame for downstream blocks
    # -------------------------------------------------------------------------
    df.attrs["source_file"]   = str(csv_path.name)
    df.attrs["dataset_name"]  = "australian_rentals"
    df.attrs["load_strategy"] = dataset_cfg["load_strategy"]
    df.attrs["bronze_schema"] = dataset_cfg["bronze_schema"]
    df.attrs["bronze_table"]  = dataset_cfg["bronze_table"]
    df.attrs["config"]        = config  # Pass full config forward

    row_count = len(df)
    logger.info(f"Loaded {row_count:,} rows × {len(df.columns)} columns from {csv_path.name}")

    return df


# =============================================================================
# MAGE TEST BLOCK — Runs automatically after the loader in the Mage UI
# =============================================================================

@test
def test_output(df: pd.DataFrame, *args) -> None:
    """Validates the loader output before passing to the transformer."""

    assert df is not None,          "Loader returned None — check CSV path"
    assert len(df) > 0,             "DataFrame is empty — CSV may be missing or malformed"
    assert "locality" in df.columns, "Required column 'locality' missing from DataFrame"
    assert "price_display_raw" in df.columns, "Required column 'price_display_raw' missing"

    # Warn if locality nulls are high (>20% is suspicious)
    null_locality_pct = df["locality"].isna().mean()
    if null_locality_pct > 0.2:
        logger.warning(
            f"High null rate in 'locality': {null_locality_pct:.1%}. Check source data."
        )

    logger.info(f"Loader test PASSED. Shape: {df.shape}")
