import os
import logging
import yaml
import pandas as pd
from pathlib import Path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader, test

logger = logging.getLogger(__name__)

def load_schema_config() -> dict:
    config_path = Path("/home/src/cre_platform/cre_platform/config/schema_config.yml")
    if not config_path.exists():
        raise FileNotFoundError(f"Schema config not found at {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    config      = load_schema_config()
    dataset_cfg = config["datasets"]["australian_rentals"]
    col_defs    = dataset_cfg["columns"]

    filename = os.environ.get("CSV_FILENAME") or dataset_cfg["source_file"]
    csv_path = Path("/home/src/data") / filename

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found at: {csv_path}")

    col_mapping    = {col["csv_name"]: col["target_name"] for col in col_defs}
    source_columns = list(col_mapping.keys())

    df = pd.read_csv(
        csv_path,
        encoding=dataset_cfg.get("encoding", "utf-8"),
        dtype=str,
        na_values=["", "N/A", "n/a", "null", "NULL", "None", "-"],
        keep_default_na=True,
    )

    existing = [c for c in source_columns if c in df.columns]
    missing  = [c for c in source_columns if c not in df.columns]
    if missing:
        logger.warning(f"Columns in config but not in CSV: {missing}")
    df = df[existing]

    df.rename(columns=col_mapping, inplace=True)

    for col in col_defs:
        if col["target_name"] not in df.columns:
            df[col["target_name"]] = pd.NA

    df.attrs["source_file"]   = str(csv_path.name)
    df.attrs["dataset_name"]  = "australian_rentals"
    df.attrs["load_strategy"] = dataset_cfg["load_strategy"]
    df.attrs["bronze_schema"] = dataset_cfg["bronze_schema"]
    df.attrs["bronze_table"]  = dataset_cfg["bronze_table"]
    df.attrs["config"]        = config

    logger.info(f"Loaded {len(df):,} rows from {csv_path.name}")
    return df

@test
def test_output(df, *args) -> None:
    assert df is not None,           "Loader returned None"
    assert len(df) > 0,              "DataFrame is empty"
    assert "locality" in df.columns, "Required column 'locality' missing"
    logger.info(f"Loader test PASSED. Shape: {df.shape}")