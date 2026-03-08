# CRE Market Intelligence Platform

> A cloud-native data engineering pipeline that ingests, transforms, and serves Australian rental market data through a Bronze → Silver → Gold medallion architecture.

---

## Overview

This project demonstrates a production-grade, end-to-end data engineering platform built entirely with open-source tools running locally via Docker. Raw CSV data enters as Bronze, gets cleaned and typed in Silver, and emerges as investment-grade aggregations in Gold — fully automated, idempotent, and tested.

**Key result:** 6,767 raw rental listings → 3 analyst-ready Gold tables, 22/22 data quality tests passing, across 8 Australian states.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     LOCAL MACHINE                           │
│                                                             │
│  ┌──────────────┐    ┌─────────────────────────────────┐   │
│  │  Terraform   │───▶│         Docker Network          │   │
│  │ (IaC)        │    │                                 │   │
│  └──────────────┘    │  ┌─────────────┐ ┌───────────┐ │   │
│                       │  │  Mage.ai    │ │ PostgreSQL│ │   │
│  ┌──────────────┐    │  │ (Ingestion) │ │(Warehouse)│ │   │
│  │  Source CSV  │───▶│  └──────┬──────┘ └─────┬─────┘ │   │
│  └──────────────┘    │         │               │       │   │
│                       │         ▼               │       │   │
│  ┌──────────────┐    │  ┌─────────────┐        │       │   │
│  │     dbt      │───▶│  │   Bronze    │────────┘       │   │
│  │(Transform)   │    │  │   Silver    │                │   │
│  └──────────────┘    │  │    Gold     │                │   │
│                       │  └─────────────┘                │   │
│                       └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Medallion Architecture

| Layer | Owner | Schema | Purpose |
|-------|-------|--------|---------|
| **Bronze** | Mage.ai | `bronze` | Raw data, never modified. Audit columns added. |
| **Silver** | dbt | `silver` | Cleaned, typed, validated. Business-ready. |
| **Gold** | dbt | `gold` | Investment-grade aggregations for analysts. |

---

## Tech Stack

| Tool | Role | Version |
|------|------|---------|
| **Terraform** | Infrastructure as Code — provisions Docker containers | `~> 1.x` |
| **Docker** | Containerises Mage.ai and PostgreSQL | Desktop |
| **Mage.ai** | Orchestration — ingestion pipeline with 3 blocks + daily schedule trigger | Latest |
| **PostgreSQL** | Data warehouse — hosts Bronze/Silver/Gold schemas | `16` |
| **dbt-core** | Transformation — Silver and Gold models | `1.11.7` |
| **Python** | Mage pipeline blocks | `3.12` |

---

## Project Structure

```
cre-market-platform/
│
├── main.tf                         # Terraform — Docker provider, network, volumes, containers
├── docker-compose.yml              # Docker Compose (external resources owned by Terraform)
├── .env                            # Credentials (gitignored)
├── .env.example                    # Safe credential template for repo
│
├── data/
│   └── australian_rental_market_2026.csv   # Source data (6,767 rows × 16 columns)
│
├── postgres/
│   └── init.sql                    # Schema bootstrap: creates bronze/silver/gold schemas
│
├── mage_ai/                        # Mage.ai project (bind-mounted into container)
│   ├── io_config.yaml              # DB connection config (uses env vars)
│   ├── metadata.yaml               # Mage project metadata
│   ├── cre_platform/
│   │   └── config/
│   │       └── schema_config.yml   # Column mapping, quality rules, load strategy
│   ├── data_loaders/
│   │   └── load_rental_data.py     # Reads CSV, applies column mapping from config
│   ├── transformers/
│   │   └── transform_rental_data.py # Cleans HTML, trims whitespace, adds audit columns, DQ checks
│   ├── data_exporters/
│   │   └── export_to_postgres.py   # Truncate + Insert into bronze.australian_rentals
│   └── pipelines/
│       └── cre_bronze_ingestion/   # DAG: loader → transformer → exporter
│
└── dbt_project/                    # dbt project
    ├── dbt_project.yml             # Project config, schema routing, vars
    ├── profiles.yml                # DB connection (reads env vars)
    ├── macros/
    │   └── generate_schema_name.sql # Prevents dbt schema name concatenation bug
    └── models/
        ├── schema.yml              # Column docs + 22 automated tests
        ├── staging/
        │   └── sources.yml         # Declares bronze.australian_rentals as dbt source
        ├── silver/
        │   └── stg_rentals.sql     # Cleans + types Bronze data
        └── gold/
            ├── gold_suburb_summary.sql   # Avg rent, market signal by suburb (2,071 rows)
            ├── gold_property_type.sql    # Rent per bedroom by property type (66 rows)
            └── gold_state_summary.sql    # National overview by state (8 rows)
```

---

## Module 1 — Infrastructure (Terraform + Docker)

Infrastructure is fully declarative. A single command provisions everything:

```powershell
# Set DB password and apply
$env:TF_VAR_db_password="your_password"
terraform apply
```

Terraform creates:
- Docker network `cre_platform_network`
- Docker volume `cre_postgres_data` (persistent)
- PostgreSQL container `cre_postgres` (port 5432)
- Mage.ai container `cre_mage` (port 6789)

PostgreSQL is initialised with `init.sql` which creates the three schemas at startup:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

---

## Module 2 — Ingestion (Mage.ai)

Pipeline: **`cre_bronze_ingestion`**

```
load_rental_data → transform_rental_data → export_to_postgres
```

Runs automatically every day at **7:00 AM Philippine Time (23:00 UTC)** via Mage's built-in schedule trigger.

### Block 1 — Loader
- Reads `schema_config.yml` for column mapping configuration
- Loads CSV from `/home/src/data/australian_rental_market_2026.csv`
- Selects and renames columns per config

### Block 2 — Transformer
- Strips HTML tags from text fields
- Trims whitespace column-by-column
- Adds audit columns: `_ingested_at`, `_source_file`, `_pipeline_run_id`
- Runs configurable data quality checks → `_dq_warnings` column
- **Bronze Principle:** never drops rows — flags issues only

### Block 3 — Exporter
- Reads target schema/table from `schema_config.yml`
- Executes `TRUNCATE + INSERT` for idempotency
- Verifies row count post-load

**Result:** `bronze.australian_rentals` — 6,767 rows with 4 audit columns.

---

## Module 3 — Transformation (dbt)

### Silver Model: `stg_rentals`
Reads from `bronze.australian_rentals`, produces `silver.stg_rentals`.

Key transformations:
- `price_display_raw` → `weekly_rent` (NUMERIC) with regex validation
- `bedrooms_raw` / `bathrooms_raw` / `parking_raw` → INTEGER with safe casting
- `listed_date_raw` → DATE with format detection
- Price tier classification: Budget / Mid-Range / Premium / Luxury
- Data quality boolean flags: `has_valid_price`, `has_valid_locality`, `has_valid_bedrooms`

### Gold Models

**`gold_suburb_summary`** — 2,071 rows
Investment summary by suburb including avg/median rent, price tier distribution, and market signal classification (e.g. "High Demand - Premium").

**`gold_property_type`** — 66 rows
Rental breakdown by property type × state. Includes `avg_rent_per_bedroom` as a yield indicator and market share within each state.

**`gold_state_summary`** — 8 rows
National overview ranked by average weekly rent.

| State | Avg Weekly Rent | Listings | National Rank |
|-------|----------------|----------|---------------|
| WA | $796.78 | 1,020 | 1 |
| NSW | $784.61 | 2,698 | 2 |
| QLD | $769.77 | 852 | 3 |
| NT | $710.00 | 19 | 4 |
| ACT | $660.34 | 266 | 5 |
| VIC | $634.35 | 1,118 | 6 |
| SA | $609.12 | 742 | 7 |
| TAS | $569.71 | 52 | 8 |

---

## Running the Platform

### Prerequisites
- Docker Desktop (with WSL2 backend on Windows)
- Terraform
- Python + pip
- dbt-postgres (`pip install dbt-postgres`)

### 1 — Start Infrastructure

```powershell
cd C:\Portfolio\cre-market-platform
$env:TF_VAR_db_password="cremp_2026"
terraform apply
```

### 2 — Run Ingestion Pipeline

The pipeline runs automatically every day at **7:00 AM Philippine Time** via the Mage schedule trigger.

To run it manually, open Mage UI at `http://localhost:6789`, navigate to `cre_bronze_ingestion`, and click **Run Pipeline**.

Verify:
```powershell
docker exec -it cre_postgres psql -U cre_user -d cre_db -c "SELECT COUNT(*) FROM bronze.australian_rentals;"
# Expected: 6767
```

### 3 — Run dbt Transformations

```powershell
cd C:\Portfolio\cre-market-platform\dbt_project

dbt debug   # Verify connection
dbt run     # Execute all models
dbt test    # Run 22 data quality tests
```

### 4 — Verify Gold Tables

```powershell
docker exec -it cre_postgres psql -U cre_user -d cre_db -c "
SELECT state, avg_weekly_rent, total_listings, national_rent_rank
FROM gold.gold_state_summary
ORDER BY national_rent_rank;"
```

### Teardown

```powershell
cd C:\Portfolio\cre-market-platform
terraform destroy
```

---

## Data Quality

The platform implements a two-layer DQ strategy:

**Layer 1 — Mage (Bronze)**
Configurable rules in `schema_config.yml`:
- `not_null` checks on critical columns
- `not_empty_string` checks
- `min_row_count` threshold (raises error if < 100 rows)
- Violations logged to `_dq_warnings` — rows are **never dropped** in Bronze

**Layer 2 — dbt (Silver + Gold)**
22 automated tests defined in `schema.yml`:
- `not_null` tests on key columns
- `unique` tests (e.g. state in `gold_state_summary`)
- `accepted_values` tests (price tiers, market signals, state codes)

---

## Automation

The pipeline is scheduled to run automatically via Mage's built-in trigger system — no external scheduler required.

| Automation | Tool | Config |
|---|---|---|
| Daily schedule | Mage Triggers | Every day at 7:00 AM PHT (23:00 UTC) |

The trigger is configured in the Mage UI under **Triggers → daily_7am_pht**. When active, Mage automatically runs the full Bronze ingestion pipeline on schedule. dbt transformations are run manually after ingestion or can be wired in as a subsequent step.

---

## Key Design Decisions

**Why Terraform instead of just Docker Compose?**

Terraform enforces infrastructure as code with state management. It prevents configuration drift and makes the setup fully reproducible from a single `terraform apply`.

**Why bind mounts for Mage?**

Live editing — changes to pipeline code are reflected immediately without container restarts. Essential for iterative development.

**Why `TRUNCATE + INSERT` for Bronze?**

Full idempotency. Re-running the pipeline on the same source file always produces an identical Bronze table. No phantom duplicates.

**Why dbt for Silver/Gold instead of more Mage blocks?**

Clear separation of concerns. Mage owns orchestration and ingestion; dbt owns transformation and testing. Each tool does what it's best at.

**Why never drop rows in Bronze?**

Bronze is a historical record. Data quality issues are flagged and passed downstream — Silver dbt models decide what to filter. This makes debugging and reprocessing trivial.

---

## Environment Variables

| Variable | Description | Used By |
|----------|-------------|---------|
| `POSTGRES_HOST` | DB hostname (`cre_postgres` in Docker, `localhost` locally) | Mage, dbt |
| `POSTGRES_PORT` | DB port (5432) | Mage, dbt |
| `POSTGRES_DB` | Database name (`cre_db`) | Mage, dbt |
| `POSTGRES_USER` | DB user (`cre_user`) | Mage, dbt |
| `POSTGRES_PASSWORD` | DB password | Mage, dbt |
| `DBT_POSTGRES_HOST` | dbt-specific host override | dbt |
| `DBT_POSTGRES_PASSWORD` | dbt-specific password | dbt |

Copy `.env.example` to `.env` and fill in your values. Never commit `.env`.

---

## Author

Jfbfacistol

Infrastructure as Code · Pipeline Orchestration · Medallion Architecture · Analytics Engineering · Data Quality Testing · Pipeline Scheduling
