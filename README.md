# Electrolux Senior Data Engineer Assessment

**Product-Focused Data Engineering & Agentic Engineering**

A production-minded ingestion and transformation pipeline for payment transaction data, built on Databricks. The pipeline fetches transactions from a Supabase REST API, validates them against a JSON schema, quarantines invalid records, deduplicates by natural key, and produces a daily account-level summary for analysts.

---

## Table of Contents

* [Overview](#overview)
* [Project Structure](#project-structure)
* [Key Features](#key-features)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
* [How to Run](#how-to-run)
* [Pipeline Tasks](#pipeline-tasks)
* [Data Quality & Testing](#data-quality--testing)
* [Configuration](#configuration)
* [Diagrams](#diagrams)
* [Production Considerations](#production-considerations)

---

## Overview

This project implements a complete data engineering pipeline across four tasks:

1. **Task 1 — Raw Ingestion**: Fetch transactions from a REST API with pagination and exponential-backoff retry, then validate, quarantine, and deduplicate.
2. **Task 2 — Daily Account Summary**: Aggregate completed transactions into a per-account, per-day summary with net amounts, top merchant categories, and currency lists.
3. **Task 3 — Incremental Ingestion**: Demonstrate watermark-based incremental loading with a 24-hour lookback window and cross-batch deduplication for idempotent runs.
4. **Task 4 — Design Note**: A product and platform judgment document covering productionization, monitoring, reusability, and trade-offs (see [docs/product_platform_note.md](docs/product_platform_note.md)).

---

## Project Structure

```
electorolux_tech_assessment/
├── .env                         # API credentials (gitignored, loaded via python-dotenv)
├── README.md                    # This file
├── pipeline_run                 # Databricks notebook — pipeline orchestrator
├── config/
│   ├── __init__.py
│   └── pipeline_config.py        # Shared config: API settings, schema rules, enums, regex
├── ingestion/
│   ├── __init__.py
│   ├── api_client.py             # API fetch with pagination, retry, CSV fallback
│   └── incremental.py            # WatermarkStore, lookback window, cross-batch dedup
├── quality/
│   ├── __init__.py
│   ├── validator.py              # Schema validation & quarantine (10+ checks per record)
│   └── dedup.py                  # Natural-key duplicate detection
├── transform/
│   ├── __init__.py
│   └── daily_summary.py          # Spark DataFrame creation & daily account summary
├── tests/
│   ├── __init__.py
│   └── quality_tests.py          # 6 dbt-style data quality tests
└── docs/
    ├── architecture_diagram.md   # System architecture (Mermaid diagram + explanation)
    ├── flow_diagram.md           # Pipeline flow diagram (Mermaid sequence diagram)
    └── product_platform_note.md  # Task 4 design note
```

---

## Key Features

* **Offset-based pagination** — fetches all records without hardcoding a total count; stops on empty or partial pages.
* **Exponential backoff retry** — retries HTTP 429/5xx, timeouts, and connection errors with configurable backoff (1s initial, 2x multiplier, 5 max attempts).
* **CSV fallback** — automatically falls back to a pre-uploaded CSV in DBFS if the API is unreachable.
* **Schema validation** — 12 validation checks per record: required fields, regex patterns, enum constraints, date range, real calendar dates, amount positivity, and `additionalProperties: false`.
* **Quarantine layer** — invalid records are routed to a dead-letter layer with `_error_reason` and `_ingested_at` metadata.
* **Natural-key deduplication** — detects duplicates across all fields except `transaction_id`; first occurrence is kept as canonical.
* **Incremental ingestion** — `WatermarkStore` tracks the high-water mark with a 24-hour lookback window for late-arriving data; cross-batch dedup by `transaction_id` ensures idempotent runs.
* **Daily account summary** — one row per `(account_id, transaction_date)` with debit/credit totals, net amount, transaction count, distinct merchants, top category by spend, and currency list.
* **Data quality tests** — 6 dbt-style assertions: not-null, unique combo, expression check, non-negative, positive count, and idempotency.
* **Environment-based secrets** — API credentials loaded from a `.env` file via `python-dotenv`; no hardcoded keys in source code.

---

## Prerequisites

* **Databricks workspace** with serverless or all-purpose compute
* **Python 3.12+** (provided by the Databricks runtime)
* **python-dotenv** — installed automatically by the pipeline notebook via `%pip install`
* **PySpark** — available in the Databricks environment
* **requests** — available in the Databricks runtime

---

## Setup

### 1. Environment File

Create a `.env` file at the project root (`/electorolux_tech_assessment/electorolux_tech_assessment/.env`) with the following variables:

```env
ASSESSMENT_API_BASE_URL=https://your-supabase-project.supabase.co/rest/v1
ASSESSMENT_API_KEY=your_api_key_here
ASSESSMENT_AUTH_TOKEN=your_auth_token_here
```

The `pipeline_config.py` module loads this file automatically at import time using `python-dotenv`. The `.env` file should **not** be committed to version control.

### 2. CSV Fallback (Optional)

If the API is network-restricted in your environment, upload the transactions CSV to DBFS and update `CSV_FALLBACK_PATH` in `config/pipeline_config.py`.

---

## How to Run

1. Open the **`pipeline_run`** notebook in your Databricks workspace.
2. Attach compute (serverless is auto-selected).
3. Run all cells from top to bottom:
   * **Setup & Imports** — installs `python-dotenv`, imports all modules, adds project root to `sys.path`.
   * **Task 1: Raw Ingestion** — fetches, validates, quarantines, deduplicates, and persists as Spark DataFrames.
   * **Task 2: Daily Account Summary** — builds the summary and runs data quality tests.
   * **Task 3: Incremental Ingestion** — demonstrates watermark-based incremental runs (full backfill, then incremental with no duplicates).
   * **Task 4: Design Note** — markdown summary of the product and platform judgment note.

---

## Pipeline Tasks

### Task 1 — Raw Ingestion with Data Quality Handling

| Stage | Module | Function | Output |
| --- | --- | --- | --- |
| Fetch | `ingestion/api_client.py` | `ingest()` → `fetch_all_transactions()` | 352 raw records (list of dicts) |
| Validate | `quality/validator.py` | `validate_and_quarantine()` | 349 valid, 3 quarantined |
| Deduplicate | `quality/dedup.py` | `detect_duplicates()` | 344 deduplicated, 5 duplicates |
| Persist | `transform/daily_summary.py` | `create_raw_dataframe()` etc. | `raw_df`, `quarantine_df`, `duplicate_df` |

### Task 2 — Daily Account Summary

| Stage | Module | Function | Output |
| --- | --- | --- | --- |
| Aggregate | `transform/daily_summary.py` | `build_daily_account_summary()` | 238 rows (20 accounts \u00d7 85 dates) |
| Test | `tests/quality_tests.py` | `run_all_tests()` | 6 tests, all passing |

**Summary columns**: `account_id`, `transaction_date`, `total_debit_amount`, `total_credit_amount`, `net_amount`, `transaction_count`, `distinct_merchants`, `top_category`, `currencies`, `updated_at`.

### Task 3 — Incremental Ingestion & Watermark Logic

| Run | Watermark | Records Fetched | New Records | Description |
| --- | --- | --- | --- | --- |
| Run 1 | None (initial) | 352 | 352 | Full backfill |
| Run 2 | 2024-03-31T23:59:59Z (adjusted) | 352 | 0 | Incremental — all records already seen |

The `WatermarkStore` applies a 24-hour lookback window (`LOOKBACK_WINDOW_HOURS = 24`) to handle late-arriving data. Cross-batch deduplication by `transaction_id` ensures no duplicate inserts.

---

## Data Quality & Testing

Six dbt-style data quality tests run against the `daily_account_summary` DataFrame:

| # | Test | Description |
| --- | --- | --- |
| 1 | `not_null` | `account_id` and `transaction_date` are never null |
| 2 | `unique` | One row per `(account_id, transaction_date)` combination |
| 3 | `expression` | `net_amount = total_credit_amount - total_debit_amount` |
| 4 | `non_negative` | `total_debit_amount` and `total_credit_amount` \u2265 0 |
| 5 | `positive` | `transaction_count > 0` for every row |
| 6 | `idempotent` | Rerunning the summary produces the same row count |

---

## Configuration

All configuration is centralized in [config/pipeline_config.py](config/pipeline_config.py). Key settings:

| Setting | Default | Description |
| --- | --- | --- |
| `API_BASE_URL` | Supabase REST endpoint | Base URL for the transactions API |
| `API_KEY` | Loaded from `.env` | API key for authentication (`apikey` header) |
| `AUTH_TOKEN` | Loaded from `.env` | Bearer token for authorization |
| `PAGE_SIZE` | 100 | Records per API page |
| `MAX_RETRIES` | 5 | Max retry attempts for transient failures |
| `INITIAL_BACKOFF` | 1.0 seconds | Initial exponential backoff delay |
| `BACKOFF_MULTIPLIER` | 2 | Backoff doubling factor |
| `LOOKBACK_WINDOW_HOURS` | 24 | Lookback window for late-arriving data |
| `CSV_FALLBACK_PATH` | DBFS path | Fallback CSV location when API is unreachable |

Schema rules (required fields, valid enums, regex patterns, date ranges) are also defined here and consumed by the validation and deduplication modules.

---

## Diagrams

* **[Architecture Diagram](docs/architecture_diagram.md)** — System architecture showing all components, layers, and their relationships (Mermaid flowchart + step-by-step explanation).
* **[Flow Diagram](docs/flow_diagram.md)** — Pipeline flow showing the step-by-step data journey from API source to final summary output (Mermaid sequence diagram).

---

## Production Considerations

See [docs/product_platform_note.md](docs/product_platform_note.md) for the full design note covering:

* Assumptions to challenge before productionizing
* Production monitoring and alerting thresholds
* Reusability patterns for the next 10 APIs
* Making `daily_account_summary` a trustworthy data product
* Lineage, ownership, and documentation in Unity Catalog
* Trade-offs made due to time constraints
* AI tool usage disclosure

---

## License

This project is a take-home assessment submission. All code and documentation are the candidate's own work, with AI assistance used as a productivity accelerator under the disclosure in the design note.
