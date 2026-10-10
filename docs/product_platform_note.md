# Task 4: Product & Platform Judgment Note

## 1. Assumptions to Challenge Before Productionizing

- **API rate limits and SLA**: The current retry logic handles 429/5xx, but we should confirm the API's documented rate limits and negotiate an SLA. If the API has hard daily call limits, pagination with small page sizes could exhaust the budget.
- **Schema stability**: The JSON schema is assumed to be fixed. In production, schema evolution (new fields, changed enums) must be handled gracefully — either via schema registry or by making the validator configurable rather than hardcoded.
- **Single-tenant assumption**: The pipeline assumes one data source. If the platform serves multiple APIs with different schemas, the validation layer should be pluggable (schema-per-source).
- **Idempotency via transaction_id**: We assume transaction_id is truly unique and immutable. If the source can reassign IDs, we need a composite key or an ingestion-level surrogate key.
- **Time zone handling**: All timestamps are UTC. If the source ever introduces non-UTC timestamps, the parser and watermark logic must handle timezone conversion.

## 2. Production Monitoring & Alerts

| Metric | Alert Condition | Severity |
|---|---|---|
| Ingestion success rate | < 95% over 24h | P1 |
| Records quarantined per run | Spike > 3σ from 7-day rolling average | P2 |
| Duplicate rate per run | > 5% of fetched records | P2 |
| API latency per page | p95 > 10s | P3 |
| Watermark staleness | No successful run in > 26h | P1 |
| Daily summary row count | Deviation > 20% from 7-day average | P2 |
| Pipeline duration | > 30 min (expected ~5 min) | P3 |

Additionally: data freshness SLAs (max lag between source and raw layer), quarantine queue depth monitoring, and alerting on schema drift (unexpected new fields in source data).

## 3. Reusability for the Next 10 APIs

- **Config-driven validation**: Schema rules are centralized in `config/pipeline_config.py`. For new APIs, only a new config file is needed. The validator reads config, not hardcoded constants.
- **Generic ingestion framework**: The fetch-validate-quarantine-dedup pipeline is modular. New APIs plug in: base URL, auth config, pagination strategy, schema config, and natural key definition.
- **Standardized layer schema**: All raw layers share the same metadata columns (`_ingested_at`, `_source_api`, `_batch_id`). Quarantine tables share `_error_reason`, `_ingested_at`. This enables unified monitoring across sources.
- **Watermark store as a service**: A single watermark table tracks all pipelines, enabling centralized monitoring of data freshness across the platform.

## 4. Making daily_account_summary a Trustworthy Data Product

- **Documentation**: Column-level descriptions, data types, and business definitions published in Unity Catalog. Analysts can discover the table and understand each field without asking the data team.
- **Data quality visibility**: Surface test results (pass/fail counts, freshness) via a data quality dashboard or system tables. Consumers can check quality status before trusting the data.
- **SLA and freshness guarantees**: Define and publish an SLA (e.g., "data updated by 8 AM UTC daily"). Monitor and alert on SLA breaches.
- **Change management**: Version the summary schema. Breaking changes (renamed columns, changed logic) go through a review process with downstream consumer notification.
- **Lineage**: Unity Catalog lineage should show raw → summary flow, so analysts can trace any summary value back to its source records.

## 5. Lineage, Ownership, Documentation & Data Quality Exposure

- **Lineage**: Unity Catalog automatically captures table-level lineage for Spark and SQL operations. Column-level lineage can be enabled for finer-grained tracing. Display lineage in the Catalog Explorer UI.
- **Ownership**: Assign a named owner (team or individual) to each table via Unity Catalog `ALTER TABLE ... OWNER TO`. Surface ownership in the catalog UI.
- **Documentation**: Use `COMMENT ON TABLE` and `COMMENT ON COLUMN` to embed descriptions directly in the catalog. Alternatively, use Databricks dashboards for richer documentation.
- **Data quality status**: Publish test results to a `data_quality_results` table. Create a dashboard showing pass/fail trends. Optionally, add a "quality badge" column to the summary table (e.g., `dq_status = 'passed'`) that consumers can filter on.

## 6. Trade-offs Due to Time Limit

- **In-memory watermark store**: A production implementation would persist the watermark in a Delta table or key-value store. The in-memory store is for demonstration only.
- **No Delta MERGE**: Production would use `MERGE INTO` for idempotent upserts. The current implementation uses in-Python deduplication, which is correct but less scalable.
- **CSV fallback**: The API may be network-restricted in some environments. The CSV fallback ensures the pipeline is runnable, but a production system would not need this.
- **No streaming**: The pipeline is batch-only. A streaming variant with Structured Streaming + Auto Loader would handle real-time ingestion.
- **Limited test coverage**: Six tests are included. A production system would have unit tests for each validation rule, integration tests for the API client, and end-to-end pipeline tests.

## 7. AI Tool Usage Disclosure

- **Tool used**: Databricks Genie Code (AI assistant) was used to scaffold the notebook structure, generate boilerplate code for pagination/retry logic, and draft this design note.
- **Verification**: All generated code was reviewed for correctness — schema validation rules were checked against the JSON schema, duplicate detection logic was verified against the assessment requirements, and test assertions were manually validated against expected outcomes.
- **Accountability**: The design decisions, trade-offs, and architecture are the candidate's own. AI was used as a productivity accelerator, not as a replacement for engineering judgment.
