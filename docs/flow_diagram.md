# Pipeline Flow Diagram

This document shows the step-by-step data flow of the transaction ingestion pipeline, from API source to the final daily account summary output.

---

## High-Level Flow

```mermaid
flowchart LR
    %% ─── Source ───
    API["🌐 Supabase REST API<br/>/rest/v1/transactions"]
    CSV["📄 CSV Fallback<br/>DBFS upload"]
    ENV["🔑 .env File<br/>API credentials"]

    %% ─── Ingestion ───
    FETCH["🔄 Fetch with Pagination<br/>100 records/page<br/>Exponential backoff retry"]
    STRIP["✂️ Strip Supabase id field"]

    %% ─── Validation ───
    VALIDATE["✅ Schema Validation<br/>12 checks per record"]

    %% ─── Split ───
    QUAR{"Valid?"}

    %% ─── Dedup ───
    DEDUP["🔍 Natural-Key Dedup<br/>All fields except transaction_id"]

    %% ─── Split 2 ───
    DUPCHK{"Duplicate?"}

    %% ─── Transform ───
    FILTER["🔎 Filter: status = completed"]
    AGG["📊 Aggregate<br/>per account + date"]
    TOPCAT["🏷️ Top category by spend"]

    %% ─── Outputs ───
    RAW_OUT["📦 raw_df<br/>344 records"]
    QUAR_OUT["⚠️ quarantine_df<br/>3 records + error_reason"]
    DUP_OUT["🔁 duplicate_df<br/>5 records + duplicate_of"]
    GOLD_OUT["🏆 daily_account_summary<br/>238 rows"]
    TESTS["🧪 6 Quality Tests<br/>All passing"]

    %% ─── Flow ───
    ENV -->|loads credentials| FETCH
    API -->|HTTPS GET| FETCH
    CSV -->|fallback if API down| FETCH
    FETCH --> STRIP
    STRIP -->|352 records| VALIDATE
    VALIDATE --> QUAR
    QUAR -->|valid: 349| DEDUP
    QUAR -->|invalid: 3| QUAR_OUT
    DEDUP --> DUPCHK
    DUPCHK -->|canonical: 344| RAW_OUT
    DUPCHK -->|duplicate: 5| DUP_OUT
    RAW_OUT --> FILTER
    FILTER -->|265 completed| AGG
    AGG --> TOPCAT
    TOPCAT --> GOLD_OUT
    GOLD_OUT --> TESTS

    %% ─── Styling ───
    style API fill:#ff6b6b,color:#fff
    style CSV fill:#ffa502,color:#fff
    style ENV fill:#5f27cd,color:#fff
    style FETCH fill:#74b9ff,color:#333
    style VALIDATE fill:#55efc4,color:#333
    style QUAR fill:#fdcb6e,color:#333
    style DEDUP fill:#55efc4,color:#333
    style DUPCHK fill:#fdcb6e,color:#333
    style RAW_OUT fill:#00b894,color:#fff
    style QUAR_OUT fill:#e17055,color:#fff
    style DUP_OUT fill:#e17055,color:#fff
    style GOLD_OUT fill:#0984e3,color:#fff
    style TESTS fill:#a29bfe,color:#fff
```

---

## Detailed Sequence: Full Ingestion Run

```mermaid
sequenceDiagram
    autonumber
    participant NB as pipeline_run notebook
    participant CFG as pipeline_config.py
    participant ENV as .env file
    participant API as Supabase API
    participant VAL as validator.py
    participant DED as dedup.py
    participant SUM as daily_summary.py
    participant TST as quality_tests.py

    Note over NB: Setup & Imports
    NB->>ENV: load_dotenv(.env)
    ENV-->>NB: API_KEY, AUTH_TOKEN, API_BASE_URL
    NB->>CFG: import pipeline_config
    CFG-->>NB: schema rules, enums, regex, config values

    Note over NB: Task 1 — Raw Ingestion
    NB->>API: GET /transactions?limit=100&offset=0
    API-->>NB: 100 records
    NB->>API: GET /transactions?limit=100&offset=100
    API-->>NB: 100 records
    NB->>API: GET /transactions?limit=100&offset=200
    API-->>NB: 100 records
    NB->>API: GET /transactions?limit=100&offset=300
    API-->>NB: 52 records (partial page → stop)

    Note over NB: 352 raw records fetched
    NB->>VAL: validate_and_quarantine(352 records)
    VAL->>VAL: Check 12 rules per record
    VAL-->>NB: 349 valid + 3 quarantined

    NB->>DED: detect_duplicates(349 valid)
    DED->>DED: Group by natural key (9 fields)
    DED-->>NB: 344 deduplicated + 5 duplicates

    Note over NB: Task 2 — Daily Account Summary
    NB->>SUM: build_daily_account_summary(raw_df)
    SUM->>SUM: Filter status = completed (265 rows)
    SUM->>SUM: Aggregate per account_id + date
    SUM->>SUM: Compute top_category by spend
    SUM-->>NB: daily_account_summary (238 rows)

    NB->>TST: run_all_tests(summary, completed_df)
    TST->>TST: 1. not_null check ✓
    TST->>TST: 2. unique combo check ✓
    TST->>TST: 3. net_amount expression ✓
    TST->>TST: 4. non-negative check ✓
    TST->>TST: 5. positive count check ✓
    TST->>TST: 6. idempotency check ✓
    TST-->>NB: 6/6 tests passed

    Note over NB: Task 3 — Incremental Ingestion
    NB->>NB: Run 1 — full backfill (watermark = None)
    NB->>NB: Run 2 — incremental (watermark with 24h lookback)
    NB->>NB: Cross-batch dedup by transaction_id → 0 new records
```

---

## Incremental Ingestion Flow

```mermaid
flowchart TD
    START([Pipeline Trigger]) --> CHECK{Watermark exists?}

    CHECK -->|No — first run| FULL["Fetch ALL records<br/>No date filter"]
    CHECK -->|Yes — subsequent run| LB["Apply 24h lookback<br/>watermark - 24 hours"]

    FULL --> FETCH1[Fetch from API with watermark = None]
    LB --> FETCH2["Fetch from API with<br/>watermark = effective_wm"]

    FETCH1 --> VALIDATE[Validate & Quarantine]
    FETCH2 --> VALIDATE

    VALIDATE --> DEDUP1[Cross-batch dedup<br/>by transaction_id]
    DEDUP1 --> NEW{New records?}

    NEW -->|Yes| MERGE[Merge new records<br/>into existing dataset]
    NEW -->|No| SKIP[No new data — skip merge]

    MERGE --> UPDATE[Update high-water mark<br/>to max transaction_date]
    SKIP --> UPDATE

    UPDATE --> DONE([Run Complete])

    style START fill:#00b894,color:#fff
    style DONE fill:#00b894,color:#fff
    style FULL fill:#74b9ff,color:#333
    style LB fill:#74b9ff,color:#333
    style MERGE fill:#0984e3,color:#fff
    style SKIP fill:#a29bfe,color:#fff
```

---

## Data Layer Summary

```mermaid
flowchart TD
    subgraph BRONZE [Bronze / Raw Layer]
        RAW["raw_df — 344 rows<br/>Valid + deduplicated transactions<br/>Columns: 10 schema fields + _ingested_at + transaction_ts"]
    end

    subgraph QUARANTINE [Quarantine Layer]
        Q["quarantine_df — 3 rows<br/>Invalid records with _error_reason<br/>Dead-letter for investigation"]
    end

    subgraph DUPLICATE [Duplicate Layer]
        D["duplicate_df — 5 rows<br/>Duplicate records with _duplicate_of<br/>Links to canonical transaction_id"]
    end

    subgraph GOLD [Gold / Summary Layer]
        G["daily_account_summary — 238 rows<br/>20 accounts × 85 dates<br/>Net amounts, top categories, currencies"]
    end

    subgraph TEST [Quality Test Layer]
        T["6 dbt-style tests<br/>not_null, unique, expression<br/>non_negative, positive, idempotent"]
    end

    RAW --> G
    Q -.->|not included in summary| G
    D -.->|not included in summary| G
    G --> T

    style RAW fill:#cd9933,color:#fff
    style Q fill:#e17055,color:#fff
    style D fill:#e17055,color:#fff
    style G fill:#0984e3,color:#fff
    style T fill:#a29bfe,color:#fff
```
