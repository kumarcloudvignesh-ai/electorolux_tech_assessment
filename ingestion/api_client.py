"""
API client for fetching transactions from the Supabase REST API.

Handles:
  - Offset-based pagination (fetch all records without hardcoding count)
  - Exponential backoff retry for HTTP 429, 5xx, and timeouts
  - Optional date watermark filtering for incremental ingestion
  - CSV fallback for offline / network-restricted environments
"""
import json
import time
import requests
from pyspark.sql import SparkSession

from config.pipeline_config import (
    API_BASE_URL, API_KEY, AUTH_TOKEN,
    PAGE_SIZE, MAX_RETRIES, INITIAL_BACKOFF, BACKOFF_MULTIPLIER,
    CSV_FALLBACK_PATH, API_INTERNAL_FIELDS,
)


def _fetch_page_with_retry(url, headers, params=None):
    """Fetch a single API page with retry logic for 429, 5xx, and timeouts."""
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429 or response.status_code >= 500:
                print(f"  Retryable error {response.status_code} on attempt "
                      f"{attempt}/{MAX_RETRIES}. Backing off {backoff:.1f}s...")
                time.sleep(backoff)
                backoff *= BACKOFF_MULTIPLIER
            else:
                print(f"  Non-retryable error {response.status_code}: "
                      f"{response.text[:200]}")
                return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"  Timeout/connection error on attempt {attempt}/{MAX_RETRIES}: "
                  f"{e}. Backing off {backoff:.1f}s...")
            time.sleep(backoff)
            backoff *= BACKOFF_MULTIPLIER
    print(f"  Failed after {MAX_RETRIES} retries.")
    return None


def fetch_all_transactions(watermark=None):
    """
    Fetch all transactions from the API using offset-based pagination.

    Args:
        watermark: Optional ISO 8601 UTC timestamp. If provided, only fetches
            records with transaction_date >= watermark (incremental mode).

    Returns:
        List of raw transaction dicts, or None if the API is unreachable.
    """
    headers = {
        "apikey": API_KEY,
        "Authorization": f"Bearer {AUTH_TOKEN}",
    }
    all_records = []
    offset = 0

    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "order": "transaction_date.asc",
        }
        if watermark:
            params["transaction_date"] = f"gte.{watermark}"

        print(f"Fetching offset={offset}...")
        batch = _fetch_page_with_retry(
            f"{API_BASE_URL}/transactions", headers, params
        )

        if batch is None:
            print(f"  API error at offset={offset}. Will use CSV fallback.")
            return None

        if len(batch) == 0:
            print(f"  Empty response at offset={offset}. Pagination complete.")
            break

        all_records.extend(batch)
        print(f"  Received {len(batch)} records (total so far: {len(all_records)})")

        if len(batch) < PAGE_SIZE:
            print(f"  Fewer than {PAGE_SIZE} records received. Pagination complete.")
            break

        offset += PAGE_SIZE

    print(f"\nTotal records fetched from API: {len(all_records)}")

    # Strip Supabase internal fields before returning
    for record in all_records:
        for field in API_INTERNAL_FIELDS:
            record.pop(field, None)

    return all_records


def load_csv_fallback():
    """Load transactions from the uploaded CSV file as a fallback data source."""
    print("Loading from CSV fallback...")
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    df = spark.read.csv(CSV_FALLBACK_PATH, header=True, inferSchema=False)
    df = df.withColumn("amount", df["amount"].cast("double"))
    records = [row.asDict() for row in df.collect()]
    print(f"Loaded {len(records)} records from CSV.")
    return records


def ingest(watermark=None):
    """
    Main entry point: fetch transactions from API with pagination.
    Falls back to CSV if the API is unreachable.

    Returns:
        List of raw transaction dicts.
    """
    records = fetch_all_transactions(watermark=watermark)

    if records is None:
        print("\n⚠️  API unreachable — using CSV fallback for demonstration.")
        records = load_csv_fallback()
    else:
        print(f"\n✅ Successfully fetched {len(records)} records from API.")

    if records:
        print(f"\nSample record:\n{json.dumps(records[0], indent=2, default=str)}")

    return records
