"""
Shared configuration for the transaction ingestion pipeline.

All API settings, schema validation rules, and enum constraints are centralized
here so that every module (ingestion, quality, transform) references a single
source of truth.

API credentials are loaded from a .env file at the project root. In
production, use Databricks Secret Scope and retrieve via
dbutils.secrets.get().
"""
import os
import re
from pathlib import Path

from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────
# Load environment variables from .env file at project root
# ──────────────────────────────────────────────────────────────────────

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)

# ──────────────────────────────────────────────────────────────────────
# API CONFIGURATION
# ──────────────────────────────────────────────────────────────────────

API_BASE_URL = os.environ.get(
    "ASSESSMENT_API_BASE_URL",
    "https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1",
)
API_KEY = os.environ.get(
    "ASSESSMENT_API_KEY",
    "",
)
AUTH_TOKEN = os.environ.get(
    "ASSESSMENT_AUTH_TOKEN",
    "",
)

PAGE_SIZE = 100           # records per API page
MAX_RETRIES = 5           # max retry attempts for transient failures
INITIAL_BACKOFF = 1.0     # initial backoff in seconds
BACKOFF_MULTIPLIER = 2    # exponential backoff factor

# CSV fallback path (uploaded attachment)
CSV_FALLBACK_PATH = "idbfs:/2026-10-09/10/_8d2cf79c-a13e-46a7-a35d-c31ea78bfca3"

# ──────────────────────────────────────────────────────────────────────
# WATERMARK / INCREMENTAL SETTINGS
# ──────────────────────────────────────────────────────────────────────

LOOKBACK_WINDOW_HOURS = 24  # lookback window for late-arriving data

# ──────────────────────────────────────────────────────────────────────
# SCHEMA DEFINITION — derived from transactions_schema.json
# ──────────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "transaction_id", "account_id", "transaction_date", "amount",
    "currency", "transaction_type", "merchant_name", "merchant_category",
    "status", "country_code",
]

# Natural key for duplicate detection: all fields except transaction_id
NATURAL_KEY_FIELDS = [f for f in REQUIRED_FIELDS if f != "transaction_id"]

VALID_CURRENCIES = {"USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD"}
VALID_TRANSACTION_TYPES = {"debit", "credit"}
VALID_MERCHANT_CATEGORIES = {
    "e-commerce", "travel", "food_and_beverage", "groceries",
    "electronics", "retail", "entertainment", "health",
    "transportation", "home_and_garden", "payroll", "transfer",
}
VALID_STATUSES = {"completed", "pending", "failed", "reversed"}

# Officially assigned ISO 3166-1 alpha-2 country codes.
# 'UK' is NOT valid — the correct code is 'GB'. 'EN' is not assigned.
VALID_COUNTRY_CODES = {
    "US", "GB", "DE", "FR", "IT", "ES", "NL", "BE", "AT", "CH",
    "SE", "NO", "DK", "FI", "PL", "PT", "IE", "LU", "CZ", "GR",
    "HU", "RO", "BG", "HR", "SK", "SI", "LT", "LV", "EE", "CY",
    "MT", "JP", "CN", "KR", "IN", "SG", "HK", "TW", "TH", "MY",
    "ID", "PH", "VN", "AU", "NZ", "CA", "MX", "BR", "AR", "CL",
    "CO", "PE", "ZA", "EG", "MA", "NG", "KE", "SA", "AE", "IL",
    "TR", "RU", "UA", "KZ", "UZ", "PK", "BD", "LK", "JO", "LB",
    "QA", "KW", "BH", "OM", "VE", "EC", "PY", "UY", "BO",
    "GT", "PA", "CR", "DO", "JM", "TT", "BS", "BB", "KH",
}

# Regex patterns from JSON schema
PATTERNS = {
    "transaction_id": re.compile(r"^TXN-[A-Z0-9]+$"),
    "account_id": re.compile(r"^ACC-\d{4}$"),
    "transaction_date": re.compile(
        r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$"
    ),
    "merchant_name": re.compile(r".*\S.*"),
    "country_code": re.compile(r"^[A-Z]{2}$"),
}

# Valid date range for transaction_date
DATE_RANGE_MIN = "2024-01-01T00:00:00Z"
DATE_RANGE_MAX = "2024-03-31T23:59:59Z"

# Fields to strip from API response before validation (Supabase internal fields)
API_INTERNAL_FIELDS = {"id"}
