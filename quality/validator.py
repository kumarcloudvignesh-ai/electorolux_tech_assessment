"""
Schema validation and quarantine layer.

Validates each transaction record against the JSON schema rules defined
in transactions_schema.json. Invalid records are routed to a quarantine
/dead-letter layer with error_reason and ingestion_timestamp.

Validation checks:
  - Required fields present (all 10 fields)
  - additionalProperties: false (no unexpected fields)
  - transaction_id: TXN-[A-Z0-9]+ pattern
  - account_id: ACC-NNNN pattern
  - transaction_date: ISO 8601 UTC, real calendar date, within date range
  - amount: strictly positive, rounded to 2 decimal places
  - currency: enum (USD, EUR, GBP, CHF, JPY, AUD, CAD)
  - transaction_type: enum (debit, credit) — lowercase only
  - merchant_name: non-empty, non-whitespace
  - merchant_category: enum (12 values)
  - status: enum (completed, pending, failed, reversed) — lowercase only
  - country_code: ISO 3166-1 alpha-2, officially assigned (UK and EN invalid)
"""
from datetime import datetime, timezone

from config.pipeline_config import (
    REQUIRED_FIELDS, VALID_CURRENCIES, VALID_TRANSACTION_TYPES,
    VALID_MERCHANT_CATEGORIES, VALID_STATUSES, VALID_COUNTRY_CODES,
    PATTERNS, DATE_RANGE_MIN, DATE_RANGE_MAX,
)


def validate_record(record):
    """
    Validate a single transaction record against the JSON schema rules.

    Returns:
        (is_valid: bool, error_reason: str or None)
    """
    errors = []

    # Check for additional properties (additionalProperties: false)
    # Ignore internal metadata fields prefixed with '_'
    extra_fields = (
        set(record.keys()) - set(REQUIRED_FIELDS)
        - {k for k in record if k.startswith("_")}
    )
    if extra_fields:
        errors.append(f"Unexpected fields: {sorted(extra_fields)}")

    # Check required fields are present
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None or record[field] == "":
            errors.append(f"Missing or empty required field: {field}")

    # If missing required fields, return early
    if errors:
        return False, "; ".join(errors)

    # transaction_id pattern
    if not PATTERNS["transaction_id"].match(str(record["transaction_id"])):
        errors.append(
            f"transaction_id '{record['transaction_id']}' does not match TXN-[A-Z0-9]+"
        )

    # account_id pattern
    if not PATTERNS["account_id"].match(str(record["account_id"])):
        errors.append(
            f"account_id '{record['account_id']}' does not match ACC-NNNN"
        )

    # transaction_date: format + real calendar date + date range
    date_str = str(record["transaction_date"])
    if not PATTERNS["transaction_date"].match(date_str):
        errors.append(
            f"transaction_date '{date_str}' is not valid ISO 8601 UTC format"
        )
    else:
        try:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            min_date = datetime.strptime(DATE_RANGE_MIN, "%Y-%m-%dT%H:%M:%SZ")
            max_date = datetime.strptime(DATE_RANGE_MAX, "%Y-%m-%dT%H:%M:%SZ")
            if parsed_date < min_date:
                errors.append(
                    f"transaction_date '{date_str}' is before {DATE_RANGE_MIN}"
                )
            if parsed_date > max_date:
                errors.append(
                    f"transaction_date '{date_str}' is after {DATE_RANGE_MAX}"
                )
        except ValueError as e:
            errors.append(
                f"transaction_date '{date_str}' is not a real calendar date: {e}"
            )

    # amount: strictly positive, multiple of 0.01
    try:
        amount = float(record["amount"])
        if amount <= 0:
            errors.append(f"amount {amount} must be strictly positive (> 0)")
        if round(amount, 2) != amount:
            errors.append(f"amount {amount} must be rounded to 2 decimal places")
    except (ValueError, TypeError):
        errors.append(f"amount '{record['amount']}' is not a valid number")

    # currency enum
    if str(record["currency"]) not in VALID_CURRENCIES:
        errors.append(
            f"currency '{record['currency']}' not in {sorted(VALID_CURRENCIES)}"
        )

    # transaction_type enum (case-sensitive, lowercase)
    if str(record["transaction_type"]) not in VALID_TRANSACTION_TYPES:
        errors.append(
            f"transaction_type '{record['transaction_type']}' not in "
            f"{sorted(VALID_TRANSACTION_TYPES)}"
        )

    # merchant_name: non-empty, has non-whitespace
    merchant = str(record["merchant_name"])
    if len(merchant) < 1 or not PATTERNS["merchant_name"].match(merchant):
        errors.append(
            f"merchant_name '{merchant}' is empty or whitespace-only"
        )

    # merchant_category enum
    if str(record["merchant_category"]) not in VALID_MERCHANT_CATEGORIES:
        errors.append(
            f"merchant_category '{record['merchant_category']}' not in "
            f"{sorted(VALID_MERCHANT_CATEGORIES)}"
        )

    # status enum (case-sensitive, lowercase)
    if str(record["status"]) not in VALID_STATUSES:
        errors.append(
            f"status '{record['status']}' not in {sorted(VALID_STATUSES)}"
        )

    # country_code: pattern + officially assigned
    cc = str(record["country_code"])
    if not PATTERNS["country_code"].match(cc):
        errors.append(f"country_code '{cc}' is not 2 uppercase letters")
    elif cc not in VALID_COUNTRY_CODES:
        errors.append(
            f"country_code '{cc}' is not an officially assigned ISO 3166-1 alpha-2 code"
        )

    if errors:
        return False, "; ".join(errors)
    return True, None


def validate_and_quarantine(raw_records):
    """
    Validate a batch of raw records and split into valid / quarantined.

    Returns:
        (valid_records, quarantine_records)
    """
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    valid = []
    quarantine = []

    for record in raw_records:
        is_valid, error_reason = validate_record(record)
        record["_ingested_at"] = ingestion_ts
        if is_valid:
            valid.append(record)
        else:
            record["_error_reason"] = error_reason
            quarantine.append(record)

    print(f"─── Validation Summary ───")
    print(f"Total records:          {len(raw_records)}")
    print(f"Valid records:          {len(valid)}")
    print(f"Quarantined records:    {len(quarantine)}")
    print(f"Ingestion timestamp:    {ingestion_ts}")

    if quarantine:
        print(f"\n─── Quarantine Sample (first 5) ───")
        for q in quarantine[:5]:
            print(f"  {q.get('transaction_id', 'N/A')}: {q['_error_reason']}")

    return valid, quarantine
