"""
Duplicate detection based on the natural key.

Duplicate definition from the assessment:
  "records represent the same real-world transaction but have different
   transaction_id values and identical values across all other fields."
  "Duplicate detection must be based on the natural key across all fields
   except transaction_id."

Strategy: Group records by their natural key (all fields except transaction_id).
If a group has >1 record, keep the first as canonical and flag the rest as
duplicates.
"""
from collections import OrderedDict

from config.pipeline_config import NATURAL_KEY_FIELDS


def compute_natural_key(record):
    """Return a hashable tuple of all fields except transaction_id."""
    return tuple(str(record.get(f, "")) for f in NATURAL_KEY_FIELDS)


def detect_duplicates(valid_records):
    """
    Detect and remove duplicates based on the natural key.

    Args:
        valid_records: List of validated record dicts.

    Returns:
        (deduped_records, duplicate_records)
        - deduped_records: List with duplicates removed (first occurrence kept)
        - duplicate_records: List of duplicate records with _duplicate_of and
          _duplicate_reason metadata
    """
    key_groups = OrderedDict()
    for record in valid_records:
        key = compute_natural_key(record)
        if key not in key_groups:
            key_groups[key] = []
        key_groups[key].append(record)

    deduped = []
    duplicates = []

    for key, group in key_groups.items():
        if len(group) > 1:
            # Keep first as canonical, flag the rest as duplicates
            deduped.append(group[0])
            for dup in group[1:]:
                dup_copy = dict(dup)
                dup_copy["_duplicate_of"] = group[0]["transaction_id"]
                dup_copy["_duplicate_reason"] = (
                    f"Duplicate of {group[0]['transaction_id']}: "
                    f"identical values across all fields except transaction_id"
                )
                duplicates.append(dup_copy)
        else:
            deduped.append(group[0])

    print(f"─── Duplicate Detection Summary ───")
    print(f"Valid records before dedup:  {len(valid_records)}")
    print(f"Duplicate records found:     {len(duplicates)}")
    print(f"Records after dedup:         {len(deduped)}")
    print(f"Unique natural keys:         {len(key_groups)}")

    if duplicates:
        print(f"\n─── Duplicate Sample (first 5) ───")
        for d in duplicates[:5]:
            print(f"  {d['transaction_id']} is a duplicate of {d['_duplicate_of']}")
            print(f"    Natural key match: account={d['account_id']}, "
                  f"date={d['transaction_date']}, "
                  f"amount={d['amount']}, merchant={d['merchant_name']}")
    else:
        print("\nNo duplicates found in this dataset.")

    return deduped, duplicates
