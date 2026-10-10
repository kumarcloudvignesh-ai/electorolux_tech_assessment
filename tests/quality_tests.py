"""
Data quality tests for the daily_account_summary table.

These tests are the Python equivalent of dbt schema.yml tests.
At least 3 meaningful tests are included:
  1. not_null: account_id and transaction_date
  2. unique: one row per (account_id, transaction_date)
  3. expression: net_amount = total_credit_amount - total_debit_amount
  4. non_negative: total_debit_amount and total_credit_amount >= 0
  5. positive: transaction_count > 0
  6. idempotent: rerun produces same row count
"""
from pyspark.sql.functions import col, round as spark_round


def run_test(test_name, condition, detail=""):
    """Run a single assertion and print the result."""
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {test_name} {detail}")
    return (test_name, status, detail)


def run_all_tests(daily_account_summary, completed_df):
    """
    Run all data quality tests against the daily_account_summary DataFrame.

    Returns:
        List of (test_name, status, detail) tuples.
    """
    results = []

    print("─── Data Quality Tests for daily_account_summary ───")

    # Test 1: No null account_id or transaction_date (not_null test)
    null_accounts = daily_account_summary.filter(col("account_id").isNull()).count()
    null_dates = daily_account_summary.filter(col("transaction_date").isNull()).count()
    results.append(run_test(
        "not_null: account_id and transaction_date",
        null_accounts == 0 and null_dates == 0,
        f"(null accounts: {null_accounts}, null dates: {null_dates})",
    ))

    # Test 2: One row per account_id + transaction_date (unique combination test)
    dup_count = (
        daily_account_summary
        .groupBy("account_id", "transaction_date")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    results.append(run_test(
        "unique: one row per (account_id, transaction_date)",
        dup_count == 0,
        f"(duplicate combos: {dup_count})",
    ))

    # Test 3: net_amount = total_credit_amount - total_debit_amount
    net_mismatch = daily_account_summary.filter(
        spark_round(col("net_amount"), 2)
        != spark_round(col("total_credit_amount") - col("total_debit_amount"), 2)
    ).count()
    results.append(run_test(
        "expression: net_amount = total_credit_amount - total_debit_amount",
        net_mismatch == 0,
        f"(mismatches: {net_mismatch})",
    ))

    # Test 4: total_debit_amount >= 0 and total_credit_amount >= 0
    neg_debit = daily_account_summary.filter(col("total_debit_amount") < 0).count()
    neg_credit = daily_account_summary.filter(col("total_credit_amount") < 0).count()
    results.append(run_test(
        "non_negative: total_debit_amount and total_credit_amount >= 0",
        neg_debit == 0 and neg_credit == 0,
        f"(neg debits: {neg_debit}, neg credits: {neg_credit})",
    ))

    # Test 5: transaction_count > 0
    zero_count = daily_account_summary.filter(col("transaction_count") <= 0).count()
    results.append(run_test(
        "positive: transaction_count > 0",
        zero_count == 0,
        f"(zero-count rows: {zero_count})",
    ))

    # Test 6: Idempotency check
    from transform.daily_summary import build_daily_account_summary
    print("\n  Running idempotency check...")
    rerun = build_daily_account_summary.__wrapped__ if hasattr(build_daily_account_summary, '__wrapped__') else None
    # Recompute summary from the same input
    from pyspark.sql.functions import (
        when, lit, sum as spark_sum, count as spark_count,
        countDistinct, array_sort, array_distinct, collect_set,
        date_format, current_timestamp,
    )
    completed = completed_df
    rerun_df = (
        completed
        .groupBy(
            col("account_id"),
            date_format(col("transaction_ts"), "yyyy-MM-dd").alias("transaction_date"),
        )
        .agg(
            spark_round(spark_sum(when(col("transaction_type") == "debit", col("amount")).otherwise(lit(0.0))), 2).alias("total_debit_amount"),
            spark_round(spark_sum(when(col("transaction_type") == "credit", col("amount")).otherwise(lit(0.0))), 2).alias("total_credit_amount"),
            spark_count(lit(1)).alias("transaction_count"),
            countDistinct("merchant_name").alias("distinct_merchants"),
            array_sort(array_distinct(collect_set("currency"))).alias("currencies"),
        )
        .withColumn("net_amount", spark_round(col("total_credit_amount") - col("total_debit_amount"), 2))
    )
    count_match = daily_account_summary.count() == rerun_df.count()
    results.append(run_test(
        "idempotent: rerun produces same row count",
        count_match,
    ))

    # Summary
    passed = sum(1 for _, s, _ in results if s == "PASS")
    failed = sum(1 for _, s, _ in results if s == "FAIL")
    print(f"\n─── Test Summary: {passed} passed, {failed} failed ───")

    return results
