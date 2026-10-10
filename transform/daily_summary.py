"""
Daily account-level summary transformation.

Produces one row per account_id and transaction_date with:
  - total_debit_amount: Sum of debit amounts for completed transactions
  - total_credit_amount: Sum of credit amounts for completed transactions
  - net_amount: total_credit_amount minus total_debit_amount
  - transaction_count: Count of completed transactions
  - distinct_merchants: Number of distinct merchants for the day
  - top_category: Merchant category with highest total spend for the day
  - currencies: Distinct currencies used that day (sorted array)
  - updated_at: UTC timestamp when the row was last computed

Requirements:
  - Only include records where status = 'completed'
  - Exclude quarantined records from aggregations
  - Idempotent: rerunning produces identical results
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, ArrayType,
)
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp, date_format, lit, collect_set,
    sum as spark_sum, count as spark_count, countDistinct,
    round as spark_round, when, expr, array_distinct, array_sort,
)


def _get_spark():
    """Get the active SparkSession (available in Databricks notebook context)."""
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def create_raw_dataframe(deduped_records):
    """
    Create a Spark DataFrame from the deduplicated Python records.
    Ensures amount is float (API returns strings).
    """
    for r in deduped_records:
        r["amount"] = float(r["amount"])

    raw_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ])

    raw_df = _get_spark().createDataFrame(deduped_records, schema=raw_schema)
    raw_df = raw_df.withColumn(
        "transaction_ts",
        to_timestamp("transaction_date", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )
    return raw_df


def create_quarantine_dataframe(quarantine_records):
    """Create a Spark DataFrame for quarantined records."""
    for r in quarantine_records:
        try:
            r["amount"] = float(r["amount"])
        except (ValueError, TypeError):
            pass

    q_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_error_reason", StringType(), True),
    ])

    if quarantine_records:
        return _get_spark().createDataFrame(quarantine_records, schema=q_schema)
    return _get_spark().createDataFrame([], schema=q_schema)


def create_duplicate_dataframe(duplicate_records):
    """Create a Spark DataFrame for duplicate records."""
    for r in duplicate_records:
        try:
            r["amount"] = float(r["amount"])
        except (ValueError, TypeError):
            pass

    d_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_duplicate_of", StringType(), True),
        StructField("_duplicate_reason", StringType(), True),
    ])

    if duplicate_records:
        return _get_spark().createDataFrame(duplicate_records, schema=d_schema)
    return _get_spark().createDataFrame([], schema=d_schema)


def build_daily_account_summary(raw_df):
    """
    Build the daily account-level summary from the raw DataFrame.

    Only includes completed transactions. Excludes quarantined records
    (raw_df already contains only valid, deduplicated records).

    Returns:
        DataFrame with the daily_account_summary schema.
    """
    # Filter to completed transactions only
    completed_df = raw_df.filter(col("status") == "completed")

    print(f"Completed transactions: {completed_df.count()} out of "
          f"{raw_df.count()} total valid records")

    # Compute per-category spend for top_category
    category_spend = (
        completed_df
        .groupBy(
            col("account_id"),
            date_format(col("transaction_ts"), "yyyy-MM-dd").alias("transaction_date"),
            col("merchant_category"),
        )
        .agg(spark_sum("amount").alias("category_total"))
    )

    # Rank categories by total spend within each account+date group
    top_category_df = (
        category_spend
        .withColumn(
            "rank",
            expr(
                "row_number() OVER (PARTITION BY account_id, transaction_date "
                "ORDER BY category_total DESC)"
            ),
        )
        .filter(col("rank") == 1)
        .select(
            "account_id", "transaction_date",
            col("merchant_category").alias("top_category"),
        )
    )

    # Main aggregation
    daily_summary = (
        completed_df
        .groupBy(
            col("account_id"),
            date_format(col("transaction_ts"), "yyyy-MM-dd").alias("transaction_date"),
        )
        .agg(
            spark_round(
                spark_sum(when(col("transaction_type") == "debit", col("amount")).otherwise(lit(0.0))),
                2,
            ).alias("total_debit_amount"),
            spark_round(
                spark_sum(when(col("transaction_type") == "credit", col("amount")).otherwise(lit(0.0))),
                2,
            ).alias("total_credit_amount"),
            spark_count(lit(1)).alias("transaction_count"),
            countDistinct("merchant_name").alias("distinct_merchants"),
            array_sort(array_distinct(collect_set("currency"))).alias("currencies"),
        )
        .withColumn(
            "net_amount",
            spark_round(col("total_credit_amount") - col("total_debit_amount"), 2),
        )
        .withColumn("updated_at", current_timestamp())
    )

    # Join with top_category
    result = (
        daily_summary
        .join(top_category_df, on=["account_id", "transaction_date"], how="left")
        .select(
            "account_id", "transaction_date",
            "total_debit_amount", "total_credit_amount", "net_amount",
            "transaction_count", "distinct_merchants", "top_category",
            "currencies", "updated_at",
        )
        .orderBy("account_id", "transaction_date")
    )

    print(f"\n─── Daily Account Summary ───")
    print(f"Total rows: {result.count()}")
    print(f"Distinct accounts: {result.select('account_id').distinct().count()}")
    print(f"Distinct dates: {result.select('transaction_date').distinct().count()}")

    return result
