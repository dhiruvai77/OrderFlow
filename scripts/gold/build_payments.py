
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =====================================================
# 1. CONFIGURATION
# =====================================================

ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = ROOT / "data" / "silver" / "olist"
GOLD_DIR = ROOT / "data" / "gold" / "olist"

os.environ["PYSPARK_PYTHON"] = sys.executable


# =====================================================
# 2. INITIALIZE SPARK
# =====================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Gold-Payments-Fact")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - BUILD FCT_PAYMENTS")
    print("=" * 60)

    # =================================================
    # 3. READ SILVER AND GOLD DATA
    # =================================================

    payments = spark.read.parquet(
        str(SILVER_DIR / "payments")
    )

    orders = spark.read.parquet(
        str(SILVER_DIR / "orders")
    )

    dim_date = spark.read.parquet(
        str(GOLD_DIR / "dim_date")
    )

    payment_count = payments.count()

    print(f"\nSilver payment records: {payment_count:,}")

    # =================================================
    # 4. PREPARE ORDER LOOKUP
    # =================================================

    order_lookup = orders.select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_status"
    )

    duplicate_orders = (
        order_lookup.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_orders:
        raise ValueError(
            f"Order lookup contains "
            f"{duplicate_orders} duplicate keys"
        )

    # =================================================
    # 5. JOIN PAYMENTS WITH ORDERS
    # =================================================

    joined = payments.join(
        order_lookup,
        on="order_id",
        how="left"
    )

    # =================================================
    # 6. CREATE PAYMENT FACT TABLE
    # =================================================

    fact = joined.select(
        "order_id",
        "payment_sequential",
        "customer_id",

        F.date_format(
            "order_purchase_timestamp", "yyyyMMdd"
        ).cast("int").alias("purchase_date_key"),

        F.col("order_purchase_timestamp").alias(
            "purchase_timestamp"
        ),

        "order_status",
        "payment_type",
        "payment_installments",
        "payment_value",
        "is_valid_transaction"
    )

    # =================================================
    # 7. VALIDATE FACT TABLE GRAIN
    # =================================================

    fact_count = fact.count()

    duplicate_payments = (
        fact.groupBy(
            "order_id", "payment_sequential"
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_customer = fact.filter(
        F.col("customer_id").isNull()
    ).count()

    missing_dates = fact.filter(
        F.col("purchase_date_key").isNull()
    ).count()

    missing_payment_keys = fact.filter(
        F.col("order_id").isNull()
        | F.col("payment_sequential").isNull()
    ).count()

    # Validate purchase date foreign keys
    valid_dates = dim_date.select(
        F.col("date_key").alias("_date_key")
    ).distinct()

    unmatched_dates = (
        fact.select("purchase_date_key")
        .join(
            valid_dates,
            F.col("purchase_date_key")
            == F.col("_date_key"),
            "left_anti"
        )
        .count()
    )

    print("\nFACT TABLE VALIDATION")
    print(f"Expected rows: {payment_count:,}")
    print(f"Actual rows: {fact_count:,}")
    print(f"Duplicate payment keys: {duplicate_payments:,}")
    print(f"Missing payment keys: {missing_payment_keys:,}")
    print(f"Missing customer IDs: {missing_customer:,}")
    print(f"Missing purchase dates: {missing_dates:,}")
    print(f"Unmatched date keys: {unmatched_dates:,}")

    if fact_count != payment_count:
        raise ValueError(
            "Payment fact row count changed during join"
        )

    if duplicate_payments or missing_payment_keys:
        raise ValueError(
            "Payment fact key validation failed"
        )

    if missing_customer or missing_dates or unmatched_dates:
        raise ValueError(
            "Payment fact relationship validation failed"
        )

    # =================================================
    # 8. WRITE GOLD PARQUET
    # =================================================

    target = GOLD_DIR / "fct_payments"

    fact.write.mode("overwrite").parquet(
        str(target)
    )

    verified = spark.read.parquet(str(target))

    verified_count = verified.count()

    if verified_count != payment_count:
        raise ValueError(
            "Payment fact read-back validation failed"
        )

    print(f"\nVerified Gold records: {verified_count:,}")

    # =================================================
    # 9. OVERALL PAYMENT METRICS
    # =================================================

    print("\nPAYMENT SUMMARY")

    verified.agg(
        F.count("*").alias("payment_entries"),

        F.countDistinct("order_id").alias(
            "orders_with_payments"
        ),

        F.sum("payment_value").alias(
            "total_recorded_payments"
        ),

        F.round(
            F.avg("payment_value"), 2
        ).alias("avg_payment_entry_value"),

        F.sum(
            F.when(
                F.col("is_valid_transaction") == False, 1
            ).otherwise(0)
        ).alias("flagged_transactions")
    ).show(truncate=False)

    # =================================================
    # 10. PAYMENT METHOD ANALYSIS
    # =================================================

    print("\nPAYMENT METHODS")

    verified.groupBy("payment_type").agg(
        F.count("*").alias("payment_entries"),

        F.countDistinct("order_id").alias(
            "distinct_orders"
        ),

        F.sum("payment_value").alias(
            "total_payment_value"
        )
    ).orderBy(
        F.desc("total_payment_value")
    ).show(truncate=False)

    # =================================================
    # 11. MULTIPLE PAYMENT ENTRIES PER ORDER
    # =================================================

    print("\nPAYMENT ENTRY DISTRIBUTION")

    payments_per_order = (
        verified.groupBy("order_id")
        .count()
    )

    payments_per_order.groupBy(
        F.col("count").alias("entries_per_order")
    ).agg(
        F.count("*").alias("orders")
    ).orderBy(
        "entries_per_order"
    ).show(truncate=False)

    # =================================================
    # 12. SAMPLE AND SCHEMA
    # =================================================

    print("\nSAMPLE FACT RECORDS")

    verified.select(
        "order_id",
        "payment_sequential",
        "customer_id",
        "purchase_date_key",
        "payment_type",
        "payment_installments",
        "payment_value"
    ).show(5, truncate=False)

    print("\nFACT TABLE SCHEMA")
    verified.printSchema()

    print("\n" + "=" * 60)
    print("FCT_PAYMENTS COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
