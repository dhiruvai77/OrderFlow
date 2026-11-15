
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =====================================================
# 1. PROJECT CONFIGURATION
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
    .appName("OrderFlow-Gold-Orders-Fact")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - BUILD FCT_ORDERS")
    print("=" * 60)

    # =================================================
    # 3. READ SOURCE TABLES
    # =================================================

    orders = spark.read.parquet(
        str(SILVER_DIR / "orders")
    )

    items = spark.read.parquet(
        str(GOLD_DIR / "fct_order_items")
    )

    payments = spark.read.parquet(
        str(GOLD_DIR / "fct_payments")
    )

    dim_date = spark.read.parquet(
        str(GOLD_DIR / "dim_date")
    )

    expected_orders = orders.count()

    print(f"\nSilver orders: {expected_orders:,}")
    print(f"Gold item lines: {items.count():,}")
    print(f"Gold payment entries: {payments.count():,}")

    # =================================================
    # 4. AGGREGATE ITEMS TO ORDER LEVEL
    # =================================================

    item_summary = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),

        F.sum("price")
        .cast("decimal(16,2)")
        .alias("merchandise_value"),

        F.sum("freight_value")
        .cast("decimal(16,2)")
        .alias("freight_value"),

        F.sum("total_item_value")
        .cast("decimal(16,2)")
        .alias("total_listed_value")
    )

    # =================================================
    # 5. AGGREGATE PAYMENTS TO ORDER LEVEL
    # =================================================

    payment_summary = payments.groupBy("order_id").agg(
        F.count("*").alias("payment_count"),

        F.sum("payment_value")
        .cast("decimal(16,2)")
        .alias("total_payment_value")
    )

    # =================================================
    # 6. PREPARE ORDER-LEVEL DATA
    # =================================================

    order_base = orders.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "is_late_delivery",
        "is_valid_order",
        "has_delivery_anomaly",
        "is_delivery_metric_eligible"
    )

    # =================================================
    # 7. JOIN AGGREGATED DATA
    # =================================================

    fact = (
        order_base
        .join(item_summary, on="order_id", how="left")
        .join(payment_summary, on="order_id", how="left")
    )

    # =================================================
    # 8. ADD DATE KEY
    # =================================================

    fact = fact.withColumn(
        "purchase_date_key",
        F.date_format(
            F.col("order_purchase_timestamp"),
            "yyyyMMdd"
        ).cast("int")
    )

    # =================================================
    # 9. DEFINE FINANCIAL QUALITY FLAGS
    # =================================================

    # Missing data stays NULL, not zero.
    # This distinguishes "no record" from "value = 0".

    fact = fact.withColumn(
        "has_items",
        F.col("item_count").isNotNull()
    )

    fact = fact.withColumn(
        "has_payments",
        F.col("payment_count").isNotNull()
    )

    # Difference between recorded payments and
    # listed merchandise plus freight.
    # NULL when either side has no records.
    fact = fact.withColumn(
        "payment_minus_listed_value",
        (
            F.col("total_payment_value")
            - F.col("total_listed_value")
        ).cast("decimal(16,2)")
    )

    # Allow a tolerance of one cent.
    # NULL means the comparison is unavailable.
    fact = fact.withColumn(
        "is_payment_reconciled",
        F.when(
            F.col("has_items") & F.col("has_payments"),
            F.abs(
                F.col("payment_minus_listed_value")
            ) <= F.lit("0.01").cast("decimal(16,2)")
        ).otherwise(F.lit(None).cast("boolean"))
    )

    # =================================================
    # 10. VALIDATE FACT GRAIN
    # =================================================

    actual_orders = fact.count()

    duplicate_orders = (
        fact.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_order_ids = fact.filter(
        F.col("order_id").isNull()
        | (F.trim("order_id") == "")
    ).count()

    # Verify all order dates exist in dim_date.
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

    print(f"Expected orders: {expected_orders:,}")
    print(f"Actual orders: {actual_orders:,}")
    print(f"Duplicate order IDs: {duplicate_orders:,}")
    print(f"Missing order IDs: {missing_order_ids:,}")
    print(f"Unmatched purchase dates: {unmatched_dates:,}")

    if actual_orders != expected_orders:
        raise ValueError(
            "Order count changed during aggregation/join"
        )

    if duplicate_orders or missing_order_ids:
        raise ValueError(
            "Order-level grain validation failed"
        )

    if unmatched_dates:
        raise ValueError(
            "Purchase date relationship validation failed"
        )

    # =================================================
    # 11. VERIFY AGGREGATED FINANCIAL TOTALS
    # =================================================

    source_item_total = items.agg(
        F.sum("total_item_value")
    ).first()[0]

    source_payment_total = payments.agg(
        F.sum("payment_value")
    ).first()[0]

    fact_totals = fact.agg(
        F.sum("total_listed_value").alias("items"),
        F.sum("total_payment_value").alias("payments")
    ).first()

    if fact_totals["items"] != source_item_total:
        raise ValueError(
            "Aggregated item totals do not match source"
        )

    if fact_totals["payments"] != source_payment_total:
        raise ValueError(
            "Aggregated payment totals do not match source"
        )

    print("\nFINANCIAL TOTAL VALIDATION PASSED")

    # =================================================
    # 12. WRITE GOLD FACT TABLE
    # =================================================

    target = GOLD_DIR / "fct_orders"

    fact.write.mode("overwrite").parquet(
        str(target)
    )

    verified = spark.read.parquet(str(target))

    if verified.count() != expected_orders:
        raise ValueError(
            "Gold order fact read-back failed"
        )

    print("\nGOLD FCT_ORDERS SAVED SUCCESSFULLY")

    # =================================================
    # 13. FINANCIAL RECONCILIATION SUMMARY
    # =================================================

    print("\n" + "=" * 60)
    print("ORDER-LEVEL FINANCIAL RECONCILIATION")
    print("=" * 60)

    verified.agg(
        F.count("*").alias("total_orders"),

        F.sum(
            F.col("has_items").cast("int")
        ).alias("orders_with_items"),

        F.sum(
            F.col("has_payments").cast("int")
        ).alias("orders_with_payments"),

        F.sum(
            (
                ~F.col("has_items")
            ).cast("int")
        ).alias("orders_without_items"),

        F.sum(
            (
                ~F.col("has_payments")
            ).cast("int")
        ).alias("orders_without_payments"),

        F.sum(
            (
                F.col("is_payment_reconciled") == True
            ).cast("int")
        ).alias("reconciled_orders"),

        F.sum(
            (
                F.col("is_payment_reconciled") == False
            ).cast("int")
        ).alias("mismatched_orders")
    ).show(truncate=False)

    # =================================================
    # 14. FINANCIAL TOTALS
    # =================================================

    print("\nFINANCIAL TOTALS")

    verified.agg(
        F.sum("merchandise_value")
        .alias("merchandise_value"),

        F.sum("freight_value")
        .alias("freight_value"),

        F.sum("total_listed_value")
        .alias("total_listed_value"),

        F.sum("total_payment_value")
        .alias("recorded_payments")
    ).show(truncate=False)

    # =================================================
    # 15. ORDER STATUS DISTRIBUTION
    # =================================================

    print("\nORDER STATUS DISTRIBUTION")

    verified.groupBy("order_status").agg(
        F.count("*").alias("orders"),

        F.sum("total_listed_value")
        .alias("listed_value"),

        F.sum("total_payment_value")
        .alias("payment_value")
    ).orderBy(
        F.desc("orders")
    ).show(truncate=False)

    # =================================================
    # 16. SAMPLE MISMATCHED ORDERS
    # =================================================

    print("\nSAMPLE PAYMENT MISMATCHES")

    verified.filter(
        F.col("is_payment_reconciled") == False
    ).select(
        "order_id",
        "order_status",
        "item_count",
        "payment_count",
        "total_listed_value",
        "total_payment_value",
        "payment_minus_listed_value"
    ).orderBy(
        F.desc(
            F.abs("payment_minus_listed_value")
        )
    ).show(10, truncate=False)

    print("\nGOLD FACT SCHEMA")
    verified.printSchema()

    print("\n" + "=" * 60)
    print("FCT_ORDERS COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
