
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
    .appName("OrderFlow-Gold-Order-Items-Fact")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - BUILD FCT_ORDER_ITEMS")
    print("=" * 60)

    # =================================================
    # 3. READ SILVER DATA
    # =================================================

    items = spark.read.parquet(
        str(SILVER_DIR / "order_items")
    )

    orders = spark.read.parquet(
        str(SILVER_DIR / "orders")
    )

    dim_date = spark.read.parquet(
        str(GOLD_DIR / "dim_date")
    )

    item_count = items.count()
    order_count = orders.count()

    print(f"\nSilver order items: {item_count:,}")
    print(f"Silver orders: {order_count:,}")

    # =================================================
    # 4. VALIDATE ORDER LOOKUP UNIQUENESS
    # =================================================

    order_lookup = orders.select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_status"
    )

    duplicate_orders = (
        order_lookup
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_orders:
        raise ValueError(
            f"Found {duplicate_orders} duplicate order IDs"
        )

    # =================================================
    # 5. JOIN ITEMS WITH THEIR PARENT ORDERS
    # =================================================

    # Many item lines can belong to one order.
    # The order lookup must contain one row per order.

    joined = items.join(
        order_lookup,
        on="order_id",
        how="left"
    )

    # =================================================
    # 6. BUILD FACT TABLE
    # =================================================

    fact = joined.select(
        F.col("order_id"),
        F.col("order_item_id"),
        F.col("product_id"),
        F.col("seller_id"),
        F.col("customer_id"),

        F.date_format(
            F.col("order_purchase_timestamp"),
            "yyyyMMdd"
        ).cast("int").alias("purchase_date_key"),

        F.col("order_purchase_timestamp")
        .alias("purchase_timestamp"),

        F.col("shipping_limit_date"),
        F.col("order_status"),

        F.col("price"),
        F.col("freight_value"),

        (
            F.col("price") + F.col("freight_value")
        ).cast("decimal(12,2)").alias(
            "total_item_value"
        )
    )

    # =================================================
    # 7. VALIDATE FACT TABLE GRAIN
    # =================================================

    fact_count = fact.count()

    duplicate_items = (
        fact.groupBy("order_id", "order_item_id")
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

    # Verify every purchase date exists in dim_date
    valid_dates = dim_date.select(
        "date_key"
    ).distinct()

    unmatched_dates = (
        fact.select("purchase_date_key")
        .join(
            valid_dates,
            F.col("purchase_date_key")
            == F.col("date_key"),
            "left_anti"
        )
        .count()
    )

    print("\nFACT TABLE VALIDATION")
    print(f"Expected rows: {item_count:,}")
    print(f"Actual rows: {fact_count:,}")
    print(f"Duplicate item keys: {duplicate_items:,}")
    print(f"Missing customer IDs: {missing_customer:,}")
    print(f"Missing purchase dates: {missing_dates:,}")
    print(f"Unmatched date keys: {unmatched_dates:,}")

    if fact_count != item_count:
        raise ValueError(
            "Fact table row count changed during join"
        )

    if duplicate_items:
        raise ValueError(
            "Duplicate order item keys detected"
        )

    if missing_customer or missing_dates or unmatched_dates:
        raise ValueError(
            "Fact table relationship validation failed"
        )

    # =================================================
    # 8. WRITE GOLD PARQUET
    # =================================================

    target = GOLD_DIR / "fct_order_items"

    fact.write.mode("overwrite").parquet(
        str(target)
    )

    verified = spark.read.parquet(str(target))

    verified_count = verified.count()

    if verified_count != item_count:
        raise ValueError(
            "Fact table read-back validation failed"
        )

    print(f"\nVerified Gold records: {verified_count:,}")

    # =================================================
    # 9. CALCULATE BUSINESS METRICS
    # =================================================

    print("\nSALES VALUE SUMMARY")

    verified.agg(
        F.count("*").alias("item_lines"),

        F.countDistinct("order_id")
        .alias("orders_with_items"),

        F.sum("price")
        .alias("merchandise_value"),

        F.sum("freight_value")
        .alias("total_freight"),

        F.sum("total_item_value")
        .alias("total_listed_value")
    ).show(truncate=False)

    # =================================================
    # 10. DISPLAY SAMPLE
    # =================================================

    print("\nSAMPLE FACT RECORDS")

    verified.select(
        "order_id",
        "order_item_id",
        "product_id",
        "customer_id",
        "purchase_date_key",
        "price",
        "freight_value",
        "total_item_value"
    ).show(5, truncate=False)

    print("\nFACT TABLE SCHEMA")
    verified.printSchema()

    print("\n" + "=" * 60)
    print("FCT_ORDER_ITEMS COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
