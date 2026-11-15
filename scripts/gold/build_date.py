
import os
import sys
from pathlib import Path
from datetime import timedelta

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
    .appName("OrderFlow-Gold-Date-Dimension")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - BUILD DIM_DATE")
    print("=" * 60)

    # =================================================
    # 3. READ SILVER ORDERS
    # =================================================

    orders = spark.read.parquet(
        str(SILVER_DIR / "orders")
    )

    # =================================================
    # 4. DETERMINE DATE RANGE
    # =================================================

    date_range = orders.agg(
        F.min(
            F.to_date("order_purchase_timestamp")
        ).alias("min_date"),

        F.max(
            F.to_date("order_purchase_timestamp")
        ).alias("max_date")
    ).first()

    start_date = date_range["min_date"]
    end_date = date_range["max_date"]

    if start_date is None or end_date is None:
        raise ValueError(
            "Cannot generate date dimension: "
            "purchase dates are missing"
        )

    expected_days = (
        end_date - start_date
    ).days + 1

    print(f"\nStart date: {start_date}")
    print(f"End date: {end_date}")
    print(f"Expected calendar days: {expected_days:,}")

    # =================================================
    # 5. GENERATE CALENDAR DATES
    # =================================================

    calendar = spark.range(1).select(
        F.explode(
            F.sequence(
                F.lit(start_date),
                F.lit(end_date),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("date")
    )

    # =================================================
    # 6. CREATE DATE DIMENSION
    # =================================================

    dim_date = calendar.select(
        F.date_format("date", "yyyyMMdd")
        .cast("int")
        .alias("date_key"),

        F.col("date"),

        F.year("date").alias("year"),

        F.quarter("date").alias("quarter"),

        F.month("date").alias("month"),

        F.date_format("date", "MMMM")
        .alias("month_name"),

        F.dayofmonth("date").alias("day"),

        F.dayofweek("date").alias("day_of_week"),

        F.date_format("date", "EEEE")
        .alias("day_name"),

        F.weekofyear("date").alias("week_of_year"),

        F.date_format("date", "yyyy-MM")
        .alias("year_month"),

        F.dayofweek("date")
        .isin(1, 7)
        .alias("is_weekend")
    )

    # =================================================
    # 7. VALIDATE DATE DIMENSION
    # =================================================

    actual_count = dim_date.count()

    missing_keys = dim_date.filter(
        F.col("date_key").isNull()
    ).count()

    duplicate_keys = (
        dim_date.groupBy("date_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("\nVALIDATION SUMMARY")
    print(f"Expected rows: {expected_days:,}")
    print(f"Actual rows: {actual_count:,}")
    print(f"Missing date keys: {missing_keys:,}")
    print(f"Duplicate date keys: {duplicate_keys:,}")

    if actual_count != expected_days:
        raise ValueError(
            "Date dimension row-count mismatch"
        )

    if missing_keys or duplicate_keys:
        raise ValueError(
            "Date dimension key validation failed"
        )

    # =================================================
    # 8. WRITE GOLD PARQUET
    # =================================================

    target = GOLD_DIR / "dim_date"

    dim_date.write.mode("overwrite").parquet(
        str(target)
    )

    # =================================================
    # 9. READ-BACK VERIFICATION
    # =================================================

    verified = spark.read.parquet(str(target))

    verified_count = verified.count()

    if verified_count != expected_days:
        raise ValueError(
            "Date dimension read-back validation failed"
        )

    print(f"\nVerified Gold records: {verified_count:,}")

    print("\nDATE DIMENSION SCHEMA")
    verified.printSchema()

    print("\nSAMPLE CALENDAR DATES")

    verified.orderBy("date").show(
        10,
        truncate=False
    )

    print("\n" + "=" * 60)
    print("DIM_DATE COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
