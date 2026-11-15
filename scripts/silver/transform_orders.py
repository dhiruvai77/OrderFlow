
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==========================================================
# 1. PROJECT CONFIGURATION
# ==========================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data" / "bronze" / "olist" / "orders"
SILVER = ROOT / "data" / "silver" / "olist" / "orders"

# Ensure Spark uses our virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable


# ==========================================================
# 2. INITIALIZE SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Orders")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - SILVER ORDERS TRANSFORMATION")
    print("=" * 60)

    # ======================================================
    # 3. READ BRONZE DATA
    # ======================================================

    orders = spark.read.parquet(str(BRONZE))

    bronze_count = orders.count()

    print(f"\nBronze records: {bronze_count:,}")

    # ======================================================
    # 4. CONVERT TIMESTAMP COLUMNS
    # ======================================================

    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    silver = orders

    for column in timestamp_columns:
        silver = silver.withColumn(
            column,
            F.to_timestamp(
                F.col(column),
                "yyyy-MM-dd HH:mm:ss"
            )
        )

    # ======================================================
    # 5. CALCULATE DELIVERY DURATION
    # ======================================================

    # Delivery time in days (including fractional days)
    silver = silver.withColumn(
        "delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull(),

            F.round(
                (
                    F.unix_seconds(
                        "order_delivered_customer_date"
                    )
                    - F.unix_seconds(
                        "order_purchase_timestamp"
                    )
                ) / 86400.0,
                2
            )
        ).otherwise(F.lit(None).cast("double"))
    )

    # ======================================================
    # 6. IDENTIFY LATE DELIVERIES
    # ======================================================

    # Compare calendar dates, not exact clock times
    silver = silver.withColumn(
        "is_late_delivery",
        F.when(
            F.col("order_delivered_customer_date").isNull()
            | F.col("order_estimated_delivery_date").isNull(),

            F.lit(None).cast("boolean")
        ).otherwise(
            F.to_date("order_delivered_customer_date")
            > F.to_date("order_estimated_delivery_date")
        )
    )

    # ======================================================
    # 7. VALIDATE ESSENTIAL ORDER FIELDS
    # ======================================================

    silver = silver.withColumn(
        "is_valid_order",
        F.col("order_id").isNotNull()
        & (F.trim(F.col("order_id")) != "")
        & F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
        & F.col("order_purchase_timestamp").isNotNull()
    )

    # ======================================================
    # 8. DELIVERY LIFECYCLE QUALITY FLAGS
    # ======================================================

    # Flag 1: Delivered status but no customer delivery date
    silver = silver.withColumn(
        "flag_delivered_missing_date",
        (
            (F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNull()
        )
    )

    # Flag 2: Non-delivered status but delivery date exists
    silver = silver.withColumn(
        "flag_status_date_mismatch",
        F.coalesce(
            (
                (F.col("order_status") != "delivered")
                & F.col("order_delivered_customer_date").isNotNull()
            ),
            F.lit(False)
        )
    )

    # Flag 3: Customer received order before carrier handoff
    silver = silver.withColumn(
        "flag_delivery_before_carrier",
        F.coalesce(
            (
                F.col("order_delivered_customer_date")
                < F.col("order_delivered_carrier_date")
            ),
            F.lit(False)
        )
    )

    # ======================================================
    # 9. COMBINED DELIVERY ANOMALY FLAG
    # ======================================================

    silver = silver.withColumn(
        "has_delivery_anomaly",
        F.col("flag_delivered_missing_date")
        | F.col("flag_status_date_mismatch")
        | F.col("flag_delivery_before_carrier")
    )

    # ======================================================
    # 10. DELIVERY METRIC ELIGIBILITY
    # ======================================================

    # Only consistent delivered orders should contribute
    # to standard delivery performance metrics.
    silver = silver.withColumn(
        "is_delivery_metric_eligible",
        F.col("is_valid_order")
        & (F.col("order_status") == "delivered")
        & F.col("delivery_days").isNotNull()
        & (F.col("delivery_days") >= 0)
        & ~F.col("has_delivery_anomaly")
    )

    # ======================================================
    # 11. VALIDATE TRANSFORMATION
    # ======================================================

    silver_count = silver.count()

    print(f"Silver records: {silver_count:,}")

    if bronze_count != silver_count:
        raise ValueError(
            f"Row-count mismatch: "
            f"Bronze={bronze_count}, Silver={silver_count}"
        )

    print("\nROW-COUNT VALIDATION PASSED")

    # ======================================================
    # 12. WRITE SILVER DATA
    # ======================================================

    (
        silver.write
        .mode("overwrite")
        .parquet(str(SILVER))
    )

    print(f"\nSilver Parquet saved to:\n{SILVER}")

    # ======================================================
    # 13. DISPLAY SCHEMA
    # ======================================================

    print("\nSILVER SCHEMA")
    silver.printSchema()

    # ======================================================
    # 14. DISPLAY SAMPLE RECORDS
    # ======================================================

    print("\nSAMPLE TRANSFORMED ORDERS")

    silver.select(
        "order_id",
        "order_status",
        "delivery_days",
        "is_late_delivery",
        "is_valid_order",
        "has_delivery_anomaly",
        "is_delivery_metric_eligible"
    ).show(10, truncate=False)

    # ======================================================
    # 15. DELIVERY QUALITY SUMMARY
    # ======================================================

    print("\n" + "=" * 60)
    print("DELIVERY QUALITY SUMMARY")
    print("=" * 60)

    silver.agg(
        F.sum(
            F.col("flag_delivered_missing_date").cast("int")
        ).alias("delivered_missing_date"),

        F.sum(
            F.col("flag_status_date_mismatch").cast("int")
        ).alias("status_date_mismatch"),

        F.sum(
            F.col("flag_delivery_before_carrier").cast("int")
        ).alias("delivery_before_carrier"),

        F.sum(
            F.col("has_delivery_anomaly").cast("int")
        ).alias("unique_anomalous_orders"),

        F.sum(
            F.col("is_delivery_metric_eligible").cast("int")
        ).alias("eligible_for_delivery_metrics")
    ).show(truncate=False)

    # ======================================================
    # 16. DELIVERY PERFORMANCE SUMMARY
    # ======================================================

    print("\nDELIVERY PERFORMANCE SUMMARY")

    silver.filter(
        F.col("is_delivery_metric_eligible")
    ).agg(
        F.count("delivery_days").alias("eligible_orders"),

        F.round(
            F.avg("delivery_days"), 2
        ).alias("average_delivery_days"),

        F.round(
            F.min("delivery_days"), 2
        ).alias("minimum_delivery_days"),

        F.round(
            F.max("delivery_days"), 2
        ).alias("maximum_delivery_days"),

        F.sum(
            F.when(
                F.col("is_late_delivery") == True, 1
            ).otherwise(0)
        ).alias("late_deliveries")
    ).show(truncate=False)

    print("\n" + "=" * 60)
    print("SILVER ORDERS TRANSFORMATION COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
