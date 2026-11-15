
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data" / "silver" / "olist" / "orders"

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Orders-Quality")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    orders = spark.read.parquet(str(SILVER))

    print("=" * 60)
    print("ORDERFLOW - SILVER ORDERS QUALITY REPORT")
    print("=" * 60)

    total = orders.count()
    print(f"\nTotal records: {total:,}")

    print("\n1. REQUIRED FIELDS")

    checks = {
        "Missing order ID":
            F.col("order_id").isNull()
            | (F.trim("order_id") == ""),

        "Missing customer ID":
            F.col("customer_id").isNull()
            | (F.trim("customer_id") == ""),

        "Missing purchase timestamp":
            F.col("order_purchase_timestamp").isNull(),

        "Duplicate order IDs":
            None,

        "Negative delivery duration":
            F.col("delivery_days") < 0,

        "Delivered status without delivery date":
            (F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNull(),

        "Non-delivered status with delivery date":
            (F.col("order_status") != "delivered")
            & F.col("order_delivered_customer_date").isNotNull(),

        "Delivery before carrier handoff":
            F.col("order_delivered_customer_date")
            < F.col("order_delivered_carrier_date"),
    }

    for name, condition in checks.items():
        if condition is None:
            count = (
                orders.groupBy("order_id")
                .count()
                .filter(F.col("count") > 1)
                .agg(F.sum(F.col("count") - 1))
                .first()[0] or 0
            )
        else:
            count = orders.filter(condition).count()

        print(f"{name}: {count:,}")

    print("\n2. DELIVERY PERFORMANCE")

    orders.agg(
        F.count("delivery_days").alias("orders_with_duration"),
        F.round(F.avg("delivery_days"), 2).alias("avg_days"),
        F.round(F.min("delivery_days"), 2).alias("min_days"),
        F.round(F.max("delivery_days"), 2).alias("max_days"),
        F.sum(
            F.when(F.col("is_late_delivery") == True, 1)
            .otherwise(0)
        ).alias("late_deliveries"),
    ).show(truncate=False)

    print("\n3. VALID ORDER FLAG")

    orders.groupBy("is_valid_order").count().show()

    print("\nQUALITY REPORT COMPLETED")

finally:
    spark.stop()
