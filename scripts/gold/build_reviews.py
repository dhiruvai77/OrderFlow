
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
    .appName("OrderFlow-Gold-Reviews-Fact")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - BUILD FCT_REVIEWS")
    print("=" * 60)

    # =================================================
    # 3. READ SOURCE DATA
    # =================================================

    reviews = spark.read.parquet(
        str(SILVER_DIR / "reviews")
    )

    orders = spark.read.parquet(
        str(GOLD_DIR / "fct_orders")
    )

    review_count = reviews.count()

    print(f"\nSilver review records: {review_count:,}")
    print(f"Gold order records: {orders.count():,}")

    # =================================================
    # 4. PREPARE ORDER LOOKUP
    # =================================================

    order_lookup = orders.select(
        "order_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "delivery_days",
        "is_late_delivery",
        "has_delivery_anomaly",
        "is_delivery_metric_eligible"
    )

    duplicate_orders = (
        order_lookup
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_orders > 0:
        raise ValueError(
            f"Duplicate order IDs: {duplicate_orders}"
        )

    # =================================================
    # 5. JOIN REVIEWS WITH ORDERS
    # =================================================

    joined = reviews.join(
        order_lookup,
        on="order_id",
        how="left"
    )

    # =================================================
    # 6. CREATE FACT TABLE
    # =================================================

    fact = joined.select(
        "review_id",
        "order_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "delivery_days",
        "is_late_delivery",
        "has_delivery_anomaly",
        "is_delivery_metric_eligible",
        "is_valid_review"
    )

    # Review sentiment buckets based on star ratings.
    fact = fact.withColumn(
        "review_category",
        F.when(
            F.col("review_score") <= 2,
            "negative"
        ).when(
            F.col("review_score") == 3,
            "neutral"
        ).when(
            F.col("review_score") >= 4,
            "positive"
        ).otherwise("unknown")
    )

    # Check whether written feedback is available.
    fact = fact.withColumn(
        "has_written_review",
        F.coalesce(
            F.col("review_comment_message").isNotNull()
            & (
                F.trim("review_comment_message") != ""
            ),
            F.lit(False)
        )
    )

    # Calculate response time in hours.
    fact = fact.withColumn(
        "review_response_hours",
        F.when(
            F.col("review_creation_date").isNotNull()
            & F.col("review_answer_timestamp").isNotNull(),
            F.round(
                (
                    F.unix_seconds(
                        "review_answer_timestamp"
                    )
                    - F.unix_seconds(
                        "review_creation_date"
                    )
                ) / 3600.0,
                2
            )
        ).otherwise(F.lit(None).cast("double"))
    )

    # Flag impossible negative response durations.
    fact = fact.withColumn(
        "has_negative_response_time",
        F.coalesce(
            F.col("review_response_hours") < 0,
            F.lit(False)
        )
    )

    # =================================================
    # 7. VALIDATE FACT TABLE GRAIN
    # =================================================

    fact_count = fact.count()

    duplicate_reviews = (
        fact.groupBy("review_id", "order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_keys = fact.filter(
        F.col("review_id").isNull()
        | F.col("order_id").isNull()
        | (F.trim("review_id") == "")
        | (F.trim("order_id") == "")
    ).count()

    unmatched_orders = fact.filter(
        F.col("customer_id").isNull()
    ).count()

    print("\nFACT TABLE VALIDATION")
    print(f"Expected rows: {review_count:,}")
    print(f"Actual rows: {fact_count:,}")
    print(f"Duplicate review keys: {duplicate_reviews:,}")
    print(f"Missing review keys: {missing_keys:,}")
    print(f"Unmatched orders: {unmatched_orders:,}")

    if fact_count != review_count:
        raise ValueError(
            "Review row count changed during join"
        )

    if duplicate_reviews or missing_keys:
        raise ValueError(
            "Review fact key validation failed"
        )

    if unmatched_orders:
        raise ValueError(
            "Review-to-order relationship failed"
        )

    # =================================================
    # 8. WRITE GOLD PARQUET
    # =================================================

    target = GOLD_DIR / "fct_reviews"

    fact.write.mode("overwrite").parquet(
        str(target)
    )

    verified = spark.read.parquet(str(target))

    if verified.count() != review_count:
        raise ValueError(
            "Review fact read-back validation failed"
        )

    print(f"\nVerified Gold records: {review_count:,}")

    # =================================================
    # 9. REVIEW QUALITY AND SATISFACTION
    # =================================================

    print("\nREVIEW QUALITY SUMMARY")

    verified.agg(
        F.count("*").alias("review_records"),

        F.round(
            F.avg("review_score"), 2
        ).alias("average_review_score"),

        F.sum(
            F.col("has_written_review").cast("int")
        ).alias("written_reviews"),

        F.sum(
            F.col("has_negative_response_time").cast("int")
        ).alias("negative_response_durations"),

        F.round(
            F.avg(
                F.when(
                    F.col("review_response_hours") >= 0,
                    F.col("review_response_hours")
                )
            ), 2
        ).alias("avg_response_hours")
    ).show(truncate=False)

    # =================================================
    # 10. REVIEW CATEGORY DISTRIBUTION
    # =================================================

    print("\nREVIEW CATEGORY DISTRIBUTION")

    verified.groupBy("review_category").agg(
        F.count("*").alias("reviews"),
        F.round(
            F.avg("review_score"), 2
        ).alias("avg_score")
    ).orderBy(
        F.desc("reviews")
    ).show(truncate=False)

    # =================================================
    # 11. DELIVERY VS. CUSTOMER SATISFACTION
    # =================================================

    print("\nDELIVERY VS. REVIEW SCORE")

    # Include only orders with reliable delivery metrics.
    delivery_reviews = verified.filter(
        F.col("is_delivery_metric_eligible") == True
    )

    delivery_reviews.groupBy(
        "is_late_delivery"
    ).agg(
        F.count("*").alias("review_records"),

        F.round(
            F.avg("review_score"), 2
        ).alias("average_review_score"),

        F.round(
            F.avg(
                F.when(
                    F.col("review_score") <= 2,
                    1.0
                ).otherwise(0.0)
            ) * 100,
            2
        ).alias("negative_review_pct"),

        F.round(
            F.avg("delivery_days"), 2
        ).alias("average_delivery_days")
    ).orderBy(
        "is_late_delivery"
    ).show(truncate=False)

    # =================================================
    # 12. SAMPLE AND SCHEMA
    # =================================================

    print("\nSAMPLE REVIEW FACT RECORDS")

    verified.select(
        "review_id",
        "order_id",
        "review_score",
        "review_category",
        "delivery_days",
        "is_late_delivery",
        "review_response_hours"
    ).show(10, truncate=False)

    print("\nGOLD REVIEW FACT SCHEMA")
    verified.printSchema()

    print("\n" + "=" * 60)
    print("FCT_REVIEWS COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
