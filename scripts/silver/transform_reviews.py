
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==================================================
# 1. CONFIGURATION
# ==================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data" / "bronze" / "olist" / "reviews"
SILVER = ROOT / "data" / "silver" / "olist" / "reviews"

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Reviews")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("=" * 60)
    print("ORDERFLOW - SILVER REVIEWS TRANSFORMATION")
    print("=" * 60)

    # ==================================================
    # 2. READ BRONZE
    # ==================================================

    bronze = spark.read.parquet(str(BRONZE))
    bronze_count = bronze.count()

    print(f"\nBronze records: {bronze_count:,}")

    # ==================================================
    # 3. CONVERT DATA TYPES
    # ==================================================

    silver = (
        bronze
        .withColumn(
            "review_score",
            F.expr("try_cast(review_score AS INT)")
        )
        .withColumn(
            "review_creation_date",
            F.try_to_timestamp(
                F.col("review_creation_date"),
                F.lit("yyyy-MM-dd HH:mm:ss")
            )
        )
        .withColumn(
            "review_answer_timestamp",
            F.try_to_timestamp(
                F.col("review_answer_timestamp"),
                F.lit("yyyy-MM-dd HH:mm:ss")
            )
        )
    )

    # ==================================================
    # 4. DATA QUALITY FLAGS
    # ==================================================

    # Required identifiers
    silver = silver.withColumn(
        "is_valid_key",
        F.coalesce(
            F.col("review_id").isNotNull()
            & (F.trim("review_id") != "")
            & F.col("order_id").isNotNull()
            & (F.trim("order_id") != ""),
            F.lit(False)
        )
    )

    # Review scores must be between 1 and 5
    silver = silver.withColumn(
        "is_valid_score",
        F.coalesce(
            F.col("review_score").between(1, 5),
            F.lit(False)
        )
    )

    # Detect failed type conversions from source strings
    conversion_error = F.lit(False)

    for column in [
        "review_score",
        "review_creation_date",
        "review_answer_timestamp",
    ]:
        original = bronze.select(
            "review_id",
            "order_id",
            F.col(column).alias(f"original_{column}")
        )

        # Direct positional comparison is unnecessary here:
        # evaluate original source columns before conversion.
        source_value = F.col(f"_original_{column}")

    # Rebuild conversion checks using temporary source columns
    # so no joins or row-order assumptions are necessary.
    silver = bronze

    columns = {
        "review_score": "int",
        "review_creation_date": "timestamp",
        "review_answer_timestamp": "timestamp",
    }

    for column, dtype in columns.items():
        silver = silver.withColumn(
            f"_original_{column}",
            F.col(column)
        )

        if dtype == "timestamp":
            converted = F.try_to_timestamp(
                F.col(column),
                F.lit("yyyy-MM-dd HH:mm:ss")
            )
        else:
            converted = F.expr(
                f"try_cast(`{column}` AS INT)"
            )

        silver = silver.withColumn(column, converted)

        conversion_error = (
            conversion_error
            | (
                F.col(f"_original_{column}").isNotNull()
                & (F.trim(F.col(f"_original_{column}")) != "")
                & F.col(column).isNull()
            )
        )

    silver = (
        silver
        .withColumn(
            "has_conversion_error",
            F.coalesce(conversion_error, F.lit(False))
        )
        .drop(*[
            f"_original_{col}" for col in columns
        ])
    )

    # Restore validity flags on final DataFrame
    silver = (
        silver
        .withColumn(
            "is_valid_key",
            F.coalesce(
                F.col("review_id").isNotNull()
                & (F.trim("review_id") != "")
                & F.col("order_id").isNotNull()
                & (F.trim("order_id") != ""),
                F.lit(False)
            )
        )
        .withColumn(
            "is_valid_score",
            F.coalesce(
                F.col("review_score").between(1, 5),
                F.lit(False)
            )
        )
    )

    silver = silver.withColumn(
        "is_valid_review",
        F.col("is_valid_key")
        & F.col("is_valid_score")
        & ~F.col("has_conversion_error")
    )

    # ==================================================
    # 5. QUALITY SUMMARY
    # ==================================================

    silver_count = silver.count()

    if silver_count != bronze_count:
        raise ValueError("Review row-count mismatch")

    quality = silver.agg(
        F.sum(
            F.col("has_conversion_error").cast("int")
        ).alias("conversion_errors"),
        F.sum(
            (~F.col("is_valid_score")).cast("int")
        ).alias("invalid_scores"),
        F.sum(
            (~F.col("is_valid_key")).cast("int")
        ).alias("invalid_keys"),
        F.sum(
            F.col("is_valid_review").cast("int")
        ).alias("valid_reviews"),
    ).first()

    print("\nDATA QUALITY SUMMARY")

    for name, value in quality.asDict().items():
        print(f"{name}: {value:,}")

    # ==================================================
    # 6. WRITE AND VERIFY
    # ==================================================

    silver.write.mode("overwrite").parquet(str(SILVER))

    verified = spark.read.parquet(str(SILVER))
    target_count = verified.count()

    if target_count != bronze_count:
        raise ValueError("Parquet read-back failed")

    print(f"\nSilver records: {target_count:,}")

    print("\nSilver schema:")
    verified.printSchema()

    print("\nREVIEW SCORE DISTRIBUTION")
    verified.groupBy("review_score").count().orderBy(
        "review_score"
    ).show()

    print("\nSILVER REVIEWS TRANSFORMATION COMPLETED")

finally:
    spark.stop()
