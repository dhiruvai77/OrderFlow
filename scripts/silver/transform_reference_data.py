
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =====================================================
# 1. PROJECT CONFIGURATION
# =====================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = ROOT / "data" / "bronze" / "olist"
SILVER_DIR = ROOT / "data" / "silver" / "olist"

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Reference-Data")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =====================================================
# 2. GEOLOCATION TRANSFORMATION
# =====================================================

def transform_geolocation():

    print("\n" + "=" * 60)
    print("TRANSFORMING GEOLOCATION")
    print("=" * 60)

    source = BRONZE_DIR / "geolocation"
    target = SILVER_DIR / "geolocation"

    bronze = spark.read.parquet(str(source))
    source_count = bronze.count()

    # Retain raw coordinate values for validation
    silver = (
        bronze
        .withColumn(
            "_raw_lat",
            F.col("geolocation_lat")
        )
        .withColumn(
            "_raw_lng",
            F.col("geolocation_lng")
        )
        .withColumn(
            "geolocation_lat",
            F.expr(
                "try_cast(geolocation_lat AS DOUBLE)"
            )
        )
        .withColumn(
            "geolocation_lng",
            F.expr(
                "try_cast(geolocation_lng AS DOUBLE)"
            )
        )
        .withColumn(
            "geolocation_city",
            F.lower(F.trim("geolocation_city"))
        )
        .withColumn(
            "geolocation_state",
            F.upper(F.trim("geolocation_state"))
        )
    )

    # Flag numeric conversion failures
    silver = silver.withColumn(
        "has_conversion_error",
        F.coalesce(
            (
                F.col("_raw_lat").isNotNull()
                & (F.trim("_raw_lat") != "")
                & F.col("geolocation_lat").isNull()
            )
            |
            (
                F.col("_raw_lng").isNotNull()
                & (F.trim("_raw_lng") != "")
                & F.col("geolocation_lng").isNull()
            ),
            F.lit(False)
        )
    )

    silver = silver.drop("_raw_lat", "_raw_lng")

    # Validate geographic coordinate ranges
    silver = silver.withColumn(
        "is_valid_coordinate",
        F.coalesce(
            F.col("geolocation_lat").between(-90, 90)
            & F.col("geolocation_lng").between(-180, 180),
            F.lit(False)
        )
    )

    # ZIP codes are identifiers, not numeric quantities
    silver = silver.withColumn(
        "is_valid_zip",
        F.coalesce(
            F.col("geolocation_zip_code_prefix")
            .rlike(r"^[0-9]{5}$"),
            F.lit(False)
        )
    )

    silver = silver.withColumn(
        "is_valid_geolocation",
        F.col("is_valid_coordinate")
        & F.col("is_valid_zip")
        & ~F.col("has_conversion_error")
    )

    # Count exact duplicates using original source fields
    original_columns = [
        col for col in bronze.columns
        if not col.startswith("_")
    ]

    duplicate_excess = (
        bronze.groupBy(*original_columns)
        .count()
        .filter(F.col("count") > 1)
        .agg(
            F.sum(F.col("count") - 1).alias("duplicates")
        )
        .first()["duplicates"] or 0
    )

    # Preserve all records, including duplicates
    silver.write.mode("overwrite").parquet(str(target))

    verified = spark.read.parquet(str(target))
    target_count = verified.count()

    if source_count != target_count:
        raise ValueError("Geolocation row count mismatch")

    stats = verified.agg(
        F.sum(F.col("has_conversion_error").cast("int"))
        .alias("conversion_errors"),

        F.sum((~F.col("is_valid_coordinate")).cast("int"))
        .alias("invalid_coordinates"),

        F.sum((~F.col("is_valid_zip")).cast("int"))
        .alias("invalid_zip_codes"),

        F.sum(F.col("is_valid_geolocation").cast("int"))
        .alias("valid_records")
    ).first()

    print(f"Bronze records: {source_count:,}")
    print(f"Silver records: {target_count:,}")
    print(f"Exact duplicate excess rows: {duplicate_excess:,}")

    for name, value in stats.asDict().items():
        print(f"{name}: {value:,}")

    print("\nSilver schema:")
    verified.printSchema()

    print("GEOLOCATION TRANSFORMATION COMPLETED")


# =====================================================
# 3. CATEGORY TRANSLATION TRANSFORMATION
# =====================================================

def transform_categories():

    print("\n" + "=" * 60)
    print("TRANSFORMING CATEGORY TRANSLATIONS")
    print("=" * 60)

    source = BRONZE_DIR / "category_translation"
    target = SILVER_DIR / "category_translation"

    bronze = spark.read.parquet(str(source))
    source_count = bronze.count()

    silver = bronze

    for column in [
        "product_category_name",
        "product_category_name_english"
    ]:
        silver = silver.withColumn(
            column,
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column)) == ""),
                F.lit(None).cast("string")
            ).otherwise(F.lower(F.trim(F.col(column))))
        )

    silver = silver.withColumn(
        "is_valid_translation",
        F.col("product_category_name").isNotNull()
        & F.col("product_category_name_english").isNotNull()
    )

    duplicate_keys = (
        silver.groupBy("product_category_name")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_keys:
        raise ValueError(
            f"Duplicate category keys: {duplicate_keys}"
        )

    silver.write.mode("overwrite").parquet(str(target))

    verified = spark.read.parquet(str(target))
    target_count = verified.count()

    if source_count != target_count:
        raise ValueError(
            "Category translation row count mismatch"
        )

    invalid_count = verified.filter(
        ~F.col("is_valid_translation")
    ).count()

    print(f"Bronze records: {source_count:,}")
    print(f"Silver records: {target_count:,}")
    print(f"Duplicate category keys: {duplicate_keys:,}")
    print(f"Invalid translations: {invalid_count:,}")

    print("\nSample translations:")
    verified.select(
        "product_category_name",
        "product_category_name_english"
    ).show(10, truncate=False)

    print("CATEGORY TRANSFORMATION COMPLETED")


# =====================================================
# 4. EXECUTION
# =====================================================

try:
    print("=" * 60)
    print("ORDERFLOW - SILVER REFERENCE DATA")
    print("=" * 60)

    transform_geolocation()
    transform_categories()

    print("\nALL REFERENCE TRANSFORMATIONS COMPLETED")

finally:
    spark.stop()
