
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =====================================================
# 1. CONFIGURATION
# =====================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = ROOT / "data" / "bronze" / "olist"
SILVER_DIR = ROOT / "data" / "silver" / "olist"

os.environ["PYSPARK_PYTHON"] = sys.executable

DATASETS = {
    "customers": {
        "key": "customer_id",
        "numeric_columns": [],
        "text_columns": ["customer_city", "customer_state"],
    },
    "sellers": {
        "key": "seller_id",
        "numeric_columns": [],
        "text_columns": ["seller_city", "seller_state"],
    },
    "products": {
        "key": "product_id",
        "numeric_columns": [
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "text_columns": [],
    },
}

# =====================================================
# 2. SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Dimensions")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =====================================================
# 3. TRANSFORMATION
# =====================================================

def transform_dataset(name, config):

    print(f"\nProcessing: {name}")

    source = BRONZE_DIR / name
    target = SILVER_DIR / name

    df = spark.read.parquet(str(source))
    source_count = df.count()

    silver = df

    # Standardize descriptive text columns
    for column in config["text_columns"]:
        silver = silver.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None).cast("string")
            ).otherwise(
                F.lower(F.trim(F.col(column)))
            )
        )

    # Convert product attributes to numeric types
    for column in config["numeric_columns"]:
        silver = silver.withColumn(
            column,
            F.expr(
                f"try_cast(`{column}` AS INT)"
            )
        )

    # Preserve ZIP code prefixes as strings
    # to avoid losing leading zeros.

    # Check primary key presence
    key = config["key"]

    silver = silver.withColumn(
        "is_valid_key",
        F.col(key).isNotNull()
        & (F.trim(F.col(key)) != "")
    )

    # Check duplicate keys
    duplicate_count = (
        silver.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{name}: Duplicate primary keys detected"
        )

    # Preserve all records
    target_count = silver.count()

    if source_count != target_count:
        raise ValueError(
            f"{name}: Row count mismatch"
        )

    # Write Silver dataset
    (
        silver.write
        .mode("overwrite")
        .parquet(str(target))
    )

    # Read-back verification
    verified_count = spark.read.parquet(
        str(target)
    ).count()

    if verified_count != source_count:
        raise ValueError(
            f"{name}: Parquet verification failed"
        )

    print(f"Source records: {source_count:,}")
    print(f"Silver records: {verified_count:,}")
    print(f"Duplicate key groups: {duplicate_count:,}")

    invalid_keys = silver.filter(
        ~F.col("is_valid_key")
    ).count()

    print(f"Invalid keys: {invalid_keys:,}")

    print("\nSilver schema:")
    silver.printSchema()

    print(f"{name}: COMPLETED")


# =====================================================
# 4. EXECUTE
# =====================================================

try:
    print("=" * 60)
    print("ORDERFLOW - SILVER DIMENSION TRANSFORMATIONS")
    print("=" * 60)

    for name, config in DATASETS.items():
        transform_dataset(name, config)

    print("\nALL THREE DATASETS TRANSFORMED SUCCESSFULLY")

finally:
    spark.stop()
