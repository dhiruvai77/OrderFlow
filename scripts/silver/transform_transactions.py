
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
    "order_items": {
        "keys": ["order_id", "order_item_id"],
        "types": {
            "order_item_id": "int",
            "price": "decimal(12,2)",
            "freight_value": "decimal(12,2)",
            "shipping_limit_date": "timestamp",
        },
        "non_negative": ["price", "freight_value"],
    },
    "payments": {
        "keys": ["order_id", "payment_sequential"],
        "types": {
            "payment_sequential": "int",
            "payment_installments": "int",
            "payment_value": "decimal(12,2)",
        },
        "non_negative": ["payment_value"],
    },
}


# =====================================================
# 2. INITIALIZE SPARK
# =====================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Transactions")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =====================================================
# 3. REUSABLE TRANSFORMATION
# =====================================================

def transform_dataset(name, config):

    print("\n" + "=" * 60)
    print(f"PROCESSING: {name.upper()}")
    print("=" * 60)

    source_path = BRONZE_DIR / name
    target_path = SILVER_DIR / name

    bronze = spark.read.parquet(str(source_path))
    bronze_count = bronze.count()

    silver = bronze

    # Convert numeric and timestamp columns
    for column, dtype in config["types"].items():

        if dtype == "timestamp":
            silver = silver.withColumn(
                column,
                F.try_to_timestamp(
                    F.col(column),
                    F.lit("yyyy-MM-dd HH:mm:ss")
                )
            )
        else:
            silver = silver.withColumn(
                column,
                F.expr(f"try_cast(`{column}` AS {dtype})")
            )

    # Standardize payment type
    if name == "payments":
        silver = silver.withColumn(
            "payment_type",
            F.lower(F.trim(F.col("payment_type")))
        )

    # Check required identifiers
    key_conditions = []

    for key in config["keys"]:
        key_conditions.append(
            F.col(key).isNotNull()
            & (F.trim(F.col(key).cast("string")) != "")
        )

    valid_key = key_conditions[0]

    for condition in key_conditions[1:]:
        valid_key = valid_key & condition

    silver = silver.withColumn(
        "is_valid_key",
        F.coalesce(valid_key, F.lit(False))
    )

    # Flag failed type conversions
    conversion_errors = []

    for column in config["types"]:
        conversion_errors.append(
            (
                F.col(column).isNull()
                & F.col(f"source_{column}").isNotNull()
            )
        )

    # Identify invalid numeric values
    negative_conditions = [
        F.col(column) < 0
        for column in config["non_negative"]
    ]

    negative_flag = negative_conditions[0]

    for condition in negative_conditions[1:]:
        negative_flag = negative_flag | condition

    silver = silver.withColumn(
        "has_negative_amount",
        F.coalesce(negative_flag, F.lit(False))
    )

    # Validate primary/composite keys
    duplicates = (
        silver.groupBy(*config["keys"])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicates:
        raise ValueError(
            f"{name}: {duplicates} duplicate key groups"
        )

    invalid_keys = silver.filter(
        ~F.col("is_valid_key")
    ).count()

    # Verify record count
    silver_count = silver.count()

    if bronze_count != silver_count:
        raise ValueError(
            f"{name}: Row count mismatch"
        )

    # Write Silver Parquet
    (
        silver.write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    verified_count = spark.read.parquet(
        str(target_path)
    ).count()

    if verified_count != bronze_count:
        raise ValueError(
            f"{name}: Read-back validation failed"
        )

    print(f"Bronze records: {bronze_count:,}")
    print(f"Silver records: {verified_count:,}")
    print(f"Duplicate key groups: {duplicates:,}")
    print(f"Invalid keys: {invalid_keys:,}")

    negative_count = silver.filter(
        F.col("has_negative_amount")
    ).count()

    print(f"Negative monetary values: {negative_count:,}")

    print("\nSilver schema:")
    silver.printSchema()

    print(f"\n{name.upper()} TRANSFORMATION COMPLETED")


# =====================================================
# 4. EXECUTION
# =====================================================

try:
    for name, config in DATASETS.items():

        # Retain source values for conversion diagnostics
        source_path = BRONZE_DIR / name
        bronze = spark.read.parquet(str(source_path))

        print(f"\nSource columns for {name}: {len(bronze.columns)}")

        transform_dataset(name, config)

    print("\nALL TRANSACTION TRANSFORMATIONS COMPLETED")

finally:
    spark.stop()

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==========================================================
# 1. PROJECT CONFIGURATION
# ==========================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = ROOT / "data" / "bronze" / "olist"
SILVER_DIR = ROOT / "data" / "silver" / "olist"

# Ensure Spark uses the active Python virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable


# ==========================================================
# 2. DATASET CONFIGURATION
# ==========================================================

DATASETS = {
    "order_items": {
        "keys": ["order_id", "order_item_id"],
        "types": {
            "order_item_id": "int",
            "price": "decimal(12,2)",
            "freight_value": "decimal(12,2)",
            "shipping_limit_date": "timestamp",
        },
        "monetary_columns": [
            "price",
            "freight_value",
        ],
    },
    "payments": {
        "keys": ["order_id", "payment_sequential"],
        "types": {
            "payment_sequential": "int",
            "payment_installments": "int",
            "payment_value": "decimal(12,2)",
        },
        "monetary_columns": [
            "payment_value",
        ],
    },
}


# ==========================================================
# 3. INITIALIZE SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Transactions")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ==========================================================
# 4. REUSABLE TRANSFORMATION FUNCTION
# ==========================================================

def transform_dataset(name, config):

    print("\n" + "=" * 60)
    print(f"PROCESSING: {name.upper()}")
    print("=" * 60)

    source_path = BRONZE_DIR / name
    target_path = SILVER_DIR / name

    # ------------------------------------------------------
    # Read Bronze data
    # ------------------------------------------------------

    bronze = spark.read.parquet(str(source_path))

    bronze_count = bronze.count()

    silver = bronze

    print(f"\nBronze records: {bronze_count:,}")

    # ------------------------------------------------------
    # Convert data types and flag conversion failures
    # ------------------------------------------------------

    conversion_flags = []

    for column, dtype in config["types"].items():

        # Preserve original string temporarily
        original_column = f"_original_{column}"

        silver = silver.withColumn(
            original_column,
            F.col(column)
        )

        # Convert data types
        if dtype == "timestamp":
            silver = silver.withColumn(
                column,
                F.try_to_timestamp(
                    F.col(column),
                    F.lit("yyyy-MM-dd HH:mm:ss")
                )
            )
        else:
            silver = silver.withColumn(
                column,
                F.expr(
                    f"try_cast(`{column}` AS {dtype})"
                )
            )

        # Detect nonempty source values that failed conversion
        flag_name = f"_conversion_error_{column}"

        silver = silver.withColumn(
            flag_name,
            (
                F.col(original_column).isNotNull()
                & (F.trim(F.col(original_column)) != "")
                & F.col(column).isNull()
            )
        )

        conversion_flags.append(flag_name)

        # Remove temporary original column
        silver = silver.drop(original_column)

    # Combine conversion errors into a single flag
    conversion_condition = F.lit(False)

    for flag in conversion_flags:
        conversion_condition = (
            conversion_condition | F.col(flag)
        )

    silver = silver.withColumn(
        "has_conversion_error",
        conversion_condition
    )

    # Remove individual temporary conversion flags
    silver = silver.drop(*conversion_flags)

    # ------------------------------------------------------
    # Standardize payment type
    # ------------------------------------------------------

    if name == "payments":
        silver = silver.withColumn(
            "payment_type",
            F.lower(F.trim(F.col("payment_type")))
        )

    # ------------------------------------------------------
    # Validate composite keys
    # ------------------------------------------------------

    valid_key_condition = F.lit(True)

    for key in config["keys"]:
        valid_key_condition = (
            valid_key_condition
            & F.col(key).isNotNull()
            & (F.trim(F.col(key).cast("string")) != "")
        )

    silver = silver.withColumn(
        "is_valid_key",
        F.coalesce(
            valid_key_condition,
            F.lit(False)
        )
    )

    # ------------------------------------------------------
    # Identify negative monetary values
    # ------------------------------------------------------

    negative_condition = F.lit(False)

    for column in config["monetary_columns"]:
        negative_condition = (
            negative_condition
            | (F.col(column) < 0)
        )

    silver = silver.withColumn(
        "has_negative_amount",
        F.coalesce(
            negative_condition,
            F.lit(False)
        )
    )

    # ------------------------------------------------------
    # Overall quality indicator
    # ------------------------------------------------------

    silver = silver.withColumn(
        "is_valid_transaction",
        F.col("is_valid_key")
        & ~F.col("has_conversion_error")
        & ~F.col("has_negative_amount")
    )

    # ======================================================
    # 5. VALIDATE DATA
    # ======================================================

    silver_count = silver.count()

    if bronze_count != silver_count:
        raise ValueError(
            f"{name}: Row-count mismatch. "
            f"Bronze={bronze_count}, Silver={silver_count}"
        )

    # Check duplicate composite keys
    duplicate_groups = (
        silver.groupBy(*config["keys"])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_groups > 0:
        raise ValueError(
            f"{name}: Found {duplicate_groups} "
            "duplicate composite key groups"
        )

    # Calculate data quality statistics
    quality = silver.agg(
        F.sum(
            F.col("has_conversion_error").cast("int")
        ).alias("conversion_errors"),

        F.sum(
            F.col("has_negative_amount").cast("int")
        ).alias("negative_amounts"),

        F.sum(
            (~F.col("is_valid_key")).cast("int")
        ).alias("invalid_keys"),

        F.sum(
            F.col("is_valid_transaction").cast("int")
        ).alias("valid_transactions"),
    ).first()

    # ======================================================
    # 6. WRITE SILVER DATA
    # ======================================================

    (
        silver.write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    # Read back and verify
    verified = spark.read.parquet(str(target_path))

    verified_count = verified.count()

    if verified_count != bronze_count:
        raise ValueError(
            f"{name}: Parquet read-back validation failed"
        )

    # ======================================================
    # 7. DISPLAY RESULTS
    # ======================================================

    print(f"\nSilver records: {verified_count:,}")
    print(f"Duplicate key groups: {duplicate_groups:,}")

    print("\nDATA QUALITY SUMMARY")
    print(f"Conversion errors: {quality['conversion_errors']:,}")
    print(f"Negative monetary values: {quality['negative_amounts']:,}")
    print(f"Invalid keys: {quality['invalid_keys']:,}")
    print(f"Valid transactions: {quality['valid_transactions']:,}")

    print("\nSILVER SCHEMA")
    verified.printSchema()

    print("\nSAMPLE RECORDS")
    verified.show(5, truncate=False)

    print(f"\n{name.upper()} TRANSFORMATION COMPLETED")


# ==========================================================
# 8. EXECUTE ALL TRANSACTION TRANSFORMATIONS
# ==========================================================

try:
    print("=" * 60)
    print("ORDERFLOW - SILVER TRANSACTION TRANSFORMATIONS")
    print("=" * 60)

    for name, config in DATASETS.items():
        transform_dataset(name, config)

    print("\n" + "=" * 60)
    print("ALL TRANSACTION TRANSFORMATIONS COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
