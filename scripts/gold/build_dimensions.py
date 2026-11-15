
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

spark = (
    SparkSession.builder
    .appName("OrderFlow-Gold-Dimensions")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =====================================================
# 2. REUSABLE DIMENSION VALIDATION
# =====================================================

def validate_dimension(df, key, name, expected_count):

    print(f"\nValidating {name}...")

    total = df.count()

    missing_keys = df.filter(
        F.col(key).isNull()
        | (F.trim(F.col(key)) == "")
    ).count()

    duplicate_keys = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Rows: {total:,}")
    print(f"Missing keys: {missing_keys:,}")
    print(f"Duplicate key groups: {duplicate_keys:,}")

    if total != expected_count:
        raise ValueError(
            f"{name}: Expected {expected_count:,} rows, "
            f"found {total:,}"
        )

    if missing_keys > 0 or duplicate_keys > 0:
        raise ValueError(
            f"{name}: Dimension key validation failed"
        )

    print(f"{name}: VALIDATION PASSED")


# =====================================================
# 3. BUILD CUSTOMER DIMENSION
# =====================================================

def build_dim_customer():

    print("\n" + "=" * 60)
    print("BUILDING DIM_CUSTOMER")
    print("=" * 60)

    customers = spark.read.parquet(
        str(SILVER_DIR / "customers")
    )

    dim_customer = customers.select(
        "customer_id",
        "customer_unique_id",
        F.col("customer_zip_code_prefix").alias(
            "zip_code_prefix"
        ),
        F.col("customer_city").alias("city"),
        F.upper(F.col("customer_state")).alias("state"),
    )

    validate_dimension(
        dim_customer,
        "customer_id",
        "dim_customer",
        customers.count()
    )

    target = GOLD_DIR / "dim_customer"

    dim_customer.write.mode("overwrite").parquet(
        str(target)
    )

    verified = spark.read.parquet(str(target))

    if verified.count() != dim_customer.count():
        raise ValueError("dim_customer write validation failed")

    print("\nCustomer dimension sample:")
    verified.show(5, truncate=False)

    print("DIM_CUSTOMER COMPLETED")


# =====================================================
# 4. BUILD SELLER DIMENSION
# =====================================================

def build_dim_seller():

    print("\n" + "=" * 60)
    print("BUILDING DIM_SELLER")
    print("=" * 60)

    sellers = spark.read.parquet(
        str(SILVER_DIR / "sellers")
    )

    dim_seller = sellers.select(
        "seller_id",
        F.col("seller_zip_code_prefix").alias(
            "zip_code_prefix"
        ),
        F.col("seller_city").alias("city"),
        F.upper(F.col("seller_state")).alias("state"),
    )

    validate_dimension(
        dim_seller,
        "seller_id",
        "dim_seller",
        sellers.count()
    )

    target = GOLD_DIR / "dim_seller"

    dim_seller.write.mode("overwrite").parquet(
        str(target)
    )

    verified = spark.read.parquet(str(target))

    if verified.count() != dim_seller.count():
        raise ValueError("dim_seller write validation failed")

    print("\nSeller dimension sample:")
    verified.show(5, truncate=False)

    print("DIM_SELLER COMPLETED")


# =====================================================
# 5. EXECUTION
# =====================================================

try:
    print("=" * 60)
    print("ORDERFLOW - GOLD DIMENSIONS")
    print("=" * 60)

    build_dim_customer()
    build_dim_seller()

    print("\n" + "=" * 60)
    print("GOLD DIMENSIONS COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
