
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "olist"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "olist"

os.environ["PYSPARK_PYTHON"] = sys.executable

DATASETS = {
    "customers": ("olist_customers_dataset.csv", 99441),
    "geolocation": ("olist_geolocation_dataset.csv", 1000163),
    "order_items": ("olist_order_items_dataset.csv", 112650),
    "payments": ("olist_order_payments_dataset.csv", 103886),
    "reviews": ("olist_order_reviews_dataset.csv", 99224),
    "orders": ("olist_orders_dataset.csv", 99441),
    "products": ("olist_products_dataset.csv", 32951),
    "sellers": ("olist_sellers_dataset.csv", 3095),
    "category_translation": (
        "product_category_name_translation.csv", 71
    ),
}

# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OrderFlow-Bronze-Ingestion")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def ingest_dataset(name, filename, expected_count):
    source_path = RAW_DIR / filename
    target_path = BRONZE_DIR / name

    print(f"\nProcessing: {name}")

    if not source_path.is_file():
        raise FileNotFoundError(source_path)

    # Read original CSV data
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .csv(str(source_path))
    )


    source_count = df.count()

    if source_count != expected_count:
        raise ValueError(
            f"{name}: Expected {expected_count:,} rows, "
            f"found {source_count:,}"
        )

    # Add ingestion metadata
    bronze_df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(filename))
    )

    # Write Parquet files
    (
        bronze_df.write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    # Verify output
    target_count = spark.read.parquet(
        str(target_path)
    ).count()

    if source_count != target_count:
        raise ValueError(
            f"{name}: Row-count validation failed"
        )

    print(f"Source rows: {source_count:,}")
    print(f"Target rows: {target_count:,}")
    print("Validation: PASSED")

    return source_count


try:
    print("=" * 60)
    print("ORDERFLOW - BRONZE BATCH INGESTION")
    print("=" * 60)

    results = {}

    for name, (filename, expected_count) in DATASETS.items():
        results[name] = ingest_dataset(
            name, filename, expected_count
        )

    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)

    for name, count in results.items():
        print(f"{name:<25} {count:>10,} rows")

    print("\nALL DATASETS INGESTED SUCCESSFULLY")

finally:
    spark.stop()
