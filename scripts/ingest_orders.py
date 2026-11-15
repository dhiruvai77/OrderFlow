
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist"
    / "olist_orders_dataset.csv"
)

TARGET_DIR = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "olist"
    / "orders"
)

# Use our virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("OrderFlow-Bronze-Orders")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("=" * 60)
    print("ORDERFLOW - BRONZE INGESTION")
    print("=" * 60)

    if not SOURCE_FILE.is_file():
        raise FileNotFoundError(SOURCE_FILE)

    # Read the source CSV.
    # Keep source columns as strings in bronze.
    orders_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(str(SOURCE_FILE))
    )

    source_count = orders_df.count()

    print(f"\nSource records: {source_count:,}")

    # Add ingestion metadata.
    bronze_df = (
        orders_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(SOURCE_FILE.name))
    )

    print("\nBronze schema:")
    bronze_df.printSchema()

    # Write Parquet output.
    # Overwrite supports repeatable development runs.
    (
        bronze_df.write
        .mode("overwrite")
        .parquet(str(TARGET_DIR))
    )

    print(f"\nParquet written to: {TARGET_DIR}")

    # Read it back and validate record counts.
    verified_df = spark.read.parquet(str(TARGET_DIR))
    target_count = verified_df.count()

    print(f"\nSource rows: {source_count:,}")
    print(f"Target rows: {target_count:,}")

    if source_count != target_count:
        raise ValueError("Row-count validation failed!")

    print("\nROW-COUNT VALIDATION PASSED")

    print("\nSample records:")
    verified_df.select(
        "order_id",
        "customer_id",
        "order_status"
    ).show(5, truncate=False)

    print("\nBRONZE INGESTION COMPLETED SUCCESSFULLY")

finally:
    spark.stop()
