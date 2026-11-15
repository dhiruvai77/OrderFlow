
import os
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "olist"
BRONZE = ROOT / "data" / "bronze" / "olist"

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

spark = (
    SparkSession.builder
    .appName("OrderFlow-Bronze-Validation")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("=" * 60)
    print("ORDERFLOW - BRONZE VALIDATION")
    print("=" * 60)

    for name, (filename, expected_rows) in DATASETS.items():
        source_path = RAW / filename
        target_path = BRONZE / name

        # Read original CSV header
        source_columns = pd.read_csv(
            source_path, nrows=0
        ).columns.tolist()

        # Load Bronze Parquet dataset
        df = spark.read.parquet(str(target_path))

        # 1. Row-count validation
        actual_rows = df.count()
        assert actual_rows == expected_rows, (
            f"{name}: Row count mismatch"
        )

        # 2. Original column preservation
        assert df.columns[:len(source_columns)] == source_columns, (
            f"{name}: Source columns do not match"
        )

        # 3. Metadata presence and completeness
        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns

        bad_metadata = df.filter(
            F.col("_ingested_at").isNull()
            | F.col("_source_file").isNull()
            | (F.col("_source_file") != filename)
        ).count()

        assert bad_metadata == 0, (
            f"{name}: Invalid ingestion metadata"
        )

        print(f"{name:<25} {actual_rows:>10,}  PASSED")

    # 4. Compare a sample of review comments
    print("\nChecking review text preservation...")

    source_reviews = pd.read_csv(
        RAW / "olist_order_reviews_dataset.csv"
    )

    sample = source_reviews[
        source_reviews["review_comment_message"].notna()
    ].head(10)

    bronze_reviews = spark.read.parquet(
        str(BRONZE / "reviews")
    )

    for _, row in sample.iterrows():
        matches = bronze_reviews.filter(
            (F.col("review_id") == row["review_id"])
            & (F.col("order_id") == row["order_id"])
        ).select("review_comment_message").collect()

        assert len(matches) == 1, (
            "Review record missing or duplicated"
        )

        
        # Normalize newline representations for comparison
        def normalize_newlines(text):
            if text is None:
                return None

            return text.replace("\r\n", "\n").replace("\r", "\n")


        source_text = normalize_newlines(
            row["review_comment_message"]
        )

        bronze_text = normalize_newlines(
            matches[0][0]
        )

        assert source_text == bronze_text, (
            f"Review text mismatch for review_id={row['review_id']}"
        )


    print("Review text sample: PASSED")
    print("\nALL BRONZE VALIDATIONS PASSED")

finally:
    spark.stop()
