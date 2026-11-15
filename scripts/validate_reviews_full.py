
import os
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]

SOURCE = (
    ROOT / "data/raw/olist/olist_order_reviews_dataset.csv"
)

BRONZE = ROOT / "data/bronze/olist/reviews"

os.environ["PYSPARK_PYTHON"] = sys.executable


def normalize_text(value):
    if pd.isna(value):
        return ""

    return str(value).replace("\r\n", "\n").replace("\r", "\n")


spark = (
    SparkSession.builder
    .appName("OrderFlow-Full-Review-Validation")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("=" * 60)
    print("ORDERFLOW - FULL REVIEW VALIDATION")
    print("=" * 60)

    # Read original CSV using pandas
    source = pd.read_csv(
        SOURCE,
        dtype=str,
        keep_default_na=False
    )

    # Read Bronze Parquet using Spark
    bronze_df = spark.read.parquet(str(BRONZE))

    # Collect this bounded dataset for local comparison
    bronze = pd.DataFrame(
        [
            row.asDict()
            for row in bronze_df.select(*source.columns).collect()
        ]
    ).fillna("")

    print(f"\nSource rows: {len(source):,}")
    print(f"Bronze rows: {len(bronze):,}")

    assert len(source) == len(bronze)

    keys = ["review_id", "order_id"]

    # Validate the matching keys
    assert not source.duplicated(keys).any(), (
        "Duplicate source review keys detected"
    )

    assert not bronze.duplicated(keys).any(), (
        "Duplicate Bronze review keys detected"
    )

    # Compare records using their keys
    merged = source.merge(
        bronze,
        on=keys,
        how="outer",
        suffixes=("_source", "_bronze"),
        indicator=True,
        validate="one_to_one"
    )

    unmatched = (merged["_merge"] != "both").sum()

    print(f"Unmatched records: {unmatched:,}")
    assert unmatched == 0

    # Compare all non-key columns
    mismatches = {}

    for column in source.columns:
        if column in keys:
            continue

        left = merged[f"{column}_source"].map(normalize_text)
        right = merged[f"{column}_bronze"].map(normalize_text)

        mismatch_count = (left != right).sum()
        mismatches[column] = mismatch_count

        print(f"{column}: {mismatch_count:,} mismatches")

    total_mismatches = sum(mismatches.values())

    print(f"\nTotal field mismatches: {total_mismatches:,}")

    if total_mismatches:
        raise ValueError(
            "Review data validation failed. "
            "Investigate mismatched fields."
        )

    print("\nALL REVIEW RECORDS VALIDATED SUCCESSFULLY")

finally:
    spark.stop()
