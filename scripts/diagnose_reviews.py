
import os
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]

SOURCE = ROOT / "data/raw/olist/olist_order_reviews_dataset.csv"
BRONZE = ROOT / "data/bronze/olist/reviews"

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("OrderFlow-Review-Diagnostics")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    source = pd.read_csv(SOURCE, dtype=str, keep_default_na=False)
    bronze = spark.read.parquet(str(BRONZE))

    sample = (
        source[source["review_comment_message"] != ""]
        .head(20)
    )

    mismatches = 0

    for _, row in sample.iterrows():
        matches = (
            bronze.filter(
                (F.col("review_id") == row["review_id"])
                & (F.col("order_id") == row["order_id"])
            )
            .select("review_comment_message")
            .collect()
        )

        if len(matches) != 1:
            print(
                f"\nUnexpected match count: {len(matches)}"
                f"\nReview ID: {row['review_id']}"
            )
            continue

        original = row["review_comment_message"]
        stored = matches[0][0]

        if original != stored:
            mismatches += 1

            print("\n" + "=" * 60)
            print(f"MISMATCH {mismatches}")
            print(f"Review ID: {row['review_id']}")

            print("\nOriginal CSV (pandas):")
            print(repr(original[:250]))

            print("\nBronze Parquet (Spark):")
            print(repr(stored[:250] if stored else stored))

            print("\nLengths:")
            print("Original:", len(original))
            print("Bronze:", len(stored) if stored else 0)

            # Identify the first different character
            if stored is not None:
                for i, (a, b) in enumerate(zip(original, stored)):
                    if a != b:
                        print(f"\nFirst difference at position {i}")
                        print(f"Original: {repr(a)} U+{ord(a):04X}")
                        print(f"Bronze:   {repr(b)} U+{ord(b):04X}")
                        break

            if mismatches >= 3:
                break

    print("\n" + "=" * 60)
    print(f"Mismatches found: {mismatches}")
    print("DIAGNOSTIC COMPLETED")

finally:
    spark.stop()
