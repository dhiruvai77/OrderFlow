
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==================================================
# 1. CONFIGURATION
# ==================================================

ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = ROOT / "data" / "silver" / "olist"

os.environ["PYSPARK_PYTHON"] = sys.executable

EXPECTED_COUNTS = {
    "customers": 99441,
    "geolocation": 1000163,
    "order_items": 112650,
    "payments": 103886,
    "reviews": 99224,
    "orders": 99441,
    "products": 32951,
    "sellers": 3095,
    "category_translation": 71,
}

# Keys expected to be unique
PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],
    "category_translation": ["product_category_name"],
}

EXPECTED_TYPES = {
    "orders": {
        "order_purchase_timestamp": "timestamp",
        "delivery_days": "double",
        "is_late_delivery": "boolean",
    },
    "products": {
        "product_weight_g": "int",
        "product_length_cm": "int",
    },
    "order_items": {
        "order_item_id": "int",
        "price": "decimal(12,2)",
        "freight_value": "decimal(12,2)",
        "shipping_limit_date": "timestamp",
    },
    "payments": {
        "payment_sequential": "int",
        "payment_value": "decimal(12,2)",
    },
    "reviews": {
        "review_score": "int",
        "review_creation_date": "timestamp",
    },
    "geolocation": {
        "geolocation_lat": "double",
        "geolocation_lng": "double",
    },
}

# child table, foreign key, parent table, parent key
RELATIONSHIPS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("reviews", "order_id", "orders", "order_id"),
]

# ==================================================
# 2. INITIALIZE SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Silver-Validation")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

failures = []


def check(condition, message):
    if condition:
        print(f"PASS: {message}")
    else:
        print(f"FAIL: {message}")
        failures.append(message)


try:
    print("=" * 60)
    print("ORDERFLOW - SILVER LAYER VALIDATION")
    print("=" * 60)

    # ==================================================
    # 3. LOAD ALL SILVER DATASETS
    # ==================================================

    tables = {}

    for name in EXPECTED_COUNTS:
        path = SILVER_DIR / name
        tables[name] = spark.read.parquet(str(path))

    # ==================================================
    # 4. ROW COUNTS
    # ==================================================

    print("\n1. ROW COUNT VALIDATION")

    for name, expected in EXPECTED_COUNTS.items():
        actual = tables[name].count()

        check(
            actual == expected,
            f"{name}: {actual:,} rows "
            f"(expected {expected:,})"
        )

    # ==================================================
    # 5. SCHEMA VALIDATION
    # ==================================================

    print("\n2. DATA TYPE VALIDATION")

    for name, columns in EXPECTED_TYPES.items():
        actual_types = dict(tables[name].dtypes)

        for column, expected_type in columns.items():
            actual_type = actual_types.get(column)

            check(
                actual_type == expected_type,
                f"{name}.{column}: "
                f"{actual_type} (expected {expected_type})"
            )

    # ==================================================
    # 6. PRIMARY KEY VALIDATION
    # ==================================================

    print("\n3. PRIMARY KEY VALIDATION")

    for name, keys in PRIMARY_KEYS.items():
        df = tables[name]

        missing_condition = F.lit(False)

        for key in keys:
            missing_condition = (
                missing_condition
                | F.col(key).isNull()
                | (F.trim(F.col(key).cast("string")) == "")
            )

        missing = df.filter(missing_condition).count()

        duplicate_groups = (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        check(
            missing == 0 and duplicate_groups == 0,
            f"{name}: missing keys={missing:,}, "
            f"duplicate groups={duplicate_groups:,}"
        )

    # ==================================================
    # 7. REFERENTIAL INTEGRITY
    # ==================================================

    print("\n4. FOREIGN KEY VALIDATION")

    for child, fk, parent, pk in RELATIONSHIPS:

        parent_keys = (
            tables[parent]
            .select(F.col(pk).alias("_parent_key"))
            .distinct()
        )

        unmatched = (
            tables[child]
            .select(F.col(fk).alias("_child_key"))
            .join(
                parent_keys,
                F.col("_child_key") == F.col("_parent_key"),
                "left_anti"
            )
            .count()
        )

        check(
            unmatched == 0,
            f"{child}.{fk} -> {parent}.{pk}: "
            f"{unmatched:,} unmatched rows"
        )

    # ==================================================
    # 8. QUALITY FLAGS
    # ==================================================

    print("\n5. DATA QUALITY SUMMARY")

    quality_columns = {
        "orders": ["has_delivery_anomaly"],
        "order_items": [
            "has_conversion_error",
            "has_negative_amount"
        ],
        "payments": [
            "has_conversion_error",
            "has_negative_amount"
        ],
        "reviews": ["has_conversion_error"],
        "geolocation": [
            "has_conversion_error",
            "is_valid_geolocation"
        ],
    }

    for name, columns in quality_columns.items():
        print(f"\n{name.upper()}")

        for column in columns:
            if column not in tables[name].columns:
                failures.append(f"Missing {name}.{column}")
                print(f"FAIL: Missing {column}")
                continue

            flagged = tables[name].filter(
                F.col(column) == (
                    False if column.startswith("is_valid_")
                    else True
                )
            ).count()

            print(f"{column}: {flagged:,}")

            # Report quality exceptions; they are not
            # automatically fatal validation failures.
            if column != "has_delivery_anomaly":
                check(
                    flagged == 0,
                    f"{name}.{column} quality check"
                )

    # ==================================================
    # 9. FINAL RESULT
    # ==================================================

    print("\n" + "=" * 60)
    print("FINAL SILVER VALIDATION SUMMARY")
    print("=" * 60)

    if failures:
        print(f"\nFAILED CHECKS: {len(failures)}")

        for failure in failures:
            print(f"- {failure}")

        raise ValueError(
            "Silver validation failed. "
            "Review the reported issues."
        )

    print("\nALL SILVER VALIDATIONS PASSED")

finally:
    spark.stop()
