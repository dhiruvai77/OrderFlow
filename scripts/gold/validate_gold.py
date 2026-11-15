
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==========================================================
# 1. CONFIGURATION
# ==========================================================

ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = ROOT / "data" / "gold" / "olist"

os.environ["PYSPARK_PYTHON"] = sys.executable

EXPECTED_COUNTS = {
    "dim_customer": 99441,
    "dim_seller": 3095,
    "dim_product": 32951,
    "dim_date": 774,
    "fct_order_items": 112650,
    "fct_payments": 103886,
    "fct_orders": 99441,
    "fct_reviews": 99224,
}

KEYS = {
    "dim_customer": ["customer_id"],
    "dim_seller": ["seller_id"],
    "dim_product": ["product_id"],
    "dim_date": ["date_key"],
    "fct_order_items": ["order_id", "order_item_id"],
    "fct_payments": ["order_id", "payment_sequential"],
    "fct_orders": ["order_id"],
    "fct_reviews": ["review_id", "order_id"],
}

# Child table, foreign key, parent table, parent key
RELATIONSHIPS = [
    ("fct_orders", "customer_id",
     "dim_customer", "customer_id"),

    ("fct_orders", "purchase_date_key",
     "dim_date", "date_key"),

    ("fct_order_items", "order_id",
     "fct_orders", "order_id"),

    ("fct_order_items", "customer_id",
     "dim_customer", "customer_id"),

    ("fct_order_items", "product_id",
     "dim_product", "product_id"),

    ("fct_order_items", "seller_id",
     "dim_seller", "seller_id"),

    ("fct_order_items", "purchase_date_key",
     "dim_date", "date_key"),

    ("fct_payments", "order_id",
     "fct_orders", "order_id"),

    ("fct_payments", "customer_id",
     "dim_customer", "customer_id"),

    ("fct_payments", "purchase_date_key",
     "dim_date", "date_key"),

    ("fct_reviews", "order_id",
     "fct_orders", "order_id"),

    ("fct_reviews", "customer_id",
     "dim_customer", "customer_id"),

    ("fct_reviews", "purchase_date_key",
     "dim_date", "date_key"),
]


# ==========================================================
# 2. SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Gold-Validation")
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
    print("ORDERFLOW - GOLD LAYER VALIDATION")
    print("=" * 60)

    # ======================================================
    # 3. LOAD GOLD TABLES
    # ======================================================

    tables = {}

    for name in EXPECTED_COUNTS:
        tables[name] = spark.read.parquet(
            str(GOLD_DIR / name)
        )

    # ======================================================
    # 4. ROW COUNT VALIDATION
    # ======================================================

    print("\n1. ROW COUNT VALIDATION")

    for name, expected in EXPECTED_COUNTS.items():
        actual = tables[name].count()

        check(
            actual == expected,
            f"{name}: {actual:,} rows "
            f"(expected {expected:,})"
        )

    # ======================================================
    # 5. KEY UNIQUENESS AND COMPLETENESS
    # ======================================================

    print("\n2. PRIMARY / COMPOSITE KEY VALIDATION")

    for name, key_columns in KEYS.items():
        df = tables[name]

        missing_condition = F.lit(False)

        for key in key_columns:
            missing_condition = (
                missing_condition
                | F.col(key).isNull()
                | (
                    F.trim(F.col(key).cast("string"))
                    == ""
                )
            )

        missing = df.filter(missing_condition).count()

        duplicate_groups = (
            df.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        check(
            missing == 0 and duplicate_groups == 0,
            f"{name}: missing={missing:,}, "
            f"duplicate groups={duplicate_groups:,}"
        )

    # ======================================================
    # 6. REFERENTIAL INTEGRITY
    # ======================================================

    print("\n3. FOREIGN KEY VALIDATION")

    for child, fk, parent, pk in RELATIONSHIPS:

        child_keys = tables[child].select(
            F.col(fk).alias("_child_key")
        )

        parent_keys = (
            tables[parent]
            .select(F.col(pk).alias("_parent_key"))
            .distinct()
        )

        unmatched = (
            child_keys
            .join(
                parent_keys,
                F.col("_child_key")
                == F.col("_parent_key"),
                "left_anti"
            )
            .count()
        )

        check(
            unmatched == 0,
            f"{child}.{fk} -> {parent}.{pk}: "
            f"{unmatched:,} unmatched"
        )

    # ======================================================
    # 7. FINANCIAL CONSISTENCY
    # ======================================================

    print("\n4. FINANCIAL CONSISTENCY")

    items = tables["fct_order_items"]
    payments = tables["fct_payments"]
    orders = tables["fct_orders"]

    item_totals = items.agg(
        F.sum("price").alias("merchandise"),
        F.sum("freight_value").alias("freight"),
        F.sum("total_item_value").alias("listed")
    ).first()

    payment_total = payments.agg(
        F.sum("payment_value").alias("payments")
    ).first()["payments"]

    order_totals = orders.agg(
        F.sum("merchandise_value").alias("merchandise"),
        F.sum("freight_value").alias("freight"),
        F.sum("total_listed_value").alias("listed"),
        F.sum("total_payment_value").alias("payments")
    ).first()

    for column in ["merchandise", "freight", "listed"]:
        check(
            item_totals[column] == order_totals[column],
            f"Item-to-order {column} totals match"
        )

    check(
        payment_total == order_totals["payments"],
        "Payment-to-order totals match"
    )

    # Verify the calculation on individual item lines
    invalid_item_totals = items.filter(
        F.col("total_item_value")
        != (
            F.col("price") + F.col("freight_value")
        )
    ).count()

    check(
        invalid_item_totals == 0,
        "Every item-line total equals price plus freight"
    )

    # ======================================================
    # 8. RECONCILIATION CLASSIFICATION
    # ======================================================

    print("\n5. ORDER RECONCILIATION")

    reconciliation = orders.agg(
        F.sum(
            F.when(
                F.col("is_payment_reconciled") == True, 1
            ).otherwise(0)
        ).alias("reconciled"),

        F.sum(
            F.when(
                F.col("is_payment_reconciled") == False, 1
            ).otherwise(0)
        ).alias("mismatched"),

        F.sum(
            F.when(
                F.col("is_payment_reconciled").isNull(), 1
            ).otherwise(0)
        ).alias("not_comparable")
    ).first()

    for key, value in reconciliation.asDict().items():
        print(f"{key}: {value:,}")

    classified_orders = sum(reconciliation.asDict().values())

    check(
        classified_orders == EXPECTED_COUNTS["fct_orders"],
        "All orders have a reconciliation classification"
    )

    # ======================================================
    # 9. REVIEW QUALITY
    # ======================================================

    print("\n6. REVIEW QUALITY")

    reviews = tables["fct_reviews"]

    invalid_scores = reviews.filter(
        F.col("review_score").isNull()
        | ~F.col("review_score").between(1, 5)
    ).count()

    check(
        invalid_scores == 0,
        f"Invalid review scores: {invalid_scores:,}"
    )

    negative_response_times = reviews.filter(
        F.col("has_negative_response_time") == True
    ).count()

    check(
        negative_response_times == 0,
        "No negative review response intervals"
    )

    print("\n7. REVIEW / DELIVERY METRICS")

    reviews.filter(
        F.col("is_delivery_metric_eligible") == True
    ).groupBy("is_late_delivery").agg(
        F.count("*").alias("reviews"),
        F.round(
            F.avg("review_score"), 2
        ).alias("average_score")
    ).orderBy("is_late_delivery").show()

    # ======================================================
    # 10. FINAL VALIDATION SUMMARY
    # ======================================================

    print("\n" + "=" * 60)
    print("FINAL GOLD VALIDATION SUMMARY")
    print("=" * 60)

    if failures:
        print(f"\nFAILED CHECKS: {len(failures)}")

        for failure in failures:
            print(f"- {failure}")

        raise ValueError(
            "Gold validation failed. Review the issues."
        )

    print("\nALL GOLD VALIDATIONS PASSED")

finally:
    spark.stop()
