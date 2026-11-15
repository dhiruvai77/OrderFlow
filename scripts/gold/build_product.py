
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


# =====================================================
# 2. INITIALIZE SPARK
# =====================================================

spark = (
    SparkSession.builder
    .appName("OrderFlow-Gold-Product-Dimension")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:
    print("=" * 60)
    print("ORDERFLOW - BUILD DIM_PRODUCT")
    print("=" * 60)

    # =================================================
    # 3. READ SILVER DATASETS
    # =================================================

    products = spark.read.parquet(
        str(SILVER_DIR / "products")
    )

    categories = spark.read.parquet(
        str(SILVER_DIR / "category_translation")
    )

    product_count = products.count()
    category_count = categories.count()

    print(f"\nSilver products: {product_count:,}")
    print(f"Category translations: {category_count:,}")

    # =================================================
    # 4. VALIDATE CATEGORY LOOKUP
    # =================================================

    duplicate_categories = (
        categories
        .groupBy("product_category_name")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_categories > 0:
        raise ValueError(
            "Category translation lookup has "
            f"{duplicate_categories} duplicate keys"
        )

    # =================================================
    # 5. PREPARE JOIN COLUMNS
    # =================================================

    products_clean = products.withColumn(
        "category_join_key",
        F.lower(F.trim("product_category_name"))
    )

    categories_clean = categories.select(
        F.lower(
            F.trim("product_category_name")
        ).alias("category_lookup_key"),

        F.col(
            "product_category_name_english"
        ).alias("category_english")
    )

    # =================================================
    # 6. JOIN PRODUCTS AND CATEGORY TRANSLATIONS
    # =================================================

    # LEFT JOIN preserves products without translations.
    joined = products_clean.join(
        F.broadcast(categories_clean),
        products_clean["category_join_key"]
        == categories_clean["category_lookup_key"],
        "left"
    )

    # =================================================
    # 7. CREATE GOLD PRODUCT DIMENSION
    # =================================================

    dim_product = joined.select(
        F.col("product_id"),

        F.col("product_category_name").alias(
            "category_name"
        ),

        F.col("category_english").alias(
            "category_name_english"
        ),

        # Correct Olist's original "lenght" spelling
        F.col("product_name_lenght").alias(
            "name_length"
        ),

        F.col("product_description_lenght").alias(
            "description_length"
        ),

        F.col("product_photos_qty").alias(
            "photos_quantity"
        ),

        F.col("product_weight_g").alias(
            "weight_g"
        ),

        F.col("product_length_cm").alias(
            "length_cm"
        ),

        F.col("product_height_cm").alias(
            "height_cm"
        ),

        F.col("product_width_cm").alias(
            "width_cm"
        )
    )

    # Explicitly track category translation availability
    dim_product = dim_product.withColumn(
        "has_category_translation",
        F.col("category_name_english").isNotNull()
    )

    # =================================================
    # 8. VALIDATE DIMENSION GRAIN
    # =================================================

    gold_count = dim_product.count()

    if gold_count != product_count:
        raise ValueError(
            f"Row count mismatch: Silver={product_count}, "
            f"Gold={gold_count}"
        )

    duplicate_products = (
        dim_product.groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_keys = dim_product.filter(
        F.col("product_id").isNull()
        | (F.trim("product_id") == "")
    ).count()

    if duplicate_products > 0 or missing_keys > 0:
        raise ValueError(
            "Product dimension key validation failed"
        )

    # =================================================
    # 9. DATA QUALITY SUMMARY
    # =================================================

    quality = dim_product.agg(
        F.sum(
            F.col("category_name").isNull().cast("int")
        ).alias("missing_source_categories"),

        F.sum(
            (
                F.col("category_name").isNotNull()
                & ~F.col("has_category_translation")
            ).cast("int")
        ).alias("unmatched_category_translations"),

        F.sum(
            F.col("has_category_translation").cast("int")
        ).alias("products_with_translations")
    ).first()

    print("\nDATA QUALITY SUMMARY")

    print(f"Gold rows: {gold_count:,}")
    print(f"Duplicate products: {duplicate_products:,}")
    print(f"Missing product IDs: {missing_keys:,}")

    for name, value in quality.asDict().items():
        print(f"{name}: {value or 0:,}")

    # =================================================
    # 10. WRITE GOLD PARQUET
    # =================================================

    target = GOLD_DIR / "dim_product"

    dim_product.write.mode("overwrite").parquet(
        str(target)
    )

    # Read-back validation
    verified = spark.read.parquet(str(target))

    verified_count = verified.count()

    if verified_count != product_count:
        raise ValueError(
            "Product dimension read-back validation failed"
        )

    print(f"\nVerified Gold records: {verified_count:,}")

    print("\nGOLD PRODUCT SCHEMA")
    verified.printSchema()

    print("\nSAMPLE PRODUCTS")
    verified.select(
        "product_id",
        "category_name",
        "category_name_english",
        "weight_g",
        "has_category_translation"
    ).show(10, truncate=False)

    print("\n" + "=" * 60)
    print("DIM_PRODUCT COMPLETED")
    print("=" * 60)

finally:
    spark.stop()
