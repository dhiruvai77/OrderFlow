
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "olist"

def load_data(filename):
    return pd.read_csv(DATA_DIR / filename)

# Load the main datasets
orders = load_data("olist_orders_dataset.csv")
customers = load_data("olist_customers_dataset.csv")
items = load_data("olist_order_items_dataset.csv")
payments = load_data("olist_order_payments_dataset.csv")
products = load_data("olist_products_dataset.csv")
sellers = load_data("olist_sellers_dataset.csv")
reviews = load_data("olist_order_reviews_dataset.csv")

print("\n" + "=" * 60)
print("1. MISSING VALUES BY COLUMN")
print("=" * 60)

for name, df in [
    ("Orders", orders),
    ("Products", products),
    ("Reviews", reviews)
]:
    print(f"\n{name}:")
    missing = df.isna().sum()
    print(missing[missing > 0].to_string())

print("\n" + "=" * 60)
print("2. PRIMARY KEY UNIQUENESS")
print("=" * 60)

for name, df, key in [
    ("Orders", orders, "order_id"),
    ("Customers", customers, "customer_id"),
    ("Products", products, "product_id"),
    ("Sellers", sellers, "seller_id")
]:
    print(f"{name}: {df[key].duplicated().sum():,} duplicate keys")

print("\n" + "=" * 60)
print("3. REFERENTIAL INTEGRITY")
print("=" * 60)

checks = [
    ("Orders -> Customers",
     orders["customer_id"], customers["customer_id"]),

    ("Items -> Orders",
     items["order_id"], orders["order_id"]),

    ("Items -> Products",
     items["product_id"], products["product_id"]),

    ("Items -> Sellers",
     items["seller_id"], sellers["seller_id"]),

    ("Payments -> Orders",
     payments["order_id"], orders["order_id"]),

    ("Reviews -> Orders",
     reviews["order_id"], orders["order_id"])
]

for name, child, parent in checks:
    invalid = (~child.isin(parent)).sum()
    print(f"{name}: {invalid:,} unmatched records")

print("\n" + "=" * 60)
print("4. ORDER STATUS DISTRIBUTION")
print("=" * 60)

print(orders["order_status"].value_counts().to_string())

print("\n" + "=" * 60)
print("5. ORDERS WITH MULTIPLE ITEMS")
print("=" * 60)

items_per_order = items.groupby("order_id").size()

print(f"Orders containing items: {len(items_per_order):,}")
print(f"Orders with multiple items: {(items_per_order > 1).sum():,}")
print(f"Maximum items in one order: {items_per_order.max()}")

print("\nDATA PROFILING COMPLETED")
