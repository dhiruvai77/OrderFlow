
from pathlib import Path
import pandas as pd

# Path to the Olist dataset
DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "olist"

# Find all CSV files
csv_files = sorted(DATA_DIR.glob("*.csv"))

print("=" * 50)
print("ORDERFLOW - DATA INSPECTION")
print("=" * 50)

print(f"\nTotal CSV files found: {len(csv_files)}")

if not csv_files:
    raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

for file in csv_files:
    df = pd.read_csv(file)

    print(f"\nDataset: {file.name}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn names:")
    print(df.columns.tolist())

    print(f"\nMissing values: {df.isnull().sum().sum():,}")
    print(f"Duplicate rows: {df.duplicated().sum():,}")

    print("-" * 50)
