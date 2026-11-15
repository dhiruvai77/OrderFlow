# OrderFlow — Bronze Layer Architecture

## 1. Overview

The Bronze Layer is the first storage layer in the OrderFlow data platform.

Its purpose is to ingest historical e-commerce data from the Olist dataset and store it in Apache Parquet format for subsequent processing.

The original CSV files remain unchanged.

## 2. Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python 3.12 |
| Processing engine | Apache Spark / PySpark 4.1.2 |
| Java runtime | Eclipse Temurin JDK 17 |
| Hadoop libraries | Hadoop 3.4.2 |
| Input format | CSV |
| Output format | Apache Parquet |
| Execution environment | Windows, local Spark mode |
| Version control | Git and GitHub |

## 3. Data Flow

Olist CSV files → PySpark ingestion → Bronze Parquet datasets → Data validation

### Source Directory

`data/raw/olist/`

Contains the nine original Olist CSV files.

### Destination Directory

`data/bronze/olist/`

Contains nine Parquet datasets:

- customers
- geolocation
- order_items
- payments
- reviews
- orders
- products
- sellers
- category_translation

## 4. Ingestion Design

The reusable ingestion pipeline is implemented in:

`scripts/ingest_all.py`

The pipeline:

1. Reads dataset configurations from a Python dictionary.
2. Loads source CSV files using PySpark.
3. Keeps source columns as strings.
4. Adds ingestion metadata.
5. Writes Parquet output using overwrite mode.
6. Reads the output back and validates record counts.

### Ingestion Metadata

| Column | Description |
|---|---|
| `_ingested_at` | Timestamp of ingestion |
| `_source_file` | Original source filename |

## 5. CSV Parsing

The pipeline uses UTF-8 encoding, multiline CSV parsing, and escaped double quotation marks.

This configuration is necessary for customer reviews containing quoted text and multiline comments.

The Spark parser normalizes certain newline representations. The original CSV files remain available for source-level recovery.

## 6. Data Validation

The following scripts implement validation:

- `scripts/validate_bronze.py`
- `scripts/validate_reviews_full.py`

### Validation Results

All nine datasets passed:

- Expected row-count validation
- Source column-name and order validation
- Ingestion metadata checks

The complete customer review dataset was also compared against the original CSV using the combination of `review_id` and `order_id`.

Results:

- Source reviews: 99,224
- Bronze reviews: 99,224
- Unmatched review records: 0
- Field mismatches: 0

Review text comparisons normalize newline representations and treat missing and empty values as equivalent.

## 7. Current Limitations

The current implementation is a local batch-processing prototype.

- Ingestion uses overwrite mode rather than incremental loading.
- Source row counts are hard-coded validation baselines.
- Source columns remain strings.
- The Bronze representation is not byte-for-byte equivalent to the original CSV.
- Full field-level comparisons have been completed for reviews, but not for every dataset.
- There is no automated orchestration, structured logging, or error quarantine yet.

## 8. Next Phase

The Silver Layer will introduce:

- Explicit data types and schema enforcement
- Timestamp conversion
- Data quality rules
- Standardized missing values
- Referential integrity checks
- Cleaned and validated Parquet datasets

## 9. Conclusion

The Bronze Layer provides a reproducible local batch ingestion foundation for OrderFlow.

All nine Olist datasets have been successfully written to Parquet, and the implemented validation checks have passed.
