# OrderFlow — End-to-End E-Commerce Data Engineering Platform

OrderFlow is a practical data engineering portfolio project built using the **Brazilian E-Commerce Public Dataset by Olist**.

The objective is to develop a production-style e-commerce data platform covering data ingestion, transformation, data quality, dimensional modeling, data warehousing, orchestration, streaming, and business analytics.

The project is developed incrementally, beginning with a local PySpark data lake and progressively introducing additional data engineering technologies.

## 1. Project Overview

OrderFlow implements a **Medallion Architecture** consisting of three data processing layers:

- **Bronze:** Ingest and preserve source datasets in Parquet format.
- **Silver:** Clean, standardize, transform, and validate the ingested data.
- **Gold:** Create business-oriented fact and dimension tables for analytics.

The initial Bronze, Silver, and Gold pipelines have been implemented using Python and PySpark.

The current implementation runs locally, with processed datasets stored as Parquet files.

## 2. Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The dataset contains approximately 100,000 historical orders from a Brazilian e-commerce marketplace.

It consists of nine related CSV datasets:

| Dataset | Records |
|---|---:|
| Customers | 99,441 |
| Geolocation | 1,000,163 |
| Order items | 112,650 |
| Payments | 103,886 |
| Reviews | 99,224 |
| Orders | 99,441 |
| Products | 32,951 |
| Sellers | 3,095 |
| Category translations | 71 |

**Total source records:** 1,550,922

Raw and processed datasets are excluded from version control.

## 3. Technology Stack

### Implemented

| Technology | Purpose |
|---|---|
| Python | Data engineering scripts and validation |
| PySpark | Distributed-style data processing and transformations |
| Apache Parquet | Columnar storage for Bronze, Silver, and Gold datasets |
| Git and GitHub | Version control and project documentation |
| Windows CMD | Local development and pipeline execution |

### Planned

| Technology | Purpose |
|---|---|
| PostgreSQL | Relational data warehouse and SQL analytics |
| dbt | SQL transformations, testing, and documentation |
| Apache Airflow | Pipeline orchestration and scheduling |
| Apache Kafka | Event streaming and real-time data ingestion |
| Docker | Containerization and reproducible environments |
| MinIO / AWS S3 | Object storage for the data lake |
| BI dashboards | Business reporting and visualization |

These additional technologies will be introduced in later project phases.

## 4. Data Architecture

The current data pipeline follows the Medallion Architecture:

```text
Olist CSV Files
       |
       v
+-----------------------------+
| Bronze Layer                |
| PySpark CSV ingestion       |
| Parquet storage             |
| Ingestion metadata          |
+-----------------------------+
       |
       v
+-----------------------------+
| Silver Layer                |
| Data type conversion        |
| Data standardization        |
| Data quality flags          |
| Referential integrity       |
+-----------------------------+
       |
       v
+-----------------------------+
| Gold Layer                  |
| Dimension tables            |
| Fact tables                 |
| Business metrics            |
| Financial reconciliation    |
+-----------------------------+
       |
       v
PostgreSQL + dbt (Planned)
       |
       v
Business Intelligence Dashboards
```

### Current storage structure

```text
data/
├── raw/
│   └── olist/
├── bronze/
│   └── olist/
├── silver/
│   └── olist/
└── gold/
    └── olist/
```

Bronze, Silver, and Gold datasets are currently stored locally as Parquet files.

Future phases will introduce S3-compatible object storage.

## 5. Project Roadmap

### Completed

- [x] Explore and profile the Olist datasets
- [x] Implement Bronze ingestion using PySpark
- [x] Validate Bronze row counts and source-data preservation
- [x] Implement Silver transformations for all nine datasets
- [x] Add Silver data quality indicators
- [x] Validate Silver schemas, keys, and relationships
- [x] Design Gold fact and dimension models
- [x] Implement four Gold dimension tables
- [x] Implement four Gold fact tables
- [x] Validate Gold row counts, relationships, and financial consistency

### Upcoming

- [ ] Implement PostgreSQL integration
- [ ] Introduce dbt models and automated data tests
- [ ] Add Apache Airflow orchestration
- [ ] Implement Kafka-based streaming
- [ ] Integrate MinIO / AWS S3 object storage
- [ ] Build business intelligence dashboards
- [ ] Add Docker-based deployment
- [ ] Explore cloud deployment

## 6. Bronze Layer

The Bronze Layer ingests the nine Olist CSV files and stores them as Parquet datasets.

The ingestion pipeline preserves the original source columns and adds metadata:

- `_ingested_at`
- `_source_file`

### Run Bronze ingestion

```cmd
python scripts\ingest_all.py
```

### Validate Bronze data

```cmd
python scripts\validate_bronze.py
```

### Validate review preservation

```cmd
python scripts\validate_reviews_full.py
```

The review validation compares all 99,224 review records against the source, including normalized newline handling.

## 7. Silver Layer

The Silver Layer transforms all nine Bronze datasets into standardized, typed, and validated Parquet tables.

### Main transformations

- Convert timestamp columns into Spark timestamp types.
- Convert monetary values into decimal types.
- Convert product measurements and review scores into numeric types.
- Standardize selected text columns.
- Preserve ZIP code prefixes as strings.
- Calculate delivery performance metrics.
- Create data quality indicators.
- Preserve source records while flagging identified inconsistencies.

### Silver datasets

| Dataset | Records |
|---|---:|
| orders | 99,441 |
| customers | 99,441 |
| sellers | 3,095 |
| products | 32,951 |
| order_items | 112,650 |
| payments | 103,886 |
| reviews | 99,224 |
| geolocation | 1,000,163 |
| category_translation | 71 |

### Data quality findings

The Silver pipeline identified:

- 37 unique orders with delivery-related anomalies.
- 261,831 exact duplicate excess rows in the geolocation source.
- No conversion errors in the validated transaction, review, and geolocation fields.
- No unmatched records in the six tested foreign-key relationships.

Potentially problematic records are preserved and flagged rather than automatically deleted.

### Run Silver transformations

```cmd
python scripts\silver\transform_orders.py
python scripts\silver\transform_dimensions.py
python scripts\silver\transform_transactions.py
python scripts\silver\transform_reviews.py
python scripts\silver\transform_reference_data.py
```

### Validate Silver

```cmd
python scripts\silver\validate_silver.py
```

All implemented Silver validation checks passed.

## 8. Gold Layer — Dimensional Data Modeling

The Gold Layer creates business-oriented analytical models from the cleaned Silver datasets.

It contains four dimension tables and four fact tables.

### Dimension tables

| Table | Grain | Records |
|---|---|---:|
| dim_customer | One row per customer_id | 99,441 |
| dim_seller | One row per seller_id | 3,095 |
| dim_product | One row per product_id | 32,951 |
| dim_date | One row per calendar date | 774 |

The product dimension combines product attributes with English category translations.

The date dimension covers purchase dates from September 4, 2016, to October 17, 2018.

### Fact tables

| Table | Grain | Records |
|---|---|---:|
| fct_order_items | One row per order-item line | 112,650 |
| fct_payments | One row per payment entry | 103,886 |
| fct_orders | One row per order | 99,441 |
| fct_reviews | One row per review/order combination | 99,224 |

The Gold models preserve clearly defined table grains to avoid duplicate aggregation and many-to-many join fanout.

### Run Gold transformations

```cmd
python scripts\gold\build_dimensions.py
python scripts\gold\build_product.py
python scripts\gold\build_date.py
python scripts\gold\build_order_items.py
python scripts\gold\build_payments.py
python scripts\gold\build_orders.py
python scripts\gold\build_reviews.py
```

Run the scripts in the order shown because some Gold models depend on previously generated tables.

### Validate Gold

```cmd
python scripts\gold\validate_gold.py
```

The Gold validation successfully verified:

- All eight expected table row counts.
- Primary and composite key integrity.
- Thirteen foreign-key relationships.
- Item, freight, and payment financial consistency.
- Order-level reconciliation classifications.
- Review score and response-time checks.

All implemented Gold validation checks passed.

## 9. Business Insights

The Gold Layer produces reusable business measures for sales analysis, financial reconciliation, delivery performance, and customer satisfaction.

### Financial overview

| Metric | Value |
|---|---:|
| Merchandise value | R$13,591,643.70 |
| Freight charges | R$2,251,909.54 |
| Total listed merchandise and freight | R$15,843,553.24 |
| Total recorded payments | R$16,008,872.12 |

These values describe listed transaction amounts and recorded payments. They do not necessarily represent recognized revenue.

### Order financial reconciliation

| Category | Orders |
|---|---:|
| Reconciled | 98,362 |
| Mismatched | 303 |
| Not comparable | 776 |

An order is considered reconciled when recorded payments and listed item-plus-freight values are both available and differ by no more than R$0.01.

### Customer satisfaction and delivery performance

| Metric | On-time | Late |
|---|---:|---:|
| Review records | 89,921 | 6,409 |
| Average review score | 4.29 | 2.27 |
| Negative review percentage | 9.27% | 62.41% |
| Average delivery duration | 11.00 days | 33.84 days |

This analysis indicates a strong association between delivery delays and lower customer satisfaction.

The results are observational and should not be interpreted as proof of causation.

## 10. Project Documentation

Detailed project documentation is available in the `docs/` directory:

- [Data Profiling](docs/data_profiling.md)
- [Bronze Layer Architecture](docs/bronze_architecture.md)
- [Silver Layer Architecture](docs/silver_architecture.md)
- [Gold Layer Architecture](docs/gold_architecture.md)

## 11. Getting Started

### Prerequisites

The current local development environment uses:

- Python 3.12
- Java 17
- PySpark 4.1.2
- Git
- Windows CMD

Windows Spark execution may also require Hadoop-compatible native utilities and corresponding environment configuration.

### Clone the repository

```cmd
git clone https://github.com/dhiruvai77/OrderFlow.git
cd OrderFlow
```

### Create a Python virtual environment

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

### Install Python dependencies

Install the Python packages required for the implemented pipeline:

```cmd
python -m pip install pandas pyspark==4.1.2
```

A pinned `requirements.txt` will be introduced to make dependency installation more reproducible.

### Download the dataset

Download the Olist dataset from Kaggle and extract all nine CSV files into:

```text
data/raw/olist/
```

The raw datasets are not included in this GitHub repository.

### Inspect the source data

```cmd
python scripts\inspect_data.py
```

### Execute the pipeline

Run the Bronze ingestion scripts first, followed by Silver transformations and Gold model-building scripts.

Execute the scripts in the order documented above to satisfy their data dependencies.

Validate each layer before proceeding to the next.

## 12. Current Limitations

The project is an evolving portfolio implementation rather than a fully deployed production system.

Current limitations include:

- Local filesystem storage rather than S3-compatible object storage.
- Full-refresh transformations rather than incremental processing.
- Manual execution instead of automated scheduling.
- Fixed historical data rather than live streaming.
- Selected data quality checks rather than exhaustive source reconciliation.
- No implemented PostgreSQL warehouse or dbt transformation project yet.
- No implemented production monitoring or automated deployment.

These capabilities are planned for future phases.

## 13. Next Phase

The next development milestone is **PostgreSQL and dbt integration**.

The objective is to extend OrderFlow from a local Parquet-based analytical platform into a SQL-accessible data warehouse with structured transformations, automated tests, and model documentation.

Future phases will introduce orchestration, streaming, object storage, dashboards, and containerized deployment.

---

**Project status:** Bronze, Silver, and Gold pipelines implemented and validated locally. PostgreSQL and dbt integration is next.
