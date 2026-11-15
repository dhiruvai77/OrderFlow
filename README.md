# OrderFlow — End-to-End E-Commerce Data Engineering Platform

## Overview

OrderFlow is a practical data engineering project built using the Brazilian E-Commerce Public Dataset by Olist.

The objective is to develop a production-style e-commerce data platform covering ingestion, processing, data quality, warehousing, orchestration, streaming, and analytics.

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The dataset includes approximately 100,000 historical orders and nine related CSV files.

Raw datasets are excluded from version control.

## Technology Stack

- Python and PySpark
- PostgreSQL and dbt
- Apache Airflow
- Apache Kafka
- Docker
- MinIO / AWS S3
- SQL and business intelligence dashboards


## Project Roadmap

- [x] Complete Bronze ingestion and validation
- [x] Implement Silver transformations
- [x] Validate Silver schemas and relationships
- [ ] Build Gold analytical models
- [ ] Implement PostgreSQL and dbt
- [ ] Add Airflow orchestration
- [ ] Implement Kafka streaming
- [ ] Build business dashboards
- [ ] Add Docker and cloud deployment

## Architecture Documentation

- [Data Profiling](docs/data_profiling.md)
- [Bronze Layer Architecture](docs/bronze_architecture.md)

## Bronze Pipeline

Run ingestion:

    python scripts\ingest_all.py

Validate Bronze datasets:

    python scripts\validate_bronze.py

Validate all review records:

    python scripts\validate_reviews_full.py


## Silver Layer

The Silver Layer contains nine typed and validated
Parquet datasets transformed using PySpark.

### Run Silver validation

python scripts\silver\validate_silver.py

### Documentation

[Silver Layer Architecture](docs/silver_architecture.md)


## Getting Started

Create and activate a Python virtual environment:

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

Install pandas:

```cmd
python -m pip install pandas
```

Download the Olist dataset and place the nine CSV files in `data/raw/olist/`.

Inspect the dataset:

```cmd
python scripts\inspect_data.py
```
