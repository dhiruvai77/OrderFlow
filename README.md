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

- [x] Initialize project structure
- [x] Download and profile the Olist dataset
- [x] Configure Python, Java, and PySpark
- [x] Implement reusable Bronze ingestion
- [x] Convert all nine CSV datasets to Parquet
- [x] Validate Bronze datasets and review records
- [ ] Implement Silver data transformations
- [ ] Build Gold analytical models
- [ ] Implement PostgreSQL and dbt
- [ ] Add Airflow orchestration
- [ ] Implement Kafka streaming
- [ ] Build dashboards
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
