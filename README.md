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

- [x] Create local project structure
- [x] Download the Olist dataset
- [x] Run initial dataset inspection
- [ ] Complete data profiling
- [ ] Implement batch ingestion with PySpark
- [ ] Create bronze, silver, and gold layers
- [ ] Build dimensional warehouse models
- [ ] Add orchestration using Airflow
- [ ] Implement simulated real-time streaming
- [ ] Build analytics dashboards
- [ ] Add monitoring and deployment

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
