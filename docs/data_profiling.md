# Olist Dataset — Data Profiling Report

## 1. Dataset Overview

OrderFlow uses the Brazilian E-Commerce Public Dataset by Olist, consisting of nine CSV files.

Key dataset volumes:

- Orders: 99,441
- Customers: 99,441
- Order items: 112,650
- Payments: 103,886
- Reviews: 99,224
- Products: 32,951
- Sellers: 3,095
- Geolocation records: 1,000,163
- Category translations: 71

## 2. Data Quality Findings

### Primary Key Uniqueness

No duplicate primary keys were found in orders, customers, products, or sellers.

### Referential Integrity

All six tested foreign-key relationships passed, with zero unmatched records.

### Missing Values

- Orders: 4,908 missing values across approval and delivery timestamps.
- Products: 2,448 missing values across product attributes.
- Reviews: 145,903 missing values across optional review text fields.

### Geolocation Duplicates

The geolocation dataset contains 261,831 exact duplicate rows. Further investigation is required before deduplication.

## 3. Order Status Distribution

| Status | Count |
|---|---:|
| Delivered | 96,478 |
| Shipped | 1,107 |
| Canceled | 625 |
| Unavailable | 609 |
| Invoiced | 314 |
| Processing | 301 |
| Created | 5 |
| Approved | 2 |

## 4. Transaction Characteristics

- Orders containing at least one item: 98,666
- Orders containing multiple items: 9,803
- Maximum items within a single order: 21

## 5. Proposed Data Engineering Rules

1. Preserve raw source data unchanged in the bronze layer.
2. Validate primary keys and foreign-key relationships.
3. Convert date and timestamp columns into appropriate data types.
4. Preserve expected nulls in delivery and review attributes.
5. Standardize missing product categories for analytics.
6. Investigate geolocation duplicates before applying deduplication.
7. Validate order status and delivery timestamp consistency.
8. Protect analytical revenue calculations from one-to-many join multiplication.

## 6. Conclusion

The initial profiling indicates strong primary-key uniqueness and referential integrity in the tested tables. The dataset also presents realistic challenges involving missing values, geolocation duplicates, and multiple transactional granularities.

These findings will inform the PySpark batch ingestion and data quality pipelines.
