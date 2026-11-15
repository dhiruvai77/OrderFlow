# OrderFlow — Silver Layer Architecture

## 1. Overview

The Silver Layer transforms the Bronze Olist datasets into structured, typed, and validated data suitable for downstream analytical modelling.

The processing framework uses Python and PySpark 4.1.2, with Parquet as the output format.

All nine datasets have been transformed and stored in `data/silver/olist/`.

## 2. Data Pipeline

```text
Olist CSV Files
      |
      v
Bronze Layer (Parquet)
      |
      v
PySpark Transformations
      |
      +-- Type conversion
      +-- Data standardization
      +-- Quality flags
      +-- Validation checks
      |
      v
Silver Layer (Parquet)
      |
      v
Gold Layer (Planned)
```

## 3. Silver Datasets

| Dataset | Records | Main transformations |
|---|---:|---|
| Orders | 99,441 | Timestamp conversion, delivery calculations, anomaly flags |
| Customers | 99,441 | City/state standardization, key validation |
| Sellers | 3,095 | City/state standardization, key validation |
| Products | 32,951 | Numeric conversion, key validation |
| Order items | 112,650 | Decimal prices, timestamp conversion, transaction validation |
| Payments | 103,886 | Decimal values, payment type standardization |
| Reviews | 99,224 | Score and timestamp conversion, validation |
| Geolocation | 1,000,163 | Coordinate conversion, range checks, ZIP validation |
| Category translations | 71 | Standardized category names, key validation |

## 4. Transformation Design

### Orders

The pipeline calculates:

- `delivery_days`
- `is_late_delivery`
- `is_valid_order`
- `flag_delivered_missing_date`
- `flag_status_date_mismatch`
- `flag_delivery_before_carrier`
- `has_delivery_anomaly`
- `is_delivery_metric_eligible`

### Transactions

Monetary attributes are converted into `decimal(12,2)` types.

Transaction quality indicators include:

- `has_conversion_error`
- `has_negative_amount`
- `is_valid_key`
- `is_valid_transaction`

### Reviews

Review scores are converted into integers and validated against the permitted range of 1–5.

Missing review comments are preserved because written comments are optional.

### Geolocation

Coordinates are converted into double-precision numeric values and checked against valid latitude and longitude ranges.

ZIP code prefixes remain strings to preserve leading zeros.

Exact duplicate geolocation records remain in Silver pending a documented geographic aggregation strategy.

## 5. Data Quality Findings

### Orders

| Quality issue | Count |
|---|---:|
| Delivered without delivery date | 8 |
| Status/date mismatch | 6 |
| Delivery before carrier handoff | 23 |
| Unique orders with delivery anomalies | 37 |

After applying the delivery eligibility rules:

- Eligible orders: 96,447
- Average delivery duration: 12.56 days
- Late deliveries: 6,534

### Transactions

Both order items and payments passed the implemented checks for:

- Key uniqueness
- Required identifiers
- Type conversion failures
- Negative monetary values

### Reviews

All 99,224 reviews passed the implemented score, identifier, and conversion checks.

### Geolocation

- Records: 1,000,163
- Exact duplicate excess rows: 261,831
- Invalid coordinates: 0
- Invalid ZIP prefixes: 0
- Conversion errors: 0

## 6. End-to-End Validation

Validation is implemented in:

`scripts/silver/validate_silver.py`

The script verifies:

1. Record counts across all nine datasets.
2. Expected data types for selected attributes.
3. Primary and composite key uniqueness.
4. Six foreign-key relationships.
5. Selected transformation quality flags.

### Results

All implemented validation checks passed.

No unmatched foreign-key records were identified.

The 37 delivery anomalies remain flagged rather than removed.

## 7. Current Limitations

- Processing uses local Spark execution.
- Transformations use overwrite mode.
- Validation relies partly on fixed historical row-count expectations.
- Not every source attribute has a field-level equivalence test.
- Geolocation duplicates remain unresolved for reporting purposes.
- Quality flags identify selected issues but do not establish complete business validity.
- Automated orchestration, incremental processing, and monitoring are not yet implemented.

## 8. Next Phase — Gold Layer

The Gold Layer will create analytical models such as:

- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`
- `fct_orders`
- `fct_order_items`
- `fct_payments`
- `fct_reviews`

These models will support revenue analysis, delivery performance, seller comparisons, and customer satisfaction reporting.

## 9. Conclusion

The OrderFlow Silver Layer successfully transforms all nine Olist datasets into typed Parquet tables.

The implemented validation framework confirms record counts, key integrity, selected schemas, and cross-dataset relationships.

The resulting datasets provide the foundation for business-oriented modelling in the Gold Layer.
