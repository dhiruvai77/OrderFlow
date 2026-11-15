# OrderFlow — Gold Layer Architecture

## 1. Overview

The OrderFlow Gold Layer transforms validated Silver Olist data into business-oriented analytical models using PySpark and Parquet.

The Gold Layer follows dimensional modeling principles and consists of four dimensions and four fact tables.

All eight models are stored in `data/gold/olist/`.

## 2. Architecture

```text
Olist CSV Files
      |
      v
Bronze Layer (9 datasets)
      |
      v
Silver Layer (9 datasets)
      |
      v
Gold Layer (8 analytical models)
      |
      +-- dim_customer
      +-- dim_seller
      +-- dim_product
      +-- dim_date
      |
      +-- fct_order_items
      +-- fct_payments
      +-- fct_orders
      +-- fct_reviews
```

## 3. Dimension Tables

| Model | Grain | Records |
|---|---|---:|
| dim_customer | One row per customer_id | 99,441 |
| dim_seller | One row per seller_id | 3,095 |
| dim_product | One row per product_id | 32,951 |
| dim_date | One row per calendar date | 774 |

### dim_customer

Stores customer identifiers and geographic attributes. Preserves both `customer_id` and `customer_unique_id` because they represent different concepts in the Olist dataset.

### dim_seller

Stores seller identifiers and geographic attributes, including city, state, and ZIP code prefix.

### dim_product

Combines Silver products with category translations using a left join.

- Products with English translations: 32,328
- Products missing source categories: 610
- Products with source categories but no translation: 13

All 32,951 products are preserved.

### dim_date

Contains calendar attributes for purchase dates from September 4, 2016, through October 17, 2018.

The dimension includes year, quarter, month, weekday, week number, and weekend indicators.

Its current coverage is limited to the observed purchase-date range.

## 4. Fact Tables

| Model | Grain | Records |
|---|---|---:|
| fct_order_items | One row per order_id and order_item_id | 112,650 |
| fct_payments | One row per order_id and payment_sequential | 103,886 |
| fct_orders | One row per order_id | 99,441 |
| fct_reviews | One row per review_id and order_id | 99,224 |

### fct_order_items

Stores item-level merchandise prices, freight charges, product identifiers, seller identifiers, customer identifiers, and purchase dates.

`total_item_value = price + freight_value`

### fct_payments

Stores recorded payment entries, including payment methods, installments, and payment values.

Payment entries are not assumed to be equivalent to recognized revenue.

### fct_orders

Combines order-level attributes with separately aggregated item and payment measures.

Item records and payment records are each grouped by `order_id` before joining, preventing many-to-many join fanout.

Provides:

- Item and payment counts
- Merchandise and freight totals
- Recorded payment totals
- Financial reconciliation indicators
- Delivery duration and quality indicators
- Order status and purchase date

### fct_reviews

Connects review records with order-level delivery information.

Provides review scores, sentiment categories, written-feedback indicators, review response intervals, and delivery-performance attributes.

Multiple reviews relating to an order remain separate records.

## 5. Business Metric Definitions

| Metric | Definition |
|---|---|
| Merchandise value | Sum of item-line `price` |
| Freight value | Sum of item-line `freight_value` |
| Total listed value | Merchandise value plus freight value |
| Recorded payment value | Sum of `payment_value` |
| Financial difference | Recorded payment value minus listed value, at the order level |
| Reconciled order | Both values available and absolute difference no greater than R$0.01 |
| Average review score | Arithmetic mean of review scores |
| Negative review | Score of 1 or 2 |
| Neutral review | Score of 3 |
| Positive review | Score of 4 or 5 |
| Late delivery | Actual customer delivery calendar date after the estimated date |

Listed values include order items regardless of parent order status.

These measures must not be described as confirmed or recognized revenue without additional business rules.

## 6. Financial Findings

| Metric | Value |
|---|---:|
| Merchandise value | R$13,591,643.70 |
| Freight charges | R$2,251,909.54 |
| Total listed value | R$15,843,553.24 |
| Recorded payments | R$16,008,872.12 |

### Order-level reconciliation

| Category | Orders |
|---|---:|
| Reconciled | 98,362 |
| Mismatched | 303 |
| Not comparable | 776 |
| Total | 99,441 |

The 776 non-comparable orders have missing item or payment data.

Differences should be investigated rather than automatically corrected. Refunds and payment settlement outcomes are not established by the available fields.

## 7. Customer Satisfaction Findings

| Metric | On-time delivery | Late delivery |
|---|---:|---:|
| Review records | 89,921 | 6,409 |
| Average review score | 4.29 | 2.27 |
| Negative review percentage | 9.27% | 62.41% |
| Average delivery duration | 11.00 days | 33.84 days |

Analysis includes review records linked to orders eligible for delivery metrics.

These results demonstrate association rather than causation. They are review-level measures, not necessarily distinct-order measures.

## 8. Gold Layer Validation

Validation script: `scripts/gold/validate_gold.py`

Completed checks include:

- Eight row-count validations
- Eight primary/composite-key validations
- Thirteen foreign-key relationship validations
- Five financial consistency checks
- Order reconciliation classification
- Review score and response-time checks

All implemented Gold validation checks passed.

## 9. Design Considerations and Limitations

- All Gold models currently use Parquet files on the local filesystem.
- Outputs are generated using full-refresh overwrite operations.
- Financial measures have different business meanings and should not be interchanged.
- Review-level metrics can include multiple reviews for the same order.
- Purchase dates are covered by the current date dimension, but other date roles may require extending it.
- Olist geographic source duplicates remain in Silver and have not been turned into a geographic dimension.
- The current models do not implement slowly changing dimensions.
- PostgreSQL, dbt, orchestration, streaming, and object storage integration remain future work.

## 10. Next Phase

The next stage will introduce PostgreSQL and dbt for SQL-based data warehouse transformations, analytical modeling, testing, and lineage documentation.

Future infrastructure work will also introduce MinIO/S3-compatible data lake storage and workflow orchestration.

## 11. Conclusion

OrderFlow now contains a validated Medallion pipeline from Bronze through Gold.

The Gold Layer provides reusable analytical models with explicit grain, validated relationships, and consistent financial aggregates.

These models form the foundation for SQL analytics, business dashboards, and the next stage of the data engineering platform.
