# E-commerce Sales Data Processing – Databricks

## Overview
This project implements an end-to-end data engineering solution using Databricks and PySpark
to process e-commerce sales data. The pipeline follows a Medallion (Bronze–Silver–Gold)
architecture and focuses on data quality, correctness, and analytical reliability.

-------------------------------------------------------------------------------------

## Architecture
The solution is structured into three layers:

### Bronze
- Raw ingestion of customer, product, and order data
- Minimal transformations (column standardization and ingestion metadata)

### Silver
- Cleaned and enriched datasets
- Customers modeled as SCD Type 2
- Products modeled as Type 1
- Orders treated as immutable fact records with deduplication and data quality checks

### Gold
- Aggregated profit metrics for analytics and reporting
- Profit aggregated by Year, Product Category, Sub-Category, and Customer

-----------------------------------------------------------------------------------------

## SQL Outputs
The following analytical queries are implemented using SQL:
- Profit by Year
- Profit by Year and Product Category
- Profit by Customer
- Profit by Customer and Year

---------------------------------------------------------------------------------------

## Data Quality & Testing
Pytest-based data validations are included to ensure:
- Source-to-Bronze reconciliation
- SCD integrity (single active customer record)
- Fact deduplication and business key enforcement
- Handling of orphan dimension references using `UNKNOWN`
- Profit reconciliation between Silver and Gold layers

----------------------------------------------------------------------------------------

## Repository Structure
```ecommerce-databricks-medallion/
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_customers_products.py
│   ├── 03_silver_orders_enriched.py
│   ├── 04_gold_profit_aggregations.py
│   └── 05_gold_sql_outputs.sql
├── tests/
│   └── tests_pytest_validations.py
└── README.md
```

-----------------------------------------------------------------------------------------

