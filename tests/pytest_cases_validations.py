# Databricks notebook source
# MAGIC %md
# MAGIC Pytest validations focused on business correctness, SCD integrity,
# MAGIC deduplication, and analytical reconciliation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC BRONZE LAYER TESTS
# MAGIC

# COMMAND ----------

import pytest
from pyspark.sql.functions import col, sum as _sum

# COMMAND ----------

#Test 1 — Bronze row counts matches source

def test_bronze_counts_match_source():
    assert spark.table("customer").count() == spark.table("bronze.customers").count(), \
        "Bronze customers row count does not match source customer table"
    assert spark.table("products").count() == spark.table("bronze.products").count(), \
        "Bronze products row count does not match source products table"
    assert spark.table("orders").count() == spark.table("bronze.orders").count(), \
         "Bronze orders row count does not match source orders table"

# COMMAND ----------

def test_bronze_customers_schema():
    cols = spark.table("bronze.customers").columns
    assert "customer_id" in cols, "customer_id column is missing in bronze customers"
    assert "customer_name" in cols, "customer_name column is missing in bronze customers"
   

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER - CUSTOMERS (SCD TYPE 2)

# COMMAND ----------

#Test 3 : Exactly one current record per customer

def test_silver_customers_single_current_record():
    df = spark.table("silver.customers_enriched")

    invalid = (
        df.filter(col("is_current")==True)
            .groupBy("customer_id")
            .count()
            .filter(col("count")>1)
            .count()
    )
    assert invalid ==0, \
        "More than one current record found a customer (SCD type 2 Violation)"

# COMMAND ----------

#Test 4 : SCD control columns must be populated

def test_silver_customers_scd_columns_not_null():
    df = spark.table("silver.customers_enriched")

    assert df.filter(col("silver_start_ts").isNull()).count() == 0, \
        "silver_start_ts contains NULL values in customers SCD table"

# COMMAND ----------

#Negative test - FAIL if any customer has zero current records

def test_negative_customer_without_current_record():
    df = spark.table("silver.customers_enriched")

    invalid = (
        df.groupBy("customer_id")
          .agg(_sum(col("is_current").cast("int")).alias("current_cnt"))
          .filter(col("current_cnt")==0)
          .count()
    )

    assert invalid == 0, \
        "One or more customers have o active (is_current = true) record"

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER - PRODUCTS

# COMMAND ----------

#Test 5 - product_id must be unique
def test_silver_products_unique_product_id():
    df = spark.table("silver.products_enriched")

    duplicates =(
        df.groupBy("product_id")
          .count()
          .filter(col("count")>1)
          .count()
    )
    assert duplicates == 0, \
        "Duplicate product_id found in silver"
    

# COMMAND ----------

#Negative test - FAIL if price is negative
def test_negative_product_price():
    df = spark.table("silver.products_enriched")

    invalid = (
        df.filter(col("price_per_product")<0)
            .count()
    )
    assert invalid == 0, \
        "Negative price found in silver products table"


# COMMAND ----------

# MAGIC %md
# MAGIC SILVER - ORDERS (Fact Table)

# COMMAND ----------

#Test 6 : Business Keys must not be NULL
def test_silver_orders_business_keys_not_null():
    df = spark.table("silver.orders_enriched")

    assert df.filter(col("order_id").isNull()).count() == 0, \
        "order_id contains NULL values in silver orders table"
    assert df.filter(col("customer_id").isNull()).count() ==0, \
        "customer_id contains NULL values in silver orders table"
 

# COMMAND ----------

#Test 7 - Orders must be deduplicated by row_id
def test_siver_orders_no_duplicate_row_id():
    df = spark.table("silver.orders_enriched")

    duplicates = (
        df.groupBy("row_id")
          .count()
          .filter(col("count")>1)
          .count()
    )
    assert duplicates == 0, \
        "Duplicate row_id found in silver orders table"

# COMMAND ----------

#Test 8 - UNKNOWN handling enforced
def test_silver_orders_unknown_handling():
    df = spark.table("silver.orders_enriched")

    assert df.filter(col("customer_name").isNull()).count() == 0, \
        "NULL customer_name found — UNKNOWN handling failed"

    assert df.filter(col("category").isNull()).count() == 0, \
        "NULL category found — UNKNOWN handling failed"


# COMMAND ----------

#Negative Test - FAIL if quantity is non-positive
def test_negative_order_quantity():
    df = spark.table("silver.orders_enriched")

    invalid = (
        df.filter(col("quantity")<=0)
            .count()
    )
    assert invalid == 0, \
        "Negative quantity found in silver orders table"

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD - AGGREGATIONS

# COMMAND ----------

#Test 9 - Gold profit reconciles with Silver
def test_gold_profit_reconciliation_with_silver():
    silver_profit = (
        spark.table("silver.orders_enriched")
            .agg(_sum("profit").alias("p"))
            .collect()[0]["p"]
    )

    gold_profit = (
        spark.table("gold.profit_aggregated")
            .agg(_sum("total_profit").alias("p"))
            .collect()[0]["p"]
    )

    assert round(silver_profit, 2)== round(gold_profit,2), \
        "Gold total_profit does not reconcile with Silver profit"

# COMMAND ----------

#Test 10 - Gold dimensions must not be NULL
def test_gold_no_null_dimensions():
    df = spark.table("gold.profit_aggregated")

    assert df.filter(col("order_year").isNull()).count() == 0, \
        "NULL order_year found in gold table"
       
    assert df.filter(col("customer_name").isNull()).count() == 0, \
        "NULL customer_name found in gold table"

# COMMAND ----------

#Negative Test - FAIL if aggregation grain is broken
def test_gold_profit_grain_uniqueness():
    df = spark.table("gold.profit_aggregated")

    duplicates = (
        df.groupBy("customer_name", "order_year")
          .count()
          .filter(col("count") > 1)
          .count()
    )

    assert duplicates == 0, \
        "Gold aggregation grain violated : duplicate dimension combinations found"
