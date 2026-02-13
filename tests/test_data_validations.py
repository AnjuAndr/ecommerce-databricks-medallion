# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Test Suite: Data Engineering Validations  
# MAGIC   
# MAGIC Purpose:  
# MAGIC - Unit testing of transformation logic using DataFrames and fixtures  
# MAGIC - Integration testing of Silver layer correctness  
# MAGIC - Business validation and reconciliation at Gold layer  
# MAGIC
# MAGIC Testing Strategy:  
# MAGIC - Unit Tests: Isolated logic validation (fixtures, DataFrames)  
# MAGIC - Integration Tests: Table-level correctness  
# MAGIC - Business Tests: Metric reconciliation  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC BRONZE LAYER TESTS
# MAGIC

# COMMAND ----------

# Imports
import pytest
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.functions import col, sum as _sum

# COMMAND ----------

# Spark Session Fixture
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-cases-assesment").getOrCreate()
    
    yield spark
    spark.stop()

# COMMAND ----------

#Sample Data Fixtures for Unit Tests
#Orders fixture

@pytest.fixture
def sample_orders_df(spark):
    data = [
         ("O1", "C1", "P1", 100.0, 2),
        ("O2", "C2", "P2", 200.0, 1),
    ]
    columns = ["order_id", "customer_id", "product_id", "price", "quantity"]
    return spark.createDataFrame(data, columns)

# COMMAND ----------

# Transformation Functions (Unit - Testable Logic)

def calculate_profit(df):
    return df.withColumn("profit", col("price") * col("quantity"))


# COMMAND ----------

#UNIT Test - Transformation Logic

def test_calculate_profit(sample_orders_df):
    result = calculate_profit(sample_orders_df)

    row=result.filter(col("order_id")=="01").collect()[0]
    assert row["profit"]==200.0, \
        "Profit calculation failed for order 01"

# COMMAND ----------

# Parametrized Unit Tests - Edge Case Coverage

@pytest.mark.parametrize(
    "price, quantity, expected_profit",
    [
        (100, 2, 200),
        (50, 3, 150),
        (0, 5, 0),
    ]
)
def test_profit_parametrized(spark, price, quantity, expected_profit):
    df = spark.createDataFrame(
        [("01", price, quantity)],
        ["order_id", "price", "quantity"]
    )

    result = calculate_profit(df)
    assert result.collect()[0]["profit"] == expected_profit 

# COMMAND ----------

# Integration Tests - Bronze Layer Validation

def test_bronze_counts_match_source(spark):
    assert spark.table("customer").count() == spark.table("bronze.customers").count(), \
        "Bronze customers row count does not match source customer table"
    assert spark.table("products").count() == spark.table("bronze.products").count(), \
        "Bronze products row count does not match source products table"
    assert spark.table("orders").count() == spark.table("bronze.orders").count(), \
         "Bronze orders row count does not match source orders table"

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER - CUSTOMERS (SCD TYPE 2)

# COMMAND ----------

# Integration Tests - Silver Customers SCD Type 2

def test_silver_customers_single_current_record(spark):
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

#Negative test - FAIL if any customer has zero current records

def test_customer_without_current_record(spark):
    df = spark.table("silver.customers_enriched")

    invalid = (
        df.groupBy("customer_id")
          .agg(_sum(col("is_current").cast("int")).alias("active_cnt"))
          .filter(col("active_cnt")==0)
          .count()
    )

    assert invalid == 0, \
        "Customer found without active SCD record"

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER - PRODUCTS

# COMMAND ----------

# Integration Tests - Silver Products Dimension  
def test_silver_products_unique_product_id(spark):
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

# MAGIC %md
# MAGIC SILVER - ORDERS (Fact Table)

# COMMAND ----------

# Integration Tests - Silver Orders Fact Table
def test_siver_orders_no_duplicate_row_id(spark):
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

# Data Quality Tests - UNKOWN Handling
def test_silver_orders_unknown_handling(spark):
    df = spark.table("silver.orders_enriched")

    assert df.filter(col("customer_name").isNull()).count() == 0, \
        "NULL customer_name found — UNKNOWN handling failed"

    assert df.filter(col("category").isNull()).count() == 0, \
        "NULL category found — UNKNOWN handling failed"


# COMMAND ----------

# MAGIC %md
# MAGIC GOLD - AGGREGATIONS

# COMMAND ----------

# Business Validation Tests - Silver vs Gold Reconciliation
def test_gold_profit_reconciliation_with_silver(spark):
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