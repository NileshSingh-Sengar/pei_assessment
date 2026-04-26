# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

orders       = normalize_columns(spark.table(f"{BRONZE}.orders"))
dim_customer = spark.table(f"{SILVER}.dim_customer").select("customer_id", "customer_name", "country")
dim_product  = spark.table(f"{SILVER}.dim_product").select("product_id", "category", "sub_category")

# COMMAND ----------

fact_order = (
    orders
    .withColumn("order_date",     F.to_date(F.col("order_date"), "d/M/yyyy"))
    .withColumn("ship_date",      F.to_date(F.col("ship_date"),  "d/M/yyyy"))
    .withColumn("year",           F.year(F.col("order_date")))
    .withColumn("profit_rounded", F.round(F.col("profit"), 2))
    .join(dim_customer, on="customer_id", how="left")
    .join(dim_product,  on="product_id",  how="left")
)

# COMMAND ----------

fact_order_quarantine = fact_order.filter(
    F.col("customer_name").isNull() | F.col("category").isNull()
)
print(f"fact_order_quarantine rows: {fact_order_quarantine.count()}")

# COMMAND ----------

fact_order_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD}.fact_order_quarantine")

# COMMAND ----------

fact_order.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year") \
    .saveAsTable(f"{GOLD}.fact_order")

# COMMAND ----------


import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_gold/test_fact_order.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")