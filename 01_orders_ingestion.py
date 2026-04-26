# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# COMMAND ----------

orders_schema = StructType([
    StructField("Row ID",      LongType(),   True),
    StructField("Order ID",    StringType(), True),
    StructField("Order Date",  StringType(), True),
    StructField("Ship Date",   StringType(), True),
    StructField("Ship Mode",   StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID",  StringType(), True),
    StructField("Quantity",    LongType(),   True),
    StructField("Price",       DoubleType(), True),
    StructField("Discount",    DoubleType(), True),
    StructField("Profit",      DoubleType(), True),
])

# COMMAND ----------

orders_raw = (
    spark.read
    .schema(orders_schema)
    .option("multiline", "true")
    .json(f"{VOLUME_BASE}/Orders.json")
)

# COMMAND ----------

orders_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(f"{BRONZE}.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Test Runner

# COMMAND ----------

import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_bronze/test_orders.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")