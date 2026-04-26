# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

products_schema = StructType([
    StructField("Product ID",        StringType(), True),
    StructField("Category",          StringType(), True),
    StructField("Sub-Category",      StringType(), True),
    StructField("Product Name",      StringType(), True),
    StructField("State",             StringType(), True),
    StructField("Price per product", StringType(), True),
])

# COMMAND ----------

products_raw = (
    spark.read
    .schema(products_schema)
    .option("header", "true")
    .csv(f"{VOLUME_BASE}/Products.csv")
)

# COMMAND ----------

products_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(f"{BRONZE}.products")

# COMMAND ----------

# MAGIC %md
# MAGIC Test Runner

# COMMAND ----------

import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_bronze/test_products.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")