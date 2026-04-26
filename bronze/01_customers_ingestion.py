# Databricks notebook source
# MAGIC %pip install openpyxl pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

customer_schema = StructType([
    StructField("Customer ID",  StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("email",        StringType(), True),
    StructField("phone",        StringType(), True),
    StructField("address",      StringType(), True),
    StructField("Segment",      StringType(), True),
    StructField("Country",      StringType(), True),
    StructField("City",         StringType(), True),
    StructField("State",        StringType(), True),
    StructField("Postal Code",  StringType(), True),
    StructField("Region",       StringType(), True),
])

# COMMAND ----------

customers_df  = pd.read_excel(f"{VOLUME_BASE}/Customer.xlsx", dtype=str)
customers_raw = spark.createDataFrame(customers_df, schema=customer_schema)

# COMMAND ----------

customers_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(f"{BRONZE}.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC Test Runner

# COMMAND ----------

import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_bronze/test_customers.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")