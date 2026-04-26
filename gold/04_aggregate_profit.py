# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

fact_order = spark.table(f"{GOLD}.fact_order")

# COMMAND ----------

aggregate_profit = (
    fact_order
    .groupBy("year", "category", "sub_category", "customer_id", "customer_name")
    .agg(F.round(F.sum("profit"), 2).alias("total_profit"))
)

# COMMAND ----------

aggregate_profit.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD}.aggregate_profit")

# COMMAND ----------

import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_gold/test_aggregate_profit.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")