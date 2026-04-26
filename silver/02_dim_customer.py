# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

def clean_customer_name(col):
    cleaned = F.regexp_replace(col, r"[^\p{L}\s'\-]", "")
    stripped = F.regexp_replace(F.regexp_replace(cleaned, r"^\-+", ""), r"\-+$", "")
    return F.trim(F.regexp_replace(stripped, r"\s+", " "))

# COMMAND ----------

def clean_phone(col_name):
    return (
        F.when(F.col(col_name) == "#ERROR!", F.lit(None))
         .when(F.col(col_name).rlike(r"^-\d+$"), F.lit(None))
         .otherwise(F.col(col_name))
    )

# COMMAND ----------

customers = (
    normalize_columns(spark.table(f"{BRONZE}.customers"))
    .transform(trim_string_columns)
    .withColumn("customer_id", F.upper(F.col("customer_id")))
    .withColumn("address", F.regexp_replace("address", r"\n", " "))
)

# COMMAND ----------

# Quarantine: null or empty customer names
customers_suspect = customers.filter(
    F.col("customer_name").isNull() | (F.col("customer_name") == "")
)
print(f"dim_customer_quarantine rows: {customers_suspect.count()}")

customers_suspect.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER}.dim_customer_quarantine")

# COMMAND ----------

dim_customer = (
    customers
    .filter(~(F.col("customer_name").isNull() | (F.col("customer_name") == "")))
    .withColumn("customer_name", clean_customer_name(F.col("customer_name")))
    .withColumn("phone", clean_phone("phone"))
    .dropDuplicates(["customer_id"])
)
print(f"dim_customer rows: {dim_customer.count()}")

# COMMAND ----------

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER}.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC Test Runner

# COMMAND ----------

import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_silver/test_dim_customer.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")