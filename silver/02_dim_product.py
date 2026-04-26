# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import count, row_number, col

# COMMAND ----------

products = (
    normalize_columns(spark.table(f"{BRONZE}.products"))
    .transform(trim_string_columns)
)

# COMMAND ----------

#product IDs that appear more than once are stored separately.
dup_ids = (
    products
    .groupBy("product_id")
    .agg(count("*").alias("n"))
    .filter("n > 1")
    .select("product_id")
)

products_clean   = products.join(dup_ids, "product_id", "left_anti")
products_suspect = products.join(dup_ids, "product_id", "inner")
print(f"dim_product_quarantine rows: {products_suspect.count()}")

products_suspect.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER}.dim_product_quarantine")

# COMMAND ----------

# Deduplicate — keep lowest price per product_id
w = Window.partitionBy("product_id").orderBy(col("price_per_product"))
products_suspect_deduped = (
    products_suspect
    .withColumn("rn", row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

dim_product = products_clean.union(products_suspect_deduped)
print(f"dim_product rows: {dim_product.count()}")

# COMMAND ----------

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER}.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC Test Runner

# COMMAND ----------

import pytest, os, sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, f"{WORKSPACE_ROOT}/tests")

exit_code = pytest.main([f"{WORKSPACE_ROOT}/tests/test_silver/test_dim_product.py", "-vv", "--tb=short", "-p", "no:cacheprovider", "--assert=plain"])
if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code} — pipeline halted")