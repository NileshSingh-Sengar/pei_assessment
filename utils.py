# Databricks notebook source
# MAGIC %md
# MAGIC Shared helpers used across notebooks

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

def normalize_col_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")

# COMMAND ----------

def normalize_columns(df):
    return df.select([F.col(f"`{c}`").alias(normalize_col_name(c)) for c in df.columns])

# COMMAND ----------

def trim_string_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df