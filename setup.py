# Databricks notebook source
# MAGIC %md
# MAGIC Run this notebook first — creates the Unity Catalog catalog, schemas, and volume.   

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Unity Catalog Objects

# COMMAND ----------

spark.sql(f"CREATE CATALOG  IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA   IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA   IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA   IF NOT EXISTS {GOLD}")
spark.sql(f"CREATE VOLUME   IF NOT EXISTS {BRONZE}.landing")

print("Catalog, schemas, and volume created successfully")
print(f"\nUpload source files to: {VOLUME_BASE}")
print("  - Orders.json")
print("  - Products.csv")
print("  - Customer.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify setup

# COMMAND ----------

schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
print(f"Schemas in {CATALOG}: {schemas}")

try:
    files = dbutils.fs.ls(VOLUME_BASE)
    print(f"\nFiles in volume: {[f.name for f in files]}")
except Exception:
    print(f"\nVolume is empty — upload the three source files to {VOLUME_BASE}")