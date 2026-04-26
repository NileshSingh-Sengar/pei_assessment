# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profit by Year

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        round(sum(profit), 2) AS total_profit
    FROM {GOLD}.fact_order
    GROUP BY year
    ORDER BY year
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profit by Year + Product Category

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        category,
        round(sum(profit), 2) AS total_profit
    FROM {GOLD}.fact_order
    GROUP BY year, category
    ORDER BY year, category
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profit by Customer

# COMMAND ----------

spark.sql(f"""
    SELECT
        customer_id,
        customer_name,
        round(sum(profit), 2) AS total_profit
    FROM {GOLD}.fact_order
    GROUP BY customer_id, customer_name
    ORDER BY total_profit DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profit by Customer + Year

# COMMAND ----------

spark.sql(f"""
    SELECT
        customer_id,
        customer_name,
        year,
        round(sum(profit), 2) AS total_profit
    FROM {GOLD}.fact_order
    GROUP BY customer_id, customer_name, year
    ORDER BY total_profit DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

spark.sql(f"""
    SELECT
        'dim_customer_quarantine'        AS quarantine_table,
        'Null or empty customer name'    AS reason,
        count(*)                         AS row_count
    FROM {SILVER}.dim_customer_quarantine
    UNION ALL
    SELECT
        'dim_product_quarantine',
        'Duplicate product ID',
        count(*)
    FROM {SILVER}.dim_product_quarantine
    UNION ALL
    SELECT
        'fact_order_quarantine',
        'No matching customer or product',
        count(*)
    FROM {GOLD}.fact_order_quarantine
""").display()