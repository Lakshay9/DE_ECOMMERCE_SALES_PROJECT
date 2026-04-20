# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Produces the four profit rollup outputs required by the assessment.
# MAGIC All queries run against `ecommerce_agg.profit_by_year_category_customer`,
# MAGIC which is written by `03_Aggregation`.
# MAGIC
# MAGIC | Query | Dimensions |
# MAGIC |---|---|
# MAGIC | 1 | Profit by Year |
# MAGIC | 2 | Profit by Year + Product Category |
# MAGIC | 3 | Profit by Customer |
# MAGIC | 4 | Profit by Customer + Year |

# COMMAND ----------

# MAGIC %run ./Constants

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Set the active catalog and register the aggregated table as a temp view
# MAGIC so queries below do not need to fully qualify the table name.

# COMMAND ----------

spark.catalog.setCurrentCatalog(catalog)

spark.table("ecommerce_agg.profit_by_year_category_customer") \
     .createOrReplaceTempView("v_profit_by_year_category_customer")

print("Temp view registered: v_profit_by_year_category_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1 — Profit by Year
# MAGIC Total profit for each calendar year across all customers and categories.

# COMMAND ----------

print("\n═══ Profit by Year ═══")
spark.sql("""
    SELECT
        order_year,
        ROUND(SUM(total_profit), 2) AS total_profit
    FROM v_profit_by_year_category_customer
    GROUP BY order_year
    ORDER BY order_year
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2 — Profit by Year + Product Category
# MAGIC Breaks down annual profit by product category.
# MAGIC Useful for identifying which categories drive profit growth year over year.

# COMMAND ----------

print("\n═══ Profit by Year + Product Category ═══")
spark.sql("""
    SELECT
        order_year,
        category,
        ROUND(SUM(total_profit), 2) AS total_profit
    FROM v_profit_by_year_category_customer
    GROUP BY order_year, category
    ORDER BY order_year, category
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3 — Profit by Customer
# MAGIC Total profit contribution per customer across all years.
# MAGIC Ordered by `total_profit` descending to highlight the most valuable customers.

# COMMAND ----------

print("\n═══ Profit by Customer ═══")
spark.sql("""
    SELECT
        customer_id,
        customer_name,
        ROUND(SUM(total_profit), 2) AS total_profit
    FROM v_profit_by_year_category_customer
    GROUP BY customer_id, customer_name
    ORDER BY total_profit DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4 — Profit by Customer + Year
# MAGIC Per-customer profit broken down by year.
# MAGIC Useful for tracking customer value trends and identifying churn or growth.

# COMMAND ----------

print("\n═══ Profit by Customer + Year ═══")
spark.sql("""
    SELECT
        customer_id,
        customer_name,
        order_year,
        ROUND(SUM(total_profit), 2) AS total_profit
    FROM v_profit_by_year_category_customer
    GROUP BY customer_id, customer_name, order_year
    ORDER BY customer_name, order_year
""").display()