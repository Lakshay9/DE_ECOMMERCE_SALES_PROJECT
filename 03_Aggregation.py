# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Aggregation Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Reads from `ecommerce_enriched.orders` and produces pre-aggregated profit rollup tables in `ecommerce_agg`.
# MAGIC
# MAGIC Run this **after** `02_Enrichment` has completed successfully.
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `ecommerce_agg.profit_by_year_category_customer` ← source for all four SQL rollups

# COMMAND ----------

# MAGIC %run ./Constants

# COMMAND ----------

# MAGIC %run ./Configs

# COMMAND ----------

# MAGIC %run ./Common_Functions

# COMMAND ----------

# MAGIC %run ./Feature_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and Database Setup

# COMMAND ----------

create_catalog_if_not_exists(catalog)
create_database_if_not_exists(DB_DICT["AGGREGATED_DB"])
spark.catalog.setCurrentCatalog(catalog)
spark.catalog.setCurrentDatabase(DB_DICT["AGGREGATED_DB"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation Function
# MAGIC
# MAGIC The aggregation table is always fully overwritten on each run so it stays
# MAGIC in sync with the latest enriched data. No incremental logic is applied here.
# MAGIC
# MAGIC | Step | Action |
# MAGIC |---|---|
# MAGIC | 1. Read | Load the enriched source table |
# MAGIC | 2. Aggregate | Apply groupBy + metrics as defined in `AGG_TABLE_CONFIG` |
# MAGIC | 3. Write | Overwrite the aggregated Delta table |

# COMMAND ----------

def execute_agg_layer(table_key: str) -> None:
    conf = AGG_TABLE_CONFIG[table_key]

    df       = read_table(conf["source"])        # Step 1: Read enriched source
    df_final = apply_aggregation(df, conf)       # Step 2: GroupBy + metrics + sort
    write_delta_table(df_final, AGG_TABLE_CONFIG, table_key)  # Step 3: Write
    print(f"COMPLETED: {table_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation Loop
# MAGIC
# MAGIC Processes all tables in `AGG_TABLE_CONFIG`. Currently contains one table.
# MAGIC Add further rollup configs to `AGG_TABLE_CONFIG` in `Configs.py` to extend
# MAGIC without modifying this notebook.

# COMMAND ----------

failed_tables = []

for table_key in AGG_TABLE_CONFIG:
    try:
        execute_agg_layer(table_key)
    except Exception as e:
        failed_tables.append(table_key)
        print(f"FAILED: {table_key} | Error: {e}")

if failed_tables:
    raise RuntimeError(
        f"Aggregation layer incomplete. Failed tables: {failed_tables}. "
        "Fix the issue and re-run this notebook."
    )
else:
    print("\nAll aggregation tables written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Query
# MAGIC Spot-check the aggregated output. The four SQL rollups in `SQL Queries` run off this table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_agg.profit_by_year_category_customer LIMIT 20

# COMMAND ----------

