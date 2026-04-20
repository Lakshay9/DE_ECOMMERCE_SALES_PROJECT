# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestion Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Reads raw source files (CSV, JSON, XLSX) and writes them as Delta tables in `ecommerce_raw`.
# MAGIC
# MAGIC This is the **first notebook** in the pipeline. Run it before `02_Enrichment`.
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `ecommerce_raw.products`
# MAGIC - `ecommerce_raw.customers`
# MAGIC - `ecommerce_raw.orders`

# COMMAND ----------

# MAGIC %md
# MAGIC `Load shared constants, configs, and utility functions`

# COMMAND ----------

# MAGIC %run ./Constants

# COMMAND ----------

# MAGIC %run ./Configs

# COMMAND ----------

# MAGIC %run ./Common_Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and Database Setup
# MAGIC Create the catalog and raw database if they do not already exist,
# MAGIC then set them as the active context so `saveAsTable` writes to the correct location.

# COMMAND ----------

create_catalog_if_not_exists(catalog)
create_database_if_not_exists(DB_DICT["RAW_DB"])
spark.catalog.setCurrentCatalog(catalog)
spark.catalog.setCurrentDatabase(DB_DICT["RAW_DB"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Loop
# MAGIC
# MAGIC Iterates over every table defined in `BRONZE_TABLE_CONFIGS`. For each table:
# MAGIC 1. Constructs the full file path from `base_path + file_name + format`
# MAGIC 2. Reads the source file with schema enforcement
# MAGIC 3. Writes to Delta with column mapping enabled (required for spaced column names)
# MAGIC 4. Runs a post-write audit — empty tables abort the pipeline immediately
# MAGIC
# MAGIC On failure the table key is recorded, the loop breaks, and a `RuntimeError` is raised.
# MAGIC This prevents partial ingestion from silently continuing into the enrichment layer.

# COMMAND ----------

failed_tables = []

for table_key in BRONZE_TABLE_CONFIGS.keys():
    try:
        meta_data = BRONZE_TABLE_CONFIGS[table_key]
        file_path = f"{base_path}/{meta_data['file_name']}.{meta_data['format']}"

        df = read_source(file_path, BRONZE_TABLE_CONFIGS, table_key)
        write_delta_table(df, BRONZE_TABLE_CONFIGS, table_key)
        validate_bronze_layer(df, table_key, meta_data["pk"])

    except Exception as err:
        failed_tables.append(table_key)
        print(f"PIPELINE ABORTED at [{table_key}]: {err}")
        break

if failed_tables:
    raise RuntimeError(
        f"Ingestion failed. Aborted at: {failed_tables}. "
        "Fix the source issue and re-run this notebook."
    )
else:
    print("\nAll ingestion tables written and validated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries
# MAGIC Spot-check the ingested data. Run these cells manually to confirm
# MAGIC the tables look as expected before proceeding to `02_Enrichment`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders LIMIT 10

# COMMAND ----------

