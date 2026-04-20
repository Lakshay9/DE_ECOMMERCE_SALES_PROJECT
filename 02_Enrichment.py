# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Enrichment Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Reads from the ingestion layer, applies data quality checks and business logic,
# MAGIC and writes cleaned tables to `ecommerce_enriched`.
# MAGIC
# MAGIC Run this **after** `01_Ingestion` has completed successfully.
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `ecommerce_enriched.Customers`
# MAGIC - `ecommerce_enriched.Products`
# MAGIC - `ecommerce_enriched.Orders` (includes joined customer and product attributes)

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
create_database_if_not_exists(DB_DICT["ENRICHED_DB"])
spark.catalog.setCurrentCatalog(catalog)
spark.catalog.setCurrentDatabase(DB_DICT["ENRICHED_DB"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrichment Function
# MAGIC
# MAGIC Encapsulates the full transformation sequence for a single table.
# MAGIC
# MAGIC | Step | Action |
# MAGIC |---|---|
# MAGIC | 1. Read | Load from ingestion layer, filter to latest ingestion batch |
# MAGIC | 2. Rename | Standardise all column names to snake_case |
# MAGIC | 3. Trim | Strip whitespace from all string columns |
# MAGIC | 4. Transform | Date parsing, rounding, DQ cleaning (names, emails, phones, postal) |
# MAGIC | 5. Dedupe | Remove duplicate rows on the configured primary key |
# MAGIC | 6. Join | Left join customer and product attributes onto orders |
# MAGIC | 7. Nulls | Fill nulls in non-PK columns (strings → "Unknown", numerics → 0) |
# MAGIC | 8. Contract | Select only the output columns defined in `select_cols` |
# MAGIC | 9. Audit | Check row count and nulls in critical columns |
# MAGIC | 10. Write | Persist as a Delta table in `ecommerce_enriched` |

# COMMAND ----------

from pyspark.sql import functions as F

ENRICHED_DB = DB_DICT["ENRICHED_DB"]

def execute_enrichment_transformation(table_key: str) -> None:
    conf = ENRICHED_TABLE_CONFIG[table_key]

    df = read_table(conf["bronze"], conf)           # Step 1: Read latest batch
    df = rename_to_snake_case(df)                   # Step 2: snake_case columns
    df = trim_string_values(df)                     # Step 3: Trim whitespace
    df = apply_enriched_transformations(df, conf, ENRICHED_DB)  # Step 4: Business logic
    df = handle_duplicates(df, conf["pk"], table_key)           # Step 5: Deduplication

    if "joins" in conf:                             # Step 6: Left joins (orders only)
        df = execute_left_joins(df, conf["joins"], ENRICHED_DB)

    df = handle_missing_values(df, conf["pk"])      # Step 7: Fill nulls
    df_final = apply_schema_contract(df, conf["select_cols"])   # Step 8: Schema contract
    validate_layer_integrity(                       # Step 9: Integrity audit
        df_final, table_key, conf.get("dq_null_cols", conf["pk"])
    )
    write_delta_table(df_final, ENRICHED_TABLE_CONFIG, table_key)  # Step 10: Write

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrichment Loop
# MAGIC
# MAGIC Processes tables in the order defined in `ENRICHED_TABLE_CONFIG`:
# MAGIC **Customers → Products → Orders**
# MAGIC
# MAGIC This order matters — Customers and Products must exist before Orders runs its left joins.
# MAGIC On failure the pipeline aborts to prevent incomplete enriched data flowing into the aggregation layer.

# COMMAND ----------

failed_tables = []

for table_key in ENRICHED_TABLE_CONFIG:
    try:
        execute_enrichment_transformation(table_key)
        print(f"COMPLETED: {table_key}\n")
    except Exception as err:
        failed_tables.append(table_key)
        print(f"FAILED: {table_key} | Error: {err}")
        break

if failed_tables:
    raise RuntimeError(
        f"Enrichment layer incomplete. Failed at: {failed_tables}. "
        "Fix the issue and re-run this notebook."
    )
else:
    print("\nAll enrichment tables written and validated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries
# MAGIC Spot-check the enriched output before proceeding to `03_Aggregation`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_enriched.Customers LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_enriched.Products LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_enriched.Orders LIMIT 10

# COMMAND ----------

