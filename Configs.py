# Databricks notebook source
# MAGIC %md
# MAGIC # Configs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Declares all per-table configuration for the three pipeline layers.
# MAGIC Orchestration notebooks loop over these configs generically — no business logic is hard-coded in the notebooks.
# MAGIC
# MAGIC **Depends on:** `Constants.py` must be `%run` before this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Layer Configuration
# MAGIC
# MAGIC | Key | Description |
# MAGIC |---|---|
# MAGIC | `format` | Spark read format (`csv`, `json`, `xlsx`) |
# MAGIC | `file_name` | Base filename without extension |
# MAGIC | `schema` | Column definitions from Constants.py |
# MAGIC | `read_options` | Passed directly to `DataFrameReader.options()` |
# MAGIC | `pk` | Primary key columns for the post-write audit |
# MAGIC | `write_config` | Delta write mode and table properties |
# MAGIC
# MAGIC > `delta.columnMapping.mode = name` is required for all ingestion tables because source column names contain spaces.

# COMMAND ----------

BRONZE_TABLE_CONFIGS = {
    "products": {
        "format"      : "csv",
        "file_name"   : "Products",
        "schema"      : BRONZE_PRODUCT_SCHEMA,
        "read_options": {
            "header"   : "true",
            "multiLine": "true",
            "escape"   : '"'       # Handles quoted fields containing commas
        },
        "pk": ["Product ID", "Product Name", "State"],
        "write_config": {
            "mode": "overwrite",
            "delta_options": {
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion"  : "2",
                "delta.minWriterVersion"  : "5"
            }
        }
    },

    "customers": {
        "format"      : "xlsx",
        "file_name"   : "Customers",
        "schema"      : BRONZE_CUSTOMER_SCHEMA,
        "read_options": {},        # XLSX is read via pandas; no Spark read options apply
        "pk"          : ["Customer ID"],
        "write_config": {
            "mode": "overwrite",
            "delta_options": {
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion"  : "2",
                "delta.minWriterVersion"  : "5"
            }
        }
    },

    "orders": {
        "format"      : "json",
        "file_name"   : "Orders",
        "schema"      : BRONZE_ORDER_SCHEMA,
        "read_options": {"multiLine": "true"},  # Source is a single JSON array, not JSONL
        "pk"          : ["Row ID"],
        "write_config": {
            "mode": "overwrite",
            "delta_options": {
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion"  : "2",
                "delta.minWriterVersion"  : "5"
            }
        }
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrichment Layer Configuration
# MAGIC
# MAGIC | Key | Description |
# MAGIC |---|---|
# MAGIC | `bronze` | Fully-qualified source table in the ingestion layer |
# MAGIC | `pk` | Primary key columns (snake_case; applied after renaming) |
# MAGIC | `filter_latest` | If True, only the most recently ingested batch is processed |
# MAGIC | `latest_by_col` | Column used to identify the latest batch (`ingested_at`) |
# MAGIC | `name_col` | Triggers name cleaning (digits / special chars removed) |
# MAGIC | `email_col` | Triggers email format validation |
# MAGIC | `phone_col` | Triggers phone number cleaning and 10-digit normalisation |
# MAGIC | `postal_col` | Triggers zero-padding to 5 digits |
# MAGIC | `dq_null_cols` | Columns checked for nulls in the post-enrichment audit |
# MAGIC | `date_formats` | `{column: format}` pairs; parsed using `try_to_date` (null-safe) |
# MAGIC | `round_cols` | `{column: precision}` pairs for numeric rounding |
# MAGIC | `joins` | Left joins to enriched customers/products tables |
# MAGIC | `select_cols` | Final schema contract; only these columns are written out |

# COMMAND ----------

ENRICHED_TABLE_CONFIG = {
    "Customers": {
        "bronze"       : "ecommerce_raw.customers",
        "pk"           : ["customer_id"],
        "filter_latest": True,
        "latest_by_col": "ingested_at",
        "name_col"     : "customer_name",   # Clean corrupted names e.g. "Gary567 Hansen"
        "email_col"    : "email",           # Flag malformed emails as "not valid"
        "phone_col"    : "phone",           # Strip formatting, normalise to 10 digits
        "postal_col"   : "postal_code",     # Zero-pad short codes e.g. "1234" -> "01234"
        "select_cols"  : [
            "customer_id", "customer_name", "email", "phone",
            "address", "segment", "country", "city",
            "state", "postal_code", "region"
        ],
        "write_config": {"mode": "overwrite"}
    },

    "Products": {
        "bronze"       : "ecommerce_raw.products",
        "pk"           : ["product_id", "product_name", "state"],
        "filter_latest": True,
        "latest_by_col": "ingested_at",
        "select_cols"  : [
            "product_id", "category", "sub_category",
            "product_name", "state", "price_per_product"
        ],
        "write_config": {"mode": "overwrite"}
    },

    "Orders": {
        "bronze"       : "ecommerce_raw.orders",
        "pk"           : ["row_id"],
        "filter_latest": True,
        "latest_by_col": "ingested_at",
        # Columns that must not be null for a valid order record
        "dq_null_cols" : ["order_id", "customer_id", "product_id"],
        # Source dates are non-padded: "21/8/2016" not "21/08/2016"
        # d/M/yyyy handles both single and double digit day/month values
        "date_formats" : {"order_date": "d/M/yyyy", "ship_date": "d/M/yyyy"},
        "round_cols"   : {"profit": 2},
        # Left joins — unmatched FKs are retained with null category/customer fields
        "joins": [
            {"target": "customers", "on": "customer_id",
             "select": ["customer_id", "customer_name", "country"]},
            {"target": "products",  "on": "product_id",
             "select": ["product_id", "category", "sub_category"]}
        ],
        "select_cols": [
            "row_id", "order_id", "order_date", "order_year", "ship_date",
            "ship_mode", "customer_id", "product_id", "quantity", "price",
            "discount", "profit", "customer_name", "country", "category", "sub_category"
        ],
        "write_config": {"mode": "overwrite"}
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation Layer Configuration
# MAGIC
# MAGIC The `profit_by_year_category_customer` table is the single source for all four SQL rollups in `SQL Queries.py`.
# MAGIC Re-aggregating at query time (`SUM of total_profit`) avoids maintaining separate pre-computed tables for every rollup combination.
# MAGIC
# MAGIC | Key | Description |
# MAGIC |---|---|
# MAGIC | `source` | Fully-qualified enriched source table |
# MAGIC | `group_by` | Dimensions to group by |
# MAGIC | `metrics` | `{column: function}` — supported: `sum`, `count`, `countDistinct` |
# MAGIC | `order_by` | Sort order applied after aggregation |
# MAGIC | `write_config` | Delta write mode |

# COMMAND ----------

AGG_TABLE_CONFIG = {
    "profit_by_year_category_customer": {
        "source"  : "ecommerce_enriched.orders",
        "group_by": ["order_year", "category", "sub_category", "customer_id", "customer_name"],
        "metrics" : {
            "profit"  : "sum",           # Summed and rounded to 2dp by apply_aggregation
            "order_id": "countDistinct"  # Distinct order count per group
        },
        "order_by"    : ["order_year", "category", "sub_category", "customer_name"],
        "write_config": {"mode": "overwrite"}
    }
}