# Databricks notebook source
# MAGIC %md
# MAGIC # Constants

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Central store for all hard-coded values used across the pipeline.
# MAGIC Import this notebook first in every other notebook via `%run`.
# MAGIC
# MAGIC Contains: Catalog name, database keys, volume path, source schemas, PK maps

# COMMAND ----------

# ── Catalog ───────────────────────────────────────────────────────────────────
# Unity Catalog name that owns all three pipeline databases.
catalog = "ecommerce"

# COMMAND ----------

# ── Database Names ────────────────────────────────────────────────────────────
# Keyed dictionary so orchestration notebooks reference layers by logical name
# rather than hard-coded strings. Add new layers here if the pipeline grows.
DB_DICT = {
    "RAW_DB"        : "ecommerce_raw",       # Ingestion layer  — raw source data
    "ENRICHED_DB"   : "ecommerce_enriched",  # Enrichment layer — cleaned & joined
    "AGGREGATED_DB" : "ecommerce_agg"        # Aggregation layer — pre-aggregated rollups
}

# COMMAND ----------

# ── Source Volume Path ────────────────────────────────────────────────────────
# Unity Catalog volume where raw source files (CSV / JSON / XLSX) are uploaded.
base_path = "/Volumes/pei_assesment/raw/files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Schema Definitions
# MAGIC Each schema is a list of `(column_name, spark_type)` tuples.
# MAGIC Column names match the source files exactly — snake_case conversion happens in the Enrichment layer.
# MAGIC
# MAGIC Supported types: `string`, `int`, `double`

# COMMAND ----------

# Products.csv — 1,851 rows, composite PK: Product ID + Product Name + State
BRONZE_PRODUCT_SCHEMA = [
    ("Product ID",        "string"),
    ("Category",          "string"),
    ("Sub-Category",      "string"),
    ("Product Name",      "string"),
    ("State",             "string"),
    ("Price per product", "double")
]

# COMMAND ----------

# Customers.xlsx — read via pandas bridge; all columns ingested as strings
# to preserve leading zeros in Postal Code and avoid type coercion on phone numbers.
BRONZE_CUSTOMER_SCHEMA = [
    ("Customer ID",   "string"),
    ("Customer Name", "string"),
    ("email",         "string"),
    ("phone",         "string"),
    ("address",       "string"),
    ("Segment",       "string"),
    ("Country",       "string"),
    ("City",          "string"),
    ("State",         "string"),
    ("Postal Code",   "string"),
    ("Region",        "string")
]

# COMMAND ----------

# Orders.json — 9,994 rows; Order Date and Ship Date kept as strings at ingestion.
# They are parsed to DateType in the Enrichment layer using try_to_date.
BRONZE_ORDER_SCHEMA = [
    ("Row ID",      "int"),
    ("Order ID",    "string"),
    ("Order Date",  "string"),
    ("Ship Date",   "string"),
    ("Ship Mode",   "string"),
    ("Customer ID", "string"),
    ("Product ID",  "string"),
    ("Quantity",    "int"),
    ("Price",       "double"),
    ("Discount",    "double"),
    ("Profit",      "double")
]

# COMMAND ----------

# ── Primary Key Map (Ingestion layer audit) ───────────────────────────────────
# Products uses a composite PK because the same Product ID can appear in
# multiple states with different pricing.
pk_column = {
    "products"  : ["Product ID", "Product Name", "State"],
    "customers" : ["Customer ID"],
    "orders"    : ["Row ID"]
}