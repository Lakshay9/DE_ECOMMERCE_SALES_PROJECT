# Databricks notebook source
# MAGIC %md
# MAGIC # Common Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Infrastructure utilities shared across all three pipeline layers.
# MAGIC Covers catalog/database setup, reading source files, writing Delta tables, joining enriched tables, and running audit checks.
# MAGIC
# MAGIC **Depends on:** `openpyxl` (installed below)

# COMMAND ----------

# openpyxl is required by pandas to read .xlsx files.
%pip install openpyxl

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog / Database Setup

# COMMAND ----------

def create_catalog_if_not_exists(catalog: str) -> None:
    """
    Creates a Unity Catalog if it does not already exist.
    Called at the start of each pipeline notebook to ensure the target
    catalog is available before any tables are read or written.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")


def create_database_if_not_exists(db_name: str) -> None:
    """
    Creates a database (schema) within the active catalog if it does not exist.
    Called once per pipeline layer before any tables are created in that layer.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Helper

# COMMAND ----------

def get_spark_schema(schema_config: list) -> StructType:
    """
    Converts the constant schema definition format [(col_name, type_str), ...]
    into a Spark StructType for use with DataFrameReader.

    Supported type strings: "string", "int", "double"
    Raises KeyError if an unsupported type is provided.

    Example:
        get_spark_schema([("Order ID", "string"), ("Quantity", "int")])
    """
    type_map = {
        "string": StringType(),
        "int"   : IntegerType(),
        "double": DoubleType()
    }
    return StructType([
        StructField(col_name, type_map[col_type.lower()], True)
        for col_name, col_type in schema_config
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source File Reader

# COMMAND ----------

def read_source(file_path: str, table_conf: dict, table_key: str):
    """
    Reads a source file into a Spark DataFrame using the format and options
    defined in BRONZE_TABLE_CONFIGS.

    Supported formats: csv, json, xlsx
    For XLSX: pandas reads the file on the driver then converts to Spark DataFrame.
    All columns are read as strings by pandas to prevent type coercion on IDs
    and postal codes before the schema is applied.

    Appends an ingested_at timestamp column to every table for use by the
    Enrichment layer's latest-batch filter.
    """
    try:
        conf   = table_conf.get(table_key)
        schema = get_spark_schema(conf["schema"])
        print(f"Reading [{table_key}] from: {file_path}")

        if conf["format"] == "xlsx":
            # pandas bridge — suitable for files that fit in driver memory.
            # dtype=str prevents pandas from coercing numeric-looking strings.
            pdf = pd.read_excel(file_path, dtype=str)
            df  = spark.createDataFrame(pdf, schema=schema)
        else:
            df = (
                spark.read
                .format(conf["format"])
                .schema(schema)
                .options(**conf["read_options"])
                .load(file_path)
            )

        return df.withColumn("ingested_at", F.current_timestamp())

    except Exception as e:
        print(f"FATAL READ ERROR [{table_key}]: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Reader

# COMMAND ----------

def read_table(table_name: str, config: dict = None):
    """
    Reads a Delta table and optionally filters to only the latest ingestion batch.

    When filter_latest is True, finds the maximum value of latest_by_col
    (typically ingested_at) and keeps only rows from that batch. This prevents
    re-running the ingestion layer from causing the enrichment layer to process
    duplicate data. The timestamp column is dropped after filtering.

    If the sort column is missing from the DataFrame, filtering is skipped
    with a warning.
    """
    print(f"Reading: {table_name}")
    df = spark.table(table_name)

    if config and config.get("filter_latest"):
        sort_col = config.get("latest_by_col")

        if sort_col in df.columns:
            print(f"   [FILTER] Keeping latest batch only — based on: {sort_col}")
            latest_ts = df.select(F.max(sort_col)).first()[0]
            df = df.filter(F.col(sort_col) == latest_ts).drop(sort_col)
        else:
            print(f"   [WARNING] Column '{sort_col}' not found — skipping latest-batch filter.")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Writer

# COMMAND ----------

def write_delta_table(df, config: dict, table_key: str) -> None:
    """
    Writes a DataFrame to a Delta table using the write_config defined in the
    layer configuration.

    overwriteSchema is always set to True so that schema changes do not cause
    the write to fail on re-runs.

    delta_options (column mapping, reader/writer versions) are only present in
    the ingestion layer config, where source column names contain spaces.
    Enrichment and aggregation tables use snake_case names and do not need
    column mapping.
    """
    try:
        w_conf = config[table_key]["write_config"]
        writer = df.write.format("delta").mode(w_conf["mode"])

        # Required to allow schema evolution across pipeline re-runs
        writer = writer.option("overwriteSchema", "true")

        # Apply any Delta table properties defined in the config (e.g. column mapping)
        for opt_key, opt_val in w_conf.get("delta_options", {}).items():
            writer = writer.option(opt_key, opt_val)

        writer.saveAsTable(table_key)
        print(f"SUCCESS: [{table_key}] written successfully.")

    except Exception as e:
        print(f"FATAL WRITE ERROR [{table_key}]: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrichment Join Helper

# COMMAND ----------

def execute_left_joins(df, join_list: list, db_name: str):
    """
    Executes a sequence of left joins against enriched tables as defined in the
    Orders config under the 'joins' key.

    Left joins are used so that orders with unresolvable customer or product IDs
    are retained in the output rather than silently dropped. These rows will have
    null values in the joined columns and are visible in the audit report.

    Parameters:
        df        — The orders DataFrame to join onto
        join_list — List of join specs from ENRICHED_TABLE_CONFIG['Orders']['joins']
        db_name   — The enriched database name (used to fully-qualify table references)
    """
    for j in join_list:
        join_df = spark.table(f"{db_name}.{j['target']}").select(*j["select"])
        df = df.join(join_df, on=j["on"], how="left")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Functions

# COMMAND ----------

def validate_bronze_layer(df, table_name: str, pk_column: list) -> None:
    """
    Runs a post-write audit check after each table is ingested.

    Checks:
      - Row count : raises an exception if the table is empty (critical failure)
      - Null PKs  : logs a warning if any primary key columns contain nulls (non-blocking)
    """
    row_count = df.count()
    null_pks  = df.filter(" OR ".join([f"`{c}` IS NULL" for c in pk_column])).count()

    print(f"--- Ingestion Audit: {table_name} ---")
    print(f"  Total Rows     : {row_count:,}")
    print(f"  Null PKs {pk_column}: {null_pks:,}")

    if row_count == 0:
        raise Exception(f"Data Load Failure: [{table_name}] is empty. Check source file.")
    if null_pks > 0:
        print(f"  [WARNING] {null_pks} records have missing primary key values.")

    print("-" * 40)


def apply_schema_contract(df, select_list: list):
    """
    Enforces the final output schema by selecting only the columns listed in
    select_cols from the enrichment config. Any intermediate columns added
    during transformation are dropped here.

    Also appends an enriched_at timestamp to record when the row was processed.
    """
    print(f"   [CONTRACT] Selecting {len(select_list)} columns for output schema.")
    return (
        df.select(*select_list)
          .withColumn("enriched_at", F.current_timestamp())
    )