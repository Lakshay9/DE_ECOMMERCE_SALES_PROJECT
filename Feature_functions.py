# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC All business logic for the Enrichment and Aggregation layers.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Column Standardisation — snake_case, trimming
# MAGIC 2. Domain-Specific Cleaning — names, emails, phones, postal codes
# MAGIC 3. Transformations — date parsing, rounding, orchestration
# MAGIC 4. Data Quality — deduplication, null handling, audit
# MAGIC 5. Aggregation — groupBy + metrics

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import expr
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Column Standardisation

# COMMAND ----------

def rename_to_snake_case(df):
    """
    Renames all DataFrame columns to snake_case.

    Steps applied in order:
      1. CamelCase split  — inserts underscore between lowercase and uppercase letters
      2. Delimiter replace — hyphens, dots, and spaces become underscores
      3. Character cleanup — removes non-alphanumeric chars, strips edge underscores
      4. Collapse doubles  — consecutive underscores collapsed to one

    Examples:
      "Price per product" -> "price_per_product"
      "Sub-Category"      -> "sub_category"
      "Customer ID"       -> "customer_id"
    """
    for col_name in df.columns:
        new_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)
        new_name = re.sub(r'[-\.\s]+', '_', new_name).lower()
        new_name = re.sub(r'[^a-z0-9_]', '', new_name).strip('_')
        new_name = re.sub(r'_+', '_', new_name)
        df = df.withColumnRenamed(col_name, new_name)
    return df

# COMMAND ----------

def trim_string_values(df):
    """
    Trims leading and trailing whitespace from all StringType columns.
    Applied immediately after snake_case renaming, before any business logic.
    """
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — Domain-Specific Cleaning

# COMMAND ----------

def validate_and_clean_names(df, name_col: str):
    """
    Cleans customer name columns by removing invalid characters and
    collapsing irregular spacing.

    Steps:
      1. Remove digits and special characters, preserving letters, spaces,
         and apostrophes (e.g. to retain names like "Mary O'Rourke").
      2. Collapse multiple consecutive spaces to a single space and trim.

    Examples:
      "Gary567 Hansen" -> "Gary Hansen"
      "Pete@#$ Jones"  -> "Pete Jones"
      "Mary O'Rourke"  -> "Mary O'Rourke"  (apostrophe preserved)
    """
    if name_col in df.columns:
        print(f"   [DQ] Cleaning name column: {name_col}")
        df = df.withColumn(name_col, F.regexp_replace(F.col(name_col), r"[^a-zA-Z\s']", ""))
        df = df.withColumn(name_col, F.trim(F.regexp_replace(F.col(name_col), r"\s+", " ")))
    return df

# COMMAND ----------

def validate_emails(df, email_col: str):
    """
    Validates email addresses against a simplified RFC 5322 pattern.
    Rows that do not match are overwritten with "not valid" rather than
    dropped, so they remain visible for downstream review.

    Valid format: local@domain.tld
    Invalid examples: "not-an-email", "user@", "userexample.com"
    """
    if email_col in df.columns:
        print(f"   [DQ] Validating email format: {email_col}")
        email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        df = df.withColumn(
            email_col,
            F.when(F.col(email_col).rlike(email_regex), F.col(email_col))
             .otherwise(F.lit("not valid"))
        )
    return df

# COMMAND ----------

def validate_and_clean_phone(df, phone_col: str):
    """
    Normalises phone numbers to a clean 10-digit string.

    Steps:
      1. Replace Excel error values (#ERROR!) with null.
      2. Strip all non-numeric characters.
      3. Remove country code prefixes (001 or leading 1 if total > 10 digits).
      4. Keep first 10 digits; numbers shorter than 10 are set to null.

    Examples:
      "421.580.0902x9815" -> "4215800902"
      "001-542-415-0246"  -> "5424150246"
      "17185624866"       -> "7185624866"
      "12345"             -> None  (too short)
      "#ERROR!"           -> None
    """
    if phone_col in df.columns:
        print(f"   [DQ] Cleaning phone column: {phone_col}")

        # Step 1: Null out Excel error values
        df = df.withColumn(
            phone_col,
            F.when(F.col(phone_col).contains("#ERROR!"), F.lit(None))
             .otherwise(F.regexp_replace(F.col(phone_col), r"\D", ""))
        )

        # Step 2: Strip country code prefixes
        df = df.withColumn(
            phone_col,
            F.when(F.col(phone_col).startswith("001"),
                   F.expr(f"substring({phone_col}, 4)"))
             .when(F.col(phone_col).startswith("1") & (F.length(F.col(phone_col)) > 10),
                   F.expr(f"substring({phone_col}, 2)"))
             .otherwise(F.col(phone_col))
        )

        # Step 3: Keep first 10 digits; anything shorter is invalid
        df = df.withColumn(
            phone_col,
            F.when(F.length(F.col(phone_col)) >= 10, F.substring(F.col(phone_col), 1, 10))
             .otherwise(F.lit(None))
        )
    return df

# COMMAND ----------

def apply_zfill_postal(df, col_name: str = "postal_code"):
    """
    Left-pads postal codes with zeros to ensure a consistent 5-digit format.

    Example: "1234" -> "01234",  "123" -> "00123",  "12345" -> "12345"
    """
    if col_name in df.columns:
        df = df.withColumn(col_name, F.lpad(F.col(col_name).cast("string"), 5, "0"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Transformations

# COMMAND ----------

def apply_enriched_transformations(df, conf: dict, db_name: str):
    """
    Orchestrates all transformation steps for a given enriched table based on
    which keys are present in its config entry.

    Execution order:
      - Cleaning (names, emails, phones) runs before date parsing and rounding.
      - order_year is derived immediately after order_date is parsed.
      - Postal zero-fill is independent and runs after other cleaning.
      - Rounding is the final numeric step before the schema contract is applied.
    """
    if "name_col" in conf:
        df = validate_and_clean_names(df, conf["name_col"])

    if "email_col" in conf:
        df = validate_emails(df, conf["email_col"])

    if "phone_col" in conf:
        df = validate_and_clean_phone(df, conf["phone_col"])

    if "date_formats" in conf:
        df = parse_dates(df, conf["date_formats"])
        # Derive order year immediately after parsing
        if "order_date" in df.columns:
            df = df.withColumn("order_year", F.year(F.col("order_date")))

    if "postal_col" in conf:
        df = apply_zfill_postal(df, conf["postal_col"])

    if "round_cols" in conf:
        for col, precision in conf["round_cols"].items():
            df = apply_rounding(df, col, precision)

    return df

# COMMAND ----------

def parse_dates(df, date_cols: dict):
    """
    Parses string columns to DateType using the format strings provided.

    Uses try_to_date (via expr) instead of to_date because Databricks runs
    with ANSI mode enabled by default. In ANSI mode, to_date raises a
    DateTimeException on malformed input rather than returning null.
    try_to_date returns null for unparseable values, which is the correct
    behaviour for a robust pipeline.

    Note: d/M/yyyy (not dd/MM/yyyy) is the correct format for this dataset.
    Source dates use non-padded values: "21/8/2016", not "21/08/2016".

    Parameters:
        date_cols — dict of {column_name: format_string}
    """
    for col_name, fmt in date_cols.items():
        df = df.withColumn(col_name, expr(f"try_to_date(`{col_name}`, '{fmt}')"))
    return df

# COMMAND ----------

def apply_rounding(df, col_name: str, precision: int = 2):
    """
    Rounds a numeric column to the specified number of decimal places.
    No-ops silently if the column is not present in the DataFrame.
    """
    if col_name in df.columns:
        df = df.withColumn(col_name, F.round(F.col(col_name), precision))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Data Quality

# COMMAND ----------

def handle_duplicates(df, pk: list, label: str):
    """
    Removes duplicate rows based on the primary key columns and logs
    how many were dropped. A count of zero means no duplicates were found.
    """
    before = df.count()
    df_out = df.dropDuplicates(pk)
    after  = df_out.count()
    if before != after:
        print(f"   [DQ] {label}: Dropped {before - after:,} duplicate rows.")
    return df_out

# COMMAND ----------

def handle_missing_values(df, pk_cols: list):
    """
    Fills null values in non-primary-key columns using a type-based strategy:
      - StringType  -> "Unknown"
      - Numeric     -> 0

    PK columns are excluded to preserve null detection in the downstream audit.
    """
    fill_cols = [c for c in df.columns if c not in pk_cols]
    fill_map  = {}

    for col_name, dtype in df.dtypes:
        if col_name in fill_cols:
            if dtype == "string":
                fill_map[col_name] = "Unknown"
            elif dtype in ["int", "double", "float", "long"]:
                fill_map[col_name] = 0

    print(f"   [DQ] Filling nulls in {len(fill_map)} non-PK columns.")
    return df.fillna(fill_map)

# COMMAND ----------

def validate_layer_integrity(df, table_name: str, pk_columns):
    """
    Post-transformation audit check for the Enrichment layer.

    Checks:
      - Row count: raises an exception if the output is empty (critical failure)
      - Null check: logs a warning if any of the specified columns contain nulls
                    (non-blocking — the record is retained but flagged)
    """
    if isinstance(pk_columns, str):
        pk_columns = [pk_columns]

    row_count   = df.count()
    null_filter = " OR ".join([f"`{c}` IS NULL" for c in pk_columns])
    null_count  = df.filter(null_filter).count()

    print(f"--- Enrichment Audit: {table_name.upper()} ---")
    print(f"  Total Rows                   : {row_count:,}")
    print(f"  Nulls in {pk_columns}: {null_count:,}")

    if row_count == 0:
        raise Exception(f"Critical Error: [{table_name}] is empty after enrichment. Aborting pipeline.")
    if null_count > 0:
        print(f"  [DQ WARNING] {null_count} records have missing values in critical columns.")

    print("-" * 40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — Aggregation

# COMMAND ----------

def apply_aggregation(df, conf: dict):
    """
    Groups a DataFrame and applies metric functions as declared in AGG_TABLE_CONFIG.

    Supported metric functions:
      "sum"          — F.sum, rounded to 2 decimal places, aliased as total_{col}
      "countDistinct"— count(distinct col), aliased as {col}_distinct_count
      "count"        — F.count, aliased as {col}_count

    Raises ValueError for any unsupported metric so misconfiguration is caught
    immediately rather than producing a silently wrong result.
    """
    agg_exprs = []

    for col_name, func in conf["metrics"].items():
        if func == "sum":
            agg_exprs.append(
                F.round(F.sum(F.col(col_name)), 2).alias(f"total_{col_name}")
            )
        elif func == "countDistinct":
            agg_exprs.append(
                F.expr(f"count(distinct {col_name})").alias(f"{col_name}_distinct_count")
            )
        elif func == "count":
            agg_exprs.append(
                F.count(F.col(col_name)).alias(f"{col_name}_count")
            )
        else:
            raise ValueError(
                f"[apply_aggregation] Unsupported metric '{func}' for column '{col_name}'. "
                f"Supported values: 'sum', 'count', 'countDistinct'."
            )

    df_agg = df.groupBy(*conf["group_by"]).agg(*agg_exprs)

    if "order_by" in conf:
        df_agg = df_agg.orderBy(*conf["order_by"])

    return df_agg