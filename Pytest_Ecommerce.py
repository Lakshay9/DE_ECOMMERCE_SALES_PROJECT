# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Pipeline — Unit Test(Pytest)
# MAGIC Standalone pytest notebook. Run independently of the pipeline — no Delta tables required.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Common Functions — `get_spark_schema`, `validate_bronze_layer`, `read_table`
# MAGIC 2. Feature Functions — cleaning, validation, aggregation helpers
# MAGIC 3. Enrichment Layer — join correctness, profit rounding, year extraction
# MAGIC 4. Aggregation Layer — SQL rollups, grain validation, distinct order counts
# MAGIC 5. Full Suite Summary

# COMMAND ----------

# MAGIC %pip install pytest ipytest --quiet

# COMMAND ----------

import ipytest
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import date

ipytest.autoconfig()
print("pytest + ipytest ready.")

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
# MAGIC ---
# MAGIC ## Section 1 — Common Functions
# MAGIC Tests for `get_spark_schema`, `validate_bronze_layer`, and `read_table`.

# COMMAND ----------

# MAGIC %%ipytest -v
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Fixtures
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def simple_customer_df():
# MAGIC     return spark.createDataFrame(
# MAGIC         [("C001", "Alice"), ("C002", "Bob")],
# MAGIC         ["Customer ID", "Customer Name"]
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def empty_df():
# MAGIC     return spark.createDataFrame(
# MAGIC         [], StructType([StructField('Customer ID', StringType(), True)])
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def null_pk_df():
# MAGIC     return spark.createDataFrame(
# MAGIC         [(None, "Alice"), ("C002", "Bob")],
# MAGIC         ["Customer ID", "Customer Name"]
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def latest_filter_df():
# MAGIC     """Creates a temp view with two batches to test filter_latest logic."""
# MAGIC     df = spark.createDataFrame(
# MAGIC         [("2024-01-01 10:00:00", "C001"), ("2024-01-02 11:00:00", "C002")],
# MAGIC         ["ingested_at", "customer_id"]
# MAGIC     ).withColumn('ingested_at', F.to_timestamp('ingested_at'))
# MAGIC     df.createOrReplaceTempView("test_bronze_customers")
# MAGIC     return df
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # get_spark_schema
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestGetSparkSchema:
# MAGIC
# MAGIC     def test_valid_schema_returns_struct_type(self):
# MAGIC         schema = get_spark_schema([("name", "string"), ("age", "int"), ("salary", "double")])
# MAGIC         assert isinstance(schema, StructType)
# MAGIC         assert len(schema.fields) == 3
# MAGIC
# MAGIC     def test_string_field_maps_correctly(self):
# MAGIC         schema = get_spark_schema([("name", "string")])
# MAGIC         assert isinstance(schema.fields[0].dataType, StringType)
# MAGIC
# MAGIC     def test_int_field_maps_correctly(self):
# MAGIC         schema = get_spark_schema([("qty", "int")])
# MAGIC         assert isinstance(schema.fields[0].dataType, IntegerType)
# MAGIC
# MAGIC     def test_double_field_maps_correctly(self):
# MAGIC         schema = get_spark_schema([("price", "double")])
# MAGIC         assert isinstance(schema.fields[0].dataType, DoubleType)
# MAGIC
# MAGIC     def test_unknown_type_raises_key_error(self):
# MAGIC         """Unsupported types like date should raise a KeyError."""
# MAGIC         with pytest.raises(KeyError):
# MAGIC             get_spark_schema([("dt", "date")])
# MAGIC
# MAGIC     def test_field_names_preserved(self):
# MAGIC         schema = get_spark_schema([("Customer ID", "string"), ("Order Date", "string")])
# MAGIC         assert schema.fields[0].name == "Customer ID"
# MAGIC         assert schema.fields[1].name == "Order Date"
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # validate_bronze_layer
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestValidateBronzeLayer:
# MAGIC
# MAGIC     def test_valid_df_passes_without_error(self, simple_customer_df):
# MAGIC         validate_bronze_layer(simple_customer_df, "test_customers", ["Customer ID"])
# MAGIC
# MAGIC     def test_empty_df_raises_exception(self, empty_df):
# MAGIC         """An empty DataFrame must abort the pipeline."""
# MAGIC         with pytest.raises(Exception, match="(?i)empty"):
# MAGIC             validate_bronze_layer(empty_df, "test_empty", ["Customer ID"])
# MAGIC
# MAGIC     def test_null_pk_does_not_raise(self, null_pk_df):
# MAGIC         """Null PKs should log a warning but not raise — pipeline continues."""
# MAGIC         validate_bronze_layer(null_pk_df, "test_null_pk", ["Customer ID"])
# MAGIC
# MAGIC     def test_string_pk_normalised_to_list(self, simple_customer_df):
# MAGIC         """Guards against the character-iteration bug when PK is a plain string."""
# MAGIC         pk = "Customer ID"
# MAGIC         if isinstance(pk, str):
# MAGIC             pk = [pk]
# MAGIC         validate_bronze_layer(simple_customer_df, "test_str_pk", pk)
# MAGIC
# MAGIC     def test_composite_pk_columns(self):
# MAGIC         df = spark.createDataFrame(
# MAGIC             [("P001", "Chair", None), ("P002", "Desk", "NY")],
# MAGIC             ["Product ID", "Product Name", "State"]
# MAGIC         )
# MAGIC         validate_bronze_layer(df, "test_composite_pk", ["Product ID", "Product Name"])
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # read_table
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestReadTable:
# MAGIC
# MAGIC     def test_read_without_filter_returns_all_rows(self, latest_filter_df):
# MAGIC         assert spark.table("test_bronze_customers").count() == 2
# MAGIC
# MAGIC     def test_filter_latest_keeps_only_latest_batch(self, latest_filter_df):
# MAGIC         """Only rows from the most recent ingested_at value should be kept."""
# MAGIC         config = {"filter_latest": True, "latest_by_col": "ingested_at"}
# MAGIC         df = read_table("test_bronze_customers", config)
# MAGIC         assert df.count() == 1
# MAGIC         assert df.collect()[0]["customer_id"] == "C002"
# MAGIC
# MAGIC     def test_filter_latest_drops_ingested_at_column(self, latest_filter_df):
# MAGIC         config = {"filter_latest": True, "latest_by_col": "ingested_at"}
# MAGIC         df = read_table("test_bronze_customers", config)
# MAGIC         assert "ingested_at" not in df.columns
# MAGIC
# MAGIC     def test_missing_sort_col_skips_filter_gracefully(self, latest_filter_df):
# MAGIC         """If the sort column doesn't exist, filter is skipped — no crash."""
# MAGIC         config = {"filter_latest": True, "latest_by_col": "nonexistent_col"}
# MAGIC         df = read_table("test_bronze_customers", config)
# MAGIC         assert df.count() == 2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2 — Feature Functions
# MAGIC Tests for `rename_to_snake_case`, `validate_and_clean_names`, `validate_emails`, `validate_and_clean_phone`, `apply_zfill_postal`, `handle_duplicates`, `parse_dates`, and `apply_aggregation`.

# COMMAND ----------

# MAGIC %%ipytest -v
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Fixtures
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def agg_input_df():
# MAGIC     """Raw order lines used to test aggregation logic."""
# MAGIC     return spark.createDataFrame([
# MAGIC         (2016, "Furniture",       "Chairs", "C001", "Alice", "ORD-001",  100.50),
# MAGIC         (2016, "Furniture",       "Chairs", "C001", "Alice", "ORD-001",  200.75),
# MAGIC         (2016, "Technology",      "Phones", "C002", "Bob",   "ORD-002",  -50.00),
# MAGIC         (2017, "Office Supplies", "Paper",  "C001", "Alice", "ORD-003",  300.00),
# MAGIC     ], ["order_year", "category", "sub_category", "customer_id",
# MAGIC         "customer_name", "order_id", "profit"])
# MAGIC
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def agg_conf():
# MAGIC     return {
# MAGIC         "group_by": ["order_year", "category", "sub_category", "customer_id", "customer_name"],
# MAGIC         "metrics": {"profit": "sum", "order_id": "countDistinct"}
# MAGIC     }
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # rename_to_snake_case
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestRenameToSnakeCase:
# MAGIC
# MAGIC     def test_spaces_replaced_with_underscore(self):
# MAGIC         df = spark.createDataFrame([("a",)], ["Product ID"])
# MAGIC         assert "product_id" in rename_to_snake_case(df).columns
# MAGIC
# MAGIC     def test_hyphens_replaced_with_underscore(self):
# MAGIC         df = spark.createDataFrame([("a",)], ["Sub-Category"])
# MAGIC         assert "sub_category" in rename_to_snake_case(df).columns
# MAGIC
# MAGIC     def test_camel_case_split(self):
# MAGIC         df = spark.createDataFrame([("a",)], ["OrderDate"])
# MAGIC         assert "order_date" in rename_to_snake_case(df).columns
# MAGIC
# MAGIC     def test_already_snake_case_unchanged(self):
# MAGIC         df = spark.createDataFrame([("a",)], ["customer_id"])
# MAGIC         assert "customer_id" in rename_to_snake_case(df).columns
# MAGIC
# MAGIC     def test_no_double_underscores_after_rename(self):
# MAGIC         df = spark.createDataFrame([("a",)], ["Price  Per  Product"])
# MAGIC         assert all("__" not in c for c in rename_to_snake_case(df).columns)
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # validate_and_clean_names
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestValidateAndCleanNames:
# MAGIC
# MAGIC     def _clean(self, name):
# MAGIC         df = spark.createDataFrame([(name,)], ["customer_name"])
# MAGIC         return validate_and_clean_names(df, "customer_name").collect()[0]["customer_name"]
# MAGIC
# MAGIC     def test_digits_removed(self):
# MAGIC         assert self._clean("Gary567 Hansen") == "Gary Hansen"
# MAGIC
# MAGIC     def test_special_chars_removed(self):
# MAGIC         assert self._clean("Pete@#$ Takahito") == "Pete Takahito"
# MAGIC
# MAGIC     def test_extra_whitespace_collapsed(self):
# MAGIC         assert self._clean("Adam    ...Hart") == "Adam Hart"
# MAGIC
# MAGIC     def test_leading_whitespace_trimmed(self):
# MAGIC         assert not self._clean("   Tracy Blumstein").startswith(" ")
# MAGIC
# MAGIC     def test_clean_name_unchanged(self):
# MAGIC         assert self._clean("Philip Fox") == "Philip Fox"
# MAGIC
# MAGIC     def test_apostrophe_preserved(self):
# MAGIC         result = self._clean("Mary O'Rourke")
# MAGIC         assert "'" in result, "Apostrophe stripped — fix regex in validate_and_clean_names"
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # validate_emails
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestValidateEmails:
# MAGIC
# MAGIC     def _validate(self, email):
# MAGIC         df = spark.createDataFrame([(email,)], ["email"])
# MAGIC         return validate_emails(df, "email").collect()[0]["email"]
# MAGIC
# MAGIC     def test_valid_email_passes_through(self):
# MAGIC         assert self._validate("test@example.com") == "test@example.com"
# MAGIC
# MAGIC     def test_invalid_email_flagged(self):
# MAGIC         assert self._validate("not-an-email") == "not valid"
# MAGIC
# MAGIC     def test_email_missing_domain_flagged(self):
# MAGIC         assert self._validate("user@") == "not valid"
# MAGIC
# MAGIC     def test_email_missing_at_flagged(self):
# MAGIC         assert self._validate("userexample.com") == "not valid"
# MAGIC
# MAGIC     def test_valid_email_with_numbers_passes(self):
# MAGIC         assert self._validate("user123@domain99.org") == "user123@domain99.org"
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # validate_and_clean_phone
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestValidateAndCleanPhone:
# MAGIC
# MAGIC     def _clean(self, phone):
# MAGIC         df = spark.createDataFrame([(str(phone),)], ["phone"])
# MAGIC         return validate_and_clean_phone(df, "phone").collect()[0]["phone"]
# MAGIC
# MAGIC     def test_clean_10_digit_unchanged(self):
# MAGIC         assert self._clean("7185624866") == "7185624866"
# MAGIC
# MAGIC     def test_removes_non_numeric_chars(self):
# MAGIC         assert self._clean("421.580.0902") == "4215800902"
# MAGIC
# MAGIC     def test_strips_001_country_prefix(self):
# MAGIC         assert self._clean("0014215800902") == "4215800902"
# MAGIC
# MAGIC     def test_strips_leading_1_from_11_digit(self):
# MAGIC         assert self._clean("17185624866") == "7185624866"
# MAGIC
# MAGIC     def test_short_number_returns_null(self):
# MAGIC         """Numbers under 10 digits are junk and should be nulled."""
# MAGIC         assert self._clean("12345") is None
# MAGIC
# MAGIC     def test_error_value_returns_null(self):
# MAGIC         df = spark.createDataFrame([("#ERROR!",)], ["phone"])
# MAGIC         assert validate_and_clean_phone(df, "phone").collect()[0]["phone"] is None
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # apply_zfill_postal
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestApplyZfillPostal:
# MAGIC
# MAGIC     def _pad(self, code):
# MAGIC         df = spark.createDataFrame([(code,)], ["postal_code"])
# MAGIC         return apply_zfill_postal(df, "postal_code").collect()[0]["postal_code"]
# MAGIC
# MAGIC     def test_4_digit_padded_to_5(self):
# MAGIC         assert self._pad("1234") == "01234"
# MAGIC
# MAGIC     def test_5_digit_unchanged(self):
# MAGIC         assert self._pad("12345") == "12345"
# MAGIC
# MAGIC     def test_3_digit_padded_to_5(self):
# MAGIC         assert self._pad("123") == "00123"
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # handle_duplicates
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestHandleDuplicates:
# MAGIC
# MAGIC     def test_duplicates_dropped(self):
# MAGIC         df = spark.createDataFrame(
# MAGIC             [("C001", "Alice"), ("C001", "Alice"), ("C002", "Bob")],
# MAGIC             ["customer_id", "customer_name"]
# MAGIC         )
# MAGIC         assert handle_duplicates(df, ["customer_id"], "test").count() == 2
# MAGIC
# MAGIC     def test_no_duplicates_unchanged(self):
# MAGIC         df = spark.createDataFrame(
# MAGIC             [("C001", "Alice"), ("C002", "Bob")],
# MAGIC             ["customer_id", "customer_name"]
# MAGIC         )
# MAGIC         assert handle_duplicates(df, ["customer_id"], "test").count() == 2
# MAGIC
# MAGIC     def test_composite_pk_dedup(self):
# MAGIC         """Two rows with same product+name+state should collapse to one."""
# MAGIC         df = spark.createDataFrame(
# MAGIC             [("P001", "Chair", "NY"), ("P001", "Chair", "NY"), ("P001", "Chair", "CA")],
# MAGIC             ["product_id", "product_name", "state"]
# MAGIC         )
# MAGIC         assert handle_duplicates(df, ["product_id", "product_name", "state"], "test").count() == 2
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # parse_dates
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestParseDates:
# MAGIC
# MAGIC     def test_valid_date_parsed_correctly(self):
# MAGIC         df = spark.createDataFrame([("21/8/2016",)], ["order_date"])
# MAGIC         result = parse_dates(df, {"order_date": "d/M/yyyy"})
# MAGIC         assert result.collect()[0]["order_date"] == date(2016, 8, 21)
# MAGIC
# MAGIC     def test_invalid_date_returns_null(self):
# MAGIC         df = spark.createDataFrame([("not-a-date",)], ["order_date"])
# MAGIC         result = parse_dates(df, {"order_date": "d/M/yyyy"})
# MAGIC         assert result.collect()[0]["order_date"] is None
# MAGIC
# MAGIC     def test_single_digit_day_month_parsed(self):
# MAGIC         """Ensures the d/M/yyyy format handles 1/1/2017 style dates correctly."""
# MAGIC         df = spark.createDataFrame([("1/1/2017",)], ["order_date"])
# MAGIC         result = parse_dates(df, {"order_date": "d/M/yyyy"})
# MAGIC         assert result.collect()[0]["order_date"] == date(2017, 1, 1)
# MAGIC
# MAGIC     def test_missing_column_skipped_no_crash(self):
# MAGIC         """parse_dates should silently skip columns not present in the df."""
# MAGIC         df = spark.createDataFrame([("value",)], ["some_col"])
# MAGIC         parse_dates(df, {"order_date": "d/M/yyyy"})
# MAGIC
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # apply_aggregation
# MAGIC # ---------------------------------------------------------------------------
# MAGIC
# MAGIC class TestApplyAggregation:
# MAGIC
# MAGIC     def test_sum_profit_correct(self, agg_input_df, agg_conf):
# MAGIC         result = apply_aggregation(agg_input_df, agg_conf)
# MAGIC         row = result.filter(
# MAGIC             (F.col('order_year') == 2016) & (F.col('category') == 'Furniture')
# MAGIC         ).collect()[0]
# MAGIC         assert row['total_profit'] == 301.25
# MAGIC
# MAGIC     def test_profit_rounded_to_2dp(self, agg_input_df, agg_conf):
# MAGIC         result = apply_aggregation(agg_input_df, agg_conf)
# MAGIC         for row in result.collect():
# MAGIC             if row['total_profit'] is not None:
# MAGIC                 assert round(row['total_profit'], 2) == row['total_profit']
# MAGIC
# MAGIC     def test_negative_profits_included(self, agg_input_df, agg_conf):
# MAGIC         """Negative profit rows are valid business data and must not be filtered out."""
# MAGIC         result = apply_aggregation(agg_input_df, agg_conf)
# MAGIC         assert result.filter(F.col('total_profit') < 0).count() == 1
# MAGIC
# MAGIC     def test_count_distinct_orders(self, agg_input_df, agg_conf):
# MAGIC         """ORD-001 appears in 2 line items — must count as 1 distinct order."""
# MAGIC         result = apply_aggregation(agg_input_df, agg_conf)
# MAGIC         row = result.filter(
# MAGIC             (F.col('order_year') == 2016) & (F.col('category') == 'Furniture')
# MAGIC         ).collect()[0]
# MAGIC         assert row['order_id_distinct_count'] == 1
# MAGIC
# MAGIC     def test_unsupported_metric_raises_value_error(self, agg_input_df):
# MAGIC         conf = {"group_by": ["order_year"], "metrics": {"profit": "median"}}
# MAGIC         with pytest.raises(ValueError):
# MAGIC             apply_aggregation(agg_input_df, conf)
# MAGIC
# MAGIC     def test_correct_number_of_groups(self, agg_input_df, agg_conf):
# MAGIC         """Input has 3 distinct year+category+subcategory+customer combinations."""
# MAGIC         assert apply_aggregation(agg_input_df, agg_conf).count() == 3
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3 — Enrichment Layer
# MAGIC Tests for join correctness, profit rounding, year extraction, and unmatched product ID handling.
# MAGIC
# MAGIC > **Note:** P999 is intentionally absent from the products fixture to validate left-join behaviour.

# COMMAND ----------

# MAGIC %%ipytest -v
# MAGIC
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def enriched_df():
# MAGIC     """Simulates the enriched orders table output after all joins and rounding."""
# MAGIC     orders = spark.createDataFrame([
# MAGIC         (1, "CA-2016-001", date(2016, 8, 21), 2016, "C001", "P001", 573.17,  63.687),
# MAGIC         (2, "CA-2016-002", date(2016, 9,  1), 2016, "C002", "P999", 100.00, -20.123),  # unmatched product
# MAGIC         (3, "CA-2017-001", date(2017, 1,  5), 2017, "C001", "P002", 200.00,  45.678),
# MAGIC     ], ["row_id", "order_id", "order_date", "order_year",
# MAGIC         "customer_id", "product_id", "price", "profit"])
# MAGIC
# MAGIC     customers = spark.createDataFrame([
# MAGIC         ("C001", "Alice Smith", "United States"),
# MAGIC         ("C002", "Bob Jones",   "United States"),
# MAGIC     ], ["customer_id", "customer_name", "country"])
# MAGIC
# MAGIC     products = spark.createDataFrame([
# MAGIC         ("P001", "Furniture",       "Chairs"),
# MAGIC         ("P002", "Office Supplies", "Paper"),
# MAGIC         # P999 intentionally missing to test left-join null handling
# MAGIC     ], ["product_id", "category", "sub_category"])
# MAGIC
# MAGIC     return (
# MAGIC         orders
# MAGIC         .join(customers, on="customer_id", how="left")
# MAGIC         .join(products,  on="product_id",  how="left")
# MAGIC         .withColumn('profit', F.round(F.col('profit'), 2))
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC class TestOrdersEnrichment:
# MAGIC
# MAGIC     def test_profit_rounded_to_2dp(self, enriched_df):
# MAGIC         for row in enriched_df.collect():
# MAGIC             assert round(row['profit'], 2) == row['profit'], \
# MAGIC                 f"Profit not rounded to 2dp: {row['profit']}"
# MAGIC
# MAGIC     def test_order_year_extracted_correctly(self, enriched_df):
# MAGIC         assert enriched_df.filter(F.col('order_id') == 'CA-2016-001').collect()[0]['order_year'] == 2016
# MAGIC         assert enriched_df.filter(F.col('order_id') == 'CA-2017-001').collect()[0]['order_year'] == 2017
# MAGIC
# MAGIC     def test_customer_name_joined_correctly(self, enriched_df):
# MAGIC         row = enriched_df.filter(F.col('customer_id') == 'C001').first()
# MAGIC         assert row['customer_name'] == 'Alice Smith'
# MAGIC
# MAGIC     def test_unmatched_product_retained_with_null_category(self, enriched_df):
# MAGIC         """Orders with no matching product must be kept — category should be null, not dropped."""
# MAGIC         unmatched = enriched_df.filter(F.col('product_id') == 'P999')
# MAGIC         assert unmatched.count() == 1
# MAGIC         assert unmatched.collect()[0]['category'] is None
# MAGIC
# MAGIC     def test_row_count_preserved_after_left_join(self, enriched_df):
# MAGIC         """Left join must never drop any order rows."""
# MAGIC         assert enriched_df.count() == 3
# MAGIC
# MAGIC     def test_category_populated_for_matched_products(self, enriched_df):
# MAGIC         row = enriched_df.filter(F.col('product_id') == 'P001').collect()[0]
# MAGIC         assert row['category'] == 'Furniture'
# MAGIC         assert row['sub_category'] == 'Chairs'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4 — Aggregation Layer
# MAGIC Tests for all 4 SQL rollups, agg grain correctness, distinct order counts, and negative profit handling.
# MAGIC
# MAGIC Expected values:
# MAGIC - Alice 2016: `301.25` (100.50 + 200.75, both on ORD-001)
# MAGIC - Bob 2016: `-50.00`
# MAGIC - Alice 2017: `300.00`
# MAGIC - Bob 2017: `75.25`

# COMMAND ----------

# MAGIC %%ipytest -v
# MAGIC
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def agg_table_df():
# MAGIC     """Builds the gold agg table and registers it as a temp view for SQL tests."""
# MAGIC     raw = spark.createDataFrame([
# MAGIC         (2016, "Furniture",       "Chairs", "C001", "Alice", "ORD-001",  100.50),
# MAGIC         (2016, "Furniture",       "Chairs", "C001", "Alice", "ORD-001",  200.75),  # same order, 2 lines
# MAGIC         (2016, "Technology",      "Phones", "C002", "Bob",   "ORD-002",  -50.00),
# MAGIC         (2017, "Office Supplies", "Paper",  "C001", "Alice", "ORD-003",  300.00),
# MAGIC         (2017, "Technology",      "Phones", "C002", "Bob",   "ORD-004",   75.25),
# MAGIC     ], ["order_year", "category", "sub_category", "customer_id",
# MAGIC         "customer_name", "order_id", "profit"])
# MAGIC
# MAGIC     conf = {
# MAGIC         "group_by": ["order_year", "category", "sub_category", "customer_id", "customer_name"],
# MAGIC         "metrics": {"profit": "sum", "order_id": "countDistinct"}
# MAGIC     }
# MAGIC     df = apply_aggregation(raw, conf)
# MAGIC     df.createOrReplaceTempView("v_profit_agg")
# MAGIC     return df
# MAGIC
# MAGIC
# MAGIC class TestAggregationLayer:
# MAGIC
# MAGIC     def test_agg_table_correct_grain(self, agg_table_df):
# MAGIC         """Each row must be unique at year + category + subcategory + customer grain."""
# MAGIC         total    = agg_table_df.count()
# MAGIC         distinct = agg_table_df.dropDuplicates(
# MAGIC             ["order_year", "category", "sub_category", "customer_id"]
# MAGIC         ).count()
# MAGIC         assert total == distinct, "Duplicate rows found at agg grain"
# MAGIC
# MAGIC     def test_profit_by_year(self, agg_table_df):
# MAGIC         result = spark.sql("""
# MAGIC             SELECT order_year, ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC             FROM v_profit_agg
# MAGIC             GROUP BY order_year
# MAGIC             ORDER BY order_year
# MAGIC         """).collect()
# MAGIC         year_map = {r['order_year']: r['total_profit'] for r in result}
# MAGIC         assert year_map[2016] == 251.25
# MAGIC         assert year_map[2017] == 375.25
# MAGIC
# MAGIC     def test_profit_by_year_and_category(self, agg_table_df):
# MAGIC         result = spark.sql("""
# MAGIC             SELECT order_year, category, ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC             FROM v_profit_agg
# MAGIC             GROUP BY order_year, category
# MAGIC             ORDER BY order_year, category
# MAGIC         """).collect()
# MAGIC         row = next(r for r in result if r['order_year'] == 2016 and r['category'] == 'Furniture')
# MAGIC         assert row['total_profit'] == 301.25
# MAGIC
# MAGIC     def test_profit_by_customer(self, agg_table_df):
# MAGIC         result = spark.sql("""
# MAGIC             SELECT customer_id, customer_name, ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC             FROM v_profit_agg
# MAGIC             GROUP BY customer_id, customer_name
# MAGIC             ORDER BY total_profit DESC
# MAGIC         """).collect()
# MAGIC         alice = next(r for r in result if r['customer_id'] == 'C001')
# MAGIC         assert alice['total_profit'] == 601.25
# MAGIC
# MAGIC     def test_profit_by_customer_and_year(self, agg_table_df):
# MAGIC         result = spark.sql("""
# MAGIC             SELECT customer_id, customer_name, order_year,
# MAGIC                    ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC             FROM v_profit_agg
# MAGIC             GROUP BY customer_id, customer_name, order_year
# MAGIC             ORDER BY customer_id, order_year
# MAGIC         """).collect()
# MAGIC         row = next(r for r in result if r['customer_id'] == 'C001' and r['order_year'] == 2016)
# MAGIC         assert row['total_profit'] == 301.25
# MAGIC
# MAGIC     def test_negative_profits_reduce_totals(self, agg_table_df):
# MAGIC         """Bob has a -50 row in 2016 — it must reduce his total, not be excluded."""
# MAGIC         result = spark.sql("""
# MAGIC             SELECT customer_id, ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC             FROM v_profit_agg
# MAGIC             GROUP BY customer_id
# MAGIC         """).collect()
# MAGIC         bob = next(r for r in result if r['customer_id'] == 'C002')
# MAGIC         assert bob['total_profit'] == 25.25
# MAGIC
# MAGIC     def test_distinct_order_count_correct(self, agg_table_df):
# MAGIC         """ORD-001 appears in 2 line items for Alice/Furniture — must count as 1 order."""
# MAGIC         row = agg_table_df.filter(
# MAGIC             (F.col('category') == 'Furniture') & (F.col('customer_id') == 'C001')
# MAGIC         ).collect()[0]
# MAGIC         assert row['order_id_distinct_count'] == 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5 — Full Summary
# MAGIC Runs all sections in one go. Use this for a final pass before submitting or merging.

# COMMAND ----------

# Runs every test class defined above across all sections
ipytest.run("-v", "--tb=short")
