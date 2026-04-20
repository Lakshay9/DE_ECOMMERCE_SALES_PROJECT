# E-Commerce Sales Data Processing with Databricks

This project implements a scalable, end-to-end data engineering pipeline for an e-commerce platform using Databricks, PySpark, and Delta Lake. It follows a medallion architecture — Bronze → Enriched → Gold — processing sales data across three source datasets (9,994 orders, 1,851 products, 793 customers) stored in a Unity Catalog (ecommerce).

Bronze layer (ecommerce_raw) ingests raw source files — JSON, CSV, and Excel — with strict schema enforcement, column mapping for space-containing field names, and ingestion timestamps for idempotent re-runs.

Enriched layer (ecommerce_enriched) applies comprehensive data quality transformations: customer name cleaning, phone number standardisation, email validation, postal code padding, date parsing from d/M/yyyy format, and order year extraction. A three-way left join produces a master orders table combining order details with customer name, country, product category, and sub-category — preserving unmatched records rather than dropping them.

Gold layer (ecommerce_agg) aggregates profit by year, product category, sub-category, and customer — with profit rounded to 2 decimal places and distinct order counts corrected for multi-line orders. Four SQL rollup queries serve business reporting needs.

The pipeline is config-driven throughout — all schemas, transformations, join definitions, and write options are declared in a central Configs notebook — making it extensible without modifying pipeline logic. A standalone Pytest_Ecommerce test suite validates every transformation layer using in-memory DataFrames, with no dependency on live Delta tables.
