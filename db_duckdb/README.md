# db_duckdb/

This directory contains scripts and utilities for managing the DuckDB-based ETL pipeline and exporting data marts for the City Cycles analytics project.

## Overview

- **ETL and Table Management:**
  - `init_raw_tables.py`: Initializes raw tables in DuckDB for NYC and London bike share data (legacy and modern formats).
  - `load_from_s3.py`: Loads raw data from S3 Parquet files into DuckDB tables.
  - `verify_data.py`: Validates the integrity and quality of loaded raw tables.

- **Export:**
  - `export_to_s3.py`: Exports dbt-generated mart tables from DuckDB to S3 as Parquet files for dashboard consumption.

- **Utilities:**
  - `duckdb_manager.py`: Utility class for managing DuckDB connections and S3 access.
  - `utils.py`: Helper for logging memory usage during ETL operations.
  - `config/duckdb_config.py`: Central configuration for DuckDB paths, S3 URIs, and table schemas.

## Usage

1. **Initialize raw tables:**
   ```bash
   python db_duckdb/init_raw_tables.py
   ```
2. **Load data from S3:**
   ```bash
   python db_duckdb/load_from_s3.py
   ```
3. **Verify data integrity:**
   ```bash
   python db_duckdb/verify_data.py
   ```
4. **Export marts to S3:**
   ```bash
   python db_duckdb/export_to_s3.py
   ```

These steps ensure that the DuckDB database is up to date and that the dashboard has access to the latest data marts via S3-managed Parquet files. 