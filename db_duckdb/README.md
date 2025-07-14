# db_duckdb/

This directory contains scripts and utilities for managing the DuckDB-based ETL pipeline and exporting data marts for the City Cycles analytics project.

## Overview

The DuckDB ETL pipeline provides a complete data processing workflow from S3 Parquet files to analytics-ready marts:

### Core ETL Components

- **`init_raw_tables.py`**: Initializes raw tables in DuckDB for NYC and London bike share data (legacy and modern formats)
- **`load_from_s3.py`**: Loads raw data from S3 Parquet files into DuckDB tables with memory management and validation
- **`verify_data.py`**: Comprehensive data validation including integrity checks, quality metrics, and summary statistics

### Export & Distribution

- **`export_to_s3.py`**: Exports dbt-generated mart tables from DuckDB to S3 as Parquet files for dashboard consumption
  - Supports both mart tables and intermediate tables
  - Includes dry-run mode for testing
  - Exports to organized S3 structure (`marts/` and `intermediate/` folders)

### Infrastructure & Utilities

- **`duckdb_manager.py`**: Core utility class for managing DuckDB connections, S3 access, and database operations
  - Automatic S3 credential configuration
  - Memory management and optimization
  - Table creation, data loading, and query execution
  - Comprehensive table information and statistics
- **`utils.py`**: Memory usage monitoring and logging utilities
- **`config/duckdb_config.py`**: Central configuration for database paths, S3 URIs, table schemas, and validation queries

## Database Schema

The pipeline manages four raw tables that serve as the foundation for dbt transformations:

- **`raw_nyc_legacy`**: NYC bike share data (legacy format, 2013-2016)
- **`raw_nyc_modern`**: NYC bike share data (modern format, 2017-present)  
- **`raw_london_legacy`**: London bike share data (legacy format, 2010-2016)
- **`raw_london_modern`**: London bike share data (modern format, 2017-present)

## Usage

### Basic Workflow

1. **Initialize raw tables:**
   ```bash
   python db_duckdb/init_raw_tables.py
   ```

2. **Load data from S3:**
   ```bash
   # Load all tables
   python db_duckdb/load_from_s3.py
   
   # Load specific table
   python db_duckdb/load_from_s3.py --table raw_nyc_modern
   
   # Dry run to see what would be loaded
   python db_duckdb/load_from_s3.py --dry-run
   
   # Append to existing tables instead of replacing
   python db_duckdb/load_from_s3.py --append
   ```

3. **Verify data integrity:**
   ```bash
   python db_duckdb/verify_data.py
   ```

4. **Export marts to S3:**
   ```bash
   # Export all mart tables
   python db_duckdb/export_to_s3.py
   
   # Export with intermediate tables
   python db_duckdb/export_to_s3.py --include-intermediate
   
   # Dry run to see what would be exported
   python db_duckdb/export_to_s3.py --dry-run
   ```

### Advanced Features

#### Data Loading Options
- **Selective loading**: Load specific tables with `--table` flag
- **Dry run mode**: Preview operations without executing with `--dry-run`
- **Append mode**: Add to existing tables instead of replacing with `--append`
- **Memory monitoring**: Automatic memory usage tracking and logging

#### Data Validation
- **Schema validation**: Ensures data matches expected table schemas
- **Quality checks**: Null value analysis, duplicate detection, date range validation
- **Comprehensive statistics**: Row counts, file counts, unique identifiers, date ranges
- **Error reporting**: Detailed error messages and troubleshooting information

#### Export Capabilities
- **Mart tables**: Primary analytics tables for dashboard consumption
- **Intermediate tables**: Optional export of intermediate and unified tables
- **S3 organization**: Automatic organization into `marts/` and `intermediate/` folders
- **Compression**: Parquet files exported with Snappy compression for optimal performance

## Configuration

### Environment Variables
- `S3_BUCKET`: S3 bucket for data storage (default: `city-cycles-data-ctr37`)
- `AWS_ACCESS_KEY_ID`: AWS access key for S3 operations
- `AWS_SECRET_ACCESS_KEY`: AWS secret key for S3 operations
- `AWS_DEFAULT_REGION`: AWS region (default: `us-east-1`)
- `DUCKDB_MEMORY_LIMIT`: Memory limit for DuckDB (default: `8GB`)
- `DUCKDB_THREADS`: Number of threads for DuckDB (default: `4`)

### Database Location
- Database file: `data/city_cycles.duckdb` (relative to project root)
- Temporary files: `./temp/` directory
- Memory usage logs: `duckdb_memory.log`

## Integration with dbt

After loading raw tables, the dbt pipeline can be executed:

```bash
# Test staging models
dbt run --select staging

# Run full pipeline
dbt run

# Generate marts for export
dbt run --select marts
```

The exported marts are then available for dashboard consumption via S3 Parquet files.

## Error Handling & Monitoring

- **Comprehensive logging**: All operations log to console and file
- **Memory management**: Automatic memory usage tracking and optimization
- **Error recovery**: Detailed error messages with troubleshooting guidance
- **Data validation**: Multi-level validation to ensure data quality
- **Dry run support**: Test operations before execution

This pipeline ensures that the DuckDB database is up to date and that the dashboard has access to the latest data marts via S3-managed Parquet files. 