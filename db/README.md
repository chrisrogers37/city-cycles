# Database Loading and Batch Processing

This directory contains scripts and utilities for loading bike share data from S3 into the PostgreSQL database.

## Batch Loading Scripts

### `batch_load_from_s3.py`

This script provides several ways to load data from S3:

#### Auto-detection Mode
This is the default mode that automatically determines which model to use for each file:

```bash
# Load all files from a prefix
python -m db.batch_load_from_s3 nyc_csv/

# Load files from a specific year
python -m db.batch_load_from_s3 nyc_csv/ --year 2023

# Load a specific file
python -m db.batch_load_from_s3 nyc_csv/ --filename 202304-citibike-tripdata.csv

# Dry run (no changes)
python -m db.batch_load_from_s3 nyc_csv/ --dry-run
```

#### Model-specific Mode
When you know which model to use, you can specify it directly:

```bash
# Load all files using a specific model
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_legacy

# Clear table and load all files
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_legacy --truncate

# Delete and reload a specific file
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_legacy --filename file.csv --reload
```

Available models:
- `london_legacy`: London Legacy Bike Share Record
- `london_modern`: London Modern Bike Share Record
- `nyc_legacy`: NYC Legacy Bike Share Record
- `nyc_modern`: NYC Modern Bike Share Record

### Command Line Options

- `s3_prefix`: Required. The S3 prefix to load files from (e.g., `nyc_csv/`)
- `--filename`: Optional. Load a specific file
- `--dry-run`: Optional. Simulate loading without making changes
- `--model`: Optional. Specify which model to use
- `--truncate`: Optional. Clear the table before loading (requires --model)
- `--reload`: Optional. Delete and reload a specific file (requires --model and --filename)

## Database Integration

The database loading process:
1. Creates staging tables if they don't exist
2. Handles database connection and transaction management
3. Supports batch loading for large files
4. Provides progress and memory usage logging
5. Implements error handling to prevent batch failures

## Error Handling & Logging

- The ETL process logs progress and memory usage for each chunk and file
- Errors in loading one file or prefix do not stop the rest of the batch
- Detailed error logs are maintained for troubleshooting 