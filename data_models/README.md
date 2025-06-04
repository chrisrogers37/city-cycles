# Data Models for Bike Share Data

This directory contains the data models for London and NYC bike share data. The models are designed to handle both legacy and modern schemas, and provide a robust way to load data from S3 into a PostgreSQL database.

## Overview

The data models are built on top of a base class (`BaseBikeShareRecord`) that provides common functionality for listing files in S3, downloading files, assigning models, and loading data into the database. Each model (e.g., `LondonLegacyBikeShareRecord`, `LondonModernBikeShareRecord`, `NYCLegacyBikeShareRecord`, `NYCModernBikeShareRecord`) is a subclass of `BaseBikeShareRecord` and defines its own schema, S3 prefix, and methods for matching files and aligning data.

## Key Features

- **Model Registry:** The base class maintains a registry of all subclasses, allowing for robust model assignment based on file names and content.
- **S3 Integration:** Methods for listing and downloading files from S3.
- **Database Loading:** Methods for loading data into the correct table in the database, supporting chunked, memory-efficient loading with progress and memory logging.
- **Schema Generation & Execution:** Generate and execute SQL DDLs for creating tables directly from the models.

## Example Usage

### Listing Files in S3

```python
from data_models.base import BaseBikeShareRecord

# List all files in the 'london_csv/' prefix
files = BaseBikeShareRecord.list_s3_files(prefix='london_csv/')
print(files)

# List files for a specific year
files_2021 = BaseBikeShareRecord.list_s3_files(prefix='london_csv/', year=2021)
print(files_2021)
```

### Assigning Models

```python
from data_models.base import BaseBikeShareRecord

# Assign a model to a file (now requires s3_prefix)
filename = 'london_2021.csv'
s3_prefix = 'london_csv/'
model = BaseBikeShareRecord.assign_model(filename, s3_prefix)
if model:
    print(f"Matched model: {model.__name__} (table: {model.staging_table})")
else:
    print("No model matched.")
```

### Generating and Executing SQL DDLs

```python
from data_models.london_bike import LondonLegacyBikeShareRecord

# Generate SQL DDL for the London Legacy table
ddl = LondonLegacyBikeShareRecord.get_schema_sql()
print(ddl)

# Create the table in the database
LondonLegacyBikeShareRecord.create_table()

# Create all tables for all models
from data_models.base import BaseBikeShareRecord
BaseBikeShareRecord.create_all_tables()
```

### Loading Data into the Database (Chunked, Memory-Efficient)

```python
from data_models.base import BaseBikeShareRecord

# Load all files in the 'london_csv/' prefix, processing in memory-efficient chunks
BaseBikeShareRecord.load_from_s3(prefix='london_csv/', chunksize=10000)
```

### Batch Loading

You can also use the batch loading scripts to load all files for a specific prefix or all prefixes:

```bash
# Load all files in the 'london_csv/' prefix
python db/batch_load_from_s3.py london_csv/

# Load files for a specific year
python db/batch_load_from_s3.py london_csv/ 2021

# Dry run
python db/batch_load_from_s3.py london_csv/ --dry-run

# Load all prefixes
python db/batch_load_all_from_s3.py
```

### Error Handling & Logging

- The ETL process logs progress and memory usage for each chunk and file.
- Errors in loading one file or prefix do not stop the rest of the batch.
