# Data Models for Bike Share Data

This directory contains the data models for London and NYC bike share data. The models are designed to handle both legacy and modern schemas, and provide a robust way to load data from S3 into a PostgreSQL database.

## Overview

The data models are built on top of a base class (`BaseBikeShareRecord`) that provides common functionality for listing files in S3, downloading files, assigning models, and loading data into the database. Each model (e.g., `LondonLegacyBikeShareRecord`, `LondonModernBikeShareRecord`, `NYCLegacyBikeShareRecord`, `NYCModernBikeShareRecord`) is a subclass of `BaseBikeShareRecord` and defines its own schema, S3 prefix, and methods for matching files and aligning data.

## Key Features

- **Model Registry**: The base class maintains a registry of all subclasses, allowing for robust model assignment based on file names and content.
- **S3 Integration**: Methods for listing and downloading files from S3.
- **Database Loading**: Methods for loading data into the correct table in the database.
- **Schema Generation**: Methods for generating SQL DDLs for creating tables.

## Example Usage

### Listing Files in S3

To list all CSV files in a specific S3 prefix:

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

To assign a model to a file:

```python
from data_models.base import BaseBikeShareRecord

# Assign a model to a file
filename = 'london_2021.csv'
model = BaseBikeShareRecord.assign_model(filename)
if model:
    print(f"Matched model: {model.__name__} (table: {model.staging_table})")
else:
    print("No model matched.")
```

### Generating SQL DDLs

To generate SQL DDLs for creating tables:

```python
from data_models.london_bike import LondonLegacyBikeShareRecord

# Generate SQL DDL for the London Legacy table
ddl = LondonLegacyBikeShareRecord.get_schema_sql()
print(ddl)
```

### Loading Data into the Database

To load data from S3 into the database:

```python
from data_models.base import BaseBikeShareRecord

# Load all files in the 'london_csv/' prefix
BaseBikeShareRecord.load_from_s3(prefix='london_csv/')

# Load files for a specific year
BaseBikeShareRecord.load_from_s3(prefix='london_csv/', year=2021)

# Load a specific file
BaseBikeShareRecord.load_from_s3(prefix='london_csv/', filename='london_2021.csv')

# Dry run (no actual insertion)
BaseBikeShareRecord.load_from_s3(prefix='london_csv/', dry_run=True)
```

## Batch Loading

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
