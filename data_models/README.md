# Data Models

This directory contains the data models for different bike share systems. Each model handles the specific schema and data format for its respective system.

## Base Model

The `BaseBikeShareRecord` class provides common functionality for all bike share models:

- S3 file listing and downloading
- Schema validation
- Data loading and processing
- Database operations

## Available Models

### NYC Models

- `NYCModernBikeShareRecord`: Handles NYC Citi Bike data from 2020 onwards
- `NYCLegacyBikeShareRecord`: Handles NYC Citi Bike data from before 2020

### London Models

- `LondonModernBikeShareRecord`: Handles London Santander Cycles data from ? onwards
- `LondonLegacyBikeShareRecord`: Handles London Santander Cycles data from before 2020

## Usage

```python
from data_models.nyc_bike import NYCModernBikeShareRecord

# List files in S3
files = NYCModernBikeShareRecord.list_s3_files(prefix='nyc_csv/')

# Load data from S3
NYCModernBikeShareRecord.load_from_s3(prefix='nyc_csv/')

# Load with specific model
NYCModernBikeShareRecord.load_from_s3_with_model(
    NYCModernBikeShareRecord,
    prefix='nyc_csv/',
    dry_run=True
)
```

## Adding New Models

1. Create a new class inheriting from `BaseBikeShareRecord`
2. Implement required methods:
   - `validate_schema()`: Check if a DataFrame matches the model's schema
   - `to_dataframe()`: Convert raw data to DataFrame format
   - `to_database()`: Insert data into database
3. Add tests in `tests/` directory
