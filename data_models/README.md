# Data Models for Bike Share Data

This directory contains the data models for London and NYC bike share data. The models are designed to handle both legacy and modern schemas, and provide a robust way to validate and transform data from CSV files into standardized formats for analytics.

## Overview

The data models are built on top of a base class (`BaseDataRecord`) that provides common functionality for schema validation and data transformation. Each model defines its own schema, validation rules, and transformation logic to handle the specific formats of bike share data from different cities and time periods.

**Note**: This package focuses solely on schema validation and data transformation. S3 operations are handled by the `extracted_file_manager` module, and database operations are handled by the `db_duckdb` module.

### Core Components

- **`base.py`**: Base class providing common functionality for all bike share data models
- **`nyc_bike.py`**: NYC bike share data models (legacy and modern formats)
- **`london_bike.py`**: London bike share data models (legacy and modern formats)
- **`weather.py`**: Weather data model (`HourlyWeatherRecord` dataclass) for Open-Meteo API data
- **`registry.py`**: Central registry of all available models
- **`__init__.py`**: Module exports for easy access to models

## Data Models

### NYC Bike Share Models

#### `NYCLegacyBikeShareRecord` (2013-2016)
- **Staging Table**: `raw_nyc_legacy`
- **S3 Prefix**: `nyc_csv/`
- **Key Fields**: `tripduration`, `bikeid`, `starttime`, `stoptime`, station data, user demographics
- **Schema**: Includes birth year, gender, and detailed station coordinates

#### `NYCModernBikeShareRecord` (2017-present)
- **Staging Table**: `raw_nyc_modern`
- **S3 Prefix**: `nyc_csv/`
- **Key Fields**: `ride_id`, `rideable_type`, `started_at`, `ended_at`, station data, member type
- **Schema**: Simplified schema with ride IDs and member/casual classification

### London Bike Share Models

#### `LondonLegacyBikeShareRecord` (2010-2022)
- **Staging Table**: `raw_london_legacy`
- **S3 Prefix**: `london_csv/`
- **Key Fields**: `rental_id`, `bike_id`, `start_date`, `end_date`, station data
- **Schema**: Includes duration and detailed station information

#### `LondonModernBikeShareRecord` (2022-present)
- **Staging Table**: `raw_london_modern`
- **S3 Prefix**: `london_csv/`
- **Key Fields**: `number`, `bike_number`, `bike_model`, `start_date`, `end_date`, station data
- **Schema**: Enhanced schema with bike model information and millisecond duration

## Key Features

### Schema Validation
- **Automatic Detection**: Models automatically validate CSV schemas using required column lists stored in `_required_columns` attribute
- **Debug Support**: Detailed validation output when `EXTRACTED_FILE_MANAGER_DEBUG=1` is set
- **Flexible Matching**: Handles column name variations and missing fields gracefully

### Data Transformation
- **Column Mapping**: Automatic renaming of CSV columns to standardized field names
- **Type Conversion**: Proper handling of data types (strings, integers, floats, datetimes)
- **Data Quality**: Built-in handling of coordinate precision, station ID formats, and date parsing
- **Source Tracking**: Automatic addition of `source_file` field for data lineage

### Integration Points
- **Extracted File Manager**: Used for schema validation during CSV to Parquet conversion
- **DuckDB Pipeline**: Provides data structures that match hardcoded table schemas
- **dbt Transformations**: Serves as the foundation for staging and mart models

## Usage Examples

### Schema Validation

```python
from data_models.nyc_bike import NYCModernBikeShareRecord
import pandas as pd

# Load a sample CSV
df = pd.read_csv('sample_nyc_data.csv')

# Validate schema
if NYCModernBikeShareRecord.validate_schema(df):
    print("Schema validation passed")
else:
    print("Schema validation failed")
```

### Data Transformation

```python
from data_models.london_bike import LondonLegacyBikeShareRecord
import pandas as pd

# Load raw CSV data
df = pd.read_csv('london_legacy_data.csv')

# Transform to standardized format
transformed_df = LondonLegacyBikeShareRecord.to_dataframe(df, 'source_file.csv')
print(transformed_df.columns)
```

### Model Registry Access

```python
from data_models.base import BaseDataRecord

# Access all registered models
for model in BaseDataRecord._registry:
    print(f"Model: {model.__name__}")
    print(f"Table: {model.staging_table}")
    print(f"S3 Prefix: {model.s3_prefix}")
```

## Integration with Extracted File Manager

The data models are automatically used by the Extracted File Manager for:

### Automatic Model Assignment
The Extracted File Manager uses the models to:
1. **Detect Schema**: Automatically determine which model matches a CSV file
2. **Validate Data**: Ensure CSV columns match expected schema
3. **Organize Output**: Group Parquet files by schema type in S3
4. **Transform Data**: Convert CSV data to standardized format during Parquet conversion

### Debug Output Example
```
DEBUG: Found 2 models to test for FileType.NYC_CSV
DEBUG: Available columns in file: ['ride_id', 'rideable_type', 'started_at', 'ended_at', ...]
DEBUG: Testing model: NYCLegacyBikeShareRecord
DEBUG: ✗ Model NYCLegacyBikeShareRecord failed - missing columns: ['tripduration', 'bikeid', ...]
DEBUG: Testing model: NYCModernBikeShareRecord
DEBUG: ✓ Model NYCModernBikeShareRecord matched successfully
```

## Integration with DuckDB Pipeline

The data models provide schema definitions for the DuckDB ETL pipeline:

### Schema Mapping
The models define the exact column types and structures used in:
- `db_duckdb/config/duckdb_config.py` for table schemas and S3 URI patterns
- `db_duckdb/operations.py` for table initialization and data loading

## Data Quality Features

### NYC Data Handling
- **Station IDs**: Ensures station IDs are treated as strings (handles alphanumeric values)
- **Coordinates**: Robust handling of latitude/longitude data with error coercion
- **Date Formats**: Flexible date parsing for various input formats

### London Data Handling
- **Date Parsing**: Handles multiple date formats (DD/MM/YYYY, YYYY-MM-DD, mixed formats)
- **Bike Numbers**: Ensures bike numbers are treated as strings
- **Duration Fields**: Proper handling of duration in both seconds and milliseconds

### Common Features
- **Source Tracking**: Every record includes the source file for data lineage
- **Type Safety**: Proper type conversion and validation
- **Error Handling**: Graceful handling of missing or malformed data

## Schema Evolution

### London Schema Change (September 2022)
The London data schema changed between:
- **Legacy**: `334JourneyDataExtract07Sep2022-11Sep2022.csv` and earlier
- **Modern**: `335JourneyDataExtract12Sep2022-18Sep2022.csv` and later

Key changes:
- `Rental Id` → `Number`
- `Bike Id` → `Bike number`
- Added `Bike model` field
- `Duration` → `Total duration` (with millisecond precision)
- Station naming conventions updated

### NYC Schema Change (2017)
The NYC data schema changed between:
- **Legacy**: 2013-2016 data
- **Modern**: 2017-present data

Key changes:
- `tripduration` → `ride_id` (unique identifier)
- Added `rideable_type` field
- `usertype` → `member_casual`
- Removed demographic fields (birth year, gender)
- Simplified coordinate handling

## Best Practices

1. **Always Validate**: Use schema validation before processing data
2. **Check Debug Output**: Enable debug mode when troubleshooting validation issues
3. **Monitor Data Quality**: Review transformation results for unexpected data patterns
4. **Update Models**: Keep models synchronized with actual data schema changes
5. **Test Transformations**: Verify data transformations work with sample files

## Error Handling

- **Missing Columns**: Models report specific missing columns for debugging
- **Type Mismatches**: Automatic type conversion with error handling
- **Date Parsing**: Multiple fallback strategies for date format variations
- **Data Corruption**: Graceful handling of malformed or incomplete data

## Important Notes

### Module Exports
The `__init__.py` file exports all models in `__all__` for easy access:
```python
from data_models import (
    NYCModernBikeShareRecord,
    NYCLegacyBikeShareRecord,
    LondonModernBikeShareRecord,
    LondonLegacyBikeShareRecord,
    HourlyWeatherRecord
)
```

### Database Integration
The data models are designed to work with DuckDB exclusively. The DuckDB pipeline in the `db_duckdb/` module handles all database operations using hardcoded schemas that match the data model structures.

### Required Columns
Each model stores its required columns in the `_required_columns` class attribute, which is used for detailed validation and error reporting.

### Architecture Notes
This package has been streamlined to focus on its core responsibilities:
- **Schema Validation**: Validating CSV files against expected schemas
- **Data Transformation**: Converting raw CSV data to standardized formats
- **Model Registry**: Providing access to all available data models

S3 operations, database operations, and other infrastructure concerns are handled by dedicated modules:
- **S3 Operations**: `extracted_file_manager/` module
- **Database Operations**: `db_duckdb/` module
- **Data Extraction**: `extraction/` module

---

For more details on integration with the broader pipeline, see the documentation for `extracted_file_manager/` and `db_duckdb/`.
