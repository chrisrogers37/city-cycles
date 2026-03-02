---
description: "Run data validation and schema checks"
---

Perform comprehensive data validation across the pipeline:

## 1. Schema Validation

Run data model tests to ensure schemas are valid:

```bash
# Test NYC data models
python -m pytest tests/test_nyc_models.py -v

# Test London data models
python -m pytest tests/test_london_models.py -v

# Run all data model integration tests
python -m pytest tests/test_data_models_integration.py -v

# Run weather data model tests
python -m pytest tests/test_weather_extraction.py -v
```

## 2. File Processing Validation

Check that file processing is working correctly:

```bash
# Test file extraction and conversion
python -m pytest tests/test_extracted_file_manager_current.py -v

# Validate Parquet file structure
python -c "
import pyarrow.parquet as pq
# Add validation code for specific parquet files
"
```

## 3. Database Validation

Verify DuckDB data integrity:

```bash
# Test database operations
python -m pytest tests/test_db_duckdb_cli.py tests/test_db_duckdb_operations.py -v

# Check row counts (if DuckDB client available)
# python -m db_duckdb.validate_raw_tables
```

## 4. dbt Data Quality Tests

Run dbt's built-in data quality tests:

```bash
cd dbt_city_cycles
dbt test
cd ..
```

Look for:
- Unique key violations
- Null value checks
- Referential integrity
- Custom data quality tests

## 5. Report Validation Results

Summarize findings:
- ✅ All schemas valid
- ✅ File processing working
- ✅ Database integrity confirmed
- ✅ dbt tests passing
- ❌ Any failures with details

If validation fails, identify the root cause and recommend fixes.
