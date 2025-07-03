# Extracted File Manager

Manages extracted ZIP and CSV files on S3 with a complete pipeline for data processing.

## Pipeline Overview

The system implements a complete data pipeline:

```
extracted_bike_ride_zips/{city}/     # Raw ZIP files from extraction
    ↓ (ZIP → CSV conversion)
extracted_bike_ride_csvs/{city}/     # Extracted CSV files
    ↓ (Schema validation)
extracted_bike_ride_parquet/{city}/{schema}/  # Parquet files organized by schema
```

### Pipeline Steps

1. **ZIP Extraction**: NYC ZIP files are extracted to CSV format
2. **Schema Validation**: CSV files are validated against data models
3. **Parquet Conversion**: Validated CSV files are converted to Parquet with schema-based organization

## File Types

- `NYC_ZIP`: Raw ZIP files from NYC CitiBike
- `NYC_CSV`: Extracted CSV files from NYC
- `LONDON_CSV`: CSV files from London TfL
- `NYC_PARQUET`: Parquet files from NYC data
- `LONDON_PARQUET`: Parquet files from London data

## File Statuses

- `EXTRACTED`: File has been downloaded to S3
- `CSV_CONVERTED`: ZIP has been converted to CSV
- `VALIDATED`: File has been schema validated
- `PARQUET_CONVERTED`: CSV has been converted to Parquet
- `PROCESSED`: File has been loaded into database
- `FAILED`: File processing failed
- `DELETED`: File has been deleted

## Usage

### Basic Usage

```python
from extracted_file_manager import ExtractedFileManager

# Initialize manager
manager = ExtractedFileManager()

# Scan for new files
new_files = manager.scan_s3_files()

# Process entire pipeline
results = manager.process_all_pipelines()

# Get summary
manager.print_summary()
```

### CLI Usage

```bash
# Scan for new files
python -m extracted_file_manager.cli scan

# Validate all files
python -m extracted_file_manager.cli validate

# Convert specific ZIP to CSV
python -m extracted_file_manager.cli convert-zip --file 201906-citibike-tripdata.zip

# Convert specific CSV to Parquet
python -m extracted_file_manager.cli convert-csv --file 201906-citibike-tripdata.csv

# Process entire pipeline for all files
python -m extracted_file_manager.cli pipeline

# List files by status
python -m extracted_file_manager.cli list --status validated

# Show summary
python -m extracted_file_manager.cli summary
```

## Schema Organization

Parquet files are organized by schema determined by data model validation:

```
extracted_bike_ride_parquet/
├── nyc/
│   ├── nyclegacybikesharerecord/
│   │   └── 201906-citibike-tripdata.parquet
│   └── nycmodernbikesharerecord/
│       └── 202312-citibike-tripdata.parquet
└── london/
    └── londonbikesharerecord/
        └── JourneyDataExtract_201912.csv.parquet
```

## Data Model Integration

The system automatically determines the correct schema using your existing data models:

- `NYCLegacyBikeShareRecord` for legacy NYC format
- `NYCModernBikeShareRecord` for modern NYC format  
- `LondonBikeShareRecord` for London format

## Metadata Tracking

All file operations are tracked in S3 at `extracted_file_manager/metadata.json` including:

- File locations and sizes
- Processing timestamps
- Validation results
- Error messages
- Pipeline relationships

## Error Handling

- Failed operations are marked with `FAILED` status
- Error messages are stored in metadata
- Files can be reprocessed using `reprocess_file()`
- Pipeline continues processing other files even if some fail 