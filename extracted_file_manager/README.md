# Simplified Extracted File Manager

A streamlined file management system for processing bike share data from ZIP archives through CSV extraction to optimized Parquet storage.

## Overview

This package has been **simplified** to remove over-engineered metadata tracking and replace it with simple file existence checks. The result is a much more reliable, debuggable, and maintainable system.

### Key Changes

- ❌ **Removed**: Complex `metadata.json` tracking system
- ❌ **Removed**: Over-engineered CLI with 10+ commands (completely deleted)
- ❌ **Removed**: Complex metadata models (completely deleted)
- ❌ **Removed**: Complex state transitions and error recovery
- ❌ **Removed**: Backward compatibility stubs (cleaned up)
- ✅ **Added**: Simple file existence checks for idempotent operations
- ✅ **Added**: Streamlined pipeline functions
- ✅ **Added**: Simple CLI with just 3 commands

## Architecture

### Core Components

- **`manager.py`**: Simplified `ExtractedFileManager` class with file existence checks
- **`simplified_pipeline.py`**: End-to-end pipeline functions
- **`filetree.py`**: ZIP processing utilities (unchanged)
- **`cli.py`**: Simple CLI with 3 commands (replaces old complex CLI)
- **`__init__.py`**: Clean package exports

### Removed Components

- **`cli.py`**: Old complex CLI (completely removed)
- **`models.py`**: Old metadata models (completely removed)

## Usage

### Simple Pipeline Commands

```bash
# Run the complete pipeline (extraction + conversion)
python -m extracted_file_manager.cli run

# Run extraction only (download files from web)
python -m extracted_file_manager.cli extract

# Run conversion only (ZIP → CSV → Parquet)
python -m extracted_file_manager.cli convert
```

### Programmatic Usage

```python
from extracted_file_manager.simplified_pipeline import run_full_pipeline

# Run complete pipeline
results = run_full_pipeline()

# Or run individual phases
from extracted_file_manager.simplified_pipeline import run_extraction_only, run_conversion_only
extraction_results = run_extraction_only()
conversion_results = run_conversion_only()
```

### Direct Manager Usage

```python
from extracted_file_manager import ExtractedFileManager

manager = ExtractedFileManager()

# Extract all ZIPs (skips if CSVs already exist)
zip_results = manager.extract_all_zips_simple()

# Convert all CSVs to parquet (skips if parquet already exists)
csv_results = manager.convert_all_csvs_simple()

# Run complete pipeline
pipeline_results = manager.run_simplified_pipeline()
```

## Pipeline Flow

```
1. EXTRACTION PHASE
   ├── NYC: Download ZIPs from CitiBike S3 (skips if exist)
   └── London: Download CSVs from TfL website (skips if exist)

2. CONVERSION PHASE  
   ├── Extract ZIPs to CSVs (always process, skip existing during upload)
   └── Convert CSVs to Parquet (skips if parquet exists)
```

## File Organization

### S3 Structure
```
extracted_bike_ride_zips/nyc/           # Raw ZIP files
extracted_bike_ride_csvs/nyc/           # Extracted CSVs (flat)
extracted_bike_ride_csvs/london/        # Raw CSVs from London
extracted_bike_ride_parquet/nyc/schema/ # Parquet files by schema
extracted_bike_ride_parquet/london/schema/
```

### Schema Organization
Parquet files are automatically organized by detected schema:
- `nyclegacybikesharerecord/` - Legacy NYC format (2013-2016)
- `nycmodernbikesharerecord/` - Modern NYC format (2017-present)
- `londonlegacybikesharerecord/` - Legacy London format (2010-2016)  
- `londonmodernbikesharerecord/` - Modern London format (2017-present)

## Key Benefits

### 🚀 **Simplicity**
- No complex metadata management
- No state transitions to debug
- No CLI complexity
- Just check if files exist

### 🔄 **Idempotent**
- Run multiple times safely
- Only uploads CSV files that don't already exist
- Always processes ZIPs but skips existing files during upload
- Self-healing (can restart from any point)

### 🐛 **Debuggable**  
- Just look at S3 to see what's done
- No metadata corruption issues
- Clear file organization

### 🛡️ **Reliable**
- No single point of failure (metadata.json)
- No race conditions
- No complex error recovery needed

## Clean Interface

The package provides a clean, simple interface:

```python
# Simple imports
from extracted_file_manager import ExtractedFileManager, run_full_pipeline

# Use the simplified manager
manager = ExtractedFileManager()
manager.extract_all_zips_simple()
manager.convert_all_csvs_simple()

# Or use the pipeline functions
run_full_pipeline()
```

## Environment Setup

### Required Environment Variables
- **`S3_BUCKET`**: AWS S3 bucket name for file storage

### Dependencies
- `boto3` - AWS S3 client
- `pandas` - Data manipulation  
- `pyarrow` - Parquet processing
- `python-dotenv` - Environment variable loading

## Migration from Old System

If you were using the old complex CLI:

### Old Commands → New Commands
```bash
# Old: Complex multi-step process
python -m extracted_file_manager.cli scan
python -m extracted_file_manager.cli extract_zips
python -m extracted_file_manager.cli convert_csvs

# New: Simple pipeline
python -m extracted_file_manager.cli run
```

### Old Programmatic Usage → New Usage
```python
# Old: Complex metadata management
manager = ExtractedFileManager()
manager.scan_s3_files()
manager.extract_zips()
manager.convert_csvs()

# New: Simple pipeline
from extracted_file_manager.simplified_pipeline import run_full_pipeline
run_full_pipeline()
```

## Troubleshooting

### Common Issues

**"No matching schema found"**: The CSV file doesn't match any known schema
- Check the CSV columns against the data models in `~/data_models/`
- The system will skip files it can't process

**"File already exists"**: Normal behavior - the system skips existing files
- This is expected and indicates the pipeline is working correctly
- The system is idempotent by design

**Memory issues**: Large files may cause memory problems
- The system uses streaming processing to minimize memory usage
- Consider processing smaller batches if needed

**ZIP processing**: The system always processes ZIPs but skips existing CSV files during upload
- This ensures all files are extracted and prevents partial extractions
- Only uploads CSV files that don't already exist in S3
- Automatically filters out MacOSX artifacts (files starting with `._` or in `__MACOSX/` directories)
- **Memory management**: Processes ZIPs in configurable batches with explicit garbage collection
- **OOM protection**: Monitors memory usage and performs cleanup between operations

### Debugging

To see what files exist in S3:
```bash
aws s3 ls s3://your-bucket/extracted_bike_ride_csvs/nyc/
aws s3 ls s3://your-bucket/extracted_bike_ride_parquet/nyc/
```

To check if a specific file was processed:
```bash
aws s3 ls s3://your-bucket/extracted_bike_ride_parquet/nyc/nycmodernbikesharerecord/
```

### Memory Management

The system includes built-in memory management to prevent OOM (Out of Memory) errors:

- **Batch Processing**: ZIPs are processed in configurable batches (default: 5)
- **Sequential Nested ZIP Processing**: Nested ZIPs are processed one at a time, not all simultaneously
- **Garbage Collection**: Explicit cleanup after each ZIP and CSV operation
- **Memory Monitoring**: Logs memory usage at each stage for debugging
- **Temporary File Cleanup**: Ensures temporary files are properly deleted
- **Content Cleanup**: CSV content is explicitly deleted from memory after upload

If you encounter OOM errors, reduce the `batch_size` parameter:
```python
manager = ExtractedFileManager(batch_size=2)  # Process fewer ZIPs at once
```

**Key Memory Optimizations:**
- **No Tree Building**: Avoids building complex file tree structures in memory
- **Streaming Processing**: Processes files one by one instead of loading all at once
- **Immediate Cleanup**: Deletes temporary files and content immediately after use
- **Chunked CSV Processing**: Processes large CSV files in manageable chunks
- **String Type Preservation**: Forces string types for station IDs to prevent conversion errors

## API Reference

### ExtractedFileManager Class

#### Core Methods
- `extract_all_zips_simple()` → `Dict[str, bool]`
- `convert_all_csvs_simple()` → `Dict[str, bool]`
- `run_simplified_pipeline()` → `Dict[str, Dict[str, bool]]`

### Pipeline Functions

- `run_full_pipeline()` → `Dict[str, Any]`
- `run_extraction_only()` → `Dict[str, Any]`
- `run_conversion_only()` → `Dict[str, Dict[str, bool]]`

---

The simplified system eliminates complexity while maintaining all essential functionality. No more metadata corruption, race conditions, or complex debugging! 