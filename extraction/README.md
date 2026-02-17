# Extraction Package: Bike Data Scraping & Download

The extraction package handles the single concern of scraping files from the web and getting them into S3. It provides automated data extraction for bike share systems in New York City and London, with utilities for S3 integration and future weather data ingestion.

## Overview

This package is responsible for the initial data acquisition phase of the City Cycles Analytics pipeline. It downloads raw bike share data from public sources and stores it in the project's S3 bucket for further processing.

### Core Components

- **`nyc.py`**: Downloads CitiBike ZIP files from NYC's public S3 bucket
- **`london.py`**: Scrapes Santander Cycles CSV files from Transport for London website
- **`utils.py`**: Shared utilities for S3 operations and file management
- **`weather.py`**: Weather data extraction from Open-Meteo API (historical backfill + incremental)
- **`__init__.py`**: Package initialization

## Data Sources

### NYC CitiBike Data (`nyc.py`)

**Source:** Public S3 bucket (`tripdata`) containing CitiBike ZIP files

**Method:** Uses `boto3` with unsigned access to list and download ZIP files

**Storage:** Uploads ZIP files to `extracted_bike_ride_zips/nyc/` in project S3 bucket

**Features:**
- Year-based filtering (default: 2019 to current year)
- Duplicate detection (skips files already in S3)
- ZIP validation (ensures downloaded files are valid archives)
- Efficient batch processing

**Usage:**
```python
from extraction.nyc import download_all_zips

# Download all files from 2019 to current year
download_all_zips()

# Download files for specific year range
download_all_zips(start_year=2020, end_year=2023)
```

### London Santander Cycles Data (`london.py`)

**Source:** Transport for London (TfL) website (`cycling.data.tfl.gov.uk`)

**Method:** Uses Playwright for headless browser automation (no direct S3 access)

**Storage:** Downloads CSV files directly to `extracted_bike_ride_csvs/london/` in project S3 bucket

**Features:**
- Dynamic page scrolling to load all file links
- File pattern matching for journey data extracts
- XLS-to-CSV conversion (handles TfL website bug)
- Respectful delays between downloads
- Duplicate detection

**Usage:**
```python
from extraction.london import process_and_upload_london_files

# Download all available CSV files
process_and_upload_london_files()
```

## S3 Integration (`utils.py`)

The package provides shared utilities for S3 operations:

### Core Functions

- **`upload_to_s3(local_path, s3_key)`**: Uploads local files to S3
- **`file_exists_in_s3(s3_key)`**: Checks if a file already exists in S3
- **`check_s3_bucket()`**: Validates S3 bucket configuration

### Environment Requirements

- **`S3_BUCKET`**: AWS S3 bucket name for file storage (required)

### Usage Example:
```python
from extraction.utils import upload_to_s3, file_exists_in_s3

# Check if file exists before downloading
s3_key = "extracted_bike_ride_csvs/london/sample.csv"
if not file_exists_in_s3(s3_key):
    # Download and upload file
    upload_to_s3(local_file_path, s3_key)
```

## Weather Data Extraction (`weather.py`)

The `weather.py` module fetches hourly weather data from the Open-Meteo API for NYC and London, storing it as Parquet files in S3.

**Source:** [Open-Meteo API](https://open-meteo.com/) — free, no authentication required

**Variables collected:** temperature, precipitation, rain, snowfall, wind speed/gusts, weather code (WMO), cloud cover, relative humidity

**Modes:**
- **Backfill:** Fetch full years of historical data for each city
- **Incremental:** Fetch recent data with configurable lookback (default: 35 days)

**Storage:** `extracted_weather_parquet/nyc/` and `extracted_weather_parquet/london/` in S3

**Usage:**
```python
from extraction.weather import backfill_all, incremental_update_all

# Backfill historical data for all cities
backfill_all(start_year=2019, end_year=2025)

# Incremental update (used in monthly pipeline)
incremental_update_all(days_back=35)
```

## Installation & Setup

### Prerequisites

1. **AWS Credentials**: Configure AWS credentials with S3 access
2. **Environment Variables**: Set `S3_BUCKET` environment variable
3. **Python Dependencies**: Install required packages

### Required Dependencies

```bash
pip install boto3 playwright python-dotenv requests
```

### Playwright Setup

For London data extraction, Playwright requires browser installation:

```bash
playwright install chromium
```

### Environment Configuration

Create a `.env` file in the project root:

```env
S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=your-region
```

## Usage Examples

### Download NYC Data

```python
from extraction.nyc import download_all_zips

# Download all available files (2019 to current year)
download_all_zips()

# Download specific year range
download_all_zips(start_year=2022, end_year=2023)

# Download single year
download_all_zips(start_year=2023, end_year=2023)
```

### Download London Data

```python
from extraction.london import process_and_upload_london_files

# Download all available CSV files
process_and_upload_london_files()
```

### Check S3 Status

```python
from extraction.utils import file_exists_in_s3

# Check if specific file exists
exists = file_exists_in_s3("extracted_bike_ride_zips/nyc/2023-01-citibike-tripdata.zip")
print(f"File exists: {exists}")
```

## File Structure

### NYC Data Flow
```
Public S3 (tripdata) → Local Temp → Project S3 (extracted_bike_ride_zips/nyc/)
```

### London Data Flow
```
TfL Website → Local Temp → Project S3 (extracted_bike_ride_csvs/london/)
```

### S3 Organization
```
extracted_bike_ride_zips/
├── nyc/
│   ├── 2019-01-citibike-tripdata.zip
│   ├── 2019-02-citibike-tripdata.zip
│   └── ...
extracted_bike_ride_csvs/
├── london/
│   ├── JourneyDataExtract18Dec2019-24Dec2019.csv
│   ├── JourneyDataExtract25Dec2019-31Dec2019.csv
│   └── ...
```

## Error Handling

### NYC Extraction
- **Invalid ZIP files**: Automatically detected and skipped
- **Network errors**: Retry logic with error logging
- **S3 upload failures**: Local cleanup and error reporting

### London Extraction
- **Website changes**: Pattern matching for file detection
- **Download failures**: Individual file error handling
- **XLS file handling**: Automatic conversion to CSV format

### General
- **Duplicate detection**: Prevents re-downloading existing files
- **Local cleanup**: Temporary files are automatically removed
- **Error logging**: Detailed error messages for troubleshooting

## Performance Considerations

### NYC Data
- **Efficient listing**: Uses S3 pagination for large file lists
- **Batch processing**: Processes multiple files in sequence
- **ZIP validation**: Ensures data integrity before storage

### London Data
- **Browser automation**: Uses headless Chromium for reliability
- **Dynamic scrolling**: Handles large file lists on TfL website
- **Respectful delays**: 1-second delays between downloads

### Memory Management
- **Streaming downloads**: Files are streamed to disk, not loaded into memory
- **Temporary storage**: Uses `/tmp` directory for local file storage
- **Automatic cleanup**: Temporary files are removed after S3 upload

## Monitoring & Logging

### Download Progress
```
Using S3 bucket: city-cycles-data
Listing files in s3://tripdata/ ...
Matched 48 files for years 2019-2023.
Sample files: ['201901-citibike-tripdata.zip', '201902-citibike-tripdata.zip', ...]
Found 48 files to process.
Downloading s3://tripdata/201901-citibike-tripdata.zip to /tmp/extracted_bike_ride_zips/nyc/201901-citibike-tripdata.zip ...
Stored ZIP file in S3: extracted_bike_ride_zips/nyc/201901-citibike-tripdata.zip
```

### Summary Reports
```
Download Summary:
Total files found: 48
New files downloaded: 12
Files already in S3: 36
```

## Troubleshooting

### Common Issues

**S3 Bucket Not Set**
```
ValueError: S3_BUCKET environment variable is not set!
```
**Solution:** Set the `S3_BUCKET` environment variable

**Playwright Browser Not Found**
```
playwright.errors.Error: Browser not found
```
**Solution:** Run `playwright install chromium`

**Network Timeout (London)**
```
TimeoutError: Timeout 20000ms exceeded
```
**Solution:** Check internet connection and TfL website availability

**Invalid ZIP File (NYC)**
```
ERROR: Invalid ZIP file: /tmp/extracted_bike_ride_zips/nyc/corrupted.zip
```
**Solution:** File will be skipped automatically, check source data

### Debug Mode

Enable detailed logging for troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Integration with Pipeline

The extraction package is the first step in the City Cycles Analytics pipeline:

1. **Extraction** (`extraction/`) → Downloads raw data to S3
2. **File Processing** (`extracted_file_manager/`) → Processes files into Parquet format
3. **Data Loading** (`db_duckdb/`) → Loads data into DuckDB
4. **Transformation** (`dbt_city_cycles/`) → Transforms data into analytics marts
5. **Visualization** (`dashboard/`) → Creates interactive dashboard

## Future Enhancements

### Planned Features
- **Additional data sources** (other cities, bike share systems)
- **Parallel processing** (concurrent downloads for NYC + London)
- **Data validation** (schema checking during download)

## Best Practices

1. **Run regularly**: Set up automated extraction jobs
2. **Monitor storage**: Track S3 storage costs and usage
3. **Validate data**: Check downloaded files for completeness
4. **Respect rate limits**: Use appropriate delays between requests
5. **Error handling**: Monitor and address extraction failures
6. **Backup strategy**: Consider S3 versioning for data protection

---

For more details on the overall pipeline, see the main project README and other package documentation.
