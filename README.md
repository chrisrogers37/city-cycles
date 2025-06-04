# City Cycles Data Pipeline

This project contains scripts and tools for loading bike share data from various cities into a database.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your database and S3 credentials
```

## Usage

### Loading Data from S3

The main script for loading data is `db/batch_load_from_s3.py`. It supports several options:

```bash
# Load all files from a prefix
python -m db.batch_load_from_s3 nyc_csv/

# Load a specific file
python -m db.batch_load_from_s3 nyc_csv/ --filename 202001-citibike-tripdata.csv

# Use a specific model
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_modern

# Dry run (no changes)
python -m db.batch_load_from_s3 nyc_csv/ --dry-run

# Truncate table before loading
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_modern --truncate

# Reload a specific file
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_modern --filename 202001-citibike-tripdata.csv --reload

# Control chunk size for memory management
python -m db.batch_load_from_s3 nyc_csv/ --model nyc_modern --chunksize 1000
```

### Available Models

- `nyc_modern`: NYC Citi Bike data (2020 onwards)
- `nyc_legacy`: NYC Citi Bike data (pre-2020)
- `london_modern`: London Santander Cycles data (2020 onwards)
- `london_legacy`: London Santander Cycles data (pre-2020)

## Development

### Adding New Models

1. Create a new model class in `data_models/` directory
2. Inherit from `BaseBikeShareRecord`
3. Implement required methods:
   - `validate_schema()`
   - `to_dataframe()`
   - `to_database()`

### Testing

```bash
# Run tests
pytest

# Run specific test file
pytest tests/test_nyc_bike.py
```

## Project Structure

- `data_models/`: Data model definitions and schema handling
  - [Data Models Documentation](data_models/README.md)
- `db/`: Database loading and batch processing
  - [Database Documentation](db/README.md)
- `scripts/`: Utility scripts for data processing
- `tests/`: Test suite

## Documentation

- [Data Models](data_models/README.md): Detailed documentation of the data models and schema handling
- [Database Loading](db/README.md): Information about database loading and batch processing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details
