# City Cycles - Bike Share Data ETL

A robust ETL pipeline for processing and loading bike share data from London and NYC into a PostgreSQL database.

## Overview

This project provides a flexible and maintainable way to:
- Process bike share data from multiple sources (London, NYC)
- Handle different data formats and schemas (legacy and modern)
- Load data efficiently into a PostgreSQL database
- Support batch processing and incremental updates

## Quick Start

1. **Setup Environment**
   ```bash
   # Create and activate virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Configure Database**
   ```bash
   # Set up your database connection in .env
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Load Data**
   ```bash
   # Load all London data
   python -m db.batch_load_from_s3 london_csv/
   
   # Load specific year of NYC data
   python -m db.batch_load_from_s3 nyc_csv/ --year 2023
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

## Development

### Running Tests
```bash
pytest
```

### Adding New Data Sources
1. Create a new model class in `data_models/`
2. Implement required methods from `BaseBikeShareRecord`
3. Add tests in `tests/`
4. Update documentation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details
