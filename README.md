# City Cycles Data Pipeline

This project provides a robust, model-driven pipeline for ingesting, processing, and loading bike share data from London and NYC into a PostgreSQL database.

## Key Features

- **Data Ingestion:** Scraping and downloading bike share data from websites and uploading it to S3.
- **Data Models:** Python dataclasses for both legacy and modern schemas for London and NYC, with schema alignment and validation.
- **Model-Driven ETL:** Batch loading from S3 to PostgreSQL using memory-efficient, chunked processing with detailed logging and error handling.
- **Automated DDL Generation:** Table schemas are generated and created directly from the data model classes.
- **Easy Table Initialization:** Use `python db/init_tables.py` to create all required tables in your database.
- **Robust Logging:** Progress and memory usage are logged for each chunk and file, aiding in monitoring and debugging.

## Setup

### Prerequisites

- Python 3.8+
- PostgreSQL DB
- AWS S3 bucket
- Required Python packages (see `requirements.txt`)

### Environment Variables

Set the following environment variables in a `.env` file:

```
S3_BUCKET=your_s3_bucket_name
DB_HOST=your_db_host
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_db_name
DB_PORT=5432
```

## Table Initialization

To create all required tables in your database, run:
```bash
python db/init_tables.py
```

## Execution

### Scraping Data to S3

*[Placeholder: Instructions for scraping data from websites and uploading to S3 will be added later.]*

### Loading Data from S3 to PostgreSQL

To load data from S3 into the PostgreSQL database, use the provided batch loading scripts:

```bash
# Load all files in the 'london_csv/' prefix
python db/batch_load_from_s3.py london_csv/

# Load files for a specific year
python db/batch_load_from_s3.py london_csv/ 2021

# Dry run (no actual insertion)
python db/batch_load_from_s3.py london_csv/ --dry-run

# Load all prefixes
python db/batch_load_all_from_s3.py
```

## Notes

- Legacy scripts such as `db/load.py` and static `db/init.sql` are no longer used; all operations are now model-driven and automated.
- For more details on the data models and their functionality, refer to `data_models/README.md`.

## Conclusion

This README provides a high-level overview of the project setup and execution. For detailed information on the data models and their usage, refer to the `data_models/README.md`.
