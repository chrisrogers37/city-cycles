# DuckDB ETL Pipeline

The `db_duckdb` package manages data loading from S3 Parquet files into DuckDB, data validation, and mart export for the City Cycles analytics project.

## Overview

This module handles the database layer of the pipeline:

```
S3 Parquet Files → DuckDB Raw Tables → (dbt transforms) → Mart Export to S3
```

### Core Components

| File | Purpose |
|------|---------|
| `cli.py` | Unified CLI with 7 commands |
| `operations.py` | Business logic for all ETL operations (`DuckDBOperations` class) |
| `pipeline.py` | Pipeline orchestration (`DuckDBPipeline` class) |
| `duckdb_manager.py` | Low-level DuckDB connection and query management (`DuckDBManager` class) |
| `utils.py` | Memory usage logging utilities |
| `config/duckdb_config.py` | Table schemas, S3 URIs, validation queries, database configuration |

## Quick Start

```bash
# Initialize raw tables
python -m db_duckdb.cli init

# Load data from S3 parquet files
python -m db_duckdb.cli load

# Verify data integrity
python -m db_duckdb.cli verify

# Export marts to S3
python -m db_duckdb.cli export

# List tables in database
python -m db_duckdb.cli list

# Run complete ETL pipeline (init + load + verify + export)
python -m db_duckdb.cli pipeline

# Check pipeline status
python -m db_duckdb.cli status
```

## CLI Reference

### Global Options

All commands support:
- `--db-path PATH` — Custom database path (default: `data/city_cycles.duckdb`)
- `--verbose, -v` — Enable debug logging

### `init` — Initialize Raw Tables

Creates the four raw tables in DuckDB.

```bash
python -m db_duckdb.cli init [OPTIONS]

Options:
  --verify / --no-verify   Verify tables after creation (default: verify)
  --dry-run                Show what would be done without executing
```

### `load` — Load Data from S3

Loads Parquet files from S3 into DuckDB raw tables. By default, tables are fully replaced each run.

```bash
python -m db_duckdb.cli load [OPTIONS]

Options:
  -t, --table TEXT   Load specific table only (e.g., raw_nyc_modern)
  --dry-run          Show what would be loaded without actually loading
  --append           Append to existing tables instead of replacing
```

### `verify` — Verify Data Integrity

Performs data validation including schema checks, null value counts, duplicate detection, and date range verification.

```bash
python -m db_duckdb.cli verify [OPTIONS]

Options:
  -t, --table TEXT   Verify specific table only
  --detailed         Include detailed quality checks (nulls, duplicates, date ranges)
```

### `export` — Export Marts to S3

Exports dbt-generated mart tables from DuckDB to S3 as Parquet files for dashboard consumption.

```bash
python -m db_duckdb.cli export [OPTIONS]

Options:
  --include-intermediate   Also export intermediate and unified tables
  -t, --table TEXT         Export specific table only
  --dry-run                Show what would be exported without actually exporting
```

**Exported mart tables:**
- `mart_daily_metrics.parquet`
- `mart_hourly_patterns.parquet`
- `mart_nyc_member_analysis.parquet`
- `mart_station_growth.parquet`
- `mart_daily_metrics_long.parquet`

Export destination: `s3://{S3_BUCKET}/marts/`

### `list` — List Tables

```bash
python -m db_duckdb.cli list [OPTIONS]

Options:
  --tables    List available tables (default if no flags)
  --exports   List existing exports in S3
  --marts     List available mart tables
  --verbose   Show detailed information (row counts, sizes)
```

### `pipeline` — Run Complete ETL

Runs init, load, verify, and export in sequence.

```bash
python -m db_duckdb.cli pipeline [OPTIONS]

Options:
  --skip-verify   Skip data verification step
  --skip-export   Skip mart export step
  --dry-run       Run in dry-run mode
```

### `status` — Check Pipeline Status

Reports on table existence, data loading status, and mart availability.

```bash
python -m db_duckdb.cli status
```

## Raw Tables

Four raw tables are created from S3 Parquet files:

| Table | Source Data | S3 Path |
|-------|-----------|---------|
| `raw_nyc_legacy` | NYC CitiBike 2013-2016 | `extracted_bike_ride_parquet/nyc/nyclegacybikesharerecord/*.parquet` |
| `raw_nyc_modern` | NYC CitiBike 2017-present | `extracted_bike_ride_parquet/nyc/nycmodernbikesharerecord/*.parquet` |
| `raw_london_legacy` | London Santander 2010-2022 | `extracted_bike_ride_parquet/london/londonlegacybikesharerecord/*.parquet` |
| `raw_london_modern` | London Santander 2022-present | `extracted_bike_ride_parquet/london/londonmodernbikesharerecord/*.parquet` |

Table schemas are defined in `config/duckdb_config.py` and match the data model structures in `data_models/`.

## Programmatic Usage

```python
from db_duckdb import DuckDBOperations, DuckDBPipeline

# Use operations directly
ops = DuckDBOperations()
ops.init_tables(verify=True)
ops.load_data(dry_run=False, replace=True)
ops.verify_data(detailed=True)
ops.export_marts()

# Or use the pipeline class
pipeline = DuckDBPipeline()
results = pipeline.run_full_pipeline(skip_verify=False, skip_export=False)

# Check status
from db_duckdb import check_pipeline_status
status = check_pipeline_status()
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `city-cycles-data-ctr37` | S3 bucket for data storage |
| `DUCKDB_MEMORY_LIMIT` | `8GB` | Maximum memory for DuckDB |
| `DUCKDB_THREADS` | `4` | Number of DuckDB threads |
| `AWS_ACCESS_KEY_ID` | — | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | — | AWS secret key |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS region |

### Database Location

Default: `{project_root}/data/city_cycles.duckdb`

Override with `--db-path` CLI flag or by modifying `config/duckdb_config.py`.

## Data Loading Strategy

Raw tables are **fully replaced** each run via `CREATE TABLE AS SELECT * FROM 's3://...'`. This is a deliberate design choice:

- Incremental logic is handled downstream by dbt (staging through unified models use `source_file` tracking)
- Full replacement ensures raw tables always reflect the complete S3 dataset
- Mart tables are small aggregations rebuilt by dbt each run

For future optimization, see the incremental raw table loading proposal in `docs/planning/ROADMAP.md`.

## Integration with Pipeline

This module is Stage 3 (Database Load) and Stage 5 (Export) in the orchestrator pipeline:

```
1. Extraction → 2. File Management → 3. Database Load (db_duckdb) → 4. dbt → 5. Export (db_duckdb)
```

The orchestrator calls this module via:
```python
from db_duckdb import DuckDBOperations
ops = DuckDBOperations()
ops.init_tables()
ops.load_data()
ops.export_marts()
```

## Troubleshooting

### Out of Memory
```bash
# Reduce memory limit
export DUCKDB_MEMORY_LIMIT=4GB
export DUCKDB_THREADS=2
```

### S3 Access Errors
```bash
# Verify credentials
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('S3_BUCKET'))"

# Test S3 access
aws s3 ls s3://city-cycles-data-ctr37/extracted_bike_ride_parquet/
```

### Database Issues
```bash
# Check database exists
ls -la data/city_cycles.duckdb

# Connect directly
python -c "import duckdb; conn = duckdb.connect('data/city_cycles.duckdb'); print(conn.execute('SHOW TABLES').fetchall())"
```

### Full Reset
```bash
# Delete database and rebuild
rm data/city_cycles.duckdb
python -m db_duckdb.cli pipeline
```

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB S3 Integration](https://duckdb.org/docs/extensions/httpfs/s3api)
- `config/duckdb_config.py` — Table schemas and S3 URI patterns
- `data_models/` — Schema validation models that define raw table structures
- `orchestrator/README.md` — Pipeline orchestration that calls this module
