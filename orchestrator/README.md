# City Cycles Pipeline Orchestrator

A production-ready orchestrator for the end-to-end City Cycles ETL pipeline. Coordinates data extraction, processing, loading, transformation, and export across all subsystems.

## Overview

The orchestrator manages the complete data pipeline:

```
┌─────────────────────────────────────────────────────────────────┐
│                  CITY CYCLES ETL PIPELINE                       │
└─────────────────────────────────────────────────────────────────┘

Step 1: Extract Data from Web to S3
├─ NYC CitiBike data (public S3 bucket)
└─ London Santander Cycles data (TfL website)

Step 2: S3 File Management
├─ Extract ZIP files to CSVs
├─ Validate schemas (4 schema types)
└─ Convert CSVs to Parquet

Step 3: Load into DuckDB
├─ Load Parquet files from S3
├─ Create raw tables (4 tables)
└─ Verify data integrity

Step 4: dbt Transformations
├─ Staging layer (incremental)
├─ Intermediate layer (incremental)
├─ Unified layer (incremental)
└─ Marts layer (full rebuild)

Step 5: Export Marts to S3
└─ Export Parquet files for Streamlit dashboard
```

## Installation

The orchestrator is part of the City Cycles project and uses existing dependencies:

```bash
# Already installed if you have the project set up
pip install -r requirements.txt
```

## Quick Start

### Run Complete Pipeline

```bash
# Run all stages
python -m orchestrator.cli run

# Run with options
python -m orchestrator.cli run --skip-extraction --dbt-full-refresh
```

### Run Individual Stage

```bash
# Run just extraction
python -m orchestrator.cli stage extraction

# Run just dbt with full refresh
python -m orchestrator.cli stage dbt --full-refresh

# Run just database load
python -m orchestrator.cli stage database_load
```

### Check Pipeline Status

```bash
python -m orchestrator.cli status
```

## Usage

### Command Reference

#### `run` - Run Complete Pipeline

```bash
python -m orchestrator.cli run [OPTIONS]

Options:
  --skip-extraction      Skip data extraction (use existing S3 files)
  --skip-verify          Skip data verification after load
  --skip-export          Skip mart export to S3
  --dbt-full-refresh     Run dbt with --full-refresh
  --verbose, -v          Enable verbose logging
```

**Examples:**

```bash
# Standard monthly run
python -m orchestrator.cli run

# Quick test run (skip slow extraction)
python -m orchestrator.cli run --skip-extraction --skip-verify

# Force full rebuild of incremental models
python -m orchestrator.cli run --dbt-full-refresh

# Development run (skip extraction and export)
python -m orchestrator.cli run --skip-extraction --skip-export
```

#### `stage` - Run Individual Stage

```bash
python -m orchestrator.cli stage STAGE_NAME [OPTIONS]

Stages:
  extraction          Extract data from web to S3
  file_management     Process files (unzip, schema, Parquet)
  database_load       Load data into DuckDB
  dbt                 Run dbt transformations
  export              Export marts to S3

Options:
  --skip-verify       Skip verification (database_load stage)
  --full-refresh      Full refresh (dbt stage)
  --verbose, -v       Enable verbose logging
```

**Examples:**

```bash
# Re-run just dbt
python -m orchestrator.cli stage dbt

# Rebuild all incremental models
python -m orchestrator.cli stage dbt --full-refresh

# Reload database only
python -m orchestrator.cli stage database_load

# Just export marts
python -m orchestrator.cli stage export
```

#### `status` - Check Pipeline Status

```bash
python -m orchestrator.cli status [OPTIONS]

Options:
  --verbose, -v       Enable verbose logging
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# AWS Configuration (Required)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=city-cycles-data-ctr37

# DuckDB Configuration
DUCKDB_MEMORY_LIMIT=8GB
DUCKDB_THREADS=4

# Extraction Configuration
NYC_START_YEAR=2019
ENABLE_NYC_EXTRACTION=true
ENABLE_LONDON_EXTRACTION=true

# dbt Configuration
DBT_PROFILES_DIR=~/.dbt
DBT_TARGET=prod

# Pipeline Configuration
DEFAULT_DBT_FULL_REFRESH=false
SKIP_EXTRACTION_ON_ERROR=false
CONTINUE_ON_STAGE_FAILURE=false

# Logging
LOG_LEVEL=INFO
MAX_LOG_SIZE_MB=100
LOG_RETENTION_DAYS=30
```

### Validate Configuration

```bash
python -m orchestrator.config --validate
```

### View Current Configuration

```bash
# Show configuration (hide secrets)
python -m orchestrator.config

# Show all including secrets
python -m orchestrator.config --show-secrets
```

## Programmatic Usage

### Basic Usage

```python
from orchestrator import CityBikesOrchestrator

# Create orchestrator
orchestrator = CityBikesOrchestrator()

# Run complete pipeline
success = orchestrator.run()

# Run with options
success = orchestrator.run(
    skip_extraction=True,
    skip_verify=False,
    skip_export=False,
    dbt_full_refresh=False
)
```

### Run Individual Stages

```python
from orchestrator import CityBikesOrchestrator

orchestrator = CityBikesOrchestrator()

# Run specific stage
orchestrator.run_stage('extraction')
orchestrator.run_stage('dbt', full_refresh=True)
orchestrator.run_stage('database_load', skip_verify=True)
```

### Custom Configuration

```python
from pathlib import Path
from orchestrator import CityBikesOrchestrator

config = {
    'skip_extraction_on_error': True,
    'enable_nyc': True,
    'enable_london': True,
}

orchestrator = CityBikesOrchestrator(
    project_root=Path('/custom/path'),
    config=config
)

success = orchestrator.run()
```

## Pipeline Stages

### Stage 1: Extraction

**Duration:** ~20-30 minutes

Extracts bike share data from source systems:
- **NYC:** Downloads ZIP files from CitiBike public S3 bucket
- **London:** Scrapes CSV files from TfL website using Playwright

**Skip this stage** if you're testing or already have recent data:
```bash
python -m orchestrator.cli run --skip-extraction
```

### Stage 2: File Management

**Duration:** ~5-10 minutes

Processes raw files for analytics:
- Extracts ZIPs to CSVs
- Validates schemas (4 types: NYC Legacy/Modern, London Legacy/Modern)
- Converts CSVs to Parquet
- Organizes by schema type in S3

**Note:** This stage is idempotent - safe to re-run.

### Stage 3: Database Load

**Duration:** ~20-30 minutes (first run), ~5 minutes (subsequent)

Loads data into DuckDB:
- Reads Parquet files from S3 (all files, full replace)
- Creates/replaces raw tables
- Validates data integrity (optional)

**Note:** Raw tables are fully replaced each run, but dbt handles incrementals downstream.

### Stage 4: dbt Transformations

**Duration:** ~3-5 minutes (incremental), ~15 minutes (full refresh)

Runs dbt models with incremental strategy:
- **Staging:** Processes only new `source_file` values (incremental)
- **Intermediate:** Combines legacy/modern per city (incremental)
- **Unified:** Combines NYC + London (incremental)
- **Marts:** Aggregates for dashboard (full rebuild)

**Incremental mode** (default):
```bash
python -m orchestrator.cli run
```

**Full refresh mode** (quarterly recommended):
```bash
python -m orchestrator.cli run --dbt-full-refresh
```

### Stage 5: Mart Export

**Duration:** ~1-2 minutes

Exports mart tables to S3 as Parquet:
- `mart_daily_metrics.parquet`
- `mart_hourly_patterns.parquet`
- `mart_station_growth.parquet`
- `mart_nyc_member_analysis.parquet`
- `mart_daily_metrics_long.parquet`

Dashboard reads these files from S3.

## Scheduling

### Cron (Recommended for Monthly Runs)

```bash
# Edit crontab
crontab -e

# Add monthly run (1st of month at 2 AM)
0 2 1 * * cd /home/ubuntu/city-cycles && /home/ubuntu/city-cycles/venv/bin/python -m orchestrator.cli run >> /var/log/city-cycles/pipeline.log 2>&1

# Add quarterly full refresh (1st of Jan/Apr/Jul/Oct at 3 AM)
0 3 1 1,4,7,10 * cd /home/ubuntu/city-cycles && /home/ubuntu/city-cycles/venv/bin/python -m orchestrator.cli run --dbt-full-refresh >> /var/log/city-cycles/pipeline-full.log 2>&1
```

### Systemd Service (Alternative)

```bash
# Create service file: /etc/systemd/system/city-cycles-pipeline.service
[Unit]
Description=City Cycles Monthly Pipeline
After=network.target

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=/home/ubuntu/city-cycles
Environment=PATH=/home/ubuntu/city-cycles/venv/bin:/usr/bin
ExecStart=/home/ubuntu/city-cycles/venv/bin/python -m orchestrator.cli run

[Install]
WantedBy=multi-user.target

# Create timer: /etc/systemd/system/city-cycles-pipeline.timer
[Unit]
Description=Run City Cycles Pipeline Monthly

[Timer]
OnCalendar=monthly
Persistent=true

[Install]
WantedBy=timers.target

# Enable and start
sudo systemctl enable city-cycles-pipeline.timer
sudo systemctl start city-cycles-pipeline.timer
```

## Monitoring

### Check Logs

```bash
# View orchestrator logs
tail -f logs/orchestrator.log

# View dbt logs
tail -f dbt_city_cycles/logs/dbt.log

# View system logs (if using cron)
tail -f /var/log/city-cycles/pipeline.log
```

### Check Database

```bash
# Check raw table status
python -m db_duckdb.cli status

# List tables
python -m db_duckdb.cli list --tables --verbose

# Verify data
python -m db_duckdb.cli verify --detailed
```

### Check S3

```bash
# Check raw files
aws s3 ls s3://city-cycles-data-ctr37/extracted_bike_ride_parquet/nyc/

# Check marts
aws s3 ls s3://city-cycles-data-ctr37/marts/
```

## Error Handling

### Pipeline Failure

If the pipeline fails, check logs for the failing stage:

```bash
# View full logs
cat logs/orchestrator.log | grep ERROR

# Run failed stage manually
python -m orchestrator.cli stage <stage_name> --verbose
```

### Common Issues

#### 1. AWS Credentials Error

```
Error: AWS credentials not found
```

**Solution:**
```bash
# Set credentials in .env
echo "AWS_ACCESS_KEY_ID=your_key" >> .env
echo "AWS_SECRET_ACCESS_KEY=your_secret" >> .env

# Or configure AWS CLI
aws configure
```

#### 2. DuckDB Memory Error

```
Error: Out of memory
```

**Solution:**
```bash
# Increase memory limit in .env
echo "DUCKDB_MEMORY_LIMIT=16GB" >> .env

# Or reduce memory usage
echo "DUCKDB_THREADS=2" >> .env
```

#### 3. dbt Incremental Issues

```
Error: duplicate key value violates unique constraint
```

**Solution:**
```bash
# Run with full refresh
python -m orchestrator.cli stage dbt --full-refresh
```

#### 4. Extraction Timeout

```
Error: Playwright timeout exceeded
```

**Solution:**
```bash
# Skip extraction and use existing data
python -m orchestrator.cli run --skip-extraction

# Or run extraction separately later
python -m orchestrator.cli stage extraction
```

## Performance Optimization

### For Monthly Runs

**Recommended settings:**
```bash
# Standard run
python -m orchestrator.cli run

# Expected duration: ~35 minutes
# - Extraction: 25 min
# - File processing: 5 min
# - Database load: 30 min
# - dbt (incremental): 3 min
# - Export: 2 min
```

### For Testing

**Fast test run:**
```bash
python -m orchestrator.cli run \
  --skip-extraction \
  --skip-verify \
  --skip-export

# Expected duration: ~35 minutes (just raw load + dbt)
```

### For Development

**Iterative development:**
```bash
# Test dbt changes
python -m orchestrator.cli stage dbt

# Test with full refresh
python -m orchestrator.cli stage dbt --full-refresh
```

## Best Practices

### 1. Monthly Full Pipeline

Run complete pipeline monthly:
```bash
# Cron: 1st of each month at 2 AM
0 2 1 * * python -m orchestrator.cli run
```

### 2. Quarterly Full Refresh

Run with `--dbt-full-refresh` quarterly to prevent incremental drift:
```bash
# Cron: 1st of Jan/Apr/Jul/Oct at 3 AM
0 3 1 1,4,7,10 * python -m orchestrator.cli run --dbt-full-refresh
```

### 3. Monitor and Alert

Set up monitoring:
- Check exit codes in cron
- Monitor log file sizes
- Alert on pipeline failures
- Track run duration

### 4. Data Validation

After each run:
```bash
# Check row counts
python -m db_duckdb.cli verify --detailed

# Check latest data dates
# (Add custom validation queries)
```

### 5. Backup Strategy

Regular backups:
```bash
# Backup DuckDB database
cp data/city_cycles.duckdb data/backups/city_cycles_$(date +%Y%m%d).duckdb

# S3 versioning enabled for data files
aws s3api put-bucket-versioning \
  --bucket city-cycles-data-ctr37 \
  --versioning-configuration Status=Enabled
```

## Troubleshooting

### Debug Mode

```bash
# Enable verbose logging
python -m orchestrator.cli run --verbose

# Run with Python debugger
python -m pdb -m orchestrator.cli run
```

### Manual Recovery

If orchestrator fails, run stages manually:

```bash
# 1. Check what's in DuckDB
python -m db_duckdb.cli status

# 2. If raw tables are empty, reload
python -m db_duckdb.cli load

# 3. If dbt failed, re-run
cd dbt_city_cycles
dbt run

# 4. If export failed, re-export
python -m db_duckdb.cli export
```

### Reset Pipeline

Complete reset:
```bash
# 1. Drop all dbt models
cd dbt_city_cycles
dbt run --full-refresh

# 2. Reload raw tables
python -m db_duckdb.cli init
python -m db_duckdb.cli load

# 3. Rebuild everything
python -m orchestrator.cli run --dbt-full-refresh
```

## Architecture

### Design Principles

1. **Separation of Concerns:** Each subsystem is independent
2. **Idempotency:** Safe to re-run stages
3. **Fail-Fast:** Stop on errors, report clearly
4. **Observable:** Comprehensive logging at each stage
5. **Configurable:** Environment-based configuration

### Error Strategy

- **Extraction failures:** Continue if one city fails
- **Processing failures:** Stop pipeline, report error
- **Load failures:** Stop pipeline, preserve data
- **dbt failures:** Stop pipeline, show dbt output
- **Export failures:** Log error, continue

### Logging Strategy

Each stage logs:
- Start/end timestamps
- Success/failure status
- Key metrics (row counts, file counts)
- Error details with stack traces

## Contributing

When adding new stages or modifying the orchestrator:

1. Update `main.py` with new stage method
2. Update CLI with new command/options
3. Update configuration if needed
4. Update this README
5. Test end-to-end
6. Update deployment guide

## References

- [Incremental Pipeline Architecture](../docs/incremental-pipeline-architecture.md)
- [dbt Documentation](../dbt_city_cycles/)
- [DuckDB CLI](../db_duckdb/)
- [Extraction Documentation](../extraction/)
- [File Manager Documentation](../extracted_file_manager/)

## License

See main project LICENSE file.

