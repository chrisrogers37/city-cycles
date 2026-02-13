# City Cycles System Guide

> Last updated: 2026-01-28
> This document summarizes recent refactoring work and provides guidance for running the system.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests (always do this first)
python -m pytest tests/ -v

# Check pipeline status
python -m orchestrator.cli status

# Run full pipeline (requires AWS credentials)
python -m orchestrator.cli run
```

## Recent Refactoring Summary

### Code Cleanup (Session: 2026-01-28)

**Files Modified:**
- `extracted_file_manager/manager.py` - Fixed bare except clause to use `ClientError`
- `db_duckdb/pipeline.py` - Fixed type hints (`any` → `Any`)
- `data_models/nyc_bike.py` - Removed no-op column renames

**Tests Added:**
- `tests/test_orchestrator.py` - 34 new tests for orchestrator module
- Fixed 2 failing tests in `test_extracted_file_manager_current.py`

**Test Coverage:** 83 tests passing (up from 49)

### Architecture Changes (Prior Session)

The intermediate dbt layer was removed to reduce data duplication:

**Before:**
```
Raw → Staging → Intermediate → Unified → Marts
      (200M)    (200M)         (200M)    (aggregated)
```

**After:**
```
Raw → Staging → Unified → Marts
      (200M)    (200M)    (aggregated)
```

**Deleted Files:**
- `dbt_city_cycles/models/intermediate/int_nyc_rides.sql`
- `dbt_city_cycles/models/intermediate/int_london_rides.sql`

## System Architecture

### Pipeline Stages

| Stage | Module | Description |
|-------|--------|-------------|
| 1. Extraction | `extraction/` | Download bike data from NYC/London to S3 |
| 2. File Management | `extracted_file_manager/` | Unzip, validate schemas, convert to Parquet |
| 3. Database Load | `db_duckdb/` | Load Parquet into DuckDB raw tables |
| 4. dbt Transform | `dbt_city_cycles/` | Run staging → unified → marts |
| 5. Export | `db_duckdb/` | Export marts to S3 for dashboard |

### Data Flow

```
Web Sources (NYC CitiBike, London TfL)
    ↓
S3: extracted_bike_ride_zips/
    ↓
S3: extracted_bike_ride_csvs/
    ↓
S3: extracted_bike_ride_parquet/{city}/{schema}/
    ↓
DuckDB: raw_nyc_legacy, raw_nyc_modern, raw_london_legacy, raw_london_modern
    ↓
dbt: stg_* → unified_rides → mart_*
    ↓
S3: mart_exports/*.parquet
    ↓
Streamlit Dashboard
```

## Commands Reference

### Pipeline Orchestration

```bash
# Run complete pipeline
python -m orchestrator.cli run

# Run with dbt full refresh (use quarterly or after schema changes)
python -m orchestrator.cli run --dbt-full-refresh

# Skip specific stages
python -m orchestrator.cli run --skip-extraction
python -m orchestrator.cli run --skip-export

# Run individual stage
python -m orchestrator.cli stage extraction
python -m orchestrator.cli stage file_management
python -m orchestrator.cli stage database_load
python -m orchestrator.cli stage dbt
python -m orchestrator.cli stage export

# Check status
python -m orchestrator.cli status
```

### Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_orchestrator.py -v

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

### DuckDB Operations

```bash
# Initialize raw tables
python -m db_duckdb.cli init

# Load data from S3
python -m db_duckdb.cli load

# Verify data integrity
python -m db_duckdb.cli verify

# Export marts to S3
python -m db_duckdb.cli export

# List tables
python -m db_duckdb.cli list

# Full ETL pipeline
python -m db_duckdb.cli pipeline

# Check status
python -m db_duckdb.cli status
```

### dbt Operations

```bash
cd dbt_city_cycles

# Run all models
dbt run

# Full refresh (rebuilds all tables)
dbt run --full-refresh

# Run specific model
dbt run --select unified_rides

# Test data quality
dbt test

# Generate docs
dbt docs generate && dbt docs serve
```

## Environment Setup

### Required Environment Variables

Create a `.env` file in project root:

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=city-cycles-data-ctr37
```

### Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install playwright browsers (for London extraction)
python -m playwright install
```

## Data Schemas

### NYC Legacy (pre-2021)
Fields: tripduration, bikeid, starttime, stoptime, start_station_*, end_station_*, usertype, birth_year, gender

### NYC Modern (2021+)
Fields: ride_id, rideable_type, started_at, ended_at, start_station_*, end_station_*, start_lat, start_lng, end_lat, end_lng, member_casual

### London Legacy
Fields: Rental Id, Bike Id, Start Date, End Date, StartStation Id/Name, EndStation Id/Name, Duration

### London Modern
Fields: Number, Bike number, Bike model, Start/End date, Total duration, Start/End station number/name

## Troubleshooting

### Tests Failing
```bash
# Install test dependencies
pip install pytest

# Check for import errors
python -c "from orchestrator.main import CityBikesOrchestrator"
```

### S3 Access Issues
```bash
# Verify credentials
aws s3 ls s3://city-cycles-data-ctr37/

# Check .env file is loaded
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('S3_BUCKET'))"
```

### DuckDB Issues
```bash
# Check database exists
ls -la data/city_cycles.duckdb

# Connect directly
python -c "import duckdb; conn = duckdb.connect('data/city_cycles.duckdb'); print(conn.execute('SHOW TABLES').fetchall())"
```

### dbt Issues
```bash
# Check profiles
cat ~/.dbt/profiles.yml

# Debug connection
cd dbt_city_cycles && dbt debug
```

## Future Work (from ROADMAP.md)

### High Priority
1. **Local S3-Direct Pipeline** - Query S3 parquets directly via DuckDB httpfs (reduces EC2 costs from $130/mo to ~$5/mo)
2. **Cost Optimization** - Stop EC2 when not in use, use spot instances

### Pending Implementation
- `orchestrator/local.py` - Local orchestrator for S3-direct queries
- `db_duckdb/s3_sources.py` - S3 source view management
- Variable materialization in dbt models (view vs incremental)

See `docs/planning/ROADMAP.md` for full details.

## Key Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Project instructions for Claude Code |
| `CHANGELOG.md` | Version history (Keep a Changelog format) |
| `requirements.txt` | Python dependencies |
| `.env` | AWS credentials (DO NOT commit) |
| `docs/planning/ROADMAP.md` | Enhancement roadmap |
| `docs/planning/COST-OPTIMIZATION.md` | AWS cost reduction strategies |

## Git Workflow

```bash
# Check current state
git status
git log --oneline -5

# Feature branch naming
git checkout -b claude/feature-name-XXXXX

# Commit with session link
git commit -m "type: description

https://claude.ai/code/session_XXXXX"

# Push
git push -u origin branch-name
```

---

*This guide is maintained alongside the codebase. Update it when making significant changes.*
