# CLAUDE.md - City Cycles Analytics Project

This file provides project-specific guidance for Claude Code when working on the City Cycles Analytics project. Update this file whenever Claude does something incorrectly so it learns not to repeat mistakes.

## Project Overview

City Cycles is an end-to-end data analytics pipeline comparing bike share systems in NYC (CitiBike) and London (Santander Cycles). The project demonstrates:

- **Data Engineering:** Automated ETL pipeline with schema validation and data transformation
- **Analytics Infrastructure:** DuckDB + dbt for data modeling and analytics
- **Cloud Architecture:** AWS S3 storage + EC2 orchestration
- **Data Visualization:** Streamlit dashboard deployed on Streamlit Cloud
- **Automation:** Monthly batch processing with cron scheduling

### Tech Stack
- **Languages:** Python 3.8+
- **Database:** DuckDB (embedded analytics database)
- **Transformation:** dbt (Data Build Tool)
- **Cloud:** AWS S3, AWS EC2
- **Testing:** pytest
- **Dashboard:** Streamlit + Plotly
- **Data Processing:** pandas, pyarrow
- **Validation:** pydantic (data models)
- **Web Scraping:** playwright

### Project Architecture

```
extraction/                  → Download bike data from web to S3
    ↓
extracted_file_manager/     → Process ZIPs, validate schemas, convert to Parquet
    ↓
db_duckdb/                  → Load Parquet files into DuckDB raw tables
    ↓
dbt_city_cycles/            → Transform raw data into analytics marts
    ↓
dashboard/                  → Visualize data in Streamlit dashboard
```

**Orchestration:** `orchestrator/` coordinates the entire pipeline from a single entry point

## Development Workflow

### Verification Loop (CRITICAL for Quality)

Always give Claude a way to verify its work. Run these checks after making changes:

1. **Run tests:** `python -m pytest tests/ -v`
2. **Type checking:** `python -m mypy <module_name>/` (if mypy is configured)
3. **Data validation:** Test with sample data before running full pipeline
4. **Pipeline stages:** Test individual orchestrator stages before full runs
5. **Code formatting:** Code is auto-formatted on Write/Edit via PostToolUse hook

### Testing Strategy

- Write tests for new data models and transformations
- Use pytest fixtures for test data
- Mock S3 and AWS operations in tests
- Test schema validation thoroughly (data_models)
- Test idempotent operations (extracted_file_manager)

## Code Style & Conventions

### Python Style
- Follow PEP 8 conventions
- Use type hints for function signatures
- Prefer `dataclass` or `pydantic` models for structured data
- Use descriptive variable names (no single-letter vars except loop counters)
- Keep functions focused and single-purpose
- Write docstrings for modules, classes, and non-obvious functions

### Data Engineering Conventions
- **Idempotency:** All operations should be safely re-runnable
- **Schema Validation:** Always validate data against expected schemas before processing
- **Memory Management:** Use pandas chunking for large files (extracted_file_manager patterns)
- **Error Handling:** Log errors with context, don't swallow exceptions
- **File Organization:** Organize S3 data by schema type (nyc_legacy, nyc_modern, london_legacy, london_modern)

### dbt Conventions
- Use incremental models for fact tables
- Stage all raw data in `staging/` models
- Build unified datasets in `intermediate/` models
- Create analytics-ready tables in `marts/`
- Document models with descriptions and column definitions

## Project Structure

### Core Modules

| Module | Purpose | Key Files |
|--------|---------|-----------|
| `orchestrator/` | Pipeline coordination & CLI | `cli.py`, `pipeline.py` |
| `extraction/` | Download bike data to S3 | `extract_nyc_data.py`, `extract_london_data.py` |
| `extracted_file_manager/` | ZIP → CSV → Parquet processing | `file_processor.py` |
| `data_models/` | Schema validation (pydantic) | `nyc_models.py`, `london_models.py` |
| `db_duckdb/` | DuckDB ETL operations | `load_raw_tables.py`, `export_marts.py` |
| `dbt_city_cycles/` | dbt transformations | `models/staging/`, `models/marts/` |
| `dashboard/` | Streamlit dashboard | `streamlit_app.py` (likely in streamlit_data_manager/) |
| `tests/` | pytest test suite | Test files for each module |

### Important Files
- `requirements.txt` — Python dependencies
- `.env` — AWS credentials and configuration (DO NOT commit)
- `README.md` — Project documentation
- `city-cycles-ec2-key.pem` — EC2 SSH key (DO NOT commit or expose)

## Commands Reference

### Testing & Validation
```bash
# Run full test suite
python -m pytest tests/ -v

# Run tests for specific module
python -m pytest tests/test_data_models.py -v

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

### Pipeline Orchestration
```bash
# Run full pipeline
python -m orchestrator.cli run

# Run with dbt full refresh (quarterly)
python -m orchestrator.cli run --dbt-full-refresh

# Run individual stage
python -m orchestrator.cli stage extract
python -m orchestrator.cli stage file_management
python -m orchestrator.cli stage database_load
python -m orchestrator.cli stage dbt
python -m orchestrator.cli stage mart_export

# Check pipeline status
python -m orchestrator.cli status
```

### Data Extraction
```bash
# Extract NYC data
python -m extraction.extract_nyc_data

# Extract London data
python -m extraction.extract_london_data
```

### File Processing
```bash
# Extract ZIPs to CSVs
python -m extracted_file_manager.cli extract_zips

# Convert CSVs to Parquet
python -m extracted_file_manager.cli convert_csvs
```

### DuckDB Operations
```bash
# Load raw tables
python -m db_duckdb.load_raw_tables

# Export marts to S3
python -m db_duckdb.export_marts
```

### dbt Operations
```bash
# Navigate to dbt directory
cd dbt_city_cycles

# Run dbt models
dbt run

# Run with full refresh
dbt run --full-refresh

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Git Workflow
```bash
git status              # Check current state
git diff                # Review changes
git log --oneline -10   # View recent commits
gh pr create            # Create pull request (using GitHub CLI)
```

## Things Claude Should NOT Do

### Security & Safety
- **NEVER** commit `.env` files or expose AWS credentials
- **NEVER** commit `city-cycles-ec2-key.pem` or any SSH keys
- **NEVER** run destructive commands without explicit user approval (e.g., delete S3 buckets, drop tables)
- **NEVER** modify production EC2 cron jobs without user review

### Code Quality
- Don't use `any` type hints without explicit approval
- Don't skip error handling in ETL operations (silent failures cause data issues)
- Don't bypass schema validation in data_models
- Don't remove idempotency checks in file processing
- Don't commit without running tests first
- Don't make breaking changes to data models without updating downstream consumers

### Data Engineering Best Practices
- Don't load data without schema validation
- Don't process large files without memory management (use pandas chunking)
- Don't skip file existence checks (they enable idempotency)
- Don't modify raw data tables directly (use dbt transformations)
- Don't hardcode AWS bucket names or paths (use environment variables)

## Project-Specific Patterns

### Schema Evolution
When bike share data schemas change:
1. Create new model class in `data_models/` (e.g., `NYCModernBikeShareRecord`)
2. Update model registry in `data_models/__init__.py`
3. Update file processor to detect new schema
4. Create corresponding dbt staging model
5. Update intermediate/marts to incorporate new schema

### Adding New Data Sources
1. Create extraction script in `extraction/`
2. Define pydantic model in `data_models/`
3. Add to file processor schema detection
4. Create DuckDB raw table
5. Create dbt staging model
6. Integrate into existing marts or create new ones

### AWS S3 Structure
```
s3://city-cycles-bucket/
├── extracted_bike_ride_zips/nyc/*.zip
├── extracted_bike_ride_csvs/
│   ├── nyc/*.csv
│   └── london/*.csv
├── extracted_bike_ride_parquet/
│   ├── nyc_legacy/*.parquet
│   ├── nyc_modern/*.parquet
│   ├── london_legacy/*.parquet
│   └── london_modern/*.parquet
└── mart_exports/*.parquet
```

### Memory Management Pattern (from extracted_file_manager)
```python
# Use pandas chunking for large files
for chunk in pd.read_csv(csv_path, chunksize=100_000):
    # Process chunk
    # Monitor memory with psutil
    # Clean up with gc.collect() if needed
```

### Idempotency Pattern (from extracted_file_manager)
```python
# Check if output exists before processing
if not s3_client.exists(output_path):
    process_file(input_path, output_path)
else:
    logger.info(f"Output already exists, skipping: {output_path}")
```

## Environment Setup

### Local Development
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On macOS/Linux

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env  # If .env.example exists
# Edit .env with AWS credentials

# Install playwright browsers (for London extraction)
python -m playwright install
```

### Environment Variables (.env)
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=city-cycles-bucket
```

## Common Issues & Solutions

### Issue: Out of Memory during CSV processing
**Solution:** Use pandas chunking with smaller chunk sizes (see `extracted_file_manager/file_processor.py`)

### Issue: Schema validation failures
**Solution:** Check if upstream data source changed schema, create new model if needed

### Issue: dbt incremental models not updating
**Solution:** Run with `--full-refresh` flag quarterly or when schema changes

### Issue: S3 access denied errors
**Solution:** Check `.env` file for correct AWS credentials, verify IAM permissions

### Issue: Playwright browser download failures
**Solution:** Run `python -m playwright install` to download browser binaries

---

## Documentation References

- `orchestrator/README.md` — Orchestrator usage and pipeline coordination
- `data_models/README.md` — Schema validation and data models
- `db_duckdb/README.md` — DuckDB ETL pipeline
- `extracted_file_manager/README.md` — File processing pipeline
- `docs/incremental-processing-guide.md` — Monthly update strategy
- `docs/ec2-deployment-guide.md` — Production deployment guide

---

_Update this file continuously. Every mistake Claude makes is a learning opportunity. When reviewing PRs, add learnings to this file so the whole team benefits._
