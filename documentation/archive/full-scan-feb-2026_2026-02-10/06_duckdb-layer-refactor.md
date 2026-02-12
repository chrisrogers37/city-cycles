# Phase 06: DuckDB Layer Refactor ✅ COMPLETE
**Completed:** 2026-02-11

**PR Title:** `refactor(db): metadata-driven quality checks, fix SQL injection, consolidate CLI patterns`
**Risk Level:** Medium
**Estimated Effort:** Large (4-6 hours)
**Dependencies:** None (Phase 01 touches `duckdb_manager.py` for dead code removal, but these changes target different lines)
**Blocks:** Phase 09 (test coverage)

---

## Summary

The DuckDB layer (`db_duckdb/`) has five categories of tech debt:

1. **SQL injection in credential setup** -- AWS credentials are interpolated into SQL `SET` commands via f-strings (`duckdb_manager.py` lines 64-66).
2. **122-line data quality function** -- `_run_data_quality_checks` in `operations.py` (lines 254-375) uses a long if-elif chain with hardcoded SQL for each table. Replace with a metadata-driven approach.
3. **Duplicated CLI error handling** -- Every CLI command in `cli.py` repeats the same 8-line try/except/log/raise pattern.
4. **Unimplemented TODO stubs** -- `cli.py` lines 325-326 and 332-333 contain TODO placeholders for S3 export listing and mart table listing.
5. **Bare except with swallowed context** -- `operations.py` line 574 catches `Exception` but doesn't log the exception details.

---

## Files Modified

| # | File | Action |
|---|------|--------|
| 1 | `db_duckdb/duckdb_manager.py` | Fix SQL injection in `_setup_s3_access` (lines 64-66) |
| 2 | `db_duckdb/operations.py` | Refactor `_run_data_quality_checks` to metadata-driven; fix bare except on line 574 |
| 3 | `db_duckdb/cli.py` | Extract `_run_cli_operation` helper; implement TODO stubs for `--exports` and `--marts` |

---

## Problem 1: SQL Injection in Credential Setup

### Current code (`db_duckdb/duckdb_manager.py`, lines 53-69)

```python
def _setup_s3_access(self):
    """Configure S3 access for DuckDB."""
    load_dotenv()

    # Get AWS credentials from environment
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    if aws_access_key_id and aws_secret_access_key:
        # Set S3 credentials
        self.con.execute(f"SET s3_region='{aws_region}'")
        self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
        logger.info("S3 access configured")
    else:
        logger.warning("AWS credentials not found. S3 access may be limited.")
```

### Why this is a problem

While these values come from environment variables (not direct user input), they are still interpolated into SQL without any sanitization. If an env var contained a single quote (e.g., a malformed credential), it would break the SQL or potentially allow injection. This is a defense-in-depth concern.

### Fix: Input validation before interpolation

DuckDB's `SET` command does not support parameterized queries (`$1` syntax). DuckDB 1.3.1 (the project's version per `requirements.txt`) does support `CREATE SECRET` but with limited parameter binding for the `SET` variant. The safest approach is to validate that the values contain only characters valid for AWS credentials before interpolating.

**AFTER** (`db_duckdb/duckdb_manager.py`, lines 1-69):

Add a validation helper at the module level (after the `logger` definition, around line 11):

```python
import re

def _validate_aws_credential(value: str, name: str) -> str:
    """Validate that an AWS credential value contains only safe characters.

    AWS access keys contain only alphanumeric characters, forward slashes,
    plus signs, and equals signs. Region names contain only lowercase
    letters, digits, and hyphens.

    Args:
        value: The credential value to validate
        name: Human-readable name for error messages

    Returns:
        The validated value (unchanged)

    Raises:
        ValueError: If the value contains unexpected characters
    """
    if not re.match(r'^[A-Za-z0-9_/+=\-]+$', value):
        raise ValueError(
            f"AWS credential '{name}' contains invalid characters. "
            f"Expected only alphanumeric, _, /, +, =, - characters."
        )
    return value
```

Then update `_setup_s3_access`:

**BEFORE** (lines 62-66):
```python
    if aws_access_key_id and aws_secret_access_key:
        # Set S3 credentials
        self.con.execute(f"SET s3_region='{aws_region}'")
        self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
```

**AFTER:**
```python
    if aws_access_key_id and aws_secret_access_key:
        # Validate credentials before interpolating into SQL
        _validate_aws_credential(aws_region, "AWS_DEFAULT_REGION")
        _validate_aws_credential(aws_access_key_id, "AWS_ACCESS_KEY_ID")
        _validate_aws_credential(aws_secret_access_key, "AWS_SECRET_ACCESS_KEY")

        # Set S3 credentials (SET does not support parameterized queries)
        self.con.execute(f"SET s3_region='{aws_region}'")
        self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
```

### Full diff for `duckdb_manager.py`

The only changes to this file are:

1. Add `import re` at the top (after `import os` on line 3).
2. Add `_validate_aws_credential()` function after the logger definition (around line 11).
3. Add three validation calls before the `SET` statements (lines 62-66 area).

No existing function signatures, return types, or public APIs change.

---

## Problem 2: `_run_data_quality_checks` (operations.py, lines 254-375)

### Current code

The method is 122 lines long with a repeated pattern:

```python
def _run_data_quality_checks(self, table_name: str, db: DuckDBManager) -> Dict:
    quality_checks = {}
    try:
        # Check for null values in key columns
        if table_name == 'raw_nyc_legacy':
            null_check_query = """
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN tripduration IS NULL THEN 1 ELSE 0 END) as null_tripduration,
                    SUM(CASE WHEN bikeid IS NULL THEN 1 ELSE 0 END) as null_bikeid,
                    SUM(CASE WHEN starttime IS NULL THEN 1 ELSE 0 END) as null_starttime,
                    SUM(CASE WHEN stoptime IS NULL THEN 1 ELSE 0 END) as null_stoptime
                FROM raw_nyc_legacy
            """
        elif table_name == 'raw_nyc_modern':
            null_check_query = """..."""  # Same pattern, different columns
        elif table_name == 'raw_london_legacy':
            null_check_query = """..."""  # Same pattern, different columns
        elif table_name == 'raw_london_modern':
            null_check_query = """..."""  # Same pattern, different columns
        else:
            return quality_checks

        # ... null check execution ...

        # Check for duplicate records (another if-elif chain)
        if table_name == 'raw_nyc_modern':
            duplicate_check_query = """..."""
        elif table_name == 'raw_london_legacy':
            duplicate_check_query = """..."""
        # ... etc ...

        # Check date ranges (yet another if-elif chain)
        if table_name == 'raw_nyc_legacy':
            date_range_query = """..."""
        elif table_name == 'raw_nyc_modern':
            date_range_query = """..."""
        # ... etc ...
```

### Fix: Metadata-driven approach

Add a `TABLE_QUALITY_CONFIG` dictionary at the **module level** (after the imports, before the `DuckDBOperations` class definition, around line 17). This dictionary encodes the per-table metadata that the quality checks need.

```python
# Quality check configuration for each raw table.
# Used by _run_data_quality_checks to generate SQL dynamically.
TABLE_QUALITY_CONFIG = {
    'raw_nyc_legacy': {
        'null_check_columns': ['tripduration', 'bikeid', 'starttime', 'stoptime'],
        'duplicate_key': None,  # No single natural key for legacy NYC data
        'date_columns': ('starttime', 'stoptime'),
    },
    'raw_nyc_modern': {
        'null_check_columns': ['ride_id', 'started_at', 'ended_at'],
        'duplicate_key': 'ride_id',
        'date_columns': ('started_at', 'ended_at'),
    },
    'raw_london_legacy': {
        'null_check_columns': ['rental_id', 'bike_id', 'start_date', 'end_date'],
        'duplicate_key': 'rental_id',
        'date_columns': ('start_date', 'end_date'),
    },
    'raw_london_modern': {
        'null_check_columns': ['number', 'bike_number', 'start_date', 'end_date'],
        'duplicate_key': 'number',
        'date_columns': ('start_date', 'end_date'),
    },
}
```

Then replace the 122-line `_run_data_quality_checks` method (lines 254-375) with this implementation:

```python
def _run_data_quality_checks(self, table_name: str, db: DuckDBManager) -> Dict:
    """Run data quality checks for a table using metadata-driven SQL generation.

    Checks performed:
    - Null value counts for key columns
    - Duplicate record counts (where a natural key exists)
    - Date range boundaries
    """
    quality_checks = {}
    config = TABLE_QUALITY_CONFIG.get(table_name)
    if not config:
        return quality_checks

    try:
        # 1. Null checks
        null_cases = ", ".join(
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_{col}"
            for col in config['null_check_columns']
        )
        null_query = f"SELECT COUNT(*) as total_rows, {null_cases} FROM {table_name}"
        null_result = db.execute_query(null_query)
        if null_result:
            quality_checks['null_checks'] = null_result[0]

        # 2. Duplicate checks (only if a natural key is defined)
        if config['duplicate_key']:
            key_col = config['duplicate_key']
            dup_query = f"""
                SELECT COUNT(*) as duplicate_count FROM (
                    SELECT {key_col}, COUNT(*) as cnt
                    FROM {table_name}
                    GROUP BY {key_col}
                    HAVING COUNT(*) > 1
                )
            """
            dup_result = db.execute_query(dup_query)
            if dup_result:
                quality_checks['duplicate_checks'] = dup_result[0]

        # 3. Date range checks
        start_col, end_col = config['date_columns']
        date_query = f"""
            SELECT
                MIN({start_col}) as earliest_date,
                MAX({end_col}) as latest_date
            FROM {table_name}
        """
        date_result = db.execute_query(date_query)
        if date_result:
            quality_checks['date_ranges'] = date_result[0]

    except Exception as e:
        logger.warning(f"Data quality checks failed for {table_name}: {e}")
        quality_checks['error'] = str(e)

    return quality_checks
```

### What changed

- **Lines 254-375 (122 lines)** replaced with **~50 lines** of metadata-driven code.
- The `TABLE_QUALITY_CONFIG` dict is added at module level (~15 lines).
- Net reduction: ~55 lines.
- The behavior is identical: same SQL queries are generated, same result structure is returned.

### Verifying behavioral equivalence

For each table, verify that the generated SQL matches the original hardcoded SQL:

**`raw_nyc_legacy` null check -- original (lines 261-268):**
```sql
SELECT
    COUNT(*) as total_rows,
    SUM(CASE WHEN tripduration IS NULL THEN 1 ELSE 0 END) as null_tripduration,
    SUM(CASE WHEN bikeid IS NULL THEN 1 ELSE 0 END) as null_bikeid,
    SUM(CASE WHEN starttime IS NULL THEN 1 ELSE 0 END) as null_starttime,
    SUM(CASE WHEN stoptime IS NULL THEN 1 ELSE 0 END) as null_stoptime
FROM raw_nyc_legacy
```

**Generated from config `null_check_columns: ['tripduration', 'bikeid', 'starttime', 'stoptime']`:**
```sql
SELECT COUNT(*) as total_rows, SUM(CASE WHEN tripduration IS NULL THEN 1 ELSE 0 END) as null_tripduration, SUM(CASE WHEN bikeid IS NULL THEN 1 ELSE 0 END) as null_bikeid, SUM(CASE WHEN starttime IS NULL THEN 1 ELSE 0 END) as null_starttime, SUM(CASE WHEN stoptime IS NULL THEN 1 ELSE 0 END) as null_stoptime FROM raw_nyc_legacy
```

Semantically identical. The formatting differs (single line vs multi-line) but DuckDB handles both.

**`raw_nyc_legacy` duplicate check -- original (lines 337-338):**
```sql
SELECT 0 as duplicate_count
```

**Generated:** No query is generated because `duplicate_key` is `None`. The `duplicate_checks` key will not appear in the result dict. This is a minor behavior change: previously, `quality_checks['duplicate_checks']` would contain `{'duplicate_count': 0}`; now the key is absent. If downstream code depends on the key being present, add a fallback:

```python
# Optional: preserve original behavior for raw_nyc_legacy
if not config['duplicate_key']:
    quality_checks['duplicate_checks'] = {'duplicate_count': 0}
```

I recommend checking whether `_generate_summary_report` (lines 377-438) or any consumer reads `duplicate_checks` -- if not, the `None` skip is cleaner.

Let me check: in `_generate_summary_report` (line 412-417):
```python
if quality_checks and 'null_checks' in quality_checks:
    null_checks = quality_checks['null_checks']
    report.append("  Data Quality:")
    for key, value in null_checks.items():
        if key != 'total_rows' and value > 0:
            report.append(f"    {key}: {value:,}")
```

The summary report only reads `null_checks`, not `duplicate_checks`. So omitting the key for `raw_nyc_legacy` is safe. No fallback needed.

---

## Problem 3: CLI Error Handling Duplication

### Current pattern (repeated in `init`, `load`, `verify`, `export`, `list`)

Each CLI command follows this exact pattern for the outer try/except:

```python
except Exception as e:
    logger.error("=" * 60)
    logger.error(f"✗ {OPERATION_NAME} FAILED: {e}")
    logger.error("=" * 60)
    raise click.ClickException(str(e))
```

And most commands also have an inner "no results" check:

```python
if not result:
    logger.error("=" * 60)
    logger.error(f"✗ {OPERATION_NAME} FAILED: No results returned")
    logger.error("=" * 60)
    raise click.ClickException(f"{operation_name} failed")
```

This pattern appears at:
- `init` command: lines 84-88
- `load` command: lines 142-146 (no results) and lines 148-152 (exception)
- `verify` command: lines 204-208 (no results) and lines 210-214 (exception)
- `export` command: lines 266-270 (no results) and lines 272-276 (exception)
- `list` command: lines 340-344 (exception)
- `pipeline` command: lines 376-380 (exception)
- `status` command: lines 427-431 (exception)

### Fix: Extract a helper function

Add this function at the module level, after the imports (around line 17):

```python
def _cli_error(operation_name: str, error: Exception = None, message: str = None):
    """Log a CLI operation failure and raise a ClickException.

    Args:
        operation_name: Human-readable name of the operation (e.g., "DATA LOADING")
        error: The exception that caused the failure (optional)
        message: Custom error message (optional, used when there's no exception)
    """
    detail = str(error) if error else (message or "Unknown error")
    logger.error("=" * 60)
    logger.error(f"✗ {operation_name} FAILED: {detail}")
    logger.error("=" * 60)
    raise click.ClickException(detail)
```

Then replace each occurrence. For example:

**BEFORE** (`load` command, lines 142-152):
```python
    else:
        logger.error("=" * 60)
        logger.error("✗ DATA LOADING FAILED: No results returned")
        logger.error("=" * 60)
        raise click.ClickException("Data loading failed")

except Exception as e:
    logger.error("=" * 60)
    logger.error(f"✗ DATA LOADING FAILED: {e}")
    logger.error("=" * 60)
    raise click.ClickException(str(e))
```

**AFTER:**
```python
    else:
        _cli_error("DATA LOADING", message="No results returned")

except click.ClickException:
    raise
except Exception as e:
    _cli_error("DATA LOADING", error=e)
```

**Important:** When replacing the outer `except Exception` block, add a `except click.ClickException: raise` clause BEFORE the generic `except Exception` clause. This prevents re-wrapping a `ClickException` that was raised by the inner check. Without this, the error message gets double-wrapped.

Apply the same pattern to all 7 commands. Here is the mapping:

| Command | Operation name string | Lines to replace |
|---------|----------------------|-----------------|
| `init` | `"INITIALIZATION"` | 84-88 |
| `load` | `"DATA LOADING"` | 142-146, 148-152 |
| `verify` | `"DATA VERIFICATION"` | 204-208, 210-214 |
| `export` | `"EXPORT"` | 266-270, 272-276 |
| `list` | `"LIST"` | 340-344 |
| `pipeline` | `"PIPELINE"` | 376-380 |
| `status` | `"STATUS CHECK"` | 427-431 |

---

## Problem 4: Implement TODO Stubs

### Current code (`db_duckdb/cli.py`, lines 322-334)

```python
if exports:
    logger.info("Existing exports in S3:")
    logger.info("=" * 50)
    # TODO: Implement S3 export listing
    logger.info("  S3 export listing not yet implemented")
    logger.info("")

if marts:
    logger.info("Available mart tables in database:")
    logger.info("=" * 50)
    # TODO: Implement mart table listing
    logger.info("  Mart table listing not yet implemented")
    logger.info("")
```

### Fix for `--exports`: List S3 mart files using boto3

Replace lines 322-327 with:

```python
if exports:
    logger.info("Existing exports in S3:")
    logger.info("=" * 50)
    try:
        import boto3
        from db_duckdb.config.duckdb_config import S3_BUCKET
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix='marts/')
        if 'Contents' in response:
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024 * 1024)
                last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"  s3://{S3_BUCKET}/{obj['Key']} ({size_mb:.1f} MB, {last_modified})")
        else:
            logger.info("  No exports found in S3")
    except Exception as e:
        logger.warning(f"  Could not list S3 exports: {e}")
    logger.info("")
```

### Fix for `--marts`: List mart tables from the database

Replace lines 329-334 with:

```python
if marts:
    logger.info("Available mart tables in database:")
    logger.info("=" * 50)
    try:
        table_info = operations.list_tables()
        all_tables = table_info['available_tables']
        # Mart tables are in the main_marts schema or have a mart_ prefix
        mart_tables = [t for t in all_tables if t.startswith('mart_')]
        # Also check the main_marts schema
        try:
            with DuckDBManager(db_path=db_path) as db:
                schema_tables = db.list_tables(schema='main_marts')
                for t in schema_tables:
                    qualified = f"main_marts.{t}"
                    if qualified not in mart_tables and t not in mart_tables:
                        mart_tables.append(qualified)
        except Exception:
            pass  # Schema may not exist yet

        if mart_tables:
            for table_name in sorted(mart_tables):
                if verbose:
                    try:
                        with DuckDBManager(db_path=db_path) as db:
                            info = db.get_table_info(table_name)
                            logger.info(f"  {table_name} ({info['row_count']:,} rows, {info['size_mb']} MB)")
                    except Exception:
                        logger.info(f"  {table_name}")
                else:
                    logger.info(f"  {table_name}")
        else:
            logger.info("  No mart tables found (run dbt first)")
    except Exception as e:
        logger.warning(f"  Could not list mart tables: {e}")
    logger.info("")
```

**Note:** The `DuckDBManager` import is already available at the top of `cli.py` via:
```python
from .operations import DuckDBOperations
from .pipeline import DuckDBPipeline, run_full_pipeline, check_pipeline_status
```

However, `DuckDBManager` itself is not directly imported in `cli.py`. You need to add:

```python
from .duckdb_manager import DuckDBManager
```

at the top of `cli.py` (around line 12, after the existing imports).

Also add:

```python
from .config.duckdb_config import S3_BUCKET
```

if you prefer to keep the S3 import at the top level rather than inline.

---

## Problem 5: Bare Except with Swallowed Context

### Current code (`db_duckdb/operations.py`, lines 571-575)

```python
try:
    table_info = db.get_table_info(table_name)
    row_count = table_info['row_count']
except Exception:
    logger.error(f"Table {table_name} does not exist in DuckDB")
    return False
```

### Fix

```python
try:
    table_info = db.get_table_info(table_name)
    row_count = table_info['row_count']
except Exception as e:
    logger.error(f"Table {table_name} does not exist in DuckDB: {e}")
    return False
```

This is a one-line change: add `as e` to the `except` clause and append `: {e}` to the log message.

---

## Full Change Summary

### `db_duckdb/duckdb_manager.py`

| Location | Change |
|----------|--------|
| Line 3 (imports) | Add `import re` |
| After line 10 (after `logger =`) | Add `_validate_aws_credential()` function (~15 lines) |
| Lines 62-66 (`_setup_s3_access`) | Add 3 validation calls before the `SET` statements |

### `db_duckdb/operations.py`

| Location | Change |
|----------|--------|
| After line 16 (after imports, before class) | Add `TABLE_QUALITY_CONFIG` dict (~20 lines) |
| Lines 254-375 (`_run_data_quality_checks`) | Replace 122-line method with ~50-line metadata-driven version |
| Line 574 | Change `except Exception:` to `except Exception as e:` and add `{e}` to log |

### `db_duckdb/cli.py`

| Location | Change |
|----------|--------|
| Line 12 (imports) | Add `from .duckdb_manager import DuckDBManager` |
| After line 17 (after logger) | Add `_cli_error()` helper function (~10 lines) |
| Lines 84-88, 142-152, 204-214, 266-276, 340-344, 376-380, 427-431 | Replace error handling with `_cli_error()` calls |
| Lines 322-327 (`--exports` branch) | Replace TODO with boto3 S3 listing |
| Lines 329-334 (`--marts` branch) | Replace TODO with database mart table listing |

---

## Verification Checklist

### 1. Existing CLI tests pass

```bash
python -m pytest tests/test_db_duckdb_cli.py -v
```

All 16 tests should pass. The help text tests verify command names and option names, which have not changed.

### 2. CLI help still works

```bash
python -m db_duckdb.cli --help
```

Should display all 7 commands: `init`, `load`, `verify`, `export`, `list`, `pipeline`, `status`.

### 3. Status command works

```bash
python -m db_duckdb.cli status
```

Should run without errors (even if no database exists yet -- it will report missing tables).

### 4. List command shows actual results

```bash
python -m db_duckdb.cli list --tables --marts --exports --verbose
```

- `--tables` should list raw tables (or "No tables found")
- `--marts` should list mart tables (or "No mart tables found (run dbt first)")
- `--exports` should list S3 files (or "No exports found in S3" or a warning if credentials are missing)

### 5. Credential validation works

```bash
python -c "
from db_duckdb.duckdb_manager import _validate_aws_credential
# Valid credentials should pass
_validate_aws_credential('AKIAIOSFODNN7EXAMPLE', 'test')
_validate_aws_credential('us-east-1', 'region')
print('Valid credentials passed')

# Invalid credential should raise ValueError
try:
    _validate_aws_credential(\"'; DROP TABLE users; --\", 'test')
    print('ERROR: Should have raised ValueError')
except ValueError as e:
    print(f'Correctly rejected: {e}')
"
```

### 6. Quality checks produce same results

If you have a loaded database, verify that the quality check output is identical:

```bash
python -m db_duckdb.cli verify --detailed
```

Compare the output against a run from before the refactor. The null check counts, duplicate counts, and date ranges should be identical.

### 7. Full test suite passes

```bash
python -m pytest tests/ -v
```

---

## What NOT to Do

1. **Do NOT change the DuckDB database schema or table structures.** The raw table schemas in `config/duckdb_config.py` are not modified by this PR.

2. **Do NOT modify the S3 paths or bucket configuration.** The `S3_URIS` dict in `config/duckdb_config.py` is unchanged.

3. **Do NOT change function signatures that are called from `orchestrator/main.py`.** The public API of `DuckDBOperations` (`init_tables`, `load_data`, `verify_data`, `export_marts`, `list_tables`) must keep their current signatures and return types.

4. **Do NOT remove the `dry_run` parameter from any function.** It is used by the orchestrator and CLI.

5. **Do NOT change the click command names or options.** The commands `init`, `load`, `verify`, `export`, `list`, `pipeline`, `status` and their option names must remain identical.

6. **Do NOT change the `execute_query` method signature in `DuckDBManager`.** Other code calls it expecting the current `query: str` interface.

7. **Do NOT add parameterized query support to `DuckDBManager.execute_query`.** The quality check SQL is generated from trusted metadata (the `TABLE_QUALITY_CONFIG` dict), not from user input. Adding parameter support would be a separate, larger change.

8. **Do NOT remove any existing imports from `cli.py`.** Only add the new `DuckDBManager` import.

9. **Do NOT change how `_generate_summary_report` reads the quality check results.** It currently only reads the `null_checks` key, which is still populated by the new code.

---

## Changelog Entry

Add this to `CHANGELOG.md` under `[Unreleased]`:

```markdown
### Fixed
- **SQL Injection in DuckDB Credential Setup** - Added input validation for AWS credentials before SQL interpolation
  - New `_validate_aws_credential()` ensures only safe characters in credential values
  - Prevents potential SQL injection via malformed environment variables

### Changed
- **Data Quality Checks** - Refactored `_run_data_quality_checks` from 122-line if-elif chain to metadata-driven approach
  - Added `TABLE_QUALITY_CONFIG` dictionary defining null-check columns, duplicate keys, and date columns per table
  - Reduced function from 122 lines to ~50 lines with identical behavior
  - New tables can be added by editing config dict instead of writing SQL

### Improved
- **CLI Error Handling** - Consolidated duplicated error handling across 7 CLI commands
  - Extracted `_cli_error()` helper function for consistent error logging and exception raising
  - Added proper `click.ClickException` re-raise to prevent double-wrapping

### Added
- **CLI List Command** - Implemented previously stubbed `--exports` and `--marts` flags
  - `--exports` lists mart Parquet files in S3 with sizes and timestamps
  - `--marts` lists mart tables in the database with optional row counts

### Fixed
- **Bare Exception in Export** - Added exception details to error log in `_export_table_to_s3`
```
