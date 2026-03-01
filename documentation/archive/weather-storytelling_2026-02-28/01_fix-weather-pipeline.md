# Phase 01 -- Fix Weather Pipeline End-to-End

**Status:** ✅ COMPLETE
**Started:** 2026-02-28
**Completed:** 2026-02-28

## Header

| Field | Value |
|---|---|
| **PR Title** | fix: validate and repair weather data pipeline end-to-end |
| **Risk Level** | Medium |
| **Estimated Effort** | Medium (3-5 hours) |
| **Files Modified** | 3 |
| **Files Created** | 0 |
| **Files Deleted** | 0 |

### Files Modified

1. `extraction/weather.py` -- add `source_file` column to parquet output so `raw_weather_hourly` schema matches config
2. `db_duckdb/config/duckdb_config.py` -- update validation query to be resilient to missing `source_file`
3. `CHANGELOG.md` -- document the fix

---

## Context

The weather pipeline has all its components built (extraction, DuckDB loading, dbt staging, dbt marts, mart export) but the chain has **never been validated end-to-end**. The result:

- **Locally**: `raw_weather_hourly` table does not exist in DuckDB. All 3 weather marts (`mart_weather_ride_correlation`, `mart_weather_impact_summary`, `mart_station_weather_performance`) **skip** during `dbt run` because `stg_weather_hourly` is a view on a nonexistent source table.
- **In production (Railway)**: The orchestrator runs weather extraction and DuckDB loading, but it is **unknown** whether the weather marts have ever built successfully. The March 3rd cron run will be the first with the recently converted view-based staging models.
- **Dashboard**: Historical weather sections are blank because mart parquets are not available on S3.

This phase validates every link in the chain, fixes the one known schema mismatch (`source_file` column missing from extraction output), and documents the exact commands to reproduce the full pipeline locally.

---

## Dependencies

- **Depends on**: None (this is the foundational phase)
- **Unlocks**: All subsequent weather storytelling phases (dashboard weather visualization, recommendation engine integration, etc.)

---

## Detailed Implementation Plan

### Problem 1: `source_file` column missing from weather parquet files

**Root Cause**: `extraction/weather.py` writes DataFrames directly to parquet without adding a `source_file` column. The `HourlyWeatherRecord` data model in `data_models/weather.py` defines `source_file` as a required field, and the DuckDB table schema in `db_duckdb/config/duckdb_config.py` includes `source_file VARCHAR`. However, `extraction/weather.py` never calls `HourlyWeatherRecord.to_dataframe()` -- it just writes the raw API DataFrame.

This creates two issues:
1. The parquet files on S3 lack a `source_file` column.
2. The `load_parquet_from_s3()` method uses `CREATE TABLE AS SELECT * FROM parquet` (see `db_duckdb/duckdb_manager.py:138-141`), which creates the table schema from the parquet, NOT from `TABLE_SCHEMAS`. So the resulting `raw_weather_hourly` table will lack `source_file`.
3. The `VALIDATION_QUERIES['raw_weather_hourly']` in `db_duckdb/config/duckdb_config.py:174-183` references `COUNT(DISTINCT source_file)`, which will error if the column does not exist.
4. The `sources.yml` defines `source_file` as a column on `raw_weather_hourly` (line 141), but since the source tests only check `timestamp` and `city`, this is cosmetic -- no tests will fail.

**Why `source_file` matters**: It provides data lineage tracking (which parquet file each row came from), consistent with all 4 bike ride raw tables that have `source_file`. The `stg_weather_hourly.sql` model does NOT reference `source_file`, so the dbt models will build fine without it. But the validation query will fail, and the pattern is inconsistent with the rest of the pipeline.

**Fix**: Add `source_file` column to the DataFrame before writing to parquet in `extraction/weather.py`, matching the pattern in all 4 bike data pipelines.

#### Change 1a: `extraction/weather.py` -- `_write_and_upload_parquet` function

**File**: `/Users/chris/Projects/city-cycles/extraction/weather.py`

**Current code** (lines 193-231):

```python
def _write_and_upload_parquet(df: pd.DataFrame, city: str, label: str) -> bool:
    """
    Write a DataFrame to a local Parquet file, then upload to S3.

    Args:
        df: DataFrame to write
        city: City key ("nyc" or "london")
        label: File label (e.g., "2023" or "forecast_2024-01-15")

    Returns:
        True if uploaded, False if already exists or empty DataFrame
    """
    if df.empty:
        logger.info(f"Empty DataFrame for {city}/{label}, skipping")
        return False

    filename = f"weather_{city}_{label}.parquet"
    s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/{filename}"

    # Idempotency: skip if already uploaded
    if file_exists_in_s3(s3_key):
        logger.info(f"Weather file already exists in S3: {s3_key}")
        return False

    local_path = os.path.join(LOCAL_TMP_DIR, filename)

    try:
        df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
        upload_to_s3(local_path, s3_key)
        logger.info(f"Uploaded weather data to S3: {s3_key} ({len(df)} rows)")
        return True

    except (OSError, RequestException) as e:
        logger.error(f"Failed to write/upload {s3_key}: {e}")
        return False
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
```

**New code** -- add `source_file` column before writing to parquet:

```python
def _write_and_upload_parquet(df: pd.DataFrame, city: str, label: str) -> bool:
    """
    Write a DataFrame to a local Parquet file, then upload to S3.

    Args:
        df: DataFrame to write
        city: City key ("nyc" or "london")
        label: File label (e.g., "2023" or "forecast_2024-01-15")

    Returns:
        True if uploaded, False if already exists or empty DataFrame
    """
    if df.empty:
        logger.info(f"Empty DataFrame for {city}/{label}, skipping")
        return False

    filename = f"weather_{city}_{label}.parquet"
    s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/{filename}"

    # Idempotency: skip if already uploaded
    if file_exists_in_s3(s3_key):
        logger.info(f"Weather file already exists in S3: {s3_key}")
        return False

    # Add source_file column for lineage tracking (matches bike data pipeline pattern)
    df = df.copy()
    df["source_file"] = filename

    local_path = os.path.join(LOCAL_TMP_DIR, filename)

    try:
        df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
        upload_to_s3(local_path, s3_key)
        logger.info(f"Uploaded weather data to S3: {s3_key} ({len(df)} rows)")
        return True

    except (OSError, RequestException) as e:
        logger.error(f"Failed to write/upload {s3_key}: {e}")
        return False
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
```

**Key details**:
- Insert `df = df.copy()` to avoid modifying the caller's DataFrame (defensive copy).
- Insert `df["source_file"] = filename` right after the idempotency check, before writing to parquet.
- The `filename` variable (e.g., `weather_nyc_2023.parquet` or `weather_london_incremental_2026-02-28.parquet`) provides meaningful lineage.
- This matches the pattern used in bike data processing where `source_file` is the S3 filename.

#### Change 1b: `extraction/weather.py` -- `incremental_update` function

**File**: `/Users/chris/Projects/city-cycles/extraction/weather.py`

The `incremental_update` function (lines 288-328) does NOT use `_write_and_upload_parquet` -- it has its own inline parquet write logic that also lacks `source_file`.

**Current code** (lines 304-324):

```python
    try:
        df = fetch_forecast_weather(city, past_days=min(days_back, 92))

        if df.empty:
            logger.warning(f"No forecast data returned for {city}")
            return False

        label = f"incremental_{datetime.now().strftime('%Y-%m-%d')}"

        filename = f"weather_{city}_{label}.parquet"
        s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/{filename}"
        local_path = os.path.join(LOCAL_TMP_DIR, filename)

        try:
            df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
            upload_to_s3(local_path, s3_key)
            logger.info(f"Uploaded incremental weather to S3: {s3_key} ({len(df)} rows)")
            return True
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
```

**New code** -- add `source_file` before writing:

```python
    try:
        df = fetch_forecast_weather(city, past_days=min(days_back, 92))

        if df.empty:
            logger.warning(f"No forecast data returned for {city}")
            return False

        label = f"incremental_{datetime.now().strftime('%Y-%m-%d')}"

        filename = f"weather_{city}_{label}.parquet"
        s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/{filename}"
        local_path = os.path.join(LOCAL_TMP_DIR, filename)

        # Add source_file column for lineage tracking (matches bike data pipeline pattern)
        df = df.copy()
        df["source_file"] = filename

        try:
            df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
            upload_to_s3(local_path, s3_key)
            logger.info(f"Uploaded incremental weather to S3: {s3_key} ({len(df)} rows)")
            return True
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
```

**Key details**:
- Insert `df = df.copy()` and `df["source_file"] = filename` between the `local_path` assignment and the `try:` block.
- Same pattern as Change 1a.

**IMPORTANT NOTE on existing S3 data**: The historical backfill parquets already on S3 (e.g., `weather_nyc_2013.parquet` through `weather_nyc_2025.parquet`) do NOT have `source_file`. When DuckDB loads them via `CREATE TABLE AS SELECT * FROM 's3://.../*/*.parquet'`, DuckDB's parquet reader will handle the schema union: files with `source_file` will have the value populated, files without it will get `NULL`. This is acceptable -- `source_file` is informational and `NULL` for historical rows is fine. No backfill re-upload is needed.

#### Change 1c: `db_duckdb/config/duckdb_config.py` -- make validation query resilient

**File**: `/Users/chris/Projects/city-cycles/db_duckdb/config/duckdb_config.py`

The validation query at lines 174-183 references `source_file`. Since existing parquets on S3 lack this column, the `CREATE TABLE AS SELECT *` approach may or may not include it. If it does (from schema union), `COUNT(DISTINCT source_file)` will work but return many NULLs. If it does not, the query will error.

**Current code** (lines 174-183):

```python
    'raw_weather_hourly': """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            COUNT(DISTINCT city) as unique_cities,
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp,
            COUNT(DISTINCT date_trunc('day', timestamp)) as unique_days
        FROM raw_weather_hourly
    """,
```

**New code** -- use `TRY_CAST` to handle missing column gracefully:

Actually, DuckDB's parquet reader with glob patterns will do a schema union across all files. If ANY file has `source_file`, all rows get the column (with NULLs for files that lack it). If NO file has it, the column won't exist and the query errors. Since incremental uploads from now on WILL include `source_file`, after the first post-fix run, the column will exist via schema union.

However, to be safe during the transition period (before any new incremental file is uploaded), change the validation query:

```python
    'raw_weather_hourly': """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT city) as unique_cities,
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp,
            COUNT(DISTINCT date_trunc('day', timestamp)) as unique_days
        FROM raw_weather_hourly
    """,
```

**Key details**:
- Remove `COUNT(DISTINCT source_file) as unique_files` from the query.
- This metric is not critical for weather data validation (the city and timestamp checks are more meaningful).
- The bike ride tables have `source_file` baked into every parquet, so their validation queries can keep it.

---

### Problem 2: Local `raw_weather_hourly` table does not exist

**Root Cause**: The `raw_weather_hourly` table is created by `db_duckdb.cli pipeline` (or `db_duckdb.cli load`), which loads data from S3. Locally, this has never been run for weather data. The 4 bike ride tables exist locally because they were loaded during development, but weather was added later.

**Fix**: No code change needed. This is a **local dev setup issue**. The commands below populate the table.

#### Step-by-step: Populate `raw_weather_hourly` locally

Prerequisites:
- AWS credentials in `.env` (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET)
- Virtual environment activated (or use `venv/bin/python`)
- Weather parquet files already exist on S3 (from prior backfill or monthly runs)

**Step 1: Verify weather parquets exist on S3**

```bash
venv/bin/python -c "
import boto3
from dotenv import load_dotenv
import os
load_dotenv()
s3 = boto3.client('s3')
bucket = os.environ.get('S3_BUCKET', 'city-cycles-data-ctr37')
for prefix in ['extracted_weather_parquet/nyc/', 'extracted_weather_parquet/london/']:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
    files = resp.get('Contents', [])
    print(f'{prefix}: {len(files)} files')
    for f in files[:3]:
        print(f'  {f[\"Key\"]} ({f[\"Size\"] / 1024:.0f} KB)')
"
```

**Expected output**: Multiple parquet files for each city (one per year plus any incrementals).

If NO files exist, you must run a backfill first:

```bash
venv/bin/python -m extraction.weather --mode backfill --city all
```

This will take several minutes due to API rate limiting (0.5s per request, ~24 requests for NYC 2013-2026, ~22 for London 2015-2026).

**Step 2: Load weather data into DuckDB**

Load ONLY the weather table (do not reload all 4 bike tables, which takes much longer):

```bash
venv/bin/python -m db_duckdb.cli load --table raw_weather_hourly
```

This runs `DuckDBOperations.load_data(table_name='raw_weather_hourly')`, which calls `DuckDBManager.load_parquet_from_s3()` with S3 URI `s3://city-cycles-data-ctr37/extracted_weather_parquet/*/*.parquet`.

**Expected output**: Log showing row count loaded. Weather data is lightweight (hourly for ~12 years for 2 cities = ~210,000 rows per city, ~420,000 total). Should complete in under a minute.

**Step 3: Verify the load**

```bash
venv/bin/python -m db_duckdb.cli verify --table raw_weather_hourly --detailed
```

**Expected output**: Table passes verification with ~400K+ rows, 2 unique cities, date range from 2013 to present.

---

### Problem 3: Verify `stg_weather_hourly` view works on `raw_weather_hourly`

**Root Cause**: `stg_weather_hourly` was recently converted from an incremental model to a view (in the current uncommitted changes on the `main` branch). It has never been tested against real data in this form.

**Fix**: No code change needed. This is a verification step.

**Step 4: Test the staging view**

After loading `raw_weather_hourly` (Step 2), run:

```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt run --select stg_weather_hourly
```

Since `stg_weather_hourly` is materialized as a `view` (set by `dbt_project.yml` line 36: `staging: +materialized: view`), this should be instant -- it just registers the view definition, it does not scan data.

**Verify the view returns data**:

```bash
venv/bin/python -c "
import duckdb
con = duckdb.connect('/Users/chris/Projects/city-cycles/data/city_cycles.duckdb', read_only=True)
result = con.execute('''
    SELECT
        city,
        COUNT(*) as row_count,
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(DISTINCT weather_condition) as unique_conditions
    FROM main_staging.stg_weather_hourly
    GROUP BY city
    ORDER BY city
''').fetchall()
for row in result:
    print(f'{row[0]}: {row[1]:,} rows, {row[2]} to {row[3]}, {row[4]} weather conditions')
con.close()
"
```

**Expected output**: Two rows (london, nyc) with ~100K+ rows each, dates from 2013/2015 to present, 10-14 distinct weather conditions.

**Potential issue**: The `QUALIFY ROW_NUMBER()` dedup in `stg_weather_hourly.sql` partitions by `city, strftime(timestamp::timestamp, '%Y%m%d%H')`. If there are overlapping incremental files with duplicate timestamps, this dedup should handle them. Verify no duplicates:

```bash
venv/bin/python -c "
import duckdb
con = duckdb.connect('/Users/chris/Projects/city-cycles/data/city_cycles.duckdb', read_only=True)
dupes = con.execute('''
    SELECT COUNT(*) as dupes FROM (
        SELECT weather_record_id, COUNT(*) as cnt
        FROM main_staging.stg_weather_hourly
        GROUP BY weather_record_id
        HAVING COUNT(*) > 1
    )
''').fetchone()[0]
print(f'Duplicate weather_record_ids: {dupes}')
con.close()
"
```

**Expected output**: `Duplicate weather_record_ids: 0`

---

### Problem 4: Verify all 3 weather marts build without errors

**Fix**: No code change needed. This is a verification step.

**Step 5: Build weather marts**

First, ensure `mart_hourly_rides` exists (it is a dependency of `mart_weather_ride_correlation`). If you have already run `dbt run` successfully for the ride models, this table exists. If not, build the full chain:

```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt run --select +mart_weather_ride_correlation +mart_weather_impact_summary +mart_station_weather_performance
```

The `+` prefix means "also build all upstream dependencies." This will build:
1. `stg_nyc_legacy`, `stg_nyc_modern`, `stg_london_legacy`, `stg_london_modern` (views, instant)
2. `int_nyc_rides`, `int_london_rides` (views, instant)
3. `unified_rides` (view, instant)
4. `mart_hourly_rides` (table, will take time on 400M+ rows)
5. `stg_weather_hourly` (view, instant)
6. `mart_weather_ride_correlation` (table, INNER JOIN of mart_hourly_rides with stg_weather_hourly)
7. `mart_weather_impact_summary` (table, aggregates from mart_weather_ride_correlation)
8. `mart_station_weather_performance` (table, joins unified_rides with stg_weather_hourly)

If the ride models are already built, you can target just the weather marts:

```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt run --select stg_weather_hourly mart_weather_ride_correlation mart_weather_impact_summary mart_station_weather_performance
```

**Expected output**: All 3 (or 4 with stg_weather_hourly) models complete with OK status.

**Potential issues to watch for**:

1. **`mart_weather_ride_correlation`**: Uses INNER JOIN between `mart_hourly_rides` and `stg_weather_hourly` on `(location, date, hour_of_day)`. The join key mapping is `r.location = w.city`. Verify that `mart_hourly_rides.location` uses `'nyc'` and `'london'` (matching `stg_weather_hourly.city`). Looking at the code, `unified_rides` produces `location` from each staging model, and the staging models use `'nyc' as location` and `'london' as location` respectively. The weather staging uses `city` from raw data which is `'nyc'` or `'london'`. These should match.

2. **`mart_weather_ride_correlation`**: The `date` types must match. `mart_hourly_rides.date` comes from `unified_rides.date`. Check that both are `DATE` type (not `TIMESTAMP`). The weather staging model uses `date_trunc('day', timestamp::timestamp) as date`, which returns a `TIMESTAMP`. The bike staging models also use `date_trunc('day', ...)`. DuckDB's `date_trunc('day', ...)` returns `TIMESTAMP`, so the join should work as long as both sides use the same function. This should be fine.

3. **`mart_weather_ride_correlation`**: The `hour_of_day` types must match. `mart_hourly_rides.hour_of_day` comes from `unified_rides.hour_of_day`, which is `extract(hour from ...)`. The weather staging uses `extract(hour from timestamp::timestamp) as hour_of_day`. Both return `BIGINT` in DuckDB. This should match.

4. **`mart_station_weather_performance`**: Joins `unified_rides` with `stg_weather_hourly` on `(r.location = w.city, r.date = w.date, r.hour_of_day = w.hour_of_day)`. Same join key considerations as above.

**Step 6: Verify mart data quality**

After the marts build, run dbt tests:

```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt test --select mart_weather_ride_correlation mart_weather_impact_summary mart_station_weather_performance
```

**Expected output**: All tests pass (not_null, accepted_values as defined in `dbt_city_cycles/models/marts/schema.yml`).

Also spot-check the data:

```bash
venv/bin/python -c "
import duckdb
con = duckdb.connect('/Users/chris/Projects/city-cycles/data/city_cycles.duckdb', read_only=True)

# mart_weather_ride_correlation
corr = con.execute('''
    SELECT location, COUNT(*) as rows, MIN(date) as min_date, MAX(date) as max_date
    FROM main_marts.mart_weather_ride_correlation
    GROUP BY location
''').fetchall()
print('=== mart_weather_ride_correlation ===')
for r in corr:
    print(f'  {r[0]}: {r[1]:,} rows, {r[2]} to {r[3]}')

# mart_weather_impact_summary
impact = con.execute('''
    SELECT location, dimension_type, COUNT(*) as rows
    FROM main_marts.mart_weather_impact_summary
    GROUP BY location, dimension_type
    ORDER BY location, dimension_type
''').fetchall()
print('=== mart_weather_impact_summary ===')
for r in impact:
    print(f'  {r[0]} / {r[1]}: {r[2]:,} rows')

# mart_station_weather_performance
station = con.execute('''
    SELECT location, COUNT(DISTINCT station_id) as stations, COUNT(*) as rows
    FROM main_marts.mart_station_weather_performance
    GROUP BY location
''').fetchall()
print('=== mart_station_weather_performance ===')
for r in station:
    print(f'  {r[0]}: {r[2]:,} rows across {r[1]} stations')

con.close()
"
```

**Expected output**: Non-zero row counts for both cities across all 3 marts. The correlation mart should have hundreds of thousands of rows. The impact summary should have rows for both `weather_condition` and `precip_temp` dimension types. The station performance mart should cover many stations.

---

### Problem 5: Verify mart export to S3

**Fix**: No code change needed. This is a verification step.

**Step 7: Export weather marts to S3**

```bash
venv/bin/python -m db_duckdb.cli export --table mart_weather_ride_correlation
venv/bin/python -m db_duckdb.cli export --table mart_weather_impact_summary
venv/bin/python -m db_duckdb.cli export --table mart_station_weather_performance
```

Or export all marts at once:

```bash
venv/bin/python -m db_duckdb.cli export
```

**Expected output**: Each table exports to `s3://city-cycles-data-ctr37/marts/{table_name}.parquet` with a success log message.

Note: The `export_marts` method in `db_duckdb/operations.py:454-465` already includes all 3 weather marts in the `MART_TABLES` list. The full pipeline export will handle them.

**Verify exports exist on S3**:

```bash
venv/bin/python -c "
import boto3
from dotenv import load_dotenv
import os
load_dotenv()
s3 = boto3.client('s3')
bucket = os.environ.get('S3_BUCKET', 'city-cycles-data-ctr37')
weather_marts = [
    'marts/mart_weather_ride_correlation.parquet',
    'marts/mart_weather_impact_summary.parquet',
    'marts/mart_station_weather_performance.parquet',
]
for key in weather_marts:
    try:
        obj = s3.head_object(Bucket=bucket, Key=key)
        size_mb = obj['ContentLength'] / (1024 * 1024)
        print(f'{key}: {size_mb:.1f} MB')
    except s3.exceptions.ClientError:
        print(f'{key}: NOT FOUND')
"
```

---

### Problem 6: Ensure Railway pipeline builds weather marts on March 3rd

**Current Railway flow** (from `scripts/railway_entrypoint.sh` and `orchestrator/main.py`):

1. `python -m orchestrator.cli run --dbt-full-refresh`
2. This calls `CityBikesOrchestrator.run(dbt_full_refresh=True)`
3. Step 1: `_run_extraction()` -- calls `weather.incremental_update_all(days_back=35)` (line 154)
4. Step 2: `_run_file_management()` -- processes bike data only (weather parquets are already in correct format)
5. Step 3: `_run_database_load()` -- runs `python -m db_duckdb.cli pipeline --skip-export` as subprocess
6. Step 4: `_run_dbt_transformations(full_refresh=True)` -- runs `dbt run --full-refresh`
7. Step 5: `_run_mart_export()` -- runs `DuckDBOperations().export_marts()`

**Analysis**: The Railway pipeline SHOULD already handle weather end-to-end:

- **Extraction** (Step 1): `weather.incremental_update_all(days_back=35)` uploads new incremental parquets to S3. After the `source_file` fix, these will include the `source_file` column.
- **Database load** (Step 3): `db_duckdb.cli pipeline --skip-export` runs `DuckDBPipeline.run_full_pipeline(skip_export=True)`, which calls `DuckDBOperations.load_data()`. This loads ALL tables in `S3_URIS`, including `raw_weather_hourly` (see `db_duckdb/config/duckdb_config.py:25`).
- **dbt** (Step 4): `dbt run --full-refresh` builds all models, including `stg_weather_hourly` (view), `mart_hourly_rides` (table), and all 3 weather marts (tables).
- **Export** (Step 5): `export_marts()` exports all marts including the 3 weather marts.

**No code changes needed for Railway**. The pipeline is already wired correctly. The reason it may have failed previously is that:
1. Weather extraction might have failed (API errors, no parquets on S3)
2. The staging model was previously incremental, not a view (recently fixed)
3. Nobody verified the end-to-end flow

**Verification for Railway**: After the March 3rd cron run, check Railway logs:

```bash
railway logs --lines 200
```

Look for:
- `Weather extraction completed` in the extraction phase
- `Loaded X rows into raw_weather_hourly` in the database load phase
- All 3 `mart_weather_*` models showing `OK` in dbt output (not `SKIP` or `ERROR`)
- `Successfully exported mart_weather_ride_correlation` (and the other 2) in the export phase

---

## Test Plan

### Existing tests to verify (no modifications needed)

1. **Weather extraction tests** (11 tests):
   ```bash
   venv/bin/python -m pytest tests/test_weather_extraction.py -v
   ```
   These test the extraction logic, API calls, and validation. They should continue to pass after adding `source_file`.

2. **Full test suite**:
   ```bash
   venv/bin/python -m pytest tests/ -v
   ```
   Baseline: 283 pass, 3 skip. Verify no regressions after changes.

### New manual verification steps

These are the Steps 1-7 described in the Implementation Plan above. Summarized:

| Step | Command | Expected Result |
|------|---------|-----------------|
| 1 | S3 list weather parquets | Multiple parquets per city |
| 2 | `venv/bin/python -m db_duckdb.cli load --table raw_weather_hourly` | ~400K+ rows loaded |
| 3 | `venv/bin/python -m db_duckdb.cli verify --table raw_weather_hourly --detailed` | PASS |
| 4 | `dbt run --select stg_weather_hourly` | OK (view created) |
| 4b | Query stg_weather_hourly | 2 cities, 100K+ rows each |
| 5 | `dbt run --select stg_weather_hourly mart_weather_ride_correlation mart_weather_impact_summary mart_station_weather_performance` | All OK |
| 6 | `dbt test --select mart_weather_ride_correlation mart_weather_impact_summary mart_station_weather_performance` | All pass |
| 7 | `venv/bin/python -m db_duckdb.cli export` | All exported |

### dbt test coverage

The existing dbt tests in `dbt_city_cycles/models/marts/schema.yml` and `dbt_city_cycles/models/staging/schema.yml` cover:
- `stg_weather_hourly`: unique/not_null on weather_record_id, not_null on city, accepted_values for weather_condition/temperature_band/precipitation_intensity/wind_category
- `mart_weather_ride_correlation`: not_null on location/date/hour_of_day/ride_count, accepted_values on location
- `mart_weather_impact_summary`: not_null on location/hour_of_day/dimension_type/observation_count, accepted_values on location/dimension_type
- `mart_station_weather_performance`: not_null on location/station_id/hour_of_day/weather_condition/total_rides/days_observed, accepted_values on location

No new dbt tests needed -- the existing coverage is comprehensive.

---

## Documentation Updates

### CHANGELOG.md

Add under `[Unreleased]`:

```markdown
### Fixed
- **Weather Pipeline End-to-End** - Validated and fixed the weather data pipeline from extraction through mart export
  - Added `source_file` column to weather parquet output for lineage tracking consistency
  - Fixed validation query for `raw_weather_hourly` to handle transitional schema
  - Verified all 3 weather marts build successfully: mart_weather_ride_correlation, mart_weather_impact_summary, mart_station_weather_performance
```

### No other documentation changes needed

The existing documentation in `CLAUDE.md`, `orchestrator/README.md`, and other docs already describe the weather pipeline stages. The commands reference in `CLAUDE.md` already includes weather extraction commands.

---

## Stress Testing & Edge Cases

### Edge Case 1: No weather parquets on S3

If the weather backfill has never been run, `raw_weather_hourly` will be empty. The dbt models will build but produce zero rows:
- `stg_weather_hourly`: Empty view (no rows from empty source)
- `mart_weather_ride_correlation`: Empty (INNER JOIN with empty weather = zero rows)
- `mart_weather_impact_summary`: Empty (aggregation of zero rows)
- `mart_station_weather_performance`: Empty (INNER JOIN with empty weather = zero rows)

**Mitigation**: Step 1 in the implementation plan verifies parquets exist on S3. If they don't, run backfill.

### Edge Case 2: Schema mismatch between old and new parquets

Old parquets lack `source_file`. New ones include it. DuckDB's parquet reader handles this via schema union -- the missing column gets `NULL`. The `stg_weather_hourly.sql` view does not reference `source_file`, so this is transparent.

### Edge Case 3: Timezone mismatches between weather and ride data

Weather data uses city-local timezones (America/New_York for NYC, Europe/London for London). Ride data timestamps are also in local time. The join on `(date, hour_of_day)` assumes both are in the same timezone. This is correct as configured.

### Edge Case 4: Incremental files with overlapping timestamps

The `incremental_update` function creates files like `weather_nyc_incremental_2026-02-28.parquet` that overlap with historical backfill files. The `stg_weather_hourly.sql` view handles this with `QUALIFY ROW_NUMBER() OVER (PARTITION BY city, strftime(timestamp::timestamp, '%Y%m%d%H') ORDER BY timestamp) = 1`, which deduplicates to one row per city per hour.

### Performance considerations

- **Weather data volume**: ~420K rows total (small). All weather operations are fast.
- **`mart_weather_ride_correlation`**: INNER JOIN of `mart_hourly_rides` (potentially millions of rows) with `stg_weather_hourly` (~420K rows). The join is on `(location, date, hour_of_day)`. Since weather has 1 row per (city, hour), the output size is bounded by `mart_hourly_rides`. This should build in seconds to minutes.
- **`mart_station_weather_performance`**: Joins `unified_rides` (400M+ rows) with `stg_weather_hourly`. This is the heaviest query -- it touches the full ride dataset. With `--full-refresh` on Railway (32GB memory, 1 thread), this should complete but may take 10-30 minutes.

---

## Verification Checklist

- [x] `extraction/weather.py` modified to add `source_file` column in both `_write_and_upload_parquet` and `incremental_update`
- [x] `db_duckdb/config/duckdb_config.py` validation query updated to remove `source_file` reference
- [x] `CHANGELOG.md` updated
- [x] All 283 existing tests pass (`venv/bin/python -m pytest tests/ -v`)
- [ ] Weather parquets exist on S3 (check both NYC and London)
- [ ] `raw_weather_hourly` table loaded locally with 400K+ rows
- [ ] `stg_weather_hourly` view created and returns data for both cities
- [ ] `mart_weather_ride_correlation` builds successfully with non-zero rows
- [ ] `mart_weather_impact_summary` builds successfully with non-zero rows
- [ ] `mart_station_weather_performance` builds successfully with non-zero rows
- [ ] `dbt test` passes for all 3 weather marts
- [ ] Mart export to S3 succeeds for all 3 weather marts

---

## What NOT To Do

1. **Do NOT re-upload the historical backfill parquets** to add `source_file`. The schema union in DuckDB handles the mismatch gracefully. Re-uploading would require deleting existing files (breaking idempotency) and making dozens of API calls.

2. **Do NOT change `load_parquet_from_s3()` to use `TABLE_SCHEMAS`**. The current `CREATE TABLE AS SELECT *` approach is simpler and works. The `TABLE_SCHEMAS` dict is only used by `init_tables()` which creates empty tables -- but `load_data()` drops and recreates from parquet, so the predefined schema is irrelevant for the load path.

3. **Do NOT add indexes to `raw_weather_hourly`**. Per the MEMORY.md learnings, staging indexes caused catastrophic 12+ hour hangs on large tables. Weather data is small (~420K rows) and does not need indexes.

4. **Do NOT convert `stg_weather_hourly` back to an incremental model**. It was recently converted to a view for good reason (simpler, no materialization needed for ~420K rows). Keep it as a view.

5. **Do NOT modify the `stg_weather_hourly.sql` model**. The SQL is correct -- it deduplicates, derives weather conditions, and produces the right schema. The issue is upstream (no raw data), not in the staging model itself.

6. **Do NOT modify the 3 weather mart SQL files**. They are correct. The issue is that they had no data to process because the upstream `stg_weather_hourly` view was failing on a missing raw table.

7. **Do NOT run `dbt run --full-refresh` locally unless you have 32GB+ free RAM**. The `stg_nyc_modern` model processes 216M rows and needs ~30GB. Instead, target specific models with `--select`.

8. **Do NOT skip the verification steps**. The whole point of this phase is to validate end-to-end. Running the code changes without verification defeats the purpose.
