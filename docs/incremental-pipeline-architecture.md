# Incremental Pipeline Architecture

## Overview

This document describes the incremental materialization strategy for the City Cycles dbt pipeline, optimized for **monthly batch runs** on AWS EC2.

## Architecture Changes (November 2025)

### Problem Statement

The dbt pipeline had inconsistent materialization strategies:
- ❌ `stg_nyc_modern`: Full rebuild (table)
- ✅ `stg_nyc_legacy`: Incremental
- ✅ `stg_london_modern`: Incremental
- ✅ `stg_london_legacy`: Incremental
- ❌ `unified_rides`: Full rebuild (table)
- ❌ All marts: Full rebuild (table)

This caused inefficiency for monthly runs, especially as NYC Modern (2017-present) is the largest dataset.

### Solution

Implement a consistent incremental strategy using `source_file` tracking:

```
Raw Tables (FULL) → Staging (INCREMENTAL) → Intermediate (INCREMENTAL) → 
Unified (INCREMENTAL) → Marts (FULL REBUILD)
```

## Implementation Details

### 1. Staging Layer: `stg_nyc_modern.sql`

**Changed:**
```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id',
    ...
) }}

with source as (
    select * from {{ source('raw', 'raw_nyc_modern') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),
```

**Why:**
- Matches pattern of other staging models (nyc_legacy, london_legacy, london_modern)
- Only processes new files added to raw tables
- Uses `ride_id` as unique key for upsert logic
- Leverages `source_file` column for incremental detection

### 2. Unified Layer: `unified_rides.sql`

**Changed:**
```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id'
) }}

-- NYC data
SELECT ... FROM {{ ref('int_nyc_rides') }}
{% if is_incremental() %}
where source_file not in (select distinct source_file from {{ this }})
{% endif %}

UNION ALL

-- London data
SELECT ... FROM {{ ref('int_london_rides') }}
{% if is_incremental() %}
where source_file not in (select distinct source_file from {{ this }})
{% endif %}
```

**Why:**
- Both source tables (`int_nyc_rides`, `int_london_rides`) are already incremental
- Both track `source_file` for lineage
- UNION ALL with symmetric incremental filtering is straightforward
- `ride_id` is unique across both datasets (no collision risk)
- Significant time savings for monthly runs

### 3. Marts Layer

**Kept as `materialized='table'` (full rebuild):**

**Why:**
- Marts contain aggregations (GROUP BY date, location)
- New data for historical dates requires re-aggregation
- Marts are small (~365 days × 2 cities = small tables)
- Full rebuild is fast (~1-2 minutes)
- Guarantees correctness without complex incremental aggregation logic

## Pipeline Flow

### Monthly Run Sequence

```
MONTH 1 (Initial Load):
├─ Raw Tables: Load ALL Parquet files from S3 (30 min)
├─ Staging: Full refresh (15 min)
├─ Intermediate: Full refresh (5 min)
├─ Unified: Full refresh (5 min)
└─ Marts: Full rebuild (2 min)
Total: ~57 minutes

MONTH 2+ (Incremental):
├─ Raw Tables: Load ALL Parquet files from S3 (30 min)
│   └─ Note: Includes all files, but this is acceptable for monthly cadence
├─ Staging: Process only NEW source_files (2 min)
├─ Intermediate: Process only NEW source_files (1 min)
├─ Unified: Add only NEW source_files (1 min)
└─ Marts: Full rebuild from unified (2 min)
Total: ~36 minutes (40% time savings)
```

## Incremental Logic

### Source File Tracking

Every table from staging onwards includes a `source_file` VARCHAR column:
```sql
'201901-citibike-tripdata.csv'
'JourneyDataExtract18Dec2019-24Dec2019.csv'
```

### Incremental Filter Pattern

All incremental models use this pattern:
```sql
{% if is_incremental() %}
where source_file not in (select distinct source_file from {{ this }})
{% endif %}
```

**How it works:**
1. Query existing table for list of processed `source_file` values
2. Exclude rows from source that match existing `source_file` values
3. Only process rows from NEW files
4. Append new rows to existing table

### Unique Key Strategy

| Model | Unique Key | Purpose |
|-------|-----------|---------|
| `stg_nyc_modern` | `ride_id` | Natural unique ID from source |
| `stg_nyc_legacy` | `['bike_id', 'start_time', 'stop_time', 'start_station_id']` | Composite key |
| `stg_london_modern` | `ride_id` (mapped from `number`) | Natural unique ID |
| `stg_london_legacy` | `ride_id` (mapped from `rental_id`) | Natural unique ID |
| `int_nyc_rides` | `ride_id` | Natural unique ID (added post-initial release) |
| `int_london_rides` | `ride_id` | Natural unique ID |
| `unified_rides` | `ride_id` | Natural unique ID |

## Edge Cases

### 1. Late-Arriving Data

**Scenario:** Month 2 discovers a file from Month 1 that was missed.

**Behavior:**
- ✅ Raw: Full reload includes the file
- ✅ Staging: New `source_file`, processes incrementally
- ✅ Intermediate: New `source_file`, processes incrementally
- ✅ Unified: New `source_file`, processes incrementally
- ✅ Marts: Full rebuild includes all data

**Conclusion:** Handles correctly.

### 2. Reprocessed Files (Same Name, Different Content)

**Scenario:** A source file is corrected and re-uploaded with the same filename.

**Behavior:**
- ✅ Raw: Full reload picks up new content
- ❌ Staging: Won't reprocess (source_file already in table)

**Solution:** Run `dbt run --full-refresh` to force reprocessing.

**Frequency:** Rare for monthly batch runs.

### 3. Data Quality Issues

**Scenario:** Bug found in dbt transformation logic.

**Solution:**
```bash
# Rebuild everything
dbt run --full-refresh

# Rebuild from specific model onwards
dbt run --full-refresh --select stg_nyc_modern+

# Rebuild just unified and downstream
dbt run --full-refresh --select unified_rides+
```

### 4. Schema Changes in Raw Data

**Scenario:** Source data schema changes (new columns, renamed columns).

**Behavior:**
- Incremental models will fail if schema is incompatible
- Need to update model definition and run full-refresh

**Mitigation:** Monitor for schema changes in extraction phase.

## Performance Benchmarks

### Expected Performance (Approximate)

| Stage | First Run | Monthly Run | Time Savings |
|-------|-----------|-------------|--------------|
| Raw Table Load | 30 min | 30 min | - |
| Staging | 15 min | 2 min | 87% ↓ |
| Intermediate | 5 min | 1 min | 80% ↓ |
| Unified | 5 min | 1 min | 80% ↓ |
| Marts | 2 min | 2 min | - |
| **Total** | **57 min** | **36 min** | **37% ↓** |

*Note: Actual performance depends on data volume and EC2 instance specs.*

## Best Practices

### 1. Monthly Full Refresh (Recommended)

Run a full refresh quarterly to prevent incremental drift:
```bash
# Every 3 months
dbt run --full-refresh
```

### 2. Validate Incremental Runs

After each monthly run:
```bash
# Check row counts
dbt test

# Verify data completeness
SELECT 
    location,
    MAX(date) as latest_date,
    COUNT(*) as total_rides
FROM unified_rides
GROUP BY location;
```

### 3. Monitor Source Files

Track which files are processed:
```sql
SELECT 
    location,
    schema_version,
    COUNT(DISTINCT source_file) as file_count,
    MAX(dbt_updated_at) as last_processed
FROM unified_rides
GROUP BY location, schema_version;
```

### 4. Emergency Full Refresh

If data looks incorrect:
```bash
# Nuclear option: rebuild everything
dbt run --full-refresh

# Then export marts
python -m db_duckdb.cli export
```

## Testing Strategy

### Pre-Deployment Testing

1. **Test on subset of data:**
   ```bash
   # Use limit in raw table queries for testing
   dbt run --vars '{"limit": 1000}'
   ```

2. **Validate incremental logic:**
   ```bash
   # First run (should be full)
   dbt run --full-refresh --select stg_nyc_modern
   
   # Second run (should be incremental)
   dbt run --select stg_nyc_modern
   # Verify: Should process 0 rows if no new source_files
   ```

3. **Test with new data:**
   - Add a new file to S3
   - Run raw table load
   - Run dbt incrementally
   - Verify only new file is processed

### Monitoring in Production

1. **Row count validation:**
   ```sql
   -- Before dbt run
   SELECT COUNT(*) FROM unified_rides;
   
   -- After dbt run
   SELECT COUNT(*) FROM unified_rides;
   -- Should increase by number of rows in new files
   ```

2. **Source file tracking:**
   ```sql
   -- Check latest processed files
   SELECT 
       source_file,
       COUNT(*) as row_count,
       MIN(date) as earliest_date,
       MAX(date) as latest_date
   FROM unified_rides
   WHERE dbt_updated_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
   GROUP BY source_file
   ORDER BY source_file DESC
   LIMIT 10;
   ```

## Rollback Procedure

If incremental logic causes issues:

1. **Identify problematic model:**
   ```bash
   dbt test
   # Check which models fail
   ```

2. **Full refresh problematic model and downstream:**
   ```bash
   dbt run --full-refresh --select <model_name>+
   ```

3. **If issues persist, rebuild entire pipeline:**
   ```bash
   # Rebuild all dbt models
   dbt run --full-refresh
   
   # Re-export marts
   python -m db_duckdb.cli export
   ```

4. **Last resort - reload raw tables:**
   ```bash
   # Reload from S3 Parquet
   python -m db_duckdb.cli load
   
   # Rebuild dbt pipeline
   dbt run --full-refresh
   ```

## Future Enhancements

### Potential Improvements

1. **Incremental raw table loading:**
   - Track processed Parquet files in metadata table
   - Only load new files from S3
   - Further reduce pipeline runtime

2. **Incremental marts:**
   - Use advanced dbt incremental strategies
   - Implement merge logic for date-based aggregations
   - More complex but potentially faster

3. **Partitioned tables:**
   - Partition by year or month
   - Faster queries on date ranges
   - Better for large historical datasets

4. **Data quality tests:**
   - Automated testing after each run
   - Alert on anomalies (missing dates, row count drops)
   - Great Expectations integration

## References

- [dbt Incremental Models Documentation](https://docs.getdbt.com/docs/build/incremental-models)
- [DuckDB Performance Tuning](https://duckdb.org/docs/guides/performance/overview)
- Original migration plan: `/resources/s3_to_duckdb_migration_plan.md`

## Changelog

### 2025-11-02: Initial Incremental Architecture
- Changed `stg_nyc_modern` from table to incremental
- Changed `unified_rides` from table to incremental
- Documented architecture and best practices
- Added testing and monitoring guidelines

