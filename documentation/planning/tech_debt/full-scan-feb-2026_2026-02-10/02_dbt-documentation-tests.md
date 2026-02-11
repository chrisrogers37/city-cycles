# Phase 02: dbt Documentation, Data Tests, and Source Freshness

**Status:** 🔧 IN PROGRESS
**Started:** 2026-02-11

**PR Title:** `feat(dbt): add schema documentation, data tests, and source freshness`
**Risk Level:** Low
**Estimated Effort:** Medium (3-4 hours)
**Dependencies:** None
**Blocks:** Phase 03 (dbt macros refactor)

---

## Summary

The dbt project currently has no schema documentation (`schema.yml` files), no data quality tests, no source freshness monitoring, and the `dbt_project.yml` is missing configuration for the `intermediate` and `unified` model layers. This PR adds all of those without touching any `.sql` model files.

---

## Files Created/Modified

| # | File | Action |
|---|------|--------|
| 1 | `dbt_city_cycles/models/staging/schema.yml` | CREATE -- Document 4 staging models with column tests |
| 2 | `dbt_city_cycles/models/intermediate/schema.yml` | CREATE -- Document 2 intermediate models |
| 3 | `dbt_city_cycles/models/unified/schema.yml` | CREATE -- Document unified_rides model |
| 4 | `dbt_city_cycles/models/marts/schema.yml` | CREATE -- Document 5 mart models |
| 5 | `dbt_city_cycles/seeds/schema.yml` | CREATE -- Document population seed |
| 6 | `dbt_city_cycles/models/staging/sources.yml` | MODIFY -- Add freshness and column tests |
| 7 | `dbt_city_cycles/dbt_project.yml` | MODIFY -- Add intermediate and unified config |

---

## Change 1: CREATE `dbt_city_cycles/models/staging/schema.yml`

Create this file from scratch. The 4 staging models are:
- `stg_nyc_legacy` -- NYC bike rides, legacy format (pre-2021)
- `stg_nyc_modern` -- NYC bike rides, modern format (2021+)
- `stg_london_legacy` -- London bike rides, legacy format (pre-2021)
- `stg_london_modern` -- London bike rides, modern format (2021+)

All 4 staging models produce the same standardized output columns (with minor differences: NYC has lat/lng and user_type, London does not). The `ride_id` column is the primary key for all models.

**File contents:**

```yaml
version: 2

models:
  - name: stg_nyc_legacy
    description: >
      Staged NYC bike share rides from the legacy CitiBike format (pre-February 2021).
      Includes subscriber/customer user types, birth year, gender, and station coordinates.
      Ride IDs are synthetically generated from bike_id + station + timestamps.
    columns:
      - name: ride_id
        description: Synthetically generated unique ride identifier (legacy data has no native ride_id)
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: duration_seconds
        description: Ride duration in seconds, calculated from start_time and stop_time
      - name: start_station_id
        description: Starting station ID
      - name: start_station_name
        description: Starting station name
      - name: start_latitude
        description: Starting station latitude
      - name: start_longitude
        description: Starting station longitude
      - name: end_station_id
        description: Ending station ID
      - name: end_station_name
        description: Ending station name
      - name: end_latitude
        description: Ending station latitude
      - name: end_longitude
        description: Ending station longitude
      - name: bike_id
        description: Bike identifier
      - name: user_type
        description: "Rider membership type: member (was Subscriber) or casual (was Customer)"
        tests:
          - accepted_values:
              values: ['member', 'casual', 'unknown']
      - name: birth_year
        description: Rider birth year (legacy data only)
      - name: gender
        description: "Rider gender code (legacy data only): 0=unknown, 1=male, 2=female"
      - name: date
        description: Ride date (truncated to day)
      - name: month
        description: Month number extracted from start_time
      - name: year
        description: Year extracted from start_time
      - name: day_type
        description: "weekday or weekend based on ISO day of week"
        tests:
          - accepted_values:
              values: ['weekday', 'weekend']
      - name: day_of_week
        description: "Day of week (0=Monday, 6=Sunday)"
      - name: hour_of_day
        description: Hour of day (0-23) extracted from start_time
      - name: source_file
        description: Original source file name for lineage tracking
      - name: location
        description: City identifier
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: schema_version
        description: "Source schema version: legacy or modern"
      - name: dbt_updated_at
        description: Timestamp of when dbt last processed this row

  - name: stg_nyc_modern
    description: >
      Staged NYC bike share rides from the modern CitiBike format (February 2021 onward).
      Includes rideable_type and member/casual user classification. Has native ride_id.
    columns:
      - name: ride_id
        description: Native unique ride identifier from CitiBike
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: duration_seconds
        description: Ride duration in seconds, calculated from start_time and stop_time
      - name: start_station_id
        description: Starting station ID
      - name: start_station_name
        description: Starting station name
      - name: start_latitude
        description: Starting station latitude
      - name: start_longitude
        description: Starting station longitude
      - name: end_station_id
        description: Ending station ID
      - name: end_station_name
        description: Ending station name
      - name: end_latitude
        description: Ending station latitude
      - name: end_longitude
        description: Ending station longitude
      - name: user_type
        description: "Rider membership type: member or casual"
        tests:
          - accepted_values:
              values: ['member', 'casual', 'unknown']
      - name: rideable_type
        description: Type of bike used (e.g., classic_bike, electric_bike, docked_bike)
      - name: date
        description: Ride date (truncated to day)
      - name: month
        description: Month number extracted from start_time
      - name: year
        description: Year extracted from start_time
      - name: day_type
        description: "weekday or weekend based on ISO day of week"
        tests:
          - accepted_values:
              values: ['weekday', 'weekend']
      - name: day_of_week
        description: "Day of week (0=Monday, 6=Sunday)"
      - name: hour_of_day
        description: Hour of day (0-23) extracted from start_time
      - name: source_file
        description: Original source file name for lineage tracking
      - name: location
        description: City identifier
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: schema_version
        description: "Source schema version: legacy or modern"
      - name: dbt_updated_at
        description: Timestamp of when dbt last processed this row

  - name: stg_london_legacy
    description: >
      Staged London bike share rides from the legacy Santander Cycles format (pre-2021).
      Uses rental_id as ride_id. No coordinates or user type information available.
    columns:
      - name: ride_id
        description: Unique ride identifier (mapped from rental_id)
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: duration_seconds
        description: Ride duration in seconds, calculated from start_time and stop_time
      - name: start_station_id
        description: Starting station ID
      - name: start_station_name
        description: Starting station name
      - name: end_station_id
        description: Ending station ID
      - name: end_station_name
        description: Ending station name
      - name: bike_id
        description: Bike identifier
      - name: date
        description: Ride date (truncated to day)
      - name: month
        description: Month number extracted from start_time
      - name: year
        description: Year extracted from start_time
      - name: day_type
        description: "weekday or weekend based on ISO day of week"
        tests:
          - accepted_values:
              values: ['weekday', 'weekend']
      - name: day_of_week
        description: "Day of week (0=Monday, 6=Sunday)"
      - name: hour_of_day
        description: Hour of day (0-23) extracted from start_time
      - name: source_file
        description: Original source file name for lineage tracking
      - name: location
        description: City identifier
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: schema_version
        description: "Source schema version: legacy or modern"
      - name: dbt_updated_at
        description: Timestamp of when dbt last processed this row

  - name: stg_london_modern
    description: >
      Staged London bike share rides from the modern Santander Cycles format (2021 onward).
      Includes bike_model field. Uses number field as ride_id. No coordinates or user type.
    columns:
      - name: ride_id
        description: Unique ride identifier (mapped from the number field)
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: duration_seconds
        description: Ride duration in seconds, calculated from start_time and stop_time
      - name: start_station_id
        description: Starting station ID (mapped from start_station_number)
      - name: start_station_name
        description: Starting station name (mapped from start_station)
      - name: end_station_id
        description: Ending station ID (mapped from end_station_number)
      - name: end_station_name
        description: Ending station name (mapped from end_station)
      - name: bike_id
        description: Bike identifier (mapped from bike_number)
      - name: bike_model
        description: Type of bike (e.g., CLASSIC, PBSC_EBIKE)
      - name: date
        description: Ride date (truncated to day)
      - name: month
        description: Month number extracted from start_time
      - name: year
        description: Year extracted from start_time
      - name: day_type
        description: "weekday or weekend based on ISO day of week"
        tests:
          - accepted_values:
              values: ['weekday', 'weekend']
      - name: day_of_week
        description: "Day of week (0=Monday, 6=Sunday)"
      - name: hour_of_day
        description: Hour of day (0-23) extracted from start_time
      - name: source_file
        description: Original source file name for lineage tracking
      - name: location
        description: City identifier
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: schema_version
        description: "Source schema version: legacy or modern"
      - name: dbt_updated_at
        description: Timestamp of when dbt last processed this row
```

---

## Change 2: CREATE `dbt_city_cycles/models/intermediate/schema.yml`

The 2 intermediate models combine legacy + modern staging data for each city.

**File contents:**

```yaml
version: 2

models:
  - name: int_nyc_rides
    description: >
      Combined NYC rides from both legacy and modern staging models.
      Unions stg_nyc_legacy and stg_nyc_modern into a single NYC ride table.
      Includes all columns from both schemas with NULLs where data is unavailable
      (e.g., birth_year and gender are NULL for modern rides, bike_id is NULL for modern rides).
    columns:
      - name: ride_id
        description: Unique ride identifier (synthetic for legacy, native for modern)
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: user_type
        description: "Rider type: member or casual"
      - name: location
        description: City identifier (always 'nyc')
        tests:
          - not_null
      - name: source_file
        description: Original source file for lineage tracking
      - name: schema_version
        description: "Source schema version: legacy or modern"

  - name: int_london_rides
    description: >
      Combined London rides from both legacy and modern staging models.
      Unions stg_london_legacy and stg_london_modern into a single London ride table.
    columns:
      - name: ride_id
        description: Unique ride identifier (rental_id for legacy, number for modern)
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: location
        description: City identifier (always 'london')
        tests:
          - not_null
      - name: source_file
        description: Original source file for lineage tracking
      - name: schema_version
        description: "Source schema version: legacy or modern"
```

---

## Change 3: CREATE `dbt_city_cycles/models/unified/schema.yml`

The unified model joins NYC and London rides into a single cross-city table.

**File contents:**

```yaml
version: 2

models:
  - name: unified_rides
    description: >
      All bike share rides from both NYC and London unified into a single table.
      NYC rides include lat/lng coordinates and user_type. London rides have NULLs
      for coordinates, user_type, birth_year, and gender. This is the primary table
      consumed by all mart models.
    columns:
      - name: ride_id
        description: Unique ride identifier across both cities
        tests:
          - unique
          - not_null
      - name: start_time
        description: Ride start timestamp
        tests:
          - not_null
      - name: stop_time
        description: Ride end timestamp
      - name: start_station_id
        description: Starting station ID
      - name: start_station_name
        description: Starting station name
      - name: end_station_id
        description: Ending station ID
      - name: end_station_name
        description: Ending station name
      - name: start_latitude
        description: Starting station latitude (NYC only, NULL for London)
      - name: start_longitude
        description: Starting station longitude (NYC only, NULL for London)
      - name: end_latitude
        description: Ending station latitude (NYC only, NULL for London)
      - name: end_longitude
        description: Ending station longitude (NYC only, NULL for London)
      - name: user_type
        description: "Rider type: member, casual, or NULL (London has no user type data)"
      - name: bike_id
        description: Bike identifier (NULL for NYC modern rides)
      - name: duration_seconds
        description: Ride duration in seconds
      - name: birth_year
        description: Rider birth year (NYC legacy only)
      - name: gender
        description: Rider gender code (NYC legacy only)
      - name: date
        description: Ride date (truncated to day)
      - name: month
        description: Month number
      - name: year
        description: Year
      - name: day_type
        description: "weekday or weekend"
      - name: day_of_week
        description: "Day of week (0=Monday, 6=Sunday)"
      - name: hour_of_day
        description: Hour of day (0-23)
      - name: source_file
        description: Original source file for lineage tracking
      - name: location
        description: City identifier
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: schema_version
        description: "Source schema version: legacy or modern"
      - name: dbt_updated_at
        description: Timestamp of when dbt last processed this row
```

---

## Change 4: CREATE `dbt_city_cycles/models/marts/schema.yml`

The 5 mart models are:
- `mart_daily_metrics` -- Daily aggregated ride metrics by location
- `mart_daily_metrics_long` -- Same data pivoted to long format (one row per metric)
- `mart_hourly_patterns` -- Ride counts by hour of day and location
- `mart_station_growth` -- Year-over-year station count growth by location
- `mart_nyc_member_analysis` -- Monthly member percentage for NYC

**File contents:**

```yaml
version: 2

models:
  - name: mart_daily_metrics
    description: >
      Daily aggregated ride metrics for both NYC and London.
      Includes total rides, average duration, member/casual splits, and per-capita
      normalization using population seed data.
    columns:
      - name: location
        description: City identifier (nyc or london)
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: date
        description: Calendar date
        tests:
          - not_null
      - name: year
        description: Year
      - name: day_type
        description: "weekday or weekend"
      - name: total_rides
        description: Total number of rides on this date for this location
        tests:
          - not_null
      - name: avg_duration_minutes
        description: Average ride duration in minutes
      - name: member_rides
        description: Number of rides by members (NULL for London)
      - name: casual_rides
        description: Number of rides by casual users (NULL for London)
      - name: unknown_user_type_rides
        description: Number of rides with unknown or NULL user type
      - name: total_minutes_biked
        description: Total minutes of bike usage
      - name: population
        description: City population for the year (from population seed)
      - name: rides_per_1000
        description: Rides per 1,000 population (normalized metric)

  - name: mart_daily_metrics_long
    description: >
      Long-format version of mart_daily_metrics where each metric is a separate row.
      Used by the Streamlit dashboard for flexible Plotly charting.
    columns:
      - name: location
        description: City identifier
        tests:
          - not_null
      - name: date
        description: Calendar date
        tests:
          - not_null
      - name: year
        description: Year
      - name: day_type
        description: "weekday or weekend"
      - name: metric_name
        description: Name of the metric
        tests:
          - not_null
          - accepted_values:
              values:
                - total_rides
                - avg_duration_minutes
                - member_rides
                - casual_rides
                - total_minutes_biked
                - population
                - rides_per_1000
      - name: metric_value
        description: Numeric value of the metric

  - name: mart_hourly_patterns
    description: >
      Ride counts aggregated by hour of day and location.
      Used to visualize peak usage hours across cities.
    columns:
      - name: location
        description: City identifier
        tests:
          - not_null
      - name: hour_of_day
        description: Hour of day (0-23)
        tests:
          - not_null
      - name: ride_count
        description: Total number of rides in this hour across all dates
        tests:
          - not_null

  - name: mart_station_growth
    description: >
      Year-over-year station count growth by location.
      Calculates distinct station counts per year, joins population data,
      and computes year-over-year growth percentage.
    columns:
      - name: location
        description: City identifier
        tests:
          - not_null
      - name: year
        description: Year
        tests:
          - not_null
      - name: station_count
        description: Number of distinct stations active in this year
        tests:
          - not_null
      - name: population
        description: City population for the year (from population seed)
      - name: stations_per_1000
        description: Stations per 1,000 population
      - name: prev_year_count
        description: Station count from the previous year (NULL for first year)
      - name: yoy_growth
        description: Year-over-year growth percentage (NULL for first year)

  - name: mart_nyc_member_analysis
    description: >
      Monthly member percentage analysis for NYC.
      Calculates what percentage of NYC rides in each month were by members.
    columns:
      - name: location
        description: City identifier (always 'nyc')
        tests:
          - not_null
      - name: month
        description: Month (truncated to first of month)
        tests:
          - not_null
      - name: member_percentage
        description: Percentage of rides by members in this month
        tests:
          - not_null
```

---

## Change 5: CREATE `dbt_city_cycles/seeds/schema.yml`

**File contents:**

```yaml
version: 2

seeds:
  - name: population
    description: >
      Annual population data for NYC and London. Used to normalize ride metrics
      on a per-capita basis (rides per 1,000 population, stations per 1,000 population).
      Manually maintained -- update annually when new census/estimate data is available.
    columns:
      - name: location
        description: "City identifier: nyc or london"
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: year
        description: Calendar year
        tests:
          - not_null
      - name: population
        description: Estimated population for the city in the given year
        tests:
          - not_null
```

---

## Change 6: MODIFY `dbt_city_cycles/models/staging/sources.yml`

Add freshness monitoring and column-level tests to the existing source definitions. The `loaded_at_field` for each source table is the timestamp column that indicates when data was loaded (using `source_file` is not a timestamp, so we use the raw ride timestamp columns instead).

**BEFORE (entire file):**
```yaml
version: 2

sources:
  - name: raw
    schema: main
    database: city_cycles
    tables:
      - name: raw_nyc_legacy
        description: Raw NYC bike share data (legacy format)
      - name: raw_nyc_modern
        description: Raw NYC bike share data (modern format)
      - name: raw_london_legacy
        description: Raw London bike share data (legacy format)
      - name: raw_london_modern
        description: Raw London bike share data (modern format)
```

**AFTER (entire file):**
```yaml
version: 2

sources:
  - name: raw
    schema: main
    database: city_cycles
    description: >
      Raw bike share data loaded from S3 Parquet files into DuckDB.
      Four tables representing legacy and modern schemas for NYC and London.
    freshness:
      warn_after:
        count: 45
        period: day
      error_after:
        count: 90
        period: day
    tables:
      - name: raw_nyc_legacy
        description: >
          Raw NYC bike share data in the legacy CitiBike format (pre-February 2021).
          Contains trip duration, station details, coordinates, user type, and demographics.
        loaded_at_field: "starttime::timestamp"
        columns:
          - name: bikeid
            description: Bike identifier
          - name: starttime
            description: Ride start timestamp
            tests:
              - not_null
          - name: stoptime
            description: Ride end timestamp
          - name: start_station_id
            description: Starting station ID
          - name: start_station_name
            description: Starting station name
          - name: end_station_id
            description: Ending station ID
          - name: end_station_name
            description: Ending station name
          - name: usertype
            description: "User type: Subscriber or Customer"
          - name: source_file
            description: Original source file path in S3

      - name: raw_nyc_modern
        description: >
          Raw NYC bike share data in the modern CitiBike format (February 2021 onward).
          Contains ride_id, rideable_type, station details, coordinates, and member/casual classification.
        loaded_at_field: "started_at::timestamp"
        columns:
          - name: ride_id
            description: Unique ride identifier
            tests:
              - not_null
          - name: started_at
            description: Ride start timestamp
            tests:
              - not_null
          - name: ended_at
            description: Ride end timestamp
          - name: start_station_id
            description: Starting station ID
          - name: end_station_id
            description: Ending station ID
          - name: member_casual
            description: "Membership type: member or casual"
          - name: source_file
            description: Original source file path in S3

      - name: raw_london_legacy
        description: >
          Raw London bike share data in the legacy Santander Cycles format (pre-2021).
          Contains rental_id, bike_id, station details, and duration.
        loaded_at_field: "start_date::timestamp"
        columns:
          - name: rental_id
            description: Unique rental identifier
            tests:
              - not_null
          - name: start_date
            description: Ride start timestamp
            tests:
              - not_null
          - name: end_date
            description: Ride end timestamp
          - name: start_station_id
            description: Starting station ID
          - name: end_station_id
            description: Ending station ID
          - name: source_file
            description: Original source file path in S3

      - name: raw_london_modern
        description: >
          Raw London bike share data in the modern Santander Cycles format (2021 onward).
          Contains number, bike_number, bike_model, station details, and duration.
        loaded_at_field: "start_date::timestamp"
        columns:
          - name: number
            description: Unique ride number
            tests:
              - not_null
          - name: start_date
            description: Ride start timestamp
            tests:
              - not_null
          - name: end_date
            description: Ride end timestamp
          - name: start_station_number
            description: Starting station ID
          - name: end_station_number
            description: Ending station ID
          - name: source_file
            description: Original source file path in S3
```

**Key decisions:**
- Freshness `warn_after: 45 days` / `error_after: 90 days` -- The pipeline runs monthly, so 45 days covers a normal cycle plus buffer. 90 days catches a missed month.
- `loaded_at_field` uses the ride timestamp column (e.g., `starttime::timestamp`, `started_at::timestamp`, `start_date::timestamp`). This tells dbt to check the max value of this column to determine data freshness.
- Not-null tests on primary key and start timestamp columns in each source table.

---

## Change 7: MODIFY `dbt_city_cycles/dbt_project.yml`

Add `intermediate` and `unified` model configuration blocks. Currently only `staging` and `marts` are configured.

**BEFORE (lines 33-40):**
```yaml
models:
  dbt_city_cycles:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

**AFTER (lines 33-46):**
```yaml
models:
  dbt_city_cycles:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: incremental
      +schema: intermediate
    unified:
      +materialized: incremental
      +schema: unified
    marts:
      +materialized: table
      +schema: marts
```

**Why this matters:** Without these entries, the intermediate and unified models rely entirely on their in-file `{{ config() }}` blocks. Adding project-level config makes the materialization strategy visible in one place and provides a fallback if a model's config block is ever removed. The values here match what the `.sql` files already specify:
- `int_nyc_rides.sql` and `int_london_rides.sql` both use `materialized='incremental'`
- `unified_rides.sql` uses `materialized='incremental'`

**IMPORTANT:** Do NOT change the `staging` or `marts` entries. They are already correct. Only ADD the `intermediate` and `unified` blocks between `staging` and `marts`.

The rest of the file (lines 42-47, the `on-run-start` section) remains unchanged.

---

## What NOT To Do

- **Do NOT modify any `.sql` model files** in this PR. All changes are YAML-only.
- **Do NOT change materialization strategies** for `staging` (view) or `marts` (table). They are correct.
- **Do NOT add tests that reference columns not present in the model output.** Cross-reference column names against the actual `.sql` files. For example, `stg_london_legacy` does NOT output `user_type`, so do not add a `user_type` test to it.
- **Do NOT add `unique` tests to mart models.** Marts are aggregations and do not have single-column unique keys (e.g., `mart_daily_metrics` groups by location+date, `mart_hourly_patterns` groups by location+hour).
- **Do NOT remove existing `sources.yml` content.** Only add to it.

---

## Verification Checklist

### 1. YAML Validation
```bash
cd dbt_city_cycles && dbt parse
```
Expected: No parsing errors. All models, sources, and seeds recognized.

### 2. Compile Check
```bash
cd dbt_city_cycles && dbt compile
```
Expected: Clean compilation. No errors about missing models or invalid YAML.

### 3. Test Definition Check
```bash
cd dbt_city_cycles && dbt test --select "stg_nyc_legacy" 2>&1 | head -20
```
Expected: Tests are defined (they may fail if no data is loaded -- that is OK for this PR). The important thing is that the test definitions are valid.

### 4. Documentation Generation
```bash
cd dbt_city_cycles && dbt docs generate
```
Expected: Docs generated successfully. Open `target/index.html` in a browser and verify:
- All 4 staging models have descriptions and column documentation
- All 2 intermediate models have descriptions
- `unified_rides` has description and column documentation
- All 5 mart models have descriptions and column documentation
- The `population` seed has description and column documentation
- Source tables show freshness configuration

### 5. File Count Verification
```bash
ls dbt_city_cycles/models/staging/schema.yml
ls dbt_city_cycles/models/intermediate/schema.yml
ls dbt_city_cycles/models/unified/schema.yml
ls dbt_city_cycles/models/marts/schema.yml
ls dbt_city_cycles/seeds/schema.yml
```
Expected: All 5 new files exist.

### 6. No SQL Files Modified
```bash
git diff --name-only | grep "\.sql$"
```
Expected: No output (no SQL files modified).

---

## PR Checklist

- [ ] 5 new `schema.yml` files created (staging, intermediate, unified, marts, seeds)
- [ ] `sources.yml` updated with freshness and column tests
- [ ] `dbt_project.yml` updated with intermediate and unified config
- [ ] `dbt parse` succeeds with no errors
- [ ] `dbt compile` succeeds with no errors
- [ ] `dbt docs generate` succeeds and shows documentation
- [ ] No `.sql` files modified
- [ ] CHANGELOG.md updated

### CHANGELOG Entry
```markdown
### Added
- **dbt Schema Documentation** - Added schema.yml files for all model layers
  - Documented all 4 staging models with column descriptions
  - Documented 2 intermediate models (int_nyc_rides, int_london_rides)
  - Documented unified_rides model with full column descriptions
  - Documented all 5 mart models with column descriptions
  - Documented population seed with column descriptions
- **dbt Data Tests** - Added data quality tests across all model layers
  - unique and not_null tests on ride_id for staging, intermediate, and unified models
  - accepted_values tests on location, user_type, day_type, and metric_name columns
  - not_null tests on key mart columns (location, date, ride_count, etc.)
  - not_null tests on source table primary keys
- **dbt Source Freshness** - Added freshness monitoring to all 4 raw source tables
  - warn_after: 45 days, error_after: 90 days
  - loaded_at_field configured for each source table
- **dbt Project Config** - Added intermediate and unified model layer configuration to dbt_project.yml
```
