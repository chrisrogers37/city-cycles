# Phase 03: dbt Macros, Unique Key Fix, and SQL Refactor

**Status:** 🔧 IN PROGRESS
**Started:** 2026-02-11

**PR Title:** `refactor(dbt): extract macros, fix unique keys, refactor SQL`
**Risk Level:** Low
**Estimated Effort:** Medium (3-4 hours)
**Dependencies:** Phase 02 (schema.yml must exist first so tests validate the refactored models)
**Blocks:** None

---

## Summary

Three improvements to the dbt project in a single PR:
1. **Extract repeated SQL logic into reusable macros** -- The `day_type` CASE expression and user type mapping logic are copy-pasted across all 4 staging models. Extract them into macros.
2. **Fix the inconsistent unique_key on `stg_nyc_legacy`** -- This model uses a composite `unique_key` (4 columns) while all other staging models use `unique_key='ride_id'`. The model already generates a `ride_id` column, so the unique_key should reference it.
3. **Refactor repeated window function in `mart_station_growth`** -- The `lag()` window function is written 4 times in the same query. Move it into a CTE.
4. **Replace hardcoded `/tmp` in `dbt_project.yml`** -- Use an environment variable with a fallback.

---

## Files Created/Modified

| # | File | Action |
|---|------|--------|
| 1 | `dbt_city_cycles/macros/day_type.sql` | CREATE -- Weekday/weekend CASE macro |
| 2 | `dbt_city_cycles/macros/user_type_mapping.sql` | CREATE -- Legacy user type mapping macro |
| 3 | `dbt_city_cycles/models/staging/stg_nyc_legacy.sql` | MODIFY -- Fix unique_key, use macros |
| 4 | `dbt_city_cycles/models/staging/stg_nyc_modern.sql` | MODIFY -- Use day_type macro |
| 5 | `dbt_city_cycles/models/staging/stg_london_legacy.sql` | MODIFY -- Use day_type macro |
| 6 | `dbt_city_cycles/models/staging/stg_london_modern.sql` | MODIFY -- Use day_type macro |
| 7 | `dbt_city_cycles/models/marts/mart_station_growth.sql` | MODIFY -- Refactor window function into CTE |
| 8 | `dbt_city_cycles/dbt_project.yml` | MODIFY -- Replace hardcoded /tmp |

---

## Change 1: CREATE `dbt_city_cycles/macros/day_type.sql`

This macro encapsulates the weekday/weekend classification logic that is currently copy-pasted in all 4 staging models. The current inline version is:
```sql
CASE WHEN extract(isodow from starttime::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END
```

The timestamp column name varies across models (`starttime`, `started_at`, `start_date`), so the macro accepts the column as a parameter.

**File contents:**

```sql
{% macro day_type(timestamp_column) %}
CASE
    WHEN extract(isodow from {{ timestamp_column }}::timestamp) < 6 THEN 'weekday'
    ELSE 'weekend'
END
{% endmacro %}
```

**Usage in models:**
```sql
-- Instead of:
CASE WHEN extract(isodow from starttime::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END AS day_type

-- Use:
{{ day_type('starttime') }} AS day_type
```

---

## Change 2: CREATE `dbt_city_cycles/macros/user_type_mapping.sql`

This macro encapsulates the legacy NYC user type mapping. Currently only used in `stg_nyc_legacy`, but extracting it makes the business logic discoverable and reusable if new data sources have similar legacy user type values.

**Current inline version (in stg_nyc_legacy.sql):**
```sql
case
    when usertype = 'Subscriber' then 'member'
    when usertype = 'Customer' then 'casual'
    else usertype
end as user_type
```

**File contents:**

```sql
{% macro user_type_mapping(column_name) %}
CASE
    WHEN {{ column_name }} = 'Subscriber' THEN 'member'
    WHEN {{ column_name }} = 'Customer' THEN 'casual'
    ELSE {{ column_name }}
END
{% endmacro %}
```

**Usage in models:**
```sql
-- Instead of:
case
    when usertype = 'Subscriber' then 'member'
    when usertype = 'Customer' then 'casual'
    else usertype
end as user_type

-- Use:
{{ user_type_mapping('usertype') }} as user_type
```

---

## Change 3: MODIFY `dbt_city_cycles/models/staging/stg_nyc_legacy.sql`

Two changes in this file:

### 3a: Fix the unique_key

The model already generates a `ride_id` column (lines 22-25) using a concatenation of key fields. The `unique_key` in the config should reference `ride_id` to be consistent with all other staging models.

**BEFORE (lines 1-10):**
```sql
{{ config(
    materialized='incremental',
    unique_key=['bike_id', 'start_time', 'stop_time', 'start_station_id'],
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}
```

**AFTER (lines 1-10):**
```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}
```

**Why this is safe:** The `ride_id` column is already computed as `'legacy_' || bikeid || '_' || start_station_id || '_' || strftime(starttime) || '_' || strftime(stoptime)` -- which is a superset of the composite key's uniqueness. The `indexes` block already declares `ride_id` as unique (line 6). So the incremental merge should behave identically; it will just use a single column match instead of a 4-column composite.

### 3b: Use macros for day_type and user_type_mapping

**BEFORE (lines 40-51 of the renamed CTE):**
```sql
        -- Map legacy user types to modern nomenclature
        case
            when usertype = 'Subscriber' then 'member'
            when usertype = 'Customer' then 'casual'
            else usertype
        end as user_type,
        birth_year::integer AS birth_year,
        gender::integer as gender,
        -- Date-derived fields
        date_trunc('day', starttime::timestamp) as date,
        extract(month from starttime::timestamp) as month,
        extract(year from starttime::timestamp) as year,
        CASE WHEN extract(isodow from starttime::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END AS day_type,
```

**AFTER:**
```sql
        -- Map legacy user types to modern nomenclature
        {{ user_type_mapping('usertype') }} as user_type,
        birth_year::integer AS birth_year,
        gender::integer as gender,
        -- Date-derived fields
        date_trunc('day', starttime::timestamp) as date,
        extract(month from starttime::timestamp) as month,
        extract(year from starttime::timestamp) as year,
        {{ day_type('starttime') }} AS day_type,
```

### Full file after both changes:

```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_nyc_legacy') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Create unique ride_id using concatenation of key fields
        'legacy_' || bikeid || '_' ||
        start_station_id || '_' ||
        strftime('%Y%m%d%H%M%S', starttime::timestamp) || '_' ||
        strftime('%Y%m%d%H%M%S', stoptime::timestamp) as ride_id,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (stoptime::timestamp - starttime::timestamp)) as duration_seconds,
        starttime::timestamp as start_time,
        stoptime::timestamp as stop_time,
        start_station_id,
        start_station_name,
        start_station_latitude::double precision as start_latitude,
        start_station_longitude::double precision as start_longitude,
        end_station_id,
        end_station_name,
        end_station_latitude::double precision as end_latitude,
        end_station_longitude::double precision as end_longitude,
        bikeid as bike_id,
        -- Map legacy user types to modern nomenclature
        {{ user_type_mapping('usertype') }} as user_type,
        birth_year::integer AS birth_year,
        gender::integer as gender,
        -- Date-derived fields
        date_trunc('day', starttime::timestamp) as date,
        extract(month from starttime::timestamp) as month,
        extract(year from starttime::timestamp) as year,
        {{ day_type('starttime') }} AS day_type,
        extract(isodow from starttime::timestamp) - 1 as day_of_week, -- 0=Monday
        extract(hour from starttime::timestamp) as hour_of_day,
        -- Add metadata
        source_file,
        'nyc' as location,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed
```

---

## Change 4: MODIFY `dbt_city_cycles/models/staging/stg_nyc_modern.sql`

Replace the inline `CASE WHEN` day_type logic with the macro.

**BEFORE (line 41):**
```sql
        CASE WHEN extract(isodow from started_at::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END AS day_type,
```

**AFTER (line 41):**
```sql
        {{ day_type('started_at') }} AS day_type,
```

No other changes to this file. The `user_type` in this model is already a direct column reference (`member_casual as user_type`) with no CASE mapping needed.

### Full file after change:

```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_nyc_modern') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Standardize column names with proper types
        ride_id,
        rideable_type,
        started_at::timestamp as start_time,
        ended_at::timestamp as stop_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat::double precision as start_latitude,
        start_lng::double precision as start_longitude,
        end_lat::double precision as end_latitude,
        end_lng::double precision as end_longitude,
        member_casual as user_type,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (ended_at::timestamp - started_at::timestamp)) as duration_seconds,
        -- Date-derived fields
        date_trunc('day', started_at::timestamp) as date,
        extract(month from started_at::timestamp) as month,
        extract(year from started_at::timestamp) as year,
        {{ day_type('started_at') }} AS day_type,
        extract(isodow from started_at::timestamp) - 1 as day_of_week, -- 0=Monday
        extract(hour from started_at::timestamp) as hour_of_day,
        -- Add metadata
        source_file,
        'nyc' as location,
        'modern' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed
```

---

## Change 5: MODIFY `dbt_city_cycles/models/staging/stg_london_legacy.sql`

Replace the inline day_type CASE expression with the macro.

**BEFORE (line 35):**
```sql
        CASE WHEN extract(isodow from start_date::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END AS day_type,
```

**AFTER (line 35):**
```sql
        {{ day_type('start_date') }} AS day_type,
```

### Full file after change:

```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_london_legacy') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Standardize column names with proper types
        rental_id as ride_id,
        bike_id,
        start_date::timestamp as start_time,
        end_date::timestamp as stop_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (end_date::timestamp - start_date::timestamp)) as duration_seconds,
        -- Date-derived fields
        date_trunc('day', start_date::timestamp) as date,
        extract(month from start_date::timestamp) as month,
        extract(year from start_date::timestamp) as year,
        {{ day_type('start_date') }} AS day_type,
        extract(isodow from start_date::timestamp) - 1 as day_of_week, -- 0=Monday
        extract(hour from start_date::timestamp) as hour_of_day,
        -- Add metadata
        source_file,
        'london' as location,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed
```

---

## Change 6: MODIFY `dbt_city_cycles/models/staging/stg_london_modern.sql`

Replace the inline day_type CASE expression with the macro.

**BEFORE (line 36):**
```sql
        CASE WHEN extract(isodow from start_date::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END AS day_type,
```

**AFTER (line 36):**
```sql
        {{ day_type('start_date') }} AS day_type,
```

### Full file after change:

```sql
{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_london_modern') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Standardize column names with proper types
        number as ride_id,
        bike_number as bike_id,
        bike_model,
        start_date::timestamp as start_time,
        end_date::timestamp as stop_time,
        start_station as start_station_name,
        start_station_number as start_station_id,
        end_station as end_station_name,
        end_station_number as end_station_id,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (end_date::timestamp - start_date::timestamp)) as duration_seconds,
        -- Date-derived fields
        date_trunc('day', start_date::timestamp) as date,
        extract(month from start_date::timestamp) as month,
        extract(year from start_date::timestamp) as year,
        {{ day_type('start_date') }} AS day_type,
        extract(isodow from start_date::timestamp) - 1 as day_of_week, -- 0=Monday
        extract(hour from start_date::timestamp) as hour_of_day,
        -- Add metadata
        source_file,
        'london' as location,
        'modern' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed
```

---

## Change 7: MODIFY `dbt_city_cycles/models/marts/mart_station_growth.sql`

Refactor the repeated `lag()` window function into a CTE. Currently the window function `lag(s.station_count) over (partition by s.location order by s.year)` is written 4 separate times in the `growth_calc` CTE:

1. Line 21: assigned to `prev_year_count`
2. Line 23: in the WHEN clause checking for 0
3. Line 24: in the WHEN clause checking for NULL
4. Lines 25-26: in the growth calculation arithmetic (twice)

This violates DRY and makes the query harder to read and maintain.

**BEFORE (entire file):**
```sql
{{ config(
    materialized='table'
) }}

with station_counts as (
    select
        location,
        extract(year from start_time) as year,
        count(distinct start_station_id) as station_count
    from {{ ref('unified_rides') }}
    where start_station_id is not null  -- Exclude rides with null station IDs
    group by 1, 2
),
growth_calc as (
    select
        s.location,
        s.year,
        s.station_count,
        p.population,
        (s.station_count::float / nullif(p.population, 0)) * 1000 as stations_per_1000,
        lag(s.station_count) over (partition by s.location order by s.year) as prev_year_count,
        case
            when lag(s.station_count) over (partition by s.location order by s.year) = 0 then null
            when lag(s.station_count) over (partition by s.location order by s.year) is null then null
            else round(((s.station_count - lag(s.station_count) over (partition by s.location order by s.year))::float /
                  lag(s.station_count) over (partition by s.location order by s.year) * 100)::float, 1)
        end as yoy_growth
    from station_counts s
    left join {{ ref('population') }} p
      on s.location = p.location
     and s.year = p.year
)
select * from growth_calc
order by location, year
```

**AFTER (entire file):**
```sql
{{ config(
    materialized='table'
) }}

with station_counts as (
    select
        location,
        extract(year from start_time) as year,
        count(distinct start_station_id) as station_count
    from {{ ref('unified_rides') }}
    where start_station_id is not null
    group by 1, 2
),

station_with_population as (
    select
        s.location,
        s.year,
        s.station_count,
        p.population,
        (s.station_count::float / nullif(p.population, 0)) * 1000 as stations_per_1000,
        lag(s.station_count) over (partition by s.location order by s.year) as prev_year_count
    from station_counts s
    left join {{ ref('population') }} p
      on s.location = p.location
     and s.year = p.year
),

growth_calc as (
    select
        location,
        year,
        station_count,
        population,
        stations_per_1000,
        prev_year_count,
        case
            when prev_year_count is null or prev_year_count = 0 then null
            else round(((station_count - prev_year_count)::float / prev_year_count * 100)::float, 1)
        end as yoy_growth
    from station_with_population
)

select * from growth_calc
order by location, year
```

**What changed:**
- Split the original `growth_calc` CTE into two: `station_with_population` (computes `prev_year_count` once via `lag()`) and `growth_calc` (uses the already-computed `prev_year_count` for the growth calculation).
- The `lag()` window function now appears exactly once instead of 4+ times.
- The CASE expression is simplified: `when prev_year_count is null or prev_year_count = 0 then null` collapses the two separate WHEN clauses.
- Output columns are identical. No column additions, removals, or renames.

---

## Change 8: MODIFY `dbt_city_cycles/dbt_project.yml`

Replace the hardcoded `/tmp` temp directory with an environment variable that defaults to `/tmp`.

**BEFORE (line 46):**
```yaml
  - "PRAGMA temp_directory='/tmp';"
```

**AFTER (line 46):**
```yaml
  - "PRAGMA temp_directory='{{ env_var(\"DBT_TEMP_DIR\", \"/tmp\") }}';"
```

**Why:** Hardcoded paths reduce portability. On Railway (the current deployment platform) and other container environments, `/tmp` may have limited space or different mount behavior. Using an environment variable allows the deployment to override this without changing code. The default remains `/tmp` so no existing behavior changes unless `DBT_TEMP_DIR` is explicitly set.

**Full on-run-start section after change (lines 42-47):**
```yaml
on-run-start:
  - "PRAGMA memory_limit='512MB';"
  - "PRAGMA threads=1;"
  - "PRAGMA max_temp_directory_size='10GB';"
  - "PRAGMA temp_directory='{{ env_var(\"DBT_TEMP_DIR\", \"/tmp\") }}';"
  - "PRAGMA preserve_insertion_order=false;"
```

---

## What NOT To Do

- **Do NOT change the materialization strategy** of any model. Staging remains `view` (in `dbt_project.yml`) / `incremental` (in `.sql` config -- the `.sql` config takes precedence). Marts remain `table`. Intermediate and unified remain `incremental`.
- **Do NOT rename any model files.** File names must stay exactly as they are.
- **Do NOT change column names in model output.** Downstream consumers (intermediate models, unified model, marts, and the Streamlit dashboard) depend on exact column names. The macro output must produce the same SQL as the inline code it replaces.
- **Do NOT remove the composite unique_key from `stg_nyc_legacy` without confirming that `ride_id` is present.** It IS present (lines 22-25 generate it). But if you are uncertain, run `dbt compile` and check `target/compiled/.../stg_nyc_legacy.sql` to confirm `ride_id` appears in the SELECT.
- **Do NOT modify schema.yml files created in Phase 02** unless a column name changed (none do in this PR).
- **Do NOT add new columns or remove existing columns from any model.**

---

## Verification Checklist

### 1. Macro Compilation
```bash
cd dbt_city_cycles && dbt compile
```
Expected: Clean compilation. Check the compiled output to verify macros expanded correctly:
```bash
cat target/compiled/dbt_city_cycles/models/staging/stg_nyc_legacy.sql | grep -A2 "day_type"
```
Expected output should show the expanded CASE expression, not the Jinja macro call.

### 2. Macro Expansion Verification
```bash
# Verify day_type macro expands correctly
cat target/compiled/dbt_city_cycles/models/staging/stg_nyc_legacy.sql | grep "isodow"
cat target/compiled/dbt_city_cycles/models/staging/stg_nyc_modern.sql | grep "isodow"
cat target/compiled/dbt_city_cycles/models/staging/stg_london_legacy.sql | grep "isodow"
cat target/compiled/dbt_city_cycles/models/staging/stg_london_modern.sql | grep "isodow"
```
Expected: Each file should contain exactly one line with `extract(isodow from ...)` -- the expanded `day_type` macro.

```bash
# Verify user_type_mapping macro expands correctly
cat target/compiled/dbt_city_cycles/models/staging/stg_nyc_legacy.sql | grep -A3 "Subscriber"
```
Expected: Should show the CASE expression mapping Subscriber to member and Customer to casual.

### 3. Full Build
```bash
cd dbt_city_cycles && dbt run --full-refresh
```
Expected: All models build successfully. Pay special attention to `stg_nyc_legacy` since its unique_key changed.

### 4. Data Tests
```bash
cd dbt_city_cycles && dbt test
```
Expected: All tests from Phase 02 pass. The schema.yml tests (unique, not_null, accepted_values) should all pass on the refactored models since the output data is unchanged.

### 5. Output Column Verification
```bash
# After dbt run --full-refresh, verify output columns are unchanged
cd dbt_city_cycles && dbt run-operation generate_schema_name --args '{custom_schema_name: staging}'
```
Or simply query the tables:
```sql
DESCRIBE staging.stg_nyc_legacy;
DESCRIBE staging.stg_nyc_modern;
DESCRIBE staging.stg_london_legacy;
DESCRIBE staging.stg_london_modern;
DESCRIBE marts.mart_station_growth;
```
Expected: Column names and types identical to before the refactor.

### 6. mart_station_growth Output Comparison

If data is loaded, compare output before and after:
```bash
# Before making changes, save baseline:
cd dbt_city_cycles && dbt run --select mart_station_growth
# Then in DuckDB:
# SELECT * FROM marts.mart_station_growth ORDER BY location, year;
# Save this output.

# After making changes:
cd dbt_city_cycles && dbt run --select mart_station_growth --full-refresh
# SELECT * FROM marts.mart_station_growth ORDER BY location, year;
# Compare -- should be identical.
```

### 7. dbt_project.yml env_var
```bash
cd dbt_city_cycles && dbt compile
cat target/compiled/dbt_city_cycles/dbt_project.yml 2>/dev/null || echo "Check on-run-start in dbt debug output"
dbt debug 2>&1 | grep -i "temp"
```
Verify no compilation errors from the env_var Jinja expression.

---

## PR Checklist

- [ ] 2 new macro files created (`day_type.sql`, `user_type_mapping.sql`)
- [ ] 4 staging models updated to use macros
- [ ] `stg_nyc_legacy` unique_key changed from composite to `ride_id`
- [ ] `mart_station_growth` window function refactored into CTE
- [ ] `dbt_project.yml` hardcoded `/tmp` replaced with env_var
- [ ] `dbt compile` succeeds
- [ ] `dbt run --full-refresh` succeeds (all models build)
- [ ] `dbt test` passes (all Phase 02 tests still pass)
- [ ] Compiled SQL output verified (macros expand correctly)
- [ ] No column names changed in any model output
- [ ] CHANGELOG.md updated

### CHANGELOG Entry
```markdown
### Changed
- **dbt Macros** - Extracted repeated SQL logic into reusable Jinja macros
  - `day_type(timestamp_column)` macro for weekday/weekend classification (used in all 4 staging models)
  - `user_type_mapping(column_name)` macro for legacy Subscriber/Customer to member/casual mapping
- **stg_nyc_legacy Unique Key** - Changed unique_key from composite 4-column key to `ride_id` for consistency with all other staging models

### Improved
- **mart_station_growth SQL** - Refactored repeated `lag()` window function (4 occurrences) into a single CTE
- **dbt_project.yml Portability** - Replaced hardcoded `/tmp` temp directory with `env_var("DBT_TEMP_DIR", "/tmp")` for deployment flexibility
```
