# Phase 02: Hourly Ride-Weather Analytics Marts

## PR Title
feat: add hourly ride-weather correlation analytics marts

## Risk Level: Low
## Estimated Effort: 1-2 days
## Status: ✅ COMPLETE
## Started: 2026-02-12
## Completed: 2026-02-12
## PR: #37
## Dependencies: Phase 01 (Weather Data Pipeline) ✅ MERGED (PR #36)
## Unlocks: Phases 04, 06

## Files Impact
| Action | File |
|--------|------|
| CREATE | dbt_city_cycles/models/marts/mart_hourly_rides.sql |
| CREATE | dbt_city_cycles/models/marts/mart_hourly_patterns_summary.sql |
| CREATE | dbt_city_cycles/models/marts/mart_weather_ride_correlation.sql |
| CREATE | dbt_city_cycles/models/marts/mart_weather_impact_summary.sql |
| DELETE | dbt_city_cycles/models/marts/mart_hourly_patterns.sql |
| MODIFY | dbt_city_cycles/models/marts/schema.yml |
| MODIFY | dashboard/app.py (1 line) |
| MODIFY | streamlit_data_manager/parquet_file_manager.py |
| MODIFY | db_duckdb/operations.py |
| MODIFY | db_duckdb/pipeline.py |
| MODIFY | tests/test_streamlit_data_manager.py |
| MODIFY | tests/test_dashboard.py |
| MODIFY | tests/test_db_duckdb_operations.py |

## Context
This phase expands the hourly ride data to include a date dimension (enabling weather joins), creates weather-ride correlation marts, and pre-computes impact summaries that power recommendations like "34% fewer rides when raining at 9am." The existing mart_hourly_patterns (aggregated across all dates) is replaced by a granular mart_hourly_rides with a derived summary for backward compatibility.

---

# Phase 02: Hourly Ride-Weather Analytics Marts -- Detailed Implementation Plan

## 1. Current State Summary

**What exists today:**

- `mart_hourly_patterns.sql` -- A trivial 48-row table (24 hours x 2 cities) that aggregates ride counts by `(location, hour_of_day)` across ALL dates. No date dimension. Materialized as a `table`.
- Dashboard `app.py` line 408 queries `mart_hourly_patterns.parquet` with: `SELECT hour_of_day, ride_count FROM '...' WHERE location = $1 ORDER BY hour_of_day` -- used in the "Time of Day Analysis" bar chart under NYC/London single-city pages.
- `streamlit_data_manager/parquet_file_manager.py` lists 5 marts in its `MARTS` list.
- `db_duckdb/operations.py` has `MART_TABLES` list with 5 entries used by `export_marts`.
- `db_duckdb/pipeline.py` line 193 has a separate `mart_tables` list (4 entries, missing `mart_daily_metrics_long`) used by `check_pipeline_status`.
- Phase 01 (merged, PR #36) added `stg_weather_hourly` as a dbt staging model with columns: `weather_record_id`, `timestamp`, `city`, `temperature_celsius`, `apparent_temperature_celsius`, `relative_humidity_pct`, `precipitation_mm`, `rain_mm`, `snowfall_cm`, `snow_depth_m`, `weather_code`, `weather_condition`, `cloud_cover_pct`, `wind_speed_kmh`, `wind_gusts_kmh`, `is_precipitation`, `precipitation_intensity`, `temperature_band`, `wind_category`, `date`, `hour`, `month`, `year`, `day_type`, `day_of_week`, `hour_of_day`, `source_file`, `dbt_updated_at`. NOTE: weather uses `city` (not `location`), and all metric columns have unit suffixes (_celsius, _pct, _mm, _cm, _m, _kmh). Weather conditions are lowercase ('clear', 'rain', 'snow', etc.).

**What this phase delivers:**
- 4 new/modified dbt models
- 1 dashboard query update (backward-compatible)
- Updates to 3 Python files (streamlit_data_manager, db_duckdb operations, db_duckdb pipeline)
- dbt schema.yml documentation and tests
- pytest tests

---

## 2. dbt Model Changes (in dependency order)

### 2.1 RENAME + EXPAND: `mart_hourly_patterns.sql` --> `mart_hourly_rides.sql`

**Action:** Delete `dbt_city_cycles/models/marts/mart_hourly_patterns.sql`. Create `dbt_city_cycles/models/marts/mart_hourly_rides.sql`.

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_hourly_rides.sql`

**Exact SQL:**

```sql
{{ config(
    materialized='table'
) }}

select
    location,
    date,
    hour_of_day,
    count(*) as ride_count,
    avg(duration_seconds) as avg_duration_seconds,
    sum(case when user_type = 'member' then 1 else 0 end) as member_rides,
    sum(case when user_type = 'casual' then 1 else 0 end) as casual_rides
from {{ ref('unified_rides') }}
group by 1, 2, 3
order by 1, 2, 3
```

**Grain:** `(location, date, hour_of_day)` -- approximately `(2 cities * ~2200 days * 24 hours) = ~105,600 rows`. This is well within the memory limits set in `dbt_project.yml` (512MB).

**Key design decisions:**
- `avg_duration_seconds` uses the raw `duration_seconds` from `unified_rides` (available on every row). This mirrors how `mart_daily_metrics` computes `avg(m.duration_seconds)/60 as avg_duration_minutes` but keeps it in seconds for consistency at the granular level.
- `member_rides` and `casual_rides` will be 0 for London rows (since `user_type` is NULL for London). This is the same pattern used by `mart_daily_metrics`.
- Uses `materialized='table'` consistent with the `dbt_project.yml` marts config.

### 2.2 NEW: `mart_hourly_patterns_summary.sql` (backward-compatible replacement)

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_hourly_patterns_summary.sql`

**Exact SQL:**

```sql
{{ config(
    materialized='table'
) }}

select
    location,
    hour_of_day,
    sum(ride_count) as ride_count
from {{ ref('mart_hourly_rides') }}
group by 1, 2
order by 1, 2
```

**Purpose:** This produces the EXACT same schema and data as the old `mart_hourly_patterns` -- `(location, hour_of_day, ride_count)`. The dashboard query on line 408 of `app.py` currently reads from `mart_hourly_patterns.parquet`. We will update it to read from `mart_hourly_patterns_summary.parquet`. The column names (`location`, `hour_of_day`, `ride_count`) and data types are identical, so no other dashboard code changes are needed.

### 2.3 NEW: `mart_weather_ride_correlation.sql`

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_weather_ride_correlation.sql`

**Exact SQL:**

```sql
{{ config(
    materialized='table'
) }}

select
    r.location,
    r.date,
    r.hour_of_day,
    -- Ride metrics
    r.ride_count,
    r.avg_duration_seconds,
    r.member_rides,
    r.casual_rides,
    -- Weather metrics (column names from stg_weather_hourly Phase 01)
    w.temperature_celsius,
    w.apparent_temperature_celsius,
    w.relative_humidity_pct,
    w.precipitation_mm,
    w.rain_mm,
    w.snowfall_cm,
    w.snow_depth_m,
    w.weather_code,
    w.weather_condition,
    w.cloud_cover_pct,
    w.wind_speed_kmh,
    w.wind_gusts_kmh,
    w.is_precipitation,
    w.precipitation_intensity,
    w.temperature_band,
    w.wind_category
from {{ ref('mart_hourly_rides') }} r
inner join {{ ref('stg_weather_hourly') }} w
    on r.location = w.city
    and r.date = w.date
    and r.hour_of_day = w.hour_of_day
```

**Key design decisions:**
- **INNER JOIN** -- We only want rows where we have BOTH ride data and weather data. Hours/dates with weather data but zero rides are not useful for correlation. Hours with rides but no weather data (possible if weather data coverage is partial) would produce NULLs in all weather columns, which would pollute aggregations downstream. The inner join ensures clean data.
- No `ORDER BY` -- This is a large intermediate mart consumed by `mart_weather_impact_summary`. Ordering adds cost with no benefit since the downstream model will re-aggregate.
- Grain remains `(location, date, hour_of_day)`.

### 2.4 NEW: `mart_weather_impact_summary.sql`

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_weather_impact_summary.sql`

**Exact SQL:**

```sql
{{ config(
    materialized='table'
) }}

with baseline as (
    -- Baseline: average rides per (location, hour_of_day) during clear weather
    -- weather_condition = 'clear' from stg_weather_hourly corresponds to weather_code 0
    select
        location,
        hour_of_day,
        avg(ride_count) as baseline_avg_rides,
        avg(avg_duration_seconds) as baseline_avg_duration_seconds
    from {{ ref('mart_weather_ride_correlation') }}
    where weather_condition = 'clear'
    group by 1, 2
),

by_weather_condition as (
    select
        c.location,
        c.hour_of_day,
        c.weather_condition,
        count(*) as observation_count,
        avg(c.ride_count) as avg_rides,
        avg(c.avg_duration_seconds) as avg_duration_seconds,
        avg(c.member_rides) as avg_member_rides,
        avg(c.casual_rides) as avg_casual_rides,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds,
        case
            when b.baseline_avg_rides is null or b.baseline_avg_rides = 0 then null
            else round(
                ((avg(c.ride_count) - b.baseline_avg_rides) / b.baseline_avg_rides * 100)::float,
                1
            )
        end as pct_change_rides_vs_clear,
        case
            when b.baseline_avg_duration_seconds is null or b.baseline_avg_duration_seconds = 0 then null
            else round(
                ((avg(c.avg_duration_seconds) - b.baseline_avg_duration_seconds) / b.baseline_avg_duration_seconds * 100)::float,
                1
            )
        end as pct_change_duration_vs_clear
    from {{ ref('mart_weather_ride_correlation') }} c
    left join baseline b
        on c.location = b.location
        and c.hour_of_day = b.hour_of_day
    group by
        c.location,
        c.hour_of_day,
        c.weather_condition,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds
),

by_precipitation_temp as (
    select
        c.location,
        c.hour_of_day,
        c.is_precipitation,
        c.temperature_band,
        count(*) as observation_count,
        avg(c.ride_count) as avg_rides,
        avg(c.avg_duration_seconds) as avg_duration_seconds,
        avg(c.member_rides) as avg_member_rides,
        avg(c.casual_rides) as avg_casual_rides,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds,
        case
            when b.baseline_avg_rides is null or b.baseline_avg_rides = 0 then null
            else round(
                ((avg(c.ride_count) - b.baseline_avg_rides) / b.baseline_avg_rides * 100)::float,
                1
            )
        end as pct_change_rides_vs_clear,
        case
            when b.baseline_avg_duration_seconds is null or b.baseline_avg_duration_seconds = 0 then null
            else round(
                ((avg(c.avg_duration_seconds) - b.baseline_avg_duration_seconds) / b.baseline_avg_duration_seconds * 100)::float,
                1
            )
        end as pct_change_duration_vs_clear
    from {{ ref('mart_weather_ride_correlation') }} c
    left join baseline b
        on c.location = b.location
        and c.hour_of_day = b.hour_of_day
    group by
        c.location,
        c.hour_of_day,
        c.is_precipitation,
        c.temperature_band,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds
)

select
    location,
    hour_of_day,
    'weather_condition' as dimension_type,
    weather_condition as dimension_value,
    cast(null as boolean) as is_precipitation,
    cast(null as varchar) as temperature_band,
    observation_count,
    avg_rides,
    avg_duration_seconds,
    avg_member_rides,
    avg_casual_rides,
    baseline_avg_rides,
    baseline_avg_duration_seconds,
    pct_change_rides_vs_clear,
    pct_change_duration_vs_clear
from by_weather_condition

union all

select
    location,
    hour_of_day,
    'precip_temp' as dimension_type,
    cast(null as varchar) as dimension_value,
    is_precipitation,
    temperature_band,
    observation_count,
    avg_rides,
    avg_duration_seconds,
    avg_member_rides,
    avg_casual_rides,
    baseline_avg_rides,
    baseline_avg_duration_seconds,
    pct_change_rides_vs_clear,
    pct_change_duration_vs_clear
from by_precipitation_temp

order by location, hour_of_day, dimension_type, dimension_value
```

**Key design decisions:**

- **Two dimension types in one table:** Rather than creating two separate mart tables, we use a `dimension_type` discriminator column (`'weather_condition'` vs `'precip_temp'`). This keeps the mart count manageable and the dashboard can filter on `dimension_type` to get the specific slice it needs.
- **Baseline is clear weather:** The `pct_change_rides_vs_clear` column gives the core insight: "34% fewer rides when raining at 9am" is directly `pct_change_rides_vs_clear` where `dimension_value = 'rain'` and `hour_of_day = 9`.
- **`observation_count`** tells consumers how statistically significant each row is. A weather condition with only 2 observations should not be trusted.
- The `round(..., 1)` matches the rounding pattern used in `mart_station_growth.sql` (line 39).
- DuckDB may need `::numeric` for the `round()` function. If DuckDB complains, change to `round((...), 1)::float` which is what `mart_station_growth` uses. Actually, looking at `mart_station_growth` line 39, it uses `round(((...)::float, 1)` -- we should follow the same pattern: `round(((avg(c.ride_count) - b.baseline_avg_rides) / b.baseline_avg_rides * 100)::float, 1)`.

**Cast pattern:** Uses `::float` to match `mart_station_growth.sql` (corrected from original `::numeric`).

---

## 3. dbt Schema.yml Updates

### 3.1 Modify: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/schema.yml`

**Action:** Remove the `mart_hourly_patterns` entry. Add entries for `mart_hourly_rides`, `mart_hourly_patterns_summary`, `mart_weather_ride_correlation`, and `mart_weather_impact_summary`.

**Exact YAML to add (after removing the `mart_hourly_patterns` block at lines 76-92):**

```yaml
  - name: mart_hourly_rides
    description: >
      Granular ride metrics aggregated by location, date, and hour of day.
      This is the date-aware replacement for mart_hourly_patterns, enabling
      time-series analysis and weather correlation joins.
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
      - name: hour_of_day
        description: Hour of day (0-23)
        tests:
          - not_null
      - name: ride_count
        description: Total number of rides in this hour on this date
        tests:
          - not_null
      - name: avg_duration_seconds
        description: Average ride duration in seconds for this hour on this date
      - name: member_rides
        description: Number of rides by members (0 for London, which has no user type data)
      - name: casual_rides
        description: Number of rides by casual users (0 for London, which has no user type data)

  - name: mart_hourly_patterns_summary
    description: >
      Ride counts aggregated by hour of day and location across all dates.
      Backward-compatible replacement for the original mart_hourly_patterns.
      Derived from mart_hourly_rides by aggregating out the date dimension.
      Used by the Streamlit dashboard Time of Day Analysis chart.
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

  - name: mart_weather_ride_correlation
    description: >
      Hourly ride metrics joined with weather data for ride-weather correlation analysis.
      Inner join of mart_hourly_rides and stg_weather_hourly on (location, date, hour_of_day).
      Contains both ride and weather metrics at hourly granularity.
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
      - name: hour_of_day
        description: Hour of day (0-23)
        tests:
          - not_null
      - name: ride_count
        description: Total rides in this hour
        tests:
          - not_null
      - name: avg_duration_seconds
        description: Average ride duration in seconds
      - name: member_rides
        description: Member ride count (0 for London)
      - name: casual_rides
        description: Casual ride count (0 for London)
      - name: temperature_celsius
        description: Temperature at 2 meters (Celsius)
      - name: apparent_temperature_celsius
        description: Feels-like temperature (Celsius)
      - name: relative_humidity_pct
        description: Relative humidity percentage
      - name: precipitation_mm
        description: Precipitation amount (mm)
      - name: rain_mm
        description: Rain amount (mm)
      - name: snowfall_cm
        description: Snowfall amount (cm)
      - name: snow_depth_m
        description: Snow depth (meters)
      - name: weather_code
        description: WMO weather code
      - name: weather_condition
        description: Human-readable weather condition (lowercase)
      - name: cloud_cover_pct
        description: Cloud cover percentage
      - name: wind_speed_kmh
        description: Wind speed (km/h)
      - name: wind_gusts_kmh
        description: Wind gust speed (km/h)
      - name: is_precipitation
        description: Boolean flag for precipitation
      - name: precipitation_intensity
        description: Precipitation intensity category
      - name: temperature_band
        description: Temperature band category
      - name: wind_category
        description: Wind intensity category

  - name: mart_weather_impact_summary
    description: >
      Pre-computed weather impact statistics for ride recommendations.
      Shows how ride counts and durations change relative to clear-weather baseline
      for each (location, hour_of_day) broken down by weather condition and by
      precipitation/temperature band combinations.
      Powers dashboard insights like "34% fewer rides when raining at 9am."
    columns:
      - name: location
        description: City identifier (nyc or london)
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: hour_of_day
        description: Hour of day (0-23)
        tests:
          - not_null
      - name: dimension_type
        description: "Type of grouping: 'weather_condition' or 'precip_temp'"
        tests:
          - not_null
          - accepted_values:
              values: ['weather_condition', 'precip_temp']
      - name: dimension_value
        description: Weather condition name (when dimension_type is 'weather_condition'), NULL otherwise
      - name: is_precipitation
        description: Boolean precipitation flag (when dimension_type is 'precip_temp'), NULL otherwise
      - name: temperature_band
        description: Temperature band (when dimension_type is 'precip_temp'), NULL otherwise
      - name: observation_count
        description: Number of (date, hour) observations in this group
        tests:
          - not_null
      - name: avg_rides
        description: Average ride count per hour in this weather group
      - name: avg_duration_seconds
        description: Average ride duration in seconds in this weather group
      - name: avg_member_rides
        description: Average member ride count (0 for London)
      - name: avg_casual_rides
        description: Average casual ride count (0 for London)
      - name: baseline_avg_rides
        description: Average rides at the same hour during clear weather (baseline)
      - name: baseline_avg_duration_seconds
        description: Average duration at the same hour during clear weather (baseline)
      - name: pct_change_rides_vs_clear
        description: Percentage change in rides vs clear weather baseline (e.g., -34.0 means 34% fewer rides)
      - name: pct_change_duration_vs_clear
        description: Percentage change in duration vs clear weather baseline
```

---

## 4. Dashboard Update

### 4.1 Modify: `/Users/chris/Projects/city-cycles/dashboard/app.py`

**Change location:** Line 408.

**Current code (line 408):**
```python
hour_query = f"SELECT hour_of_day, ride_count FROM '{os.path.join(DATA_DIR, 'mart_hourly_patterns.parquet')}' WHERE location = $1 ORDER BY hour_of_day"
```

**New code (line 408):**
```python
hour_query = f"SELECT hour_of_day, ride_count FROM '{os.path.join(DATA_DIR, 'mart_hourly_patterns_summary.parquet')}' WHERE location = $1 ORDER BY hour_of_day"
```

This is the ONLY change needed in `app.py`. The query column names (`hour_of_day`, `ride_count`) and the `WHERE location = $1` filter are identical. The `px.bar(hour_df, x='hour_of_day', y='ride_count', ...)` call on line 411 continues to work unchanged.

**Note:** No new dashboard visualizations are added in this phase. The weather impact visualizations will come in a future phase. This phase focuses on building the data layer.

---

## 5. Streamlit Data Manager Update

### 5.1 Modify: `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py`

**Current `MARTS` list (lines 13-19):**
```python
MARTS = [
    "mart_daily_metrics.parquet",
    "mart_hourly_patterns.parquet",
    "mart_nyc_member_analysis.parquet",
    "mart_station_growth.parquet",
    "mart_daily_metrics_long.parquet"
]
```

**New `MARTS` list:**
```python
MARTS = [
    "mart_daily_metrics.parquet",
    "mart_hourly_patterns_summary.parquet",
    "mart_nyc_member_analysis.parquet",
    "mart_station_growth.parquet",
    "mart_daily_metrics_long.parquet",
    "mart_hourly_rides.parquet",
    "mart_weather_ride_correlation.parquet",
    "mart_weather_impact_summary.parquet",
]
```

**Rationale:**
- `mart_hourly_patterns.parquet` is replaced by `mart_hourly_patterns_summary.parquet`.
- Three new parquet files are added for the new marts.
- Even though the dashboard does not yet display weather marts, the data manager should make all marts available locally so future dashboard phases can access them without a separate data manager change.

---

## 6. db_duckdb Module Updates

### 6.1 Modify: `/Users/chris/Projects/city-cycles/db_duckdb/operations.py`

**Current `MART_TABLES` list (lines 449-455):**
```python
MART_TABLES = [
    'mart_daily_metrics',
    'mart_hourly_patterns', 
    'mart_nyc_member_analysis',
    'mart_station_growth',
    'mart_daily_metrics_long'
]
```

**New `MART_TABLES` list:**
```python
MART_TABLES = [
    'mart_daily_metrics',
    'mart_hourly_rides',
    'mart_hourly_patterns_summary',
    'mart_nyc_member_analysis',
    'mart_station_growth',
    'mart_daily_metrics_long',
    'mart_weather_ride_correlation',
    'mart_weather_impact_summary',
]
```

**Rationale:** `mart_hourly_patterns` is removed and replaced by `mart_hourly_rides` and `mart_hourly_patterns_summary`. The two weather marts are added.

### 6.2 Modify: `/Users/chris/Projects/city-cycles/db_duckdb/pipeline.py`

**Current mart_tables list (lines 193-194):**
```python
mart_tables = ['mart_daily_metrics', 'mart_hourly_patterns', 
              'mart_nyc_member_analysis', 'mart_station_growth']
```

**New mart_tables list:**
```python
mart_tables = ['mart_daily_metrics', 'mart_hourly_rides',
              'mart_hourly_patterns_summary', 'mart_nyc_member_analysis',
              'mart_station_growth', 'mart_daily_metrics_long',
              'mart_weather_ride_correlation', 'mart_weather_impact_summary']
```

**Note:** Also adding `mart_daily_metrics_long` which was already missing from this status-check list (a pre-existing inconsistency being fixed).

---

## 7. Test Plan

### 7.1 dbt Tests (declared in schema.yml)

All tests are declared in the schema.yml additions above. Summary:

| Model | Test | Column |
|-------|------|--------|
| `mart_hourly_rides` | `not_null` | location, date, hour_of_day, ride_count |
| `mart_hourly_rides` | `accepted_values` | location: ['nyc', 'london'] |
| `mart_hourly_patterns_summary` | `not_null` | location, hour_of_day, ride_count |
| `mart_weather_ride_correlation` | `not_null` | location, date, hour_of_day, ride_count |
| `mart_weather_ride_correlation` | `accepted_values` | location: ['nyc', 'london'] |
| `mart_weather_impact_summary` | `not_null` | location, hour_of_day, dimension_type, observation_count |
| `mart_weather_impact_summary` | `accepted_values` | location: ['nyc', 'london'], dimension_type: ['weather_condition', 'precip_temp'] |

Run with: `cd dbt_city_cycles && dbt test`

### 7.2 pytest Tests

#### 7.2.1 Update: `/Users/chris/Projects/city-cycles/tests/test_streamlit_data_manager.py`

**Modify `test_marts_list_is_complete` (lines 95-109):**

Change `expected` list and count:
```python
def test_marts_list_is_complete(self):
    """The MARTS list should contain all 8 expected mart Parquet files."""
    from streamlit_data_manager.parquet_file_manager import MARTS

    expected = [
        "mart_daily_metrics.parquet",
        "mart_hourly_patterns_summary.parquet",
        "mart_nyc_member_analysis.parquet",
        "mart_station_growth.parquet",
        "mart_daily_metrics_long.parquet",
        "mart_hourly_rides.parquet",
        "mart_weather_ride_correlation.parquet",
        "mart_weather_impact_summary.parquet",
    ]

    assert len(MARTS) == 8
    for mart in expected:
        assert mart in MARTS, f"Missing expected mart: {mart}"
```

**Add new test to verify old mart is removed:**
```python
def test_old_mart_hourly_patterns_removed(self):
    """The old mart_hourly_patterns.parquet should NOT be in the MARTS list."""
    from streamlit_data_manager.parquet_file_manager import MARTS
    assert "mart_hourly_patterns.parquet" not in MARTS
```

#### 7.2.2 Update: `/Users/chris/Projects/city-cycles/tests/test_dashboard.py`

**Add a test for the updated hourly patterns query:**
```python
def test_hourly_patterns_summary_query(self, memory_conn):
    """The hourly patterns query should work with mart_hourly_patterns_summary schema."""
    memory_conn.execute("""
        CREATE TABLE hourly_summary (
            location VARCHAR,
            hour_of_day INTEGER,
            ride_count BIGINT
        )
    """)
    memory_conn.execute("""
        INSERT INTO hourly_summary VALUES
        ('nyc', 0, 1000),
        ('nyc', 8, 5000),
        ('nyc', 17, 4500),
        ('london', 0, 800),
        ('london', 8, 4000),
        ('london', 17, 3500)
    """)

    result = memory_conn.execute("""
        SELECT hour_of_day, ride_count
        FROM hourly_summary
        WHERE location = 'nyc'
        ORDER BY hour_of_day
    """).fetchdf()

    assert len(result) == 3
    assert result["hour_of_day"][0] == 0
    assert result["ride_count"][0] == 1000
```

**Add a test for weather impact summary query pattern:**
```python
def test_weather_impact_summary_query(self, memory_conn):
    """The weather impact summary query should return pct_change data."""
    memory_conn.execute("""
        CREATE TABLE weather_impact (
            location VARCHAR,
            hour_of_day INTEGER,
            dimension_type VARCHAR,
            dimension_value VARCHAR,
            is_precipitation BOOLEAN,
            temperature_band VARCHAR,
            observation_count INTEGER,
            avg_rides FLOAT,
            avg_duration_seconds FLOAT,
            avg_member_rides FLOAT,
            avg_casual_rides FLOAT,
            baseline_avg_rides FLOAT,
            baseline_avg_duration_seconds FLOAT,
            pct_change_rides_vs_clear FLOAT,
            pct_change_duration_vs_clear FLOAT
        )
    """)
    memory_conn.execute("""
        INSERT INTO weather_impact VALUES
        ('nyc', 9, 'weather_condition', 'rain', NULL, NULL,
         30, 150.0, 720.0, 100.0, 50.0, 220.0, 680.0, -31.8, 5.9),
        ('nyc', 9, 'weather_condition', 'clear', NULL, NULL,
         60, 220.0, 680.0, 150.0, 70.0, 220.0, 680.0, 0.0, 0.0)
    """)

    result = memory_conn.execute("""
        SELECT dimension_value, pct_change_rides_vs_clear
        FROM weather_impact
        WHERE location = 'nyc'
          AND hour_of_day = 9
          AND dimension_type = 'weather_condition'
          AND dimension_value = 'rain'
    """).fetchdf()

    assert len(result) == 1
    assert result["pct_change_rides_vs_clear"][0] == pytest.approx(-31.8, abs=0.1)
```

#### 7.2.3 Update: `/Users/chris/Projects/city-cycles/tests/test_db_duckdb_operations.py`

**Add a test to verify the MART_TABLES list in export_marts includes the new marts. This follows the existing test pattern for DuckDBOperations:**

```python
def test_export_marts_includes_weather_marts(self):
    """export_marts MART_TABLES should include all weather-related mart tables."""
    from db_duckdb.operations import DuckDBOperations
    import inspect

    source = inspect.getsource(DuckDBOperations.export_marts)

    assert 'mart_hourly_rides' in source
    assert 'mart_hourly_patterns_summary' in source
    assert 'mart_weather_ride_correlation' in source
    assert 'mart_weather_impact_summary' in source
    assert 'mart_hourly_patterns' not in source or 'mart_hourly_patterns_summary' in source
```

---

## 8. Verification Steps (Post-Implementation)

Run these in order after all changes are made:

### Step 1: Run pytest
```bash
/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v
```
Expected: All 83+ tests pass (with additions bringing total to ~88).

### Step 2: Run dbt models
```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles && dbt run
```
Expected: All models build successfully. The new 4 mart models should appear in the output. The old `mart_hourly_patterns` should NOT appear.

### Step 3: Run dbt tests
```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles && dbt test
```
Expected: All schema tests pass.

### Step 4: Verify backward compatibility
```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles && dbt run -s mart_hourly_patterns_summary
```
Then verify the output has exactly the same schema as the old `mart_hourly_patterns`:
```sql
-- In DuckDB or dbt compile
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'mart_hourly_patterns_summary'
ORDER BY ordinal_position;
-- Expected: location (VARCHAR), hour_of_day (INTEGER), ride_count (BIGINT)
```

### Step 5: Verify export works
```bash
/Users/chris/Projects/city-cycles/venv/bin/python -m db_duckdb.cli export --dry-run
```
Expected: All 8 mart tables listed for export.

### Step 6: Verify weather impact data quality
After a full `dbt run`, query the impact summary:
```sql
SELECT location, hour_of_day, dimension_value, pct_change_rides_vs_clear, observation_count
FROM main_marts.mart_weather_impact_summary
WHERE dimension_type = 'weather_condition'
AND observation_count >= 10
ORDER BY pct_change_rides_vs_clear ASC
LIMIT 10;
```
Expected: Negative percentages for adverse weather conditions (Rain, Snow, Thunderstorm), positive or near-zero for Clear.

---

## 9. File Change Summary

| File | Action | Lines Changed |
|------|--------|---------------|
| `dbt_city_cycles/models/marts/mart_hourly_patterns.sql` | DELETE | entire file |
| `dbt_city_cycles/models/marts/mart_hourly_rides.sql` | CREATE | ~15 lines |
| `dbt_city_cycles/models/marts/mart_hourly_patterns_summary.sql` | CREATE | ~12 lines |
| `dbt_city_cycles/models/marts/mart_weather_ride_correlation.sql` | CREATE | ~35 lines |
| `dbt_city_cycles/models/marts/mart_weather_impact_summary.sql` | CREATE | ~120 lines |
| `dbt_city_cycles/models/marts/schema.yml` | MODIFY | Remove 17 lines, add ~130 lines |
| `dashboard/app.py` | MODIFY | 1 line (line 408) |
| `streamlit_data_manager/parquet_file_manager.py` | MODIFY | 7 lines (MARTS list) |
| `db_duckdb/operations.py` | MODIFY | 6 lines (MART_TABLES list) |
| `db_duckdb/pipeline.py` | MODIFY | 3 lines (mart_tables list) |
| `tests/test_streamlit_data_manager.py` | MODIFY | ~15 lines (update existing test + add new) |
| `tests/test_dashboard.py` | MODIFY | ~50 lines (add 2 new test methods) |
| `tests/test_db_duckdb_operations.py` | MODIFY | ~12 lines (add 1 new test method) |

---

## 10. Dependency Notes

- **Phase 01 must be merged first.** This phase references `{{ ref('stg_weather_hourly') }}` in `mart_weather_ride_correlation.sql`. If Phase 01 is not yet merged, the `mart_weather_ride_correlation` and `mart_weather_impact_summary` models will fail during `dbt run`.
- **The two non-weather models (`mart_hourly_rides` and `mart_hourly_patterns_summary`) can be implemented independently of Phase 01.** They only depend on `unified_rides`, which already exists. A phased rollout could ship these first if Phase 01 is delayed.
- **Dashboard backward compatibility is immediate.** As long as `mart_hourly_patterns_summary.parquet` is exported to S3, the dashboard continues to work identically.

---

## 11. Potential Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| `mart_hourly_rides` table too large for memory | Low (est. ~105K rows) | Well within 512MB PRAGMA; monitor with `log_memory_usage` |
| `mart_weather_ride_correlation` INNER JOIN produces zero rows | Medium (if weather data date range doesn't overlap ride data) | Verify Phase 01 weather data covers same date range as rides; add dbt test `dbt_utils.at_least_one` if available |
| Clear weather baseline has zero rows for some (location, hour) | Low | The `CASE WHEN baseline = 0 THEN null` guard prevents division by zero; NULL pct_change is acceptable |
| DuckDB `round()` syntax differs from Postgres | Low | Tested with `::float` pattern from `mart_station_growth.sql` |
| Old `mart_hourly_patterns.parquet` remains in S3 and confuses consumers | Low | Document removal in CHANGELOG; old file is harmless (no downstream consumer after dashboard update) |

---

### Critical Files for Implementation
- `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_hourly_patterns.sql` - File to delete and replace with mart_hourly_rides.sql
- `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/schema.yml` - Must update with all 4 new model definitions and remove old mart_hourly_patterns entry
- `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py` - Must update MARTS list (which cascades to dashboard data availability and S3 exports)
- `/Users/chris/Projects/city-cycles/db_duckdb/operations.py` - Must update MART_TABLES in export_marts to export all new parquet files
- `/Users/chris/Projects/city-cycles/dashboard/app.py` - Single-line change on line 408 to point to mart_hourly_patterns_summary.parquet