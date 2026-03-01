# Phase 02 — Build "Similar Day" Mart

**Status:** ✅ COMPLETE
**Started:** 2026-02-28
**Completed:** 2026-02-28
**PR:** #44

## Header

| Field | Value |
|---|---|
| **PR Title** | feat(dbt): add mart_similar_day_stats for "days like today" weather queries |
| **Risk Level** | Low |
| **Estimated Effort** | Medium (3-5 hours) |
| **Files Created** | 1 (`dbt_city_cycles/models/marts/mart_similar_day_stats.sql`) |
| **Files Modified** | 1 (`dbt_city_cycles/models/marts/schema.yml`) |

---

## Context

The dashboard currently shows live weather conditions (biking score, forecast) and historical impact summaries ("34% fewer rides when raining"). What it cannot do is answer the question: **"On days like today, what does bike activity look like?"**

This mart pre-computes ride statistics grouped by weather similarity dimensions so the dashboard can take today's live conditions (temperature band, precipitation intensity, month, weekday/weekend) and instantly look up matching historical patterns. This is the core data layer that enables the "Similar Day" storytelling feature — the dashboard sends a simple parameterized query and gets back aggregated stats like average rides per hour, average duration, total days observed, and comparison to the overall average.

The mart references `mart_weather_ride_correlation` (which already joins hourly rides with weather data) as its upstream source, following the existing mart-refs-mart pattern used by `mart_weather_impact_summary`.

---

## Dependencies

- **Phase 01** (or equivalent): The weather pipeline must be end-to-end functional so that `mart_weather_ride_correlation` is populated. However, the SQL model itself can be created and tested structurally before data is available.
- **No other phase dependencies**: This is a pure dbt model addition.

### Unlocks

- **Phase 03+** (Dashboard integration): Once this mart exists and is exported to S3, the dashboard can query it to power the "Similar Day" UX component.

---

## Detailed Implementation Plan

### Step 1: Create `dbt_city_cycles/models/marts/mart_similar_day_stats.sql`

Create a new file at `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_similar_day_stats.sql` with the following complete content:

```sql
{{ config(
    materialized='table'
) }}

/*
    mart_similar_day_stats — Pre-computed ride statistics grouped by weather
    similarity dimensions. Enables "On days like today..." dashboard queries.

    Granularity: Two levels in a single table, distinguished by `grain`:
      1. 'daily'  — one row per (location, month, day_type, temperature_band, precipitation_intensity)
      2. 'hourly' — one row per (location, month, day_type, temperature_band, precipitation_intensity, hour_of_day)

    The dashboard queries this mart by matching today's live weather conditions
    to the appropriate dimension values and reading back the pre-aggregated stats.
*/

with correlation as (
    select
        location,
        date,
        hour_of_day,
        ride_count,
        avg_duration_seconds,
        member_rides,
        casual_rides,
        temperature_band,
        precipitation_intensity,
        -- Extract month number (1-12) from date for seasonal matching
        extract(month from date) as month_num,
        -- Derive day_type from day-of-week (matching existing staging pattern)
        case
            when extract(isodow from date) in (6, 7) then 'weekend'
            else 'weekday'
        end as day_type
    from {{ ref('mart_weather_ride_correlation') }}
),

-- Overall average rides per day per location (baseline for pct_vs_overall)
overall_baseline as (
    select
        location,
        avg(daily_rides) as overall_avg_daily_rides,
        avg(daily_avg_duration_seconds) as overall_avg_duration_seconds
    from (
        select
            location,
            date,
            sum(ride_count) as daily_rides,
            avg(avg_duration_seconds) as daily_avg_duration_seconds
        from correlation
        group by location, date
    ) daily
    group by location
),

-- Daily grain: aggregate per (location, month, day_type, temperature_band, precipitation_intensity)
daily_stats as (
    select
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        -- Number of distinct days matching this combination
        count(distinct c.date) as total_days_observed,
        -- Average rides per day: sum all hourly rides per day, then average across days
        avg(daily_totals.daily_rides) as avg_rides_per_day,
        -- Average duration across all rides in matching days
        avg(daily_totals.daily_avg_duration_seconds) as avg_duration_seconds,
        -- Member/casual split (average per day)
        avg(daily_totals.daily_member_rides) as avg_member_rides_per_day,
        avg(daily_totals.daily_casual_rides) as avg_casual_rides_per_day,
        -- Comparison to overall average
        case
            when b.overall_avg_daily_rides is null or b.overall_avg_daily_rides = 0 then null
            else round(
                ((avg(daily_totals.daily_rides) - b.overall_avg_daily_rides)
                 / b.overall_avg_daily_rides * 100)::float,
                1
            )
        end as pct_vs_overall
    from correlation c
    inner join (
        select
            location,
            date,
            month_num,
            day_type,
            temperature_band,
            precipitation_intensity,
            sum(ride_count) as daily_rides,
            avg(avg_duration_seconds) as daily_avg_duration_seconds,
            sum(member_rides) as daily_member_rides,
            sum(casual_rides) as daily_casual_rides
        from correlation
        group by location, date, month_num, day_type, temperature_band, precipitation_intensity
    ) daily_totals
        on c.location = daily_totals.location
        and c.date = daily_totals.date
        and c.month_num = daily_totals.month_num
        and c.day_type = daily_totals.day_type
        and c.temperature_band = daily_totals.temperature_band
        and c.precipitation_intensity = daily_totals.precipitation_intensity
    left join overall_baseline b
        on c.location = b.location
    group by
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        b.overall_avg_daily_rides,
        b.overall_avg_duration_seconds
),

-- Hourly grain: aggregate per (location, month, day_type, temperature_band, precipitation_intensity, hour_of_day)
hourly_stats as (
    select
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        c.hour_of_day,
        count(distinct c.date) as total_days_observed,
        avg(c.ride_count) as avg_rides_per_hour,
        avg(c.avg_duration_seconds) as avg_duration_seconds,
        avg(c.member_rides) as avg_member_rides_per_hour,
        avg(c.casual_rides) as avg_casual_rides_per_hour
    from correlation c
    group by
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        c.hour_of_day
)

-- Combine both grains into a single table
select
    'daily' as grain,
    location,
    month_num,
    day_type,
    temperature_band,
    precipitation_intensity,
    cast(null as integer) as hour_of_day,
    total_days_observed,
    round(avg_rides_per_day, 1) as avg_rides,
    round(avg_duration_seconds, 1) as avg_duration_seconds,
    round(avg_member_rides_per_day, 1) as avg_member_rides,
    round(avg_casual_rides_per_day, 1) as avg_casual_rides,
    pct_vs_overall
from daily_stats

union all

select
    'hourly' as grain,
    location,
    month_num,
    day_type,
    temperature_band,
    precipitation_intensity,
    hour_of_day,
    total_days_observed,
    round(avg_rides_per_hour, 1) as avg_rides,
    round(avg_duration_seconds, 1) as avg_duration_seconds,
    round(avg_member_rides_per_hour, 1) as avg_member_rides,
    round(avg_casual_rides_per_hour, 1) as avg_casual_rides,
    cast(null as float) as pct_vs_overall
from hourly_stats

order by location, month_num, day_type, temperature_band, precipitation_intensity, grain, hour_of_day
```

#### Why this design

1. **Two grains in one table**: The `grain` column (`'daily'` / `'hourly'`) lets the dashboard issue a single query with a `WHERE grain = ...` filter rather than querying two separate marts. The daily grain answers "how many rides on a day like today?" while the hourly grain answers "what does the hourly pattern look like on days like today?"

2. **Upstream ref is `mart_weather_ride_correlation`**: This model already contains ride counts joined with weather dimensions (temperature_band, precipitation_intensity) at hourly granularity. Referencing it avoids duplicating the ride-weather join logic.

3. **`month_num` instead of `month` column**: The upstream `mart_weather_ride_correlation` does not have a `month` column, so we extract it from `date`. Using `month_num` (integer 1-12) rather than a date-truncated month avoids confusion with the `month` column in staging models (which is a truncated date, not an integer).

4. **`day_type` derived from `date`**: The upstream `mart_weather_ride_correlation` does not carry `day_type`, so we derive it from `extract(isodow from date)`. This matches the logic in the `day_type` macro used by staging models (isodow 6,7 = weekend).

5. **`pct_vs_overall`**: Only computed for the daily grain because the hourly grain's "average rides per hour" is not directly comparable to an overall daily average. The daily grain's `pct_vs_overall` tells the user "days like today see X% more/fewer rides than the overall average day."

6. **No indexes**: Following the project convention (MEMORY.md note: staging models have NO indexes; index builds on large row counts are catastrophically slow).

#### How the dashboard will query this mart

The dashboard receives live weather from Open-Meteo and classifies it into `temperature_band` and `precipitation_intensity` using the same bands defined in `stg_weather_hourly.sql`. Example query:

```sql
-- Daily summary: "On days like today..."
SELECT *
FROM mart_similar_day_stats
WHERE grain = 'daily'
  AND location = 'nyc'
  AND month_num = 3          -- current month
  AND day_type = 'weekday'   -- current day type
  AND temperature_band = 'mild'
  AND precipitation_intensity = 'none';

-- Hourly curve: "Here's what the hourly pattern looks like on similar days"
SELECT hour_of_day, avg_rides, avg_duration_seconds
FROM mart_similar_day_stats
WHERE grain = 'hourly'
  AND location = 'nyc'
  AND month_num = 3
  AND day_type = 'weekday'
  AND temperature_band = 'mild'
  AND precipitation_intensity = 'none'
ORDER BY hour_of_day;
```

### Step 2: Add schema tests to `dbt_city_cycles/models/marts/schema.yml`

Open `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/schema.yml` and add the following model entry. Insert it **after** the existing `mart_station_weather_performance` entry (which ends around line 346) and **before** the `mart_nyc_member_analysis` entry (which starts around line 347).

**Existing text to locate** (find the line with `- name: mart_nyc_member_analysis`):

```yaml
  - name: mart_nyc_member_analysis
```

**Insert the following block BEFORE that line:**

```yaml
  - name: mart_similar_day_stats
    description: >
      Pre-computed ride statistics grouped by weather similarity dimensions.
      Enables "On days like today, what does bike activity look like?" queries.
      Contains two grains: 'daily' (one row per location/month/day_type/temp_band/precip)
      and 'hourly' (adds hour_of_day for intra-day patterns).
      Upstream: mart_weather_ride_correlation.
    columns:
      - name: grain
        description: "Row granularity: 'daily' or 'hourly'"
        tests:
          - not_null
          - accepted_values:
              values: ['daily', 'hourly']
      - name: location
        description: City identifier (nyc or london)
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: month_num
        description: Month number (1-12) as seasonal proxy
        tests:
          - not_null
      - name: day_type
        description: "weekday or weekend"
        tests:
          - not_null
          - accepted_values:
              values: ['weekday', 'weekend']
      - name: temperature_band
        description: "Temperature category: freezing, cold, mild, warm, hot"
        tests:
          - not_null
          - accepted_values:
              values: ['freezing', 'cold', 'mild', 'warm', 'hot']
      - name: precipitation_intensity
        description: "Precipitation category: none, light, moderate, heavy, extreme"
        tests:
          - not_null
          - accepted_values:
              values: ['none', 'light', 'moderate', 'heavy', 'extreme']
      - name: hour_of_day
        description: Hour of day (0-23). NULL for daily grain rows.
      - name: total_days_observed
        description: Number of distinct historical days matching this dimension combination
        tests:
          - not_null
      - name: avg_rides
        description: >
          For daily grain: average total rides per day.
          For hourly grain: average rides per hour.
      - name: avg_duration_seconds
        description: Average ride duration in seconds for matching conditions
      - name: avg_member_rides
        description: >
          Average member rides (per day for daily grain, per hour for hourly grain).
          0 for London which has no user type data.
      - name: avg_casual_rides
        description: >
          Average casual rides (per day for daily grain, per hour for hourly grain).
          0 for London which has no user type data.
      - name: pct_vs_overall
        description: >
          Percentage difference from overall average daily rides for this location.
          Only populated for daily grain. Positive = more rides than average,
          negative = fewer rides than average.

```

**Exact edit instructions:**

Find the line:
```
  - name: mart_nyc_member_analysis
```

Insert the entire YAML block above (from `  - name: mart_similar_day_stats` through the trailing blank line) immediately before that line.

---

## Test Plan

### dbt Tests (via schema.yml, defined above)

The schema.yml entry defines these automated tests:
- `not_null` on: grain, location, month_num, day_type, temperature_band, precipitation_intensity, total_days_observed
- `accepted_values` on: grain ('daily'/'hourly'), location ('nyc'/'london'), day_type ('weekday'/'weekend'), temperature_band (5 values), precipitation_intensity (5 values)

Run with:
```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt test --select mart_similar_day_stats
```

### Structural Validation (can run without data)

Compile the model to verify SQL syntax and ref resolution:
```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt compile --select mart_similar_day_stats
```

This should succeed even without `raw_weather_hourly` data — it only checks SQL validity and DAG references.

### Data Validation (requires populated weather pipeline)

Once the weather pipeline is end-to-end functional and `mart_weather_ride_correlation` has data, run:

```bash
cd /Users/chris/Projects/city-cycles/dbt_city_cycles
dbt run --select mart_similar_day_stats
dbt test --select mart_similar_day_stats
```

Then verify data quality with ad-hoc queries:

```sql
-- 1. Check row counts per grain
SELECT grain, count(*) FROM mart_similar_day_stats GROUP BY grain;
-- Expected: daily should have ~2*12*2*5*5 = 1200 rows max (likely fewer due to sparse combos)
-- Expected: hourly should have ~daily_combos * 24 rows max

-- 2. Verify no NULL dimension keys
SELECT count(*) FROM mart_similar_day_stats
WHERE location IS NULL OR month_num IS NULL OR day_type IS NULL
   OR temperature_band IS NULL OR precipitation_intensity IS NULL;
-- Expected: 0

-- 3. Check that daily grain has NULL hour_of_day
SELECT count(*) FROM mart_similar_day_stats
WHERE grain = 'daily' AND hour_of_day IS NOT NULL;
-- Expected: 0

-- 4. Check that hourly grain has non-NULL hour_of_day
SELECT count(*) FROM mart_similar_day_stats
WHERE grain = 'hourly' AND hour_of_day IS NULL;
-- Expected: 0

-- 5. Check pct_vs_overall is only on daily grain
SELECT count(*) FROM mart_similar_day_stats
WHERE grain = 'hourly' AND pct_vs_overall IS NOT NULL;
-- Expected: 0

-- 6. Sanity check: avg_rides should be positive
SELECT count(*) FROM mart_similar_day_stats WHERE avg_rides <= 0;
-- Expected: 0

-- 7. Spot check a known combination
SELECT * FROM mart_similar_day_stats
WHERE grain = 'daily' AND location = 'nyc' AND month_num = 7
  AND day_type = 'weekday' AND temperature_band = 'warm'
  AND precipitation_intensity = 'none';
-- Expected: 1 row with reasonable avg_rides (NYC summer weekday, warm, dry = high ridership)
```

### Existing Test Suite

Run the full existing test suite to verify no regressions:
```bash
/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v
```

Expected: 283 pass, 3 skip (no change from baseline).

---

## Documentation Updates

### CHANGELOG.md

Add under `[Unreleased]`:

```markdown
### Added
- **Similar Day Statistics Mart** - New `mart_similar_day_stats` dbt model for "days like today" weather queries
  - Pre-computes ride statistics by (location, month, day_type, temperature_band, precipitation_intensity)
  - Dual granularity: daily totals and hourly patterns in a single table
  - Includes pct_vs_overall comparison metric for daily grain
  - Full schema.yml documentation and test coverage
```

### No README changes required

The mart follows existing patterns and will be picked up by `dbt docs generate` automatically. No changes to `README.md`, `CLAUDE.md`, or module READMEs are needed for this phase.

---

## Stress Testing & Edge Cases

### Edge Case 1: Sparse dimension combinations

Some combinations like `(london, january, weekend, hot, extreme)` will have zero observations. This is handled correctly — those combinations simply will not appear in the output (the aggregation produces no rows for empty groups). The dashboard must handle the case where a query returns zero rows.

### Edge Case 2: London has no user_type data

London rides have `user_type = NULL`, so `member_rides` and `casual_rides` from `mart_hourly_rides` will be 0 for London. The mart correctly propagates these zeros via `avg_member_rides` and `avg_casual_rides` columns. No special handling needed.

### Edge Case 3: day_type derivation consistency

The `day_type` is derived using `extract(isodow from date)` where isodow 6 = Saturday, 7 = Sunday. This matches the `day_type` macro in `dbt_city_cycles/macros/day_type.sql` which is used by staging models. The values are consistent across the pipeline.

### Edge Case 4: Temperature band and precipitation_intensity values

These come directly from `stg_weather_hourly.sql` which defines:
- `temperature_band`: freezing (<0), cold (0-10), mild (10-20), warm (20-30), hot (30+)
- `precipitation_intensity`: none (0mm), light (<2.5mm), moderate (<7.5mm), heavy (<50mm), extreme (50+mm)

The dashboard's recommendation engine (`dashboard/recommendation_engine.py`) uses **different** temperature bands (7 bands including "cool" and "very_hot" with different thresholds). This is intentional — the dashboard will need to map its finer-grained engine bands to the 5-band mart values when querying. This mapping is a dashboard-layer concern (Phase 03+), not a mart-layer concern.

### Edge Case 5: Memory and performance

The mart materializes as a table (following project convention in `dbt_project.yml` line 45: `+materialized: table`). The output table is small — bounded by `2 locations * 12 months * 2 day_types * 5 temp_bands * 5 precip_intensities * (1 + 24 hours) = ~30,000 rows` maximum. This is trivial for DuckDB, even with the 32GB memory limit.

The input (`mart_weather_ride_correlation`) is an inner join of hourly rides and weather, so it only contains hours where both ride data and weather data exist. The aggregation is straightforward GROUP BY with no expensive window functions.

### Edge Case 6: `pct_vs_overall` can be very large

If a specific combination has dramatically different ridership than the overall average (e.g., freezing January weekends vs. overall average that includes warm summer days), `pct_vs_overall` could be -90% or +200%. This is expected and correct — the dashboard should display this appropriately (e.g., cap display at +/- 100% or use it as-is).

---

## Verification Checklist

1. [ ] File created: `dbt_city_cycles/models/marts/mart_similar_day_stats.sql`
2. [ ] File modified: `dbt_city_cycles/models/marts/schema.yml` — new model entry added before `mart_nyc_member_analysis`
3. [ ] `dbt compile --select mart_similar_day_stats` succeeds (syntax + ref check)
4. [ ] `dbt run --select mart_similar_day_stats` succeeds (if upstream data exists)
5. [ ] `dbt test --select mart_similar_day_stats` passes all schema tests
6. [ ] Existing test suite passes: `venv/bin/python -m pytest tests/ -v` (283 pass, 3 skip)
7. [ ] CHANGELOG.md updated with new entry under `[Unreleased]`
8. [ ] `dbt docs generate` includes the new model with descriptions
9. [ ] DAG is correct: `mart_similar_day_stats` depends on `mart_weather_ride_correlation` only

---

## What NOT To Do

1. **Do NOT add indexes to this table.** The project has learned (see MEMORY.md) that index builds on large tables cause catastrophic slowdowns. The output table is small enough that sequential scan is fine.

2. **Do NOT reference `unified_rides` or `stg_weather_hourly` directly.** The ride-weather join is already done in `mart_weather_ride_correlation`. Referencing the upstream mart avoids duplicating join logic and keeps the DAG clean.

3. **Do NOT add a `wind_category` dimension.** While `stg_weather_hourly` provides `wind_category`, adding it to the grouping key would increase cardinality by ~5x with minimal analytical value. Wind is already captured in the biking score computation (recommendation engine). If wind analysis is needed later, it can be a separate mart.

4. **Do NOT change the temperature band thresholds to match the recommendation engine's 7-band system.** The mart uses the 5-band system from `stg_weather_hourly` (freezing/cold/mild/warm/hot). The dashboard maps between the two systems at query time. Changing the mart bands would break consistency with all other weather marts.

5. **Do NOT use `month` (date-truncated) from staging models.** Use `month_num` (integer 1-12) extracted from `date`. The dashboard has today's month as an integer from Python's `datetime.now().month` — matching on integer is simpler and avoids date-truncation confusion.

6. **Do NOT create a separate `mart_similar_day_hourly.sql` file.** Both grains live in the same table, distinguished by the `grain` column. This keeps the S3 export simple (one parquet file) and the dashboard query pattern consistent.

7. **Do NOT add the model to `dbt_project.yml`.** The `marts` directory already has `+materialized: table` configured at line 45-46 of `dbt_project.yml`. Individual model files only need `{{ config(materialized='table') }}` to be explicit (matching the pattern used by every other mart), but no project-level configuration change is needed.

8. **Do NOT modify the `db_duckdb/cli.py` export list yet.** The export of this mart to S3 as a parquet file is a dashboard integration concern. It will be handled when the dashboard phase adds the parquet download for this mart. The existing export pipeline (`db_duckdb/cli.py export`) exports all marts automatically if configured — the configuration for this new mart should be added in the dashboard integration phase, not here.
