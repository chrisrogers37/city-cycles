# Phase 01: Fix Errors and Graceful Fallbacks

**Status:** COMPLETE
**Started:** 2026-03-01
**Completed:** 2026-03-01
**Impact:** High | **Effort:** Low | **Risk:** Low
**Files modified:** `dashboard/pages/comparison.py`, `dashboard/pages/ride_analytics.py`, `dashboard/pages/weather_deep_dive.py`

## Context

Three pages display raw Python errors or developer-facing messages to end users:

1. **City Comparison** — entire page crashes with `"Error loading comparison data: Not implemented Error: Unable to transform python value of type '<class 'numpy.int64'>' to DuckDB LogicalType"`. Root cause: `latest_year` from DuckDB returns `numpy.int64`, which fails when passed back as a query parameter.
2. **Ride Analytics** — "Time of Day Analysis" shows `"Error creating hourly patterns chart: IO Error: No files found that match the pattern '/mount/src/city-cycles/data/mart_hourly_patterns_summary.parquet'"`. The parquet file doesn't exist on Streamlit Cloud.
3. **Empty states** on Weather Deep Dive, Station Weather Performance, and Comparison Weather Impact show developer messages like "Run the full pipeline to generate weather mart data."

Raw errors destroy user trust. This phase replaces them all with graceful fallbacks.

## Detailed Implementation Plan

### Step 1: Fix comparison.py type casting (THE CRASH FIX)

**File:** `dashboard/pages/comparison.py`

**Root cause:** Line 45 — `latest_year` is a `numpy.int64` from DuckDB. When passed to `run_query_params` at line 72, DuckDB can't convert it back. The fix is to cast to Python `int`.

**Before (line 45):**
```python
        latest_year = run_query_params(year_query, [start_date, end_date])['latest_year'][0] or 2024
```

**After:**
```python
        latest_year = int(run_query_params(year_query, [start_date, end_date])['latest_year'][0] or 2024)
```

Also add defensive casting on lines 83-86 where metric values are extracted:

**Before (lines 83-86):**
```python
            rides = rides_df.loc[loc, 'total_rides'] if loc in rides_df.index else None
            pop = int(pop_df.loc[loc, 'population']) if loc in pop_df.index else None
            dur = duration_df.loc[loc, 'avg_ride_duration_minutes'] if loc in duration_df.index else None
            per_capita = (rides / pop * 1000) if rides and pop else None
```

**After:**
```python
            rides = int(rides_df.loc[loc, 'total_rides']) if loc in rides_df.index else None
            pop = int(pop_df.loc[loc, 'population']) if loc in pop_df.index else None
            dur = float(duration_df.loc[loc, 'avg_ride_duration_minutes']) if loc in duration_df.index else None
            per_capita = (rides / pop * 1000) if rides and pop else None
```

### Step 2: Fix hourly patterns missing file error

**File:** `dashboard/pages/ride_analytics.py`

Add a `parquet_exists()` pre-check before the hourly patterns query, matching the pattern already used for Station Weather Performance (lines 193-199).

**Before (lines 151-159):**
```python
    # --- Hourly Patterns ---
    st.subheader("Time of Day Analysis")
    hour_query = f"SELECT hour_of_day, ride_count FROM '{parquet_path('mart_hourly_patterns_summary.parquet')}' WHERE location = $1 ORDER BY hour_of_day"
    try:
        hour_df = run_query_params(hour_query, [location])
        fig = hourly_bar_chart(hour_df, f"{city_label} Rides by Hour of Day")
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating hourly patterns chart: {e}")
```

**After:**
```python
    # --- Hourly Patterns ---
    st.subheader("Time of Day Analysis")
    if not parquet_exists('mart_hourly_patterns_summary.parquet'):
        st.info(
            "Hourly ridership patterns are not yet available for this city. "
            "This chart will appear once ride data has been fully processed."
        )
    else:
        hour_query = f"SELECT hour_of_day, ride_count FROM '{parquet_path('mart_hourly_patterns_summary.parquet')}' WHERE location = $1 ORDER BY hour_of_day"
        try:
            hour_df = run_query_params(hour_query, [location])
            if not hour_df.empty:
                fig = hourly_bar_chart(hour_df, f"{city_label} Rides by Hour of Day")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"No hourly data available for {city_label}.")
        except Exception as e:
            st.error(f"Error creating hourly patterns chart: {e}")
```

### Step 3: Improve empty state messages

Replace developer-facing messages with user-friendly ones across three locations.

**File: `dashboard/pages/ride_analytics.py`, lines 196-198**

**Before:**
```python
        st.info(
            "Station weather performance data is not yet available. "
            "Run the full pipeline to generate weather mart data."
        )
```

**After:**
```python
        st.info(
            "Station weather performance data is not yet available. "
            "This section shows how individual stations respond to different weather conditions "
            "and will appear once weather data is available for this city."
        )
```

**File: `dashboard/pages/weather_deep_dive.py`, lines 37-41**

**Before:**
```python
        st.info(
            "\U0001f6a7 Weather data is being processed. Check back soon.\n\n"
            "The weather analytics charts require historical weather and ride data "
            "to be loaded. This happens automatically during the monthly pipeline run."
        )
```

**After:**
```python
        st.info(
            "Weather analytics are not yet available.\n\n"
            "This page will show how temperature, precipitation, and weather conditions "
            "affect bike ridership patterns in each city. Data is updated monthly."
        )
```

**File: `dashboard/pages/comparison.py`, lines 164-167**

**Before:**
```python
        st.info(
            "Weather impact data is not yet available. "
            "Run the full pipeline to generate weather mart data."
        )
```

**After:**
```python
        st.info(
            "Weather impact comparison data is not yet available. "
            "This section will show how weather affects ridership differently in NYC vs London."
        )
```

## Test Plan

1. **Comparison page type fix:** Navigate to City Comparison — should load metrics and charts without error
2. **Hourly patterns fallback:** If `mart_hourly_patterns_summary.parquet` doesn't exist, should show info message instead of red error
3. **Empty states:** Verify all three empty state messages read as user-friendly, no mention of "pipeline" or "run the full pipeline"
4. **No regressions:** All existing tests pass (`venv/bin/python -m pytest tests/ -v`)

## Verification Checklist

- [ ] City Comparison page loads without error
- [ ] No raw Python/DuckDB error messages visible on any page
- [ ] All empty state messages are user-friendly (no developer jargon)
- [ ] Existing tests pass
- [ ] Manual walkthrough of all 4 pages shows no new errors

## What NOT To Do

- Do NOT add logging or monitoring in this phase — this is purely about user-facing display
- Do NOT change query logic or data — only how errors/empty states are presented
- Do NOT add try/except blocks where `parquet_exists()` pre-checks are sufficient
