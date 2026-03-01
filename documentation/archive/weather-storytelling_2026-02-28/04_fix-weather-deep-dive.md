# Phase 04 -- Fix Weather Deep Dive Page

**Status:** ✅ COMPLETE
**Started:** 2026-02-28
**Completed:** 2026-02-28
**PR:** #45

## Header

| Field | Value |
|-------|-------|
| **PR Title** | fix: weather deep dive page -- fix column bug and add empty-state handling |
| **Risk Level** | Low |
| **Estimated Effort** | Low (2-3 hours) |
| **Files Modified** | 2 |
| **Files Created** | 1 |
| **Files Deleted** | 0 |

## Context

The Weather Deep Dive page (`dashboard/pages/weather_deep_dive.py`) has two problems:

1. **Column name bug**: The page queries `total_rides` from `mart_weather_ride_correlation.parquet`, but the mart column is actually named `ride_count` (defined in `dbt_city_cycles/models/marts/mart_weather_ride_correlation.sql` line 10, inherited from `mart_hourly_rides.sql` line 9). This means the temperature and precipitation charts will fail with a DuckDB error even when the mart data is populated.

2. **Silent failure on missing data**: When weather mart parquet files are absent (which is the current state -- only `mart_daily_metrics.parquet`, `mart_hourly_patterns.parquet`, and `mart_station_growth.parquet` exist locally), the page either shows nothing or catches exceptions and shows generic red error boxes. There is no user-friendly empty state explaining that weather data is being processed.

This phase fixes both issues so the page works correctly once Phase 01 populates the weather marts, and degrades gracefully when they are missing.

### Similar-Day Mart Opportunity (Phase 02)

If Phase 02 introduces a similar-day mart (matching today's conditions to historical similar days), the Weather Deep Dive page would be a natural place to add a "Days Like Today" section showing historical ride patterns for similar weather. This is an optional future enhancement and is NOT part of this phase.

## Dependencies

| Dependency | Direction | Reason |
|------------|-----------|--------|
| Phase 01 | Must complete first | Phase 01 populates the weather mart parquet files. Without them, only the empty-state UI will be visible. However, the code changes in this phase are independent of Phase 01's code -- they modify different files. |

This phase **unlocks**: Nothing directly. It is a leaf phase that makes the deep dive page functional once data exists.

**Parallel safety**: This phase modifies `dashboard/pages/weather_deep_dive.py` and `dashboard/utils/query_helpers.py`. No other phase in this session should modify these same files. The new test file `tests/test_weather_deep_dive.py` is also unique to this phase.

## Detailed Implementation Plan

### Step 1: Add a parquet-existence helper to `query_helpers.py`

The page needs a way to check whether a mart parquet file exists before querying it. The `parquet_path()` function already resolves filenames to full paths, but there is no existence check. Add one.

**File**: `/Users/chris/Projects/city-cycles/dashboard/utils/query_helpers.py`

**Current code** (lines 31-33):
```python
def parquet_path(filename: str) -> str:
    """Resolve a mart Parquet filename to its full path in DATA_DIR."""
    return os.path.join(DATA_DIR, filename)
```

**Add the following function immediately after `parquet_path`** (after line 33):

```python

def parquet_exists(filename: str) -> bool:
    """Check whether a mart Parquet file exists locally in DATA_DIR."""
    return os.path.exists(parquet_path(filename))
```

This keeps the helper simple and testable. The `os.path.exists` call is cheap and does not need caching.

### Step 2: Fix the column name bug in `weather_deep_dive.py`

**File**: `/Users/chris/Projects/city-cycles/dashboard/pages/weather_deep_dive.py`

There are two queries that reference `total_rides` where the mart column is actually `ride_count`.

**Change 1 -- Temperature query** (line 39):

Replace:
```python
        round(avg(total_rides), 0) as avg_rides,
```

With:
```python
        round(avg(ride_count), 0) as avg_rides,
```

**Change 2 -- Precipitation query** (line 70):

Replace:
```python
        round(avg(total_rides), 0) as avg_rides,
```

With:
```python
        round(avg(ride_count), 0) as avg_rides,
```

### Step 3: Add empty-state handling to `weather_deep_dive.py`

**File**: `/Users/chris/Projects/city-cycles/dashboard/pages/weather_deep_dive.py`

Replace the entire file content with the following. The key changes are:

- Import `parquet_exists` from `query_helpers`
- Add an early return with a user-friendly message when mart files are missing
- Keep all existing chart logic unchanged (except the `total_rides` -> `ride_count` fix from Step 2)
- Add `st.info()` fallback inside each chart's `if not df.empty` block (i.e., when query returns zero rows for a city)

**Full replacement content for `weather_deep_dive.py`:**

```python
"""
Weather Deep Dive page -- weather-ride correlations and impact analysis.
Uses existing mart_weather_ride_correlation and mart_weather_impact_summary.
"""

import streamlit as st
import plotly.express as px

from dashboard.utils.query_helpers import run_query, run_query_params, parquet_path, parquet_exists
from dashboard.theme.plotly_template import register_template

register_template()

# Mart files this page depends on
_CORRELATION_MART = 'mart_weather_ride_correlation.parquet'
_IMPACT_MART = 'mart_weather_impact_summary.parquet'


def _check_data_available() -> bool:
    """Check whether the required weather mart files exist locally.

    Returns True if both mart files are present, False otherwise.
    When False, the caller should show an empty-state message.
    """
    return parquet_exists(_CORRELATION_MART) and parquet_exists(_IMPACT_MART)


def render():
    """Render the weather deep dive page."""
    st.title("\U0001f321\ufe0f Weather & Ride Analysis")

    city_label = st.sidebar.radio("City:", ["NYC", "London"], key='weather_city')
    location = city_label.lower()

    # --- Empty-state check ---
    if not _check_data_available():
        st.info(
            "\U0001f6a7 Weather data is being processed. Check back soon.\n\n"
            "The weather analytics charts require historical weather and ride data "
            "to be loaded. This happens automatically during the monthly pipeline run."
        )
        return

    # --- Temperature vs Rides ---
    st.subheader("Temperature vs Ride Volume")
    st.caption("Average daily rides grouped by temperature range")

    temp_query = f"""
    SELECT
        CASE
            WHEN temperature_celsius < 0 THEN 'Below 0\u00b0C'
            WHEN temperature_celsius < 5 THEN '0-5\u00b0C'
            WHEN temperature_celsius < 10 THEN '5-10\u00b0C'
            WHEN temperature_celsius < 15 THEN '10-15\u00b0C'
            WHEN temperature_celsius < 20 THEN '15-20\u00b0C'
            WHEN temperature_celsius < 25 THEN '20-25\u00b0C'
            WHEN temperature_celsius < 30 THEN '25-30\u00b0C'
            ELSE '30\u00b0C+'
        END as temp_range,
        MIN(temperature_celsius) as temp_sort,
        round(avg(ride_count), 0) as avg_rides,
        count(*) as days_observed
    FROM '{parquet_path(_CORRELATION_MART)}'
    WHERE location = $1
    GROUP BY temp_range, temp_sort
    ORDER BY temp_sort
    """
    try:
        temp_df = run_query_params(temp_query, [location])
        if not temp_df.empty:
            fig = px.bar(temp_df, x='temp_range', y='avg_rides',
                         title=f"{city_label}: Average Daily Rides by Temperature",
                         labels={'avg_rides': 'Avg Daily Rides', 'temp_range': 'Temperature Range'},
                         template='atmospheric', color_discrete_sequence=['#5DADE2'])
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Show data"):
                st.dataframe(temp_df[['temp_range', 'avg_rides', 'days_observed']])
        else:
            st.info(f"No temperature data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading temperature data: {e}")

    # --- Precipitation Impact ---
    st.subheader("Precipitation Impact on Rides")
    precip_query = f"""
    SELECT
        CASE
            WHEN precipitation_mm = 0 THEN 'Dry'
            WHEN precipitation_mm < 2 THEN 'Light (0-2mm)'
            WHEN precipitation_mm < 10 THEN 'Moderate (2-10mm)'
            ELSE 'Heavy (10mm+)'
        END as precip_category,
        MIN(precipitation_mm) as precip_sort,
        round(avg(ride_count), 0) as avg_rides,
        count(*) as days_observed
    FROM '{parquet_path(_CORRELATION_MART)}'
    WHERE location = $1
    GROUP BY precip_category, precip_sort
    ORDER BY precip_sort
    """
    try:
        precip_df = run_query_params(precip_query, [location])
        if not precip_df.empty:
            fig = px.bar(precip_df, x='precip_category', y='avg_rides',
                         title=f"{city_label}: Average Daily Rides by Precipitation",
                         labels={'avg_rides': 'Avg Daily Rides', 'precip_category': 'Precipitation'},
                         template='atmospheric', color_discrete_sequence=['#3498DB'])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No precipitation data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading precipitation data: {e}")

    # --- Weather Condition Breakdown ---
    st.subheader("Impact by Weather Condition")
    st.caption("How each weather condition affects ride volume vs clear weather baseline")

    impact_query = f"""
    SELECT dimension_value as weather_condition,
           round(avg(pct_change_rides_vs_clear), 1) as pct_change,
           round(avg(avg_rides), 0) as avg_rides,
           sum(observation_count) as total_observations
    FROM '{parquet_path(_IMPACT_MART)}'
    WHERE location = $1
      AND dimension_type = 'weather_condition'
      AND dimension_value != 'clear'
    GROUP BY dimension_value
    ORDER BY pct_change
    """
    try:
        impact_df = run_query_params(impact_query, [location])
        if not impact_df.empty:
            fig = px.bar(impact_df, x='weather_condition', y='pct_change',
                         title=f"{city_label}: Ride Volume Change by Weather Condition",
                         labels={'pct_change': '% Change vs Clear', 'weather_condition': 'Condition'},
                         template='atmospheric',
                         color='pct_change',
                         color_continuous_scale='RdYlGn',
                         range_color=[impact_df['pct_change'].min(), 0])
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Show data"):
                st.dataframe(impact_df)
        else:
            st.info(f"No weather impact data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading weather impact data: {e}")

    # --- Hourly Weather Impact ---
    st.subheader("Weather Impact by Hour of Day")

    hour_impact_query = f"""
    SELECT hour_of_day,
           round(avg(CASE WHEN dimension_value = 'rain' THEN pct_change_rides_vs_clear END), 1) as rain_impact,
           round(avg(CASE WHEN dimension_value = 'snow' THEN pct_change_rides_vs_clear END), 1) as snow_impact,
           round(avg(CASE WHEN dimension_value = 'fog' THEN pct_change_rides_vs_clear END), 1) as fog_impact
    FROM '{parquet_path(_IMPACT_MART)}'
    WHERE location = $1 AND dimension_type = 'weather_condition'
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    try:
        hour_impact_df = run_query_params(hour_impact_query, [location])
        if not hour_impact_df.empty:
            fig = px.line(hour_impact_df, x='hour_of_day',
                          y=['rain_impact', 'snow_impact', 'fog_impact'],
                          title=f"{city_label}: Weather Impact by Hour",
                          labels={'value': '% Change vs Clear', 'hour_of_day': 'Hour'},
                          template='atmospheric')
            fig.update_layout(legend_title_text='Condition')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No hourly impact data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading hourly impact data: {e}")
```

### Summary of changes vs the current file

| Line(s) | What changed | Why |
|----------|-------------|-----|
| 9 | Added `parquet_exists` to import | Needed for empty-state check |
| 14-15 | Added `_CORRELATION_MART` and `_IMPACT_MART` constants | Avoids repeating filenames; makes empty-state check reference the same names as queries |
| 18-24 | Added `_check_data_available()` | Encapsulates the existence check for testability |
| 33-38 | Added early-return empty state block | Shows helpful `st.info()` message instead of blank page |
| 39, 70 | `total_rides` -> `ride_count` | Fixes column name mismatch with mart schema |
| After each `if not df.empty` | Added `else: st.info(...)` | Shows per-section feedback when query returns no rows for the selected city |

## Test Plan

### New test file: `tests/test_weather_deep_dive.py`

Create this file with the following tests. These test the non-Streamlit logic (following the same pattern as `tests/test_dashboard.py` which tests query patterns using standalone DuckDB connections).

```python
"""
Tests for Weather Deep Dive page logic.

Tests query patterns and data availability checks without importing Streamlit.
Follows the same pattern as test_dashboard.py -- uses standalone DuckDB connections
to validate queries against the actual mart schema.
"""

import os
import pytest
import duckdb
import pandas as pd


class TestWeatherDeepDiveQueries:
    """Test the SQL queries used by the weather deep dive page."""

    @pytest.fixture
    def conn_with_correlation_data(self, tmp_path):
        """Create a DuckDB connection with mart_weather_ride_correlation test data."""
        conn = duckdb.connect(":memory:")
        # Schema matches mart_weather_ride_correlation.sql output columns
        conn.execute("""
            CREATE TABLE correlation AS
            SELECT * FROM (VALUES
                ('nyc', '2023-06-15'::DATE, 8, 150, 720.0, 100, 50,
                 22.5, 20.0, 65.0, 0.0, 0.0, 0.0, 0.0, 0, 'clear', 10.0,
                 15.0, 25.0, false, 'none', 'warm', 'light'),
                ('nyc', '2023-06-16'::DATE, 8, 180, 700.0, 120, 60,
                 25.0, 23.0, 60.0, 2.5, 2.5, 0.0, 0.0, 61, 'rain', 20.0,
                 18.0, 30.0, true, 'light', 'warm', 'moderate'),
                ('nyc', '2023-01-10'::DATE, 8, 50, 600.0, 40, 10,
                 -2.0, -5.0, 80.0, 0.0, 0.0, 0.0, 0.0, 0, 'clear', 5.0,
                 8.0, 10.0, false, 'none', 'freezing', 'calm'),
                ('london', '2023-06-15'::DATE, 8, 200, 900.0, 0, 200,
                 18.0, 16.0, 70.0, 0.0, 0.0, 0.0, 0.0, 0, 'clear', 12.0,
                 10.0, 20.0, false, 'none', 'mild', 'light')
            ) AS t(location, date, hour_of_day, ride_count, avg_duration_seconds,
                   member_rides, casual_rides, temperature_celsius,
                   apparent_temperature_celsius, relative_humidity_pct,
                   precipitation_mm, rain_mm, snowfall_cm, snow_depth_m,
                   weather_code, weather_condition, cloud_cover_pct,
                   wind_speed_kmh, wind_gusts_kmh, is_precipitation,
                   precipitation_intensity, temperature_band, wind_category)
        """)
        # Write to parquet for file-based queries
        df = conn.execute("SELECT * FROM correlation").fetchdf()
        parquet_path = str(tmp_path / "mart_weather_ride_correlation.parquet")
        df.to_parquet(parquet_path)
        yield conn, parquet_path
        conn.close()

    def test_temperature_query_uses_ride_count(self, conn_with_correlation_data):
        """Temperature query must use ride_count (not total_rides) column."""
        conn, parquet_path = conn_with_correlation_data
        query = f"""
        SELECT
            CASE
                WHEN temperature_celsius < 0 THEN 'Below 0C'
                WHEN temperature_celsius < 5 THEN '0-5C'
                WHEN temperature_celsius < 10 THEN '5-10C'
                WHEN temperature_celsius < 15 THEN '10-15C'
                WHEN temperature_celsius < 20 THEN '15-20C'
                WHEN temperature_celsius < 25 THEN '20-25C'
                WHEN temperature_celsius < 30 THEN '25-30C'
                ELSE '30C+'
            END as temp_range,
            MIN(temperature_celsius) as temp_sort,
            round(avg(ride_count), 0) as avg_rides,
            count(*) as days_observed
        FROM '{parquet_path}'
        WHERE location = 'nyc'
        GROUP BY temp_range, temp_sort
        ORDER BY temp_sort
        """
        result = conn.execute(query).fetchdf()
        assert len(result) >= 2  # At least freezing and warm ranges
        assert 'avg_rides' in result.columns

    def test_precipitation_query_uses_ride_count(self, conn_with_correlation_data):
        """Precipitation query must use ride_count (not total_rides) column."""
        conn, parquet_path = conn_with_correlation_data
        query = f"""
        SELECT
            CASE
                WHEN precipitation_mm = 0 THEN 'Dry'
                WHEN precipitation_mm < 2 THEN 'Light (0-2mm)'
                WHEN precipitation_mm < 10 THEN 'Moderate (2-10mm)'
                ELSE 'Heavy (10mm+)'
            END as precip_category,
            MIN(precipitation_mm) as precip_sort,
            round(avg(ride_count), 0) as avg_rides,
            count(*) as days_observed
        FROM '{parquet_path}'
        WHERE location = 'nyc'
        GROUP BY precip_category, precip_sort
        ORDER BY precip_sort
        """
        result = conn.execute(query).fetchdf()
        assert len(result) >= 1
        assert 'avg_rides' in result.columns

    def test_city_filter_returns_only_selected_city(self, conn_with_correlation_data):
        """Queries filtered by location should only return that city's data."""
        conn, parquet_path = conn_with_correlation_data
        query = f"""
        SELECT DISTINCT location FROM '{parquet_path}' WHERE location = 'london'
        """
        result = conn.execute(query).fetchdf()
        assert len(result) == 1
        assert result['location'][0] == 'london'

    def test_total_rides_column_does_not_exist(self, conn_with_correlation_data):
        """The mart should NOT have a total_rides column -- it uses ride_count."""
        conn, parquet_path = conn_with_correlation_data
        result = conn.execute(f"SELECT * FROM '{parquet_path}' LIMIT 1").fetchdf()
        assert 'total_rides' not in result.columns
        assert 'ride_count' in result.columns


class TestWeatherDeepDiveImpactQueries:
    """Test the weather impact summary queries."""

    @pytest.fixture
    def conn_with_impact_data(self, tmp_path):
        """Create a DuckDB connection with mart_weather_impact_summary test data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE impact AS
            SELECT * FROM (VALUES
                ('nyc', 8, 'weather_condition', 'rain', NULL::BOOLEAN, NULL::VARCHAR,
                 30, 150.0, 720.0, 100.0, 50.0, 220.0, 680.0, -31.8, 5.9),
                ('nyc', 8, 'weather_condition', 'clear', NULL::BOOLEAN, NULL::VARCHAR,
                 60, 220.0, 680.0, 150.0, 70.0, 220.0, 680.0, 0.0, 0.0),
                ('nyc', 8, 'weather_condition', 'snow', NULL::BOOLEAN, NULL::VARCHAR,
                 10, 80.0, 500.0, 60.0, 20.0, 220.0, 680.0, -63.6, -26.5),
                ('nyc', 8, 'weather_condition', 'fog', NULL::BOOLEAN, NULL::VARCHAR,
                 15, 190.0, 700.0, 130.0, 60.0, 220.0, 680.0, -13.6, 2.9),
                ('london', 8, 'weather_condition', 'rain', NULL::BOOLEAN, NULL::VARCHAR,
                 25, 120.0, 800.0, 0.0, 120.0, 180.0, 750.0, -33.3, 6.7)
            ) AS t(location, hour_of_day, dimension_type, dimension_value,
                   is_precipitation, temperature_band, observation_count,
                   avg_rides, avg_duration_seconds, avg_member_rides,
                   avg_casual_rides, baseline_avg_rides,
                   baseline_avg_duration_seconds, pct_change_rides_vs_clear,
                   pct_change_duration_vs_clear)
        """)
        df = conn.execute("SELECT * FROM impact").fetchdf()
        parquet_path = str(tmp_path / "mart_weather_impact_summary.parquet")
        df.to_parquet(parquet_path)
        yield conn, parquet_path
        conn.close()

    def test_impact_query_excludes_clear(self, conn_with_impact_data):
        """Weather impact query should exclude clear weather from results."""
        conn, parquet_path = conn_with_impact_data
        query = f"""
        SELECT dimension_value as weather_condition,
               round(avg(pct_change_rides_vs_clear), 1) as pct_change
        FROM '{parquet_path}'
        WHERE location = 'nyc'
          AND dimension_type = 'weather_condition'
          AND dimension_value != 'clear'
        GROUP BY dimension_value
        ORDER BY pct_change
        """
        result = conn.execute(query).fetchdf()
        assert 'clear' not in result['weather_condition'].values
        assert len(result) == 3  # rain, snow, fog

    def test_hourly_impact_pivots_conditions(self, conn_with_impact_data):
        """Hourly impact query should pivot rain/snow/fog into separate columns."""
        conn, parquet_path = conn_with_impact_data
        query = f"""
        SELECT hour_of_day,
               round(avg(CASE WHEN dimension_value = 'rain' THEN pct_change_rides_vs_clear END), 1) as rain_impact,
               round(avg(CASE WHEN dimension_value = 'snow' THEN pct_change_rides_vs_clear END), 1) as snow_impact,
               round(avg(CASE WHEN dimension_value = 'fog' THEN pct_change_rides_vs_clear END), 1) as fog_impact
        FROM '{parquet_path}'
        WHERE location = 'nyc' AND dimension_type = 'weather_condition'
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        result = conn.execute(query).fetchdf()
        assert len(result) >= 1
        assert result['rain_impact'][0] == pytest.approx(-31.8, abs=0.1)
        assert result['snow_impact'][0] == pytest.approx(-63.6, abs=0.1)
        assert result['fog_impact'][0] == pytest.approx(-13.6, abs=0.1)


class TestParquetExists:
    """Test the parquet_exists helper function."""

    def test_returns_true_for_existing_file(self, tmp_path):
        """parquet_exists should return True when the file exists."""
        from unittest.mock import patch
        test_file = tmp_path / "test.parquet"
        test_file.touch()
        with patch('dashboard.utils.query_helpers.DATA_DIR', str(tmp_path)):
            from dashboard.utils.query_helpers import parquet_exists
            assert parquet_exists("test.parquet") is True

    def test_returns_false_for_missing_file(self, tmp_path):
        """parquet_exists should return False when the file does not exist."""
        from unittest.mock import patch
        with patch('dashboard.utils.query_helpers.DATA_DIR', str(tmp_path)):
            from dashboard.utils.query_helpers import parquet_exists
            assert parquet_exists("nonexistent.parquet") is False
```

### Existing tests to verify

No existing tests need modification. Run the full test suite to confirm no regressions:

```bash
/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v
```

### Coverage expectations

- `parquet_exists` helper: covered by `TestParquetExists`
- Column name fix (`ride_count`): covered by `TestWeatherDeepDiveQueries.test_temperature_query_uses_ride_count` and `test_precipitation_query_uses_ride_count`
- Empty-state logic (`_check_data_available`): indirectly covered by `TestParquetExists` (same `os.path.exists` logic)
- Streamlit UI rendering: NOT unit-testable (requires running Streamlit server). Covered by manual verification (see below).

## Documentation Updates

### CHANGELOG.md

Add the following entry to the `[Unreleased]` section:

```markdown
### Fixed
- **Weather Deep Dive Column Bug** - Fixed `total_rides` -> `ride_count` column reference in temperature and precipitation queries
  - Queries now match the actual `mart_weather_ride_correlation` schema
  - Previously caused silent failures (empty charts) even with populated data

### Added
- **Weather Deep Dive Empty States** - Added user-friendly empty-state UI when weather mart data is missing
  - Shows informational message instead of blank page or red error boxes
  - Per-section feedback when a query returns no rows for the selected city
  - Added `parquet_exists()` helper to `dashboard/utils/query_helpers.py`
```

### Inline comments

The new `_check_data_available()` function and `_CORRELATION_MART` / `_IMPACT_MART` constants have docstrings/comments explaining their purpose. No other inline comment changes needed.

## Stress Testing & Edge Cases

### Edge cases to handle

| Scenario | Expected behavior |
|----------|-------------------|
| Both mart files missing | `st.info()` message at top of page, early return, no charts rendered |
| Only one mart file missing (e.g., correlation exists but impact does not) | `st.info()` message at top, early return -- both files are required for a coherent page |
| Mart files exist but are empty (0 rows) | Queries return empty DataFrames, per-section `st.info()` messages shown |
| Mart files exist but have no data for selected city | Per-section `st.info()` messages like "No temperature data available for London." |
| City toggle switched from NYC to London | Streamlit re-runs with new `location` value, all queries re-execute |
| DuckDB query error (corrupted parquet) | Caught by existing `try/except`, shows `st.error()` with error details |

### Performance considerations

- `os.path.exists()` is called twice (once per mart file) on every page render. This is a filesystem stat call and takes microseconds -- no caching needed.
- All DuckDB queries are executed against local parquet files via in-memory DuckDB. Query performance depends on parquet file size but is typically sub-second for mart-level aggregations.

## Verification Checklist

1. **Run tests**:
   ```bash
   /Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/test_weather_deep_dive.py -v
   ```

2. **Run full test suite** (confirm no regressions):
   ```bash
   /Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v
   ```

3. **Manual verification -- empty state** (before Phase 01 populates data):
   - Start the dashboard: `cd /Users/chris/Projects/city-cycles && venv/bin/python -m streamlit run dashboard/app.py`
   - Navigate to the "Weather Deep Dive" page
   - Confirm the `st.info()` message appears: "Weather data is being processed. Check back soon."
   - Confirm no blank spaces, no red error boxes, no stack traces
   - Toggle between NYC and London -- both should show the info message

4. **Manual verification -- with data** (after Phase 01 populates data):
   - Ensure `data/mart_weather_ride_correlation.parquet` and `data/mart_weather_impact_summary.parquet` exist locally
   - Navigate to "Weather Deep Dive"
   - Confirm all 4 charts render:
     - Temperature vs Ride Volume (bar chart, 8 temperature ranges)
     - Precipitation Impact (bar chart, 4 categories)
     - Impact by Weather Condition (color-scaled bar chart, excludes "clear")
     - Weather Impact by Hour of Day (line chart with 3 lines: rain, snow, fog)
   - Toggle between NYC and London -- charts should update with city-specific data
   - All charts should use the atmospheric template (transparent background, white text)
   - Click "Show data" expanders -- data tables should display correctly

5. **Verify column fix**: After Phase 01 populates data, if the temperature or precipitation charts show data, the `ride_count` fix is working. If they showed "Error loading temperature data: ..." before this fix, that error should be gone.

## What NOT To Do

1. **Do NOT use `total_rides` in queries against `mart_weather_ride_correlation`**. The column is named `ride_count`. This was the original bug. The name `total_rides` does not exist in the mart schema.

2. **Do NOT import Streamlit in the test file**. The dashboard modules have import-time side effects (`st.set_page_config()`, S3 downloads). Test query logic using standalone DuckDB connections, following the pattern in `tests/test_dashboard.py`.

3. **Do NOT add caching to `parquet_exists()`**. It is a simple `os.path.exists()` call. Adding `@st.cache_data` or similar would introduce unnecessary complexity and could mask file changes during development.

4. **Do NOT modify `streamlit_data_manager/parquet_file_manager.py`**. The MARTS list there already includes the weather mart filenames. The download logic is correct -- it skips with a warning if a mart is not found in S3. This phase only handles what happens in the dashboard when files are absent.

5. **Do NOT add error handling around `parquet_exists()` itself**. `os.path.exists()` does not raise exceptions for missing files -- it returns False. Adding try/except around it would be dead code.

6. **Do NOT change the sidebar radio key from `'weather_city'`**. Other parts of the app may reference this session state key. Changing it would break cross-page state.

7. **Do NOT add the similar-day mart integration in this phase**. That is a separate enhancement that depends on Phase 02. If Phase 02 is completed, a future phase can add a "Days Like Today" section to this page.
