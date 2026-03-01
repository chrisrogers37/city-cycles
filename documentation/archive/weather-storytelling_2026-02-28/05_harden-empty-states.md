# Phase 05 -- Harden Dashboard Empty States

**Status:** ✅ COMPLETE
**Started:** 2026-02-28
**Completed:** 2026-02-28
**PR:** #46

## Header

| Field | Value |
|---|---|
| **PR Title** | Harden dashboard empty states for weather-dependent components |
| **Risk Level** | Low |
| **Estimated Effort** | Medium (~4-6 hours) |
| **Files Modified** | 4 |
| **Files Created** | 1 |

### Files Modified
- `dashboard/utils/query_helpers.py`
- `dashboard/pages/ride_analytics.py`
- `dashboard/pages/comparison.py`
- `dashboard/recommendation_engine.py`

### Files Created
- `tests/test_dashboard_empty_states.py`

---

## Context

The dashboard has multiple weather-dependent components spread across `ride_analytics.py` (Station Weather Performance section), `comparison.py` (Weather Impact on Ridership section), the recommendation engine's historical data lookup, and the query helpers layer. When mart parquet files are missing from S3 or contain no data for a selected city/filter, these components either render blank space, throw uncaught DuckDB errors (because the parquet file path doesn't exist on disk), or silently show nothing.

This phase adds a `parquet_exists()` pre-flight check to `query_helpers.py` and uses it in the two analytics pages to show styled `st.info()` messages instead of blank space or errors. It also hardens an edge case in the recommendation engine where `lookup_historical_impact` logs a warning but never surfaces it to the caller in a way that generates a user-visible message when ALL historical data is missing (not just sparse).

### Scope boundaries

- **NOT landing.py** -- that page's insights section is Phase 03's responsibility.
- **NOT weather_deep_dive.py** -- that entire page is Phase 04's responsibility.
- This phase covers: `ride_analytics.py` station weather section, `comparison.py` weather impact section, `recommendation_engine.py` edge cases, and `query_helpers.py` safeguards.

---

## Dependencies

- **Depends on:** None -- this phase touches disjoint files from Phases 03 and 04.
- **Unlocks:** None -- this is a standalone resilience phase.

### Parallel safety

This phase modifies:
- `dashboard/utils/query_helpers.py` -- adds a new function `parquet_exists()`. No existing functions are changed. Safe to run in parallel with any phase that only *calls* existing functions from this module.
- `dashboard/pages/ride_analytics.py` -- only the Station Weather Performance section (lines 189-262). Phase 03 does not touch this file. Phase 04 does not touch this file.
- `dashboard/pages/comparison.py` -- only the Weather Impact on Ridership section (lines 161-179). Phase 03 does not touch this file. Phase 04 does not touch this file.
- `dashboard/recommendation_engine.py` -- only the `_insight_missing_data` function (lines 618-647) and `lookup_historical_impact` return value documentation. Phase 03 does not modify the recommendation engine internals. Phase 04 does not modify these functions.

---

## Detailed Implementation Plan

### Step 1: Add `parquet_exists()` to `query_helpers.py`

**File:** `dashboard/utils/query_helpers.py`

**What exists today (full file, lines 1-33):**

```python
"""
Dashboard query helpers -- extracted from monolithic app.py.
All DuckDB queries live here, cached and parameterized.
"""

import streamlit as st
import duckdb
import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Persistent in-memory DuckDB connection."""
    return duckdb.connect(database=':memory:')


def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    return get_connection().execute(query).fetchdf()


def run_query_params(query: str, params: list) -> pd.DataFrame:
    """Execute a parameterized query and return results as a DataFrame."""
    return get_connection().execute(query, params).fetchdf()


def parquet_path(filename: str) -> str:
    """Resolve a mart Parquet filename to its full path in DATA_DIR."""
    return os.path.join(DATA_DIR, filename)
```

**What it should become (add `parquet_exists` function after `parquet_path`):**

```python
"""
Dashboard query helpers -- extracted from monolithic app.py.
All DuckDB queries live here, cached and parameterized.
"""

import streamlit as st
import duckdb
import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Persistent in-memory DuckDB connection."""
    return duckdb.connect(database=':memory:')


def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    return get_connection().execute(query).fetchdf()


def run_query_params(query: str, params: list) -> pd.DataFrame:
    """Execute a parameterized query and return results as a DataFrame."""
    return get_connection().execute(query, params).fetchdf()


def parquet_path(filename: str) -> str:
    """Resolve a mart Parquet filename to its full path in DATA_DIR."""
    return os.path.join(DATA_DIR, filename)


def parquet_exists(filename: str) -> bool:
    """Check whether a mart parquet file exists on disk.

    Use this as a pre-flight check before running queries against a mart
    parquet. Returns False if the file has not been downloaded from S3
    (e.g., because it was missing from the bucket or the download failed).

    Args:
        filename: The mart parquet filename, e.g. 'mart_weather_ride_correlation.parquet'.

    Returns:
        True if the file exists in DATA_DIR, False otherwise.
    """
    return os.path.isfile(os.path.join(DATA_DIR, filename))
```

**Why:** Every weather-dependent section needs to check whether its mart parquet file exists before attempting DuckDB queries. Without this check, DuckDB throws an `IOException: No such file or directory` when the parquet path doesn't exist on disk. This helper centralizes the check so each page doesn't need to import `os` and reconstruct the path manually.

---

### Step 2: Harden Station Weather Performance section in `ride_analytics.py`

**File:** `dashboard/pages/ride_analytics.py`

**What exists today (lines 189-262):**

```python
    # --- Station Weather Performance ---
    st.subheader("Station Weather Performance")
    conditions_query = f"""
        SELECT DISTINCT weather_condition
        FROM '{parquet_path('mart_station_weather_performance.parquet')}'
        WHERE location = $1 ORDER BY weather_condition
    """
    try:
        conditions_df = run_query_params(conditions_query, [location])
        available_conditions = conditions_df['weather_condition'].tolist()
    except Exception:
        available_conditions = ['rain', 'snow', 'partly_cloudy', 'fog']

    non_clear = [c for c in available_conditions if c != 'clear']
    if non_clear:
        selected_condition = st.selectbox("Weather Condition:", non_clear,
                                          key=f"weather_condition_{location}")
        selected_hour = st.slider("Hour Range:", min_value=0, max_value=23,
                                  value=(7, 19), key=f"weather_hour_{location}")

        resilience_query = f"""
            SELECT s.station_id, d.station_name, d.latitude, d.longitude,
                round(avg(s.pct_change_vs_clear), 1) as avg_pct_change,
                sum(s.total_rides) as total_rides_in_condition,
                round(avg(s.avg_duration_minutes), 1) as avg_duration
            FROM '{parquet_path('mart_station_weather_performance.parquet')}' s
            JOIN '{parquet_path('mart_station_directory.parquet')}' d
                ON s.location = d.location AND s.station_id = d.station_id
            WHERE s.location = $1 AND s.weather_condition = $2
              AND s.hour_of_day BETWEEN $3 AND $4
              AND s.pct_change_vs_clear IS NOT NULL
            GROUP BY s.station_id, d.station_name, d.latitude, d.longitude
            ORDER BY avg_pct_change DESC LIMIT 20
        """
        try:
            resilience_df = run_query_params(resilience_query, [
                location, selected_condition, selected_hour[0], selected_hour[1]
            ])
            if not resilience_df.empty:
                st.markdown(f"**Top 20 Most Weather-Resilient Stations ({selected_condition.replace('_', ' ').title()})**")
                st.dataframe(
                    resilience_df[['station_name', 'avg_pct_change',
                                   'total_rides_in_condition', 'avg_duration']],
                    use_container_width=True,
                    column_config={
                        'station_name': 'Station',
                        'avg_pct_change': st.column_config.NumberColumn('% Change vs Clear', format='%.1f%%'),
                        'total_rides_in_condition': st.column_config.NumberColumn('Total Rides', format='%d'),
                        'avg_duration': st.column_config.NumberColumn('Avg Duration (min)', format='%.1f'),
                    }
                )

                # NYC Map
                if location == 'nyc':
                    map_df = resilience_df.dropna(subset=['latitude', 'longitude']).copy()
                    if not map_df.empty:
                        st.markdown(f"**Station Weather Impact Map ({selected_condition.replace('_', ' ').title()})**")
                        fig_map = px.scatter_mapbox(
                            map_df, lat='latitude', lon='longitude',
                            color='avg_pct_change', size='total_rides_in_condition',
                            hover_name='station_name',
                            hover_data={'avg_pct_change': ':.1f', 'total_rides_in_condition': ':,', 'avg_duration': ':.1f'},
                            color_continuous_scale='RdYlGn',
                            range_color=[map_df['avg_pct_change'].min(), 0],
                            mapbox_style='open-street-map', zoom=11,
                            center={'lat': 40.7128, 'lon': -74.0060},
                            title=f'NYC Station Impact During {selected_condition.replace("_", " ").title()}'
                        )
                        fig_map.update_layout(height=600)
                        st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("No station weather data available for the selected filters.")
        except Exception as e:
            st.error(f"Error loading station weather data: {e}")
```

**What it should become:**

First, update the import on line 11 to include `parquet_exists`:

```python
from dashboard.utils.query_helpers import run_query, run_query_params, parquet_path, parquet_exists
```

Then replace the entire Station Weather Performance section (lines 189-262) with:

```python
    # --- Station Weather Performance ---
    st.subheader("Station Weather Performance")

    # Pre-flight: check both required parquets exist
    _swp_file = 'mart_station_weather_performance.parquet'
    _sd_file = 'mart_station_directory.parquet'
    if not parquet_exists(_swp_file) or not parquet_exists(_sd_file):
        st.info(
            "Station weather performance data is not yet available. "
            "Run the full pipeline to generate weather mart data."
        )
    else:
        conditions_query = f"""
            SELECT DISTINCT weather_condition
            FROM '{parquet_path(_swp_file)}'
            WHERE location = $1 ORDER BY weather_condition
        """
        try:
            conditions_df = run_query_params(conditions_query, [location])
            available_conditions = conditions_df['weather_condition'].tolist()
        except Exception:
            available_conditions = []

        non_clear = [c for c in available_conditions if c != 'clear']
        if not non_clear:
            st.info(f"No weather condition data available for {city_label}.")
        else:
            selected_condition = st.selectbox("Weather Condition:", non_clear,
                                              key=f"weather_condition_{location}")
            selected_hour = st.slider("Hour Range:", min_value=0, max_value=23,
                                      value=(7, 19), key=f"weather_hour_{location}")

            resilience_query = f"""
                SELECT s.station_id, d.station_name, d.latitude, d.longitude,
                    round(avg(s.pct_change_vs_clear), 1) as avg_pct_change,
                    sum(s.total_rides) as total_rides_in_condition,
                    round(avg(s.avg_duration_minutes), 1) as avg_duration
                FROM '{parquet_path(_swp_file)}' s
                JOIN '{parquet_path(_sd_file)}' d
                    ON s.location = d.location AND s.station_id = d.station_id
                WHERE s.location = $1 AND s.weather_condition = $2
                  AND s.hour_of_day BETWEEN $3 AND $4
                  AND s.pct_change_vs_clear IS NOT NULL
                GROUP BY s.station_id, d.station_name, d.latitude, d.longitude
                ORDER BY avg_pct_change DESC LIMIT 20
            """
            try:
                resilience_df = run_query_params(resilience_query, [
                    location, selected_condition, selected_hour[0], selected_hour[1]
                ])
                if not resilience_df.empty:
                    st.markdown(f"**Top 20 Most Weather-Resilient Stations ({selected_condition.replace('_', ' ').title()})**")
                    st.dataframe(
                        resilience_df[['station_name', 'avg_pct_change',
                                       'total_rides_in_condition', 'avg_duration']],
                        use_container_width=True,
                        column_config={
                            'station_name': 'Station',
                            'avg_pct_change': st.column_config.NumberColumn('% Change vs Clear', format='%.1f%%'),
                            'total_rides_in_condition': st.column_config.NumberColumn('Total Rides', format='%d'),
                            'avg_duration': st.column_config.NumberColumn('Avg Duration (min)', format='%.1f'),
                        }
                    )

                    # NYC Map
                    if location == 'nyc':
                        map_df = resilience_df.dropna(subset=['latitude', 'longitude']).copy()
                        if not map_df.empty:
                            st.markdown(f"**Station Weather Impact Map ({selected_condition.replace('_', ' ').title()})**")
                            fig_map = px.scatter_mapbox(
                                map_df, lat='latitude', lon='longitude',
                                color='avg_pct_change', size='total_rides_in_condition',
                                hover_name='station_name',
                                hover_data={'avg_pct_change': ':.1f', 'total_rides_in_condition': ':,', 'avg_duration': ':.1f'},
                                color_continuous_scale='RdYlGn',
                                range_color=[map_df['avg_pct_change'].min(), 0],
                                mapbox_style='open-street-map', zoom=11,
                                center={'lat': 40.7128, 'lon': -74.0060},
                                title=f'NYC Station Impact During {selected_condition.replace("_", " ").title()}'
                            )
                            fig_map.update_layout(height=600)
                            st.plotly_chart(fig_map, use_container_width=True)
                else:
                    st.info(f"No station weather data available for {city_label} with the selected filters.")
            except Exception as e:
                st.error(f"Error loading station weather data: {e}")
```

**Key changes explained:**

1. **Import `parquet_exists`** -- added to the existing import line.
2. **Pre-flight check** -- before any queries, verify both `mart_station_weather_performance.parquet` and `mart_station_directory.parquet` exist on disk. If not, show `st.info()` with an actionable message and skip the entire section. This prevents DuckDB `IOException` when files are missing.
3. **Empty conditions fallback** -- the old code fell back to a hardcoded list `['rain', 'snow', 'partly_cloudy', 'fog']` when the conditions query failed. This is misleading because the subsequent resilience query would also fail. Changed to fall back to an empty list and show `st.info()`.
4. **City name in messages** -- uses `city_label` (already in scope as `"NYC"` or `"London"`) in empty-state messages so users know which city has no data.

---

### Step 3: Harden Weather Impact section in `comparison.py`

**File:** `dashboard/pages/comparison.py`

**What exists today (lines 160-179):**

```python
    # --- Weather Impact Comparison ---
    st.header("Weather Impact on Ridership")
    weather_query = f"""
    SELECT location, weather_condition,
        round(avg(pct_change_vs_clear), 1) as avg_pct_change,
        sum(total_rides) as total_rides
    FROM '{parquet_path('mart_station_weather_performance.parquet')}'
    WHERE pct_change_vs_clear IS NOT NULL AND weather_condition != 'clear'
    GROUP BY location, weather_condition
    ORDER BY location, weather_condition
    """
    try:
        weather_df = run_query(weather_query)
        if not weather_df.empty:
            fig = grouped_bar_chart(weather_df, 'weather_condition', 'avg_pct_change',
                                    'Average Station Ridership Change by Weather Condition',
                                    '% Change vs Clear Weather')
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading weather comparison: {e}")
```

**What it should become:**

First, update the import on line 10 to include `parquet_exists`:

```python
from dashboard.utils.query_helpers import run_query, run_query_params, parquet_path, parquet_exists
```

Then replace lines 160-179 with:

```python
    # --- Weather Impact Comparison ---
    st.header("Weather Impact on Ridership")

    if not parquet_exists('mart_station_weather_performance.parquet'):
        st.info(
            "Weather impact data is not yet available. "
            "Run the full pipeline to generate weather mart data."
        )
    else:
        weather_query = f"""
        SELECT location, weather_condition,
            round(avg(pct_change_vs_clear), 1) as avg_pct_change,
            sum(total_rides) as total_rides
        FROM '{parquet_path('mart_station_weather_performance.parquet')}'
        WHERE pct_change_vs_clear IS NOT NULL AND weather_condition != 'clear'
        GROUP BY location, weather_condition
        ORDER BY location, weather_condition
        """
        try:
            weather_df = run_query(weather_query)
            if not weather_df.empty:
                fig = grouped_bar_chart(weather_df, 'weather_condition', 'avg_pct_change',
                                        'Average Station Ridership Change by Weather Condition',
                                        '% Change vs Clear Weather')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No weather impact data available for the selected date range.")
        except Exception as e:
            st.error(f"Error loading weather comparison: {e}")
```

**Key changes explained:**

1. **Import `parquet_exists`** -- added to the existing import line.
2. **Pre-flight check** -- if the mart parquet doesn't exist, show `st.info()` with an actionable message. Previously this would throw a DuckDB `IOException` caught only by the generic `except Exception` which showed a raw error message.
3. **Empty result handling** -- added an `else` branch after the `if not weather_df.empty` check. Previously, when the query returned no rows (e.g., if the mart existed but had no data for the date range), the section rendered nothing at all -- just blank space. Now it shows a clear info message.

---

### Step 4: Harden recommendation engine edge case

**File:** `dashboard/recommendation_engine.py`

The recommendation engine already handles missing parquet files (returns empty `HistoricalImpact()` in `lookup_historical_impact` at line 401) and sparse data (the `_insight_missing_data` function at line 618). However, there is an edge case: when the parquet file exists but contains **zero** rows matching the query parameters, the `_insight_missing_data` function correctly generates a "No historical riding data" notice. But when `sample_days` is exactly `0` (not `None`), the first branch (`sample_days < 5`) fires instead, producing the confusing message "Limited historical data ... (0 days in dataset)".

**What exists today (lines 618-647):**

```python
def _insight_missing_data(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
) -> Optional[Recommendation]:
    """Generate a notice when historical data is sparse or missing."""
    if impact.sample_days is not None and impact.sample_days < 5:
        weather_label = classified.weather_category.value.replace("_", " ")
        return Recommendation(
            text=(
                f"Limited historical data for {weather_label} conditions "
                f"at this hour ({impact.sample_days} days in dataset)"
            ),
            severity=Severity.NEUTRAL,
            metric="data_quality",
            value=float(impact.sample_days),
        )

    if impact.avg_rides is None and impact.pct_change_vs_baseline is None:
        weather_label = classified.weather_category.value.replace("_", " ")
        return Recommendation(
            text=(
                f"No historical riding data available for {weather_label} conditions "
                f"at this hour \u2014 this is a rare combination"
            ),
            severity=Severity.NEUTRAL,
            metric="data_quality",
            value=None,
        )

    return None
```

**What it should become:**

```python
def _insight_missing_data(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
) -> Optional[Recommendation]:
    """Generate a notice when historical data is sparse or missing."""
    # Fully missing: no rides data and no baseline comparison
    if impact.avg_rides is None and impact.pct_change_vs_baseline is None:
        weather_label = classified.weather_category.value.replace("_", " ")
        return Recommendation(
            text=(
                f"No historical riding data available for {weather_label} conditions "
                f"at this hour \u2014 this is a rare combination"
            ),
            severity=Severity.NEUTRAL,
            metric="data_quality",
            value=None,
        )

    # Sparse: data exists but fewer than 5 observations
    if impact.sample_days is not None and impact.sample_days < 5:
        weather_label = classified.weather_category.value.replace("_", " ")
        return Recommendation(
            text=(
                f"Limited historical data for {weather_label} conditions "
                f"at this hour ({impact.sample_days} days in dataset)"
            ),
            severity=Severity.NEUTRAL,
            metric="data_quality",
            value=float(impact.sample_days),
        )

    return None
```

**Key change explained:**

The order of the two checks is swapped. Previously, `sample_days < 5` was checked first, which meant that if `sample_days` was `0` (or any small int) but `avg_rides` was also `None`, the user would see "Limited historical data ... (0 days in dataset)" instead of the clearer "No historical riding data available" message. By checking the fully-missing case first, we ensure the most informative message is shown. When `sample_days` is 1-4 and `avg_rides` is populated, the sparse-data message still fires correctly.

---

## Test Plan

### New test file: `tests/test_dashboard_empty_states.py`

Create this file with the following tests:

```python
"""
Tests for Phase 05: Dashboard empty state handling.

Verifies that parquet_exists works correctly and that the recommendation
engine's _insight_missing_data function prioritizes fully-missing data
over sparse-data messages.
"""

import os
import pytest
import pandas as pd

from dashboard.utils.query_helpers import parquet_exists, parquet_path, DATA_DIR
from dashboard.recommendation_engine import (
    WeatherConditions,
    WeatherCategory,
    TemperatureBand,
    WindCategory,
    PrecipitationIntensity,
    Severity,
    ClassifiedConditions,
    HistoricalImpact,
    BikingScore,
    generate_insights,
    _insight_missing_data,
)


# ---------------------------------------------------------------------------
# parquet_exists tests
# ---------------------------------------------------------------------------


class TestParquetExists:
    """Tests for the parquet_exists pre-flight check."""

    def test_returns_false_for_missing_file(self):
        """parquet_exists should return False when the file does not exist."""
        assert parquet_exists("nonexistent_mart_xyz.parquet") is False

    def test_returns_true_for_existing_file(self, tmp_path, monkeypatch):
        """parquet_exists should return True when the file exists."""
        # Create a fake parquet file in a temp DATA_DIR
        monkeypatch.setattr(
            "dashboard.utils.query_helpers.DATA_DIR", str(tmp_path)
        )
        fake_file = tmp_path / "mart_test.parquet"
        fake_file.write_bytes(b"fake parquet content")
        assert parquet_exists("mart_test.parquet") is True

    def test_returns_false_for_directory(self, tmp_path, monkeypatch):
        """parquet_exists should return False when the path is a directory."""
        monkeypatch.setattr(
            "dashboard.utils.query_helpers.DATA_DIR", str(tmp_path)
        )
        subdir = tmp_path / "mart_test.parquet"
        subdir.mkdir()
        assert parquet_exists("mart_test.parquet") is False

    def test_uses_data_dir(self):
        """parquet_exists should check inside DATA_DIR, not cwd."""
        # parquet_path and parquet_exists should resolve to the same directory
        expected_path = os.path.join(DATA_DIR, "some_mart.parquet")
        assert parquet_path("some_mart.parquet") == expected_path


# ---------------------------------------------------------------------------
# _insight_missing_data edge case tests
# ---------------------------------------------------------------------------


class TestInsightMissingDataPriority:
    """Tests that _insight_missing_data prioritizes fully-missing over sparse."""

    @pytest.fixture
    def classified_rain(self) -> ClassifiedConditions:
        """Rain conditions for testing."""
        conditions = WeatherConditions(
            temperature_celsius=12.0,
            wind_speed_kmh=15.0,
            precipitation_mm=3.0,
            weather_code=63,
            location="nyc",
            hour=14,
        )
        return ClassifiedConditions(
            weather_category=WeatherCategory.RAIN,
            temperature_band=TemperatureBand.COOL,
            wind_category=WindCategory.LIGHT,
            precipitation_intensity=PrecipitationIntensity.MODERATE,
            raw=conditions,
        )

    def test_fully_missing_shows_no_data_message(self, classified_rain):
        """When avg_rides and pct_change are both None, show 'No historical data'."""
        impact = HistoricalImpact(
            avg_rides=None,
            pct_change_vs_baseline=None,
            sample_days=None,
        )
        result = _insight_missing_data(classified_rain, impact)
        assert result is not None
        assert "No historical riding data" in result.text
        assert result.metric == "data_quality"
        assert result.value is None

    def test_sparse_data_shows_limited_message(self, classified_rain):
        """When sample_days < 5 but data exists, show 'Limited historical data'."""
        impact = HistoricalImpact(
            avg_rides=500.0,
            pct_change_vs_baseline=-20.0,
            sample_days=3,
        )
        result = _insight_missing_data(classified_rain, impact)
        assert result is not None
        assert "Limited historical data" in result.text
        assert "3 days" in result.text

    def test_sufficient_data_returns_none(self, classified_rain):
        """When sample_days >= 5, no data quality notice is generated."""
        impact = HistoricalImpact(
            avg_rides=1000.0,
            pct_change_vs_baseline=-10.0,
            sample_days=50,
        )
        result = _insight_missing_data(classified_rain, impact)
        assert result is None

    def test_zero_sample_days_with_no_rides_shows_no_data(self, classified_rain):
        """Edge case: sample_days=0 with None rides should show 'No historical data'."""
        impact = HistoricalImpact(
            avg_rides=None,
            pct_change_vs_baseline=None,
            sample_days=0,
        )
        result = _insight_missing_data(classified_rain, impact)
        assert result is not None
        # Should show the fully-missing message, not the sparse message
        assert "No historical riding data" in result.text

    def test_generate_insights_includes_missing_data_notice(self, classified_rain):
        """generate_insights should include data_quality insight when data is missing."""
        score = BikingScore(score=35, label="Fair", color="#e67e22")
        impact = HistoricalImpact()  # All None

        insights = generate_insights(classified_rain, impact, score)
        data_quality = [r for r in insights if r.metric == "data_quality"]
        assert len(data_quality) == 1
        assert "No historical riding data" in data_quality[0].text
```

### Existing tests to verify (not modify)

The existing `tests/test_recommendation_engine.py` has tests that verify:
- `test_missing_data_insight_for_sparse_data` (line 555) -- this test creates an impact with `sample_days=3` **and** populated `avg_rides=525` and `pct_change_vs_baseline=-65.0`. After our reorder, this test still passes because `avg_rides` is not None, so the fully-missing branch does NOT fire, and the sparse branch fires as expected.
- `test_works_without_parquet_file` (line 612) -- verifies engine works when parquet is missing. Unaffected by our changes.

### Coverage expectations

- `parquet_exists()` -- 100% coverage via the 4 test cases.
- `_insight_missing_data()` -- the reordered branches are covered by the 5 test cases above plus the existing `test_missing_data_insight_for_sparse_data` test.
- The `st.info()` calls in `ride_analytics.py` and `comparison.py` are UI-level and not unit-tested. They are verified manually (see Verification Checklist below).

---

## Documentation Updates

### `CHANGELOG.md`

Add to `[Unreleased]` section:

```markdown
### Improved
- **Dashboard Empty States** -- Weather-dependent dashboard sections now show styled info messages when mart parquet files are missing or queries return no data, instead of rendering blank space or raw error messages
  - Added `parquet_exists()` pre-flight check to `dashboard/utils/query_helpers.py`
  - Hardened Station Weather Performance section in Ride Analytics page
  - Hardened Weather Impact section in City Comparison page
  - Fixed recommendation engine edge case where fully missing data showed confusing "0 days" message
```

### Inline comments

No additional inline comments needed beyond the docstrings already included in the code changes above.

---

## Stress Testing & Edge Cases

### Edge cases handled

| Scenario | Before | After |
|---|---|---|
| Mart parquets not downloaded from S3 | DuckDB `IOException`, caught by generic `except`, shows raw error | `st.info()` with actionable message before any query runs |
| Parquet exists but has no rows for selected city | Blank space (no chart, no message) | `st.info("No weather condition data available for [city]")` |
| Parquet exists, city has data, but selected filter combo returns empty | `ride_analytics.py` shows "No station weather data available" (already handled); `comparison.py` shows nothing | Both pages show `st.info()` for empty results |
| Recommendation engine: `sample_days=0` with `avg_rides=None` | Shows "Limited historical data ... (0 days)" | Shows "No historical riding data available" |
| S3 download partially fails (some marts missing, others present) | Pages that use missing marts error out; pages with present marts work | Each section independently checks its own required marts |

### Performance considerations

- `parquet_exists()` calls `os.path.isfile()` which is a trivial stat syscall. No performance impact.
- The pre-flight check runs once per page render, before any DuckDB queries. This is actually faster than the current behavior where DuckDB attempts to open a non-existent file and throws an exception.

---

## Verification Checklist

1. **Run existing tests to verify no regressions:**
   ```bash
   /Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/test_recommendation_engine.py -v
   ```
   All 24 tests should pass, including `test_missing_data_insight_for_sparse_data`.

2. **Run new tests:**
   ```bash
   /Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/test_dashboard_empty_states.py -v
   ```
   All 9 tests should pass (4 for `parquet_exists`, 5 for `_insight_missing_data`).

3. **Run full test suite:**
   ```bash
   /Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v
   ```
   Baseline: 283 pass, 3 skip. After this phase: 292 pass, 3 skip.

4. **Manual verification -- missing parquets:**
   - Temporarily rename `data/mart_station_weather_performance.parquet` (if it exists locally).
   - Load the Ride Analytics page -- the Station Weather Performance section should show the info message.
   - Load the City Comparison page -- the Weather Impact section should show the info message.
   - Rename the file back.

5. **Manual verification -- empty query results:**
   - On the Ride Analytics page, if data exists, select a weather condition and hour range that is unlikely to have data (e.g., "thunderstorm" + hours 0-3 in London). Verify the info message appears instead of blank space.

6. **Import check:**
   ```bash
   /Users/chris/Projects/city-cycles/venv/bin/python -c "from dashboard.utils.query_helpers import parquet_exists; print('OK')"
   ```

---

## What NOT To Do

1. **Do NOT add `parquet_exists` checks to `weather_deep_dive.py`.** That page is Phase 04's responsibility. This phase only covers `ride_analytics.py` and `comparison.py`.

2. **Do NOT modify the landing page insights section.** That is Phase 03's responsibility. The landing page already handles live API failures via `get_city_weather_cached` returning `None`.

3. **Do NOT cache `parquet_exists` with `@st.cache_data`.** The file check is a trivial `os.path.isfile()` call. Caching it would prevent the dashboard from detecting newly downloaded parquets during a session.

4. **Do NOT change `ensure_local_parquet_files()` in `parquet_file_manager.py`.** That function already logs warnings and skips missing S3 objects. This phase adds checks at the *consumer* side (dashboard pages), not the *producer* side.

5. **Do NOT add try/except around the `parquet_exists` check itself.** `os.path.isfile()` does not raise exceptions for missing files -- it returns `False`. Adding try/except would be cargo-cult error handling.

6. **Do NOT change the fallback condition list in `ride_analytics.py` to a non-empty hardcoded list.** The old code fell back to `['rain', 'snow', 'partly_cloudy', 'fog']` which was misleading because subsequent queries against those conditions would also fail. An empty list with an info message is the correct behavior.

7. **Do NOT use `st.warning()` for missing data messages.** `st.warning()` implies something is wrong that needs user action. `st.info()` is the correct choice -- it communicates "data isn't here yet" without alarm. Reserve `st.error()` for actual runtime errors (DuckDB query failures, etc.) and `st.warning()` for degraded live API connections (as the landing page already does).
