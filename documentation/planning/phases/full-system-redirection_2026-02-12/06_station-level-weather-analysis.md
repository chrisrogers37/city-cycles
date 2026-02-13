# Phase 06: Station-Level Weather Analysis

**Status:** 🔧 IN PROGRESS
**Started:** 2026-02-12

## PR Title
feat: add station-level weather performance analysis with map visualization

## Risk Level: Low
## Estimated Effort: 1-2 days
## Dependencies: Phases 01 (Weather Pipeline), 02 (Analytics Marts)
## Unlocks: Future "near me" feature (Phase 08)

## Files Impact
| Action | File |
|--------|------|
| CREATE | dbt_city_cycles/models/marts/mart_station_directory.sql |
| CREATE | dbt_city_cycles/models/marts/mart_station_weather_performance.sql |
| MODIFY | dbt_city_cycles/models/marts/schema.yml |
| MODIFY | db_duckdb/operations.py |
| MODIFY | streamlit_data_manager/parquet_file_manager.py |
| MODIFY | dashboard/app.py |
| MODIFY | tests/test_dashboard.py |
| MODIFY | tests/test_streamlit_data_manager.py |

## Context
This phase adds station-level weather analysis: which stations maintain ridership in bad weather? It creates a station directory (with NYC lat/lng coordinates) and a weather performance mart showing how each station's ridership changes under different weather conditions. Includes a Mapbox-free map visualization for NYC stations colored by weather resilience. This lays the foundation for the future "near me" feature (Phase 08).

---

# Phase 06: Station-Level Weather Analysis -- Implementation Plan

## 1. Current State Assessment

### Available Data Fields

**unified_rides** contains the following station-related columns:
- `location` -- 'nyc' or 'london'
- `start_station_id`, `start_station_name`
- `end_station_id`, `end_station_name`
- `start_latitude`, `start_longitude` -- NYC only (NULL for London)
- `end_latitude`, `end_longitude` -- NYC only (NULL for London)
- `hour_of_day` (0-23), `date`, `day_type`, `duration_seconds`

**Key constraint**: Weather data is city-level (one reference point per city). This means weather is uniform across all stations in a city, so the `pct_change_vs_clear_weather` metric measures how a specific station's ridership changes under different weather conditions relative to its own clear-weather baseline -- it does not compare micro-climate differences between stations.

**Dependency on Phase 01/02**: This plan assumes Phase 01 adds a weather data source (as a dbt seed, external table, or staging model) and Phase 02 creates a city-level weather-ride correlation mart. Phase 06 builds on top of those by joining station-level ride aggregations to the same weather data. The plan is written to be agnostic to the exact Phase 01/02 implementation but assumes a model like `int_weather_conditions` or similar exists with grain (location, date, hour_of_day, weather_condition).

### Existing Patterns to Follow

- **Mart materialization**: `materialized='table'` in `dbt_project.yml` for all marts
- **Data source**: All marts read from `{{ ref('unified_rides') }}` or from other marts
- **Population join pattern**: LEFT JOIN to seed data (see `mart_daily_metrics.sql`)
- **Export pipeline**: `MART_TABLES` list in `/Users/chris/Projects/city-cycles/db_duckdb/operations.py` (line 449) plus `MARTS` list in `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py` (line 13)
- **Dashboard pattern**: Parquet files read directly from `DATA_DIR` via DuckDB in-memory connection with `run_query_params()` for parameterized queries
- **Test pattern**: Standalone DuckDB connections in pytest, no direct Streamlit imports (see `/Users/chris/Projects/city-cycles/tests/test_dashboard.py`)

---

## 2. Implementation Plan

### Step 1: Create `mart_station_directory` dbt model

**File**: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_station_directory.sql`

This is a reference dimension table with one row per (location, station_id).

```sql
{{ config(
    materialized='table'
) }}

with station_stats as (
    select
        location,
        start_station_id as station_id,
        -- Use the most recent station name (names can change over time)
        last(start_station_name ORDER BY start_time) as station_name,
        -- NYC has coordinates; take the median to smooth GPS jitter
        median(start_latitude) as latitude,
        median(start_longitude) as longitude,
        count(*) as total_rides,
        min(date) as first_ride_date,
        max(date) as last_ride_date
    from {{ ref('unified_rides') }}
    where start_station_id is not null
    group by 1, 2
)

select
    location,
    station_id,
    station_name,
    latitude,   -- NULL for London stations
    longitude,  -- NULL for London stations
    total_rides,
    first_ride_date,
    last_ride_date
from station_stats
order by location, station_id
```

**Design decisions**:
- Uses `median()` for lat/lng to handle GPS noise from ride-level coordinates. DuckDB supports `median()` as an aggregate.
- Uses `last(...ORDER BY start_time)` to pick the most recent station name, since station names can change over time (e.g., sponsor renaming).
- Only uses `start_station_id` for the directory (departures). End stations could be added later if needed but would largely overlap.
- London stations will have NULL latitude/longitude as expected.

### Step 2: Create `mart_station_weather_performance` dbt model

**File**: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_station_weather_performance.sql`

This is the core analytical mart. It needs to join ride data with weather conditions. Since weather data comes from Phase 01/02, I will assume a model named `int_weather_conditions` (or a similarly named Phase 02 artifact) exists with grain (location, date, hour_of_day) and includes a `weather_condition` field (e.g., 'clear', 'rain', 'snow', 'cloudy', 'fog').

```sql
{{ config(
    materialized='table'
) }}

-- Step 1: Count rides per station per hour per weather condition
with station_hourly_weather as (
    select
        r.location,
        r.start_station_id as station_id,
        r.hour_of_day,
        w.weather_condition,
        count(*) as ride_count,
        avg(r.duration_seconds) / 60.0 as avg_duration_minutes,
        count(distinct r.date) as days_observed
    from {{ ref('unified_rides') }} r
    inner join {{ ref('int_weather_conditions') }} w
        on r.location = w.location
        and r.date = w.date
        and r.hour_of_day = w.hour_of_day
    where r.start_station_id is not null
    group by 1, 2, 3, 4
),

-- Step 2: Calculate average rides per observed day (normalizes for unequal sample sizes)
station_normalized as (
    select
        location,
        station_id,
        hour_of_day,
        weather_condition,
        ride_count,
        ride_count::float / nullif(days_observed, 0) as avg_rides_per_day,
        avg_duration_minutes,
        days_observed
    from station_hourly_weather
),

-- Step 3: Get each station's clear-weather baseline for comparison
clear_weather_baseline as (
    select
        location,
        station_id,
        hour_of_day,
        avg_rides_per_day as clear_avg_rides_per_day
    from station_normalized
    where weather_condition = 'clear'
),

-- Step 4: Compute percentage change vs clear weather
final as (
    select
        s.location,
        s.station_id,
        s.hour_of_day,
        s.weather_condition,
        s.ride_count as total_rides,
        round(s.avg_rides_per_day, 2) as avg_rides_per_day,
        round(s.avg_duration_minutes, 1) as avg_duration_minutes,
        s.days_observed,
        round(
            case
                when c.clear_avg_rides_per_day is null or c.clear_avg_rides_per_day = 0 then null
                else ((s.avg_rides_per_day - c.clear_avg_rides_per_day) / c.clear_avg_rides_per_day * 100)
            end,
            1
        ) as pct_change_vs_clear
    from station_normalized s
    left join clear_weather_baseline c
        on s.location = c.location
        and s.station_id = c.station_id
        and s.hour_of_day = c.hour_of_day
    -- Filter for statistical significance: station must have enough observations
    where s.ride_count >= 100
)

select * from final
order by location, station_id, hour_of_day, weather_condition
```

**Design decisions**:
- **Grain**: (location, station_id, hour_of_day, weather_condition) as specified.
- **avg_rides_per_day**: Normalizes ride counts by the number of days that weather condition was observed for that hour. Without this, a station could appear "resilient" simply because more rainy days were observed.
- **pct_change_vs_clear**: Compares each condition's avg_rides_per_day against the clear-weather baseline for the same station+hour. Negative values mean fewer rides (e.g., -40% means 40% fewer rides vs clear weather).
- **Minimum threshold of 100 total rides** per (station, hour, condition) ensures statistical significance.
- The `inner join` to weather means rides without weather data are excluded. This is intentional -- we only want weather-matched observations.

### Step 3: Add schema definitions for both new marts

**File to modify**: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/schema.yml`

Append the following entries to the existing `models:` list:

```yaml
  - name: mart_station_directory
    description: >
      Reference dimension table for all bike share stations across NYC and London.
      One row per (location, station_id). NYC stations include latitude/longitude
      coordinates; London stations have NULL for coordinates. Provides metadata
      for station-level analysis and future proximity-based features.
    columns:
      - name: location
        description: City identifier (nyc or london)
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: station_id
        description: Unique station identifier within each city
        tests:
          - not_null
      - name: station_name
        description: Most recent station name
        tests:
          - not_null
      - name: latitude
        description: Station latitude (NYC only, NULL for London)
      - name: longitude
        description: Station longitude (NYC only, NULL for London)
      - name: total_rides
        description: Total number of departures from this station across all time
        tests:
          - not_null
      - name: first_ride_date
        description: Date of the earliest recorded ride departing from this station
        tests:
          - not_null
      - name: last_ride_date
        description: Date of the most recent recorded ride departing from this station
        tests:
          - not_null

  - name: mart_station_weather_performance
    description: >
      Station-level weather impact analysis. Measures how each station's ridership
      and trip duration change under different weather conditions compared to clear
      weather. Grain is (location, station_id, hour_of_day, weather_condition).
      Only includes station-hour-condition combinations with at least 100 rides
      for statistical significance.
    columns:
      - name: location
        description: City identifier (nyc or london)
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: station_id
        description: Station identifier
        tests:
          - not_null
      - name: hour_of_day
        description: Hour of day (0-23)
        tests:
          - not_null
      - name: weather_condition
        description: "Weather condition category (e.g., clear, rain, snow, cloudy, fog)"
        tests:
          - not_null
      - name: total_rides
        description: Total rides departing from this station in this hour under this weather condition
        tests:
          - not_null
      - name: avg_rides_per_day
        description: Average rides per day for this station-hour-condition (normalized by days observed)
      - name: avg_duration_minutes
        description: Average ride duration in minutes
      - name: days_observed
        description: Number of distinct days this weather condition was observed in this hour
        tests:
          - not_null
      - name: pct_change_vs_clear
        description: >
          Percentage change in avg_rides_per_day compared to clear weather for the same station
          and hour. Negative values indicate fewer rides (e.g., -40 means 40% fewer rides).
          NULL when no clear-weather baseline exists.
```

Additionally, add a uniqueness test for the composite key:

```yaml
    tests:
      - unique:
          column_name: "location || '|' || station_id"
```

for `mart_station_directory`, and:

```yaml
    tests:
      - unique:
          column_name: "location || '|' || station_id || '|' || hour_of_day || '|' || weather_condition"
```

for `mart_station_weather_performance`. (DuckDB + dbt supports composite uniqueness tests via concatenated expressions.)

### Step 4: Register new marts in the export pipeline

Two files need to be updated to include the new mart Parquet files:

**File**: `/Users/chris/Projects/city-cycles/db_duckdb/operations.py`

Add to the `MART_TABLES` list (currently at line 449):
```python
MART_TABLES = [
    'mart_daily_metrics',
    'mart_hourly_patterns',
    'mart_nyc_member_analysis',
    'mart_station_growth',
    'mart_daily_metrics_long',
    'mart_station_directory',              # NEW
    'mart_station_weather_performance',     # NEW
]
```

**File**: `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py`

Add to the `MARTS` list (currently at line 13):
```python
MARTS = [
    "mart_daily_metrics.parquet",
    "mart_hourly_patterns.parquet",
    "mart_nyc_member_analysis.parquet",
    "mart_station_growth.parquet",
    "mart_daily_metrics_long.parquet",
    "mart_station_directory.parquet",              # NEW
    "mart_station_weather_performance.parquet",     # NEW
]
```

### Step 5: Dashboard -- Station Weather Performance Component

**File to modify**: `/Users/chris/Projects/city-cycles/dashboard/app.py`

Add a new section after the existing "Station Growth" section (around line 439) within the `applied_page in ["NYC", "London"]` branch. The component should be placed after station growth but before the end of the city-specific section.

The dashboard additions consist of three sub-components:

#### 5a. Weather Resilience Ranking Table

Show the top stations that maintain the highest ridership during bad weather. This uses `pct_change_vs_clear` -- stations where this value is closest to 0 (or positive) during rain are the most resilient.

```python
st.subheader("Station Weather Performance")

# Weather condition filter
weather_conditions_query = f"""
    SELECT DISTINCT weather_condition
    FROM '{os.path.join(DATA_DIR, 'mart_station_weather_performance.parquet')}'
    WHERE location = $1
    ORDER BY weather_condition
"""
try:
    conditions_df = run_query_params(weather_conditions_query, [applied_page.lower()])
    available_conditions = conditions_df['weather_condition'].tolist()
except Exception:
    available_conditions = ['rain', 'snow', 'cloudy', 'fog']

selected_condition = st.selectbox(
    "Weather Condition:",
    [c for c in available_conditions if c != 'clear'],
    key=f"weather_condition_{applied_page}"
)

# Hour filter
selected_hour = st.slider(
    "Hour of Day:",
    min_value=0, max_value=23, value=(7, 19),
    key=f"weather_hour_{applied_page}"
)

resilience_query = f"""
    SELECT
        s.station_id,
        d.station_name,
        d.latitude,
        d.longitude,
        round(avg(s.pct_change_vs_clear), 1) as avg_pct_change,
        sum(s.total_rides) as total_rides_in_condition,
        round(avg(s.avg_duration_minutes), 1) as avg_duration
    FROM '{os.path.join(DATA_DIR, 'mart_station_weather_performance.parquet')}' s
    JOIN '{os.path.join(DATA_DIR, 'mart_station_directory.parquet')}' d
        ON s.location = d.location AND s.station_id = d.station_id
    WHERE s.location = $1
      AND s.weather_condition = $2
      AND s.hour_of_day BETWEEN $3 AND $4
      AND s.pct_change_vs_clear IS NOT NULL
    GROUP BY s.station_id, d.station_name, d.latitude, d.longitude
    ORDER BY avg_pct_change DESC
    LIMIT 20
"""
try:
    resilience_df = run_query_params(resilience_query, [
        applied_page.lower(), selected_condition,
        selected_hour[0], selected_hour[1]
    ])

    if not resilience_df.empty:
        st.markdown(f"**Top 20 Most Weather-Resilient Stations ({selected_condition})**")
        st.dataframe(
            resilience_df[['station_name', 'avg_pct_change',
                           'total_rides_in_condition', 'avg_duration']],
            use_container_width=True,
            column_config={
                'station_name': 'Station',
                'avg_pct_change': st.column_config.NumberColumn(
                    '% Change vs Clear', format='%.1f%%'
                ),
                'total_rides_in_condition': st.column_config.NumberColumn(
                    'Total Rides', format='%d'
                ),
                'avg_duration': st.column_config.NumberColumn(
                    'Avg Duration (min)', format='%.1f'
                ),
            }
        )
    else:
        st.info("No station weather data available for the selected filters.")
except Exception as e:
    st.error(f"Error loading station weather data: {e}")
```

#### 5b. NYC Map Visualization

Only rendered when `applied_page == "NYC"` since only NYC has coordinates.

```python
if applied_page == "NYC" and not resilience_df.empty:
    map_df = resilience_df.dropna(subset=['latitude', 'longitude']).copy()
    if not map_df.empty:
        st.subheader(f"Station Weather Impact Map ({selected_condition})")

        # Color scale: green (resilient, pct_change near 0) to red (big drop)
        fig_map = px.scatter_mapbox(
            map_df,
            lat='latitude',
            lon='longitude',
            color='avg_pct_change',
            size='total_rides_in_condition',
            hover_name='station_name',
            hover_data={
                'avg_pct_change': ':.1f',
                'total_rides_in_condition': ':,',
                'avg_duration': ':.1f'
            },
            color_continuous_scale='RdYlGn',  # Red (bad) -> Yellow -> Green (resilient)
            range_color=[map_df['avg_pct_change'].min(), 0],
            mapbox_style='open-street-map',
            zoom=11,
            center={'lat': 40.7128, 'lon': -74.0060},
            title=f'NYC Station Impact During {selected_condition.title()}'
        )
        fig_map.update_layout(height=600)
        st.plotly_chart(fig_map, use_container_width=True)
```

**Design decisions for the map**:
- Uses `px.scatter_mapbox` with `open-street-map` style (no Mapbox token required).
- Color scale is `RdYlGn` (red = heavy ridership drop, green = resilient), capped at 0 on the high end so all green values truly indicate above-baseline performance.
- Bubble size encodes `total_rides_in_condition` (importance/volume of the station).
- NYC center coordinates hardcoded to `(40.7128, -74.0060)`.

#### 5c. Comparison Page Integration

For the Comparison page, add a brief section showing the weather resilience comparison across cities. This goes after the existing "Comparative Station Growth" section (around line 532):

```python
st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Weather Impact on Ridership</h2>",
            unsafe_allow_html=True)

city_weather_query = f"""
    SELECT
        location,
        weather_condition,
        round(avg(pct_change_vs_clear), 1) as avg_pct_change,
        sum(total_rides) as total_rides
    FROM '{os.path.join(DATA_DIR, 'mart_station_weather_performance.parquet')}'
    WHERE pct_change_vs_clear IS NOT NULL
      AND weather_condition != 'clear'
    GROUP BY location, weather_condition
    ORDER BY location, weather_condition
"""
try:
    weather_comparison_df = run_query_params(city_weather_query, [])
    if not weather_comparison_df.empty:
        fig_weather = px.bar(
            weather_comparison_df,
            x='weather_condition', y='avg_pct_change', color='location',
            barmode='group',
            title='Average Ridership Change by Weather Condition',
            labels={'avg_pct_change': '% Change vs Clear Weather',
                    'weather_condition': 'Weather Condition'}
        )
        st.plotly_chart(fig_weather, use_container_width=True)
except Exception as e:
    st.error(f"Error loading weather comparison: {e}")
```

### Step 6: Tests

#### 6a. dbt Tests (in schema.yml -- already covered in Step 3)

The schema.yml definitions include:
- `not_null` tests on all key columns
- `accepted_values` tests on `location`
- Composite uniqueness tests on grain columns

#### 6b. Python Tests for Dashboard Query Patterns

**File**: `/Users/chris/Projects/city-cycles/tests/test_dashboard.py`

Add a new test class to the existing file, following the established pattern of testing query logic without importing the Streamlit app:

```python
class TestStationWeatherQueryLogic:
    """
    Test the station weather performance query patterns used by the dashboard.

    Replicates the query execution approach using a local DuckDB connection
    to validate weather resilience ranking and map data queries.
    """

    @pytest.fixture
    def weather_conn(self):
        """Create an in-memory DuckDB connection with test station weather data."""
        conn = duckdb.connect(":memory:")
        # Create mart_station_directory test data
        conn.execute("""
            CREATE TABLE station_directory AS
            SELECT * FROM (VALUES
                ('nyc', 'S001', 'Central Park', 40.7829, -73.9654, 50000, '2019-01-01'::DATE, '2024-12-31'::DATE),
                ('nyc', 'S002', 'Times Square', 40.7580, -73.9855, 80000, '2019-01-01'::DATE, '2024-12-31'::DATE),
                ('london', 'L001', 'Hyde Park', NULL, NULL, 30000, '2019-01-01'::DATE, '2024-12-31'::DATE)
            ) AS t(location, station_id, station_name, latitude, longitude,
                   total_rides, first_ride_date, last_ride_date)
        """)
        # Create mart_station_weather_performance test data
        conn.execute("""
            CREATE TABLE station_weather AS
            SELECT * FROM (VALUES
                ('nyc', 'S001', 8, 'clear', 5000, 25.0, 12.5, 200, NULL),
                ('nyc', 'S001', 8, 'rain',  3000, 15.0, 14.0, 200, -40.0),
                ('nyc', 'S002', 8, 'clear', 8000, 40.0, 10.0, 200, NULL),
                ('nyc', 'S002', 8, 'rain',  7200, 36.0, 11.0, 200, -10.0),
                ('london', 'L001', 8, 'clear', 3000, 15.0, 20.0, 200, NULL),
                ('london', 'L001', 8, 'rain',  2100, 10.5, 22.0, 200, -30.0)
            ) AS t(location, station_id, hour_of_day, weather_condition,
                   total_rides, avg_rides_per_day, avg_duration_minutes,
                   days_observed, pct_change_vs_clear)
        """)
        yield conn
        conn.close()

    def test_resilience_ranking_returns_ordered_by_pct_change(self, weather_conn):
        """Weather resilience query should return stations ordered by pct_change DESC."""
        result = weather_conn.execute("""
            SELECT s.station_id, d.station_name,
                   round(avg(s.pct_change_vs_clear), 1) as avg_pct_change
            FROM station_weather s
            JOIN station_directory d ON s.location = d.location AND s.station_id = d.station_id
            WHERE s.location = 'nyc'
              AND s.weather_condition = 'rain'
              AND s.pct_change_vs_clear IS NOT NULL
            GROUP BY s.station_id, d.station_name
            ORDER BY avg_pct_change DESC
        """).fetchdf()
        assert len(result) == 2
        # S002 is more resilient (-10%) than S001 (-40%)
        assert result['station_id'].iloc[0] == 'S002'
        assert result['avg_pct_change'].iloc[0] == -10.0
        assert result['avg_pct_change'].iloc[1] == -40.0

    def test_map_data_has_coordinates_for_nyc(self, weather_conn):
        """Map query should return non-null coordinates for NYC stations."""
        result = weather_conn.execute("""
            SELECT d.latitude, d.longitude
            FROM station_directory d
            WHERE d.location = 'nyc' AND d.latitude IS NOT NULL
        """).fetchdf()
        assert len(result) == 2
        assert all(result['latitude'].notna())
        assert all(result['longitude'].notna())

    def test_london_has_null_coordinates(self, weather_conn):
        """London stations should have NULL coordinates."""
        result = weather_conn.execute("""
            SELECT latitude, longitude
            FROM station_directory
            WHERE location = 'london'
        """).fetchdf()
        assert result['latitude'].isna().all()
        assert result['longitude'].isna().all()

    def test_weather_comparison_across_cities(self, weather_conn):
        """Cross-city weather comparison should return data for both cities."""
        result = weather_conn.execute("""
            SELECT location, weather_condition,
                   round(avg(pct_change_vs_clear), 1) as avg_pct_change
            FROM station_weather
            WHERE pct_change_vs_clear IS NOT NULL
              AND weather_condition != 'clear'
            GROUP BY location, weather_condition
        """).fetchdf()
        assert len(result) == 2  # NYC rain, London rain
        locations = result['location'].tolist()
        assert 'nyc' in locations
        assert 'london' in locations
```

#### 6c. Streamlit Data Manager Tests

**File**: `/Users/chris/Projects/city-cycles/tests/test_streamlit_data_manager.py`

Update the `test_marts_list_is_complete` test (line 95) to expect 7 marts instead of 5, and add the two new entries to the `expected` list:

```python
def test_marts_list_is_complete(self):
    """The MARTS list should contain all 7 expected mart Parquet files."""
    from streamlit_data_manager.parquet_file_manager import MARTS

    expected = [
        "mart_daily_metrics.parquet",
        "mart_hourly_patterns.parquet",
        "mart_nyc_member_analysis.parquet",
        "mart_station_growth.parquet",
        "mart_daily_metrics_long.parquet",
        "mart_station_directory.parquet",
        "mart_station_weather_performance.parquet",
    ]

    assert len(MARTS) == 7
    for mart in expected:
        assert mart in MARTS, f"Missing expected mart: {mart}"
```

---

## 3. Dependency Analysis and Sequencing

### Internal Dependencies (within this phase)

1. `mart_station_directory.sql` -- depends only on `unified_rides` (already exists). **No weather dependency. Can be built immediately.**
2. `mart_station_weather_performance.sql` -- depends on `unified_rides` AND `int_weather_conditions` (from Phase 01/02). **Blocked until Phase 01/02 weather data models exist.**
3. Dashboard changes -- depend on both mart Parquet files being exported.
4. Tests -- can be written in parallel with model development.

### External Dependencies (on other phases)

- **Phase 01** (Weather Data Ingestion): Must provide weather observation data accessible as a dbt model. The critical contract is a model with columns: `location`, `date`, `hour_of_day`, `weather_condition`.
- **Phase 02** (City-Level Weather-Ride Correlation): Must define the `int_weather_conditions` intermediate model or equivalent that this phase joins against. If Phase 02 names it differently, the `{{ ref() }}` in `mart_station_weather_performance.sql` must be updated accordingly.

### Recommended Build Order

1. Create `mart_station_directory.sql` and its schema.yml entry (no weather dependency)
2. Register `mart_station_directory` in export pipeline
3. Create `mart_station_weather_performance.sql` and its schema.yml entry (after Phase 01/02)
4. Register `mart_station_weather_performance` in export pipeline
5. Add dashboard section for station directory / basic station info
6. Add dashboard weather resilience table + NYC map
7. Add comparison page weather impact section
8. Write all tests
9. Run `dbt run`, `dbt test`, `python -m pytest tests/ -v`

---

## 4. Potential Challenges and Mitigations

### Challenge 1: Weather model naming from Phase 01/02
The exact ref name for weather data is unknown. **Mitigation**: Use `int_weather_conditions` as a placeholder. When Phase 01/02 is implemented, update the single `{{ ref() }}` call.

### Challenge 2: `weather_condition` value taxonomy
Different weather APIs use different condition categories. **Mitigation**: The `pct_change_vs_clear` calculation uses `where weather_condition = 'clear'` as baseline. If Phase 01/02 uses a different name for clear weather (e.g., 'sunny', 'fair'), this predicate must be adjusted. Document this contract.

### Challenge 3: DuckDB `median()` for coordinates
DuckDB supports `median()` natively, but if this is a concern, an alternative is `approx_quantile(start_latitude, 0.5)`. **Mitigation**: Use `median()` which is simpler and well-supported.

### Challenge 4: Large mart size
`mart_station_weather_performance` could be large if there are many stations * 24 hours * N weather conditions. With ~2000 NYC stations and ~800 London stations, 24 hours, and ~5 conditions, the theoretical max is ~336,000 rows before the 100-ride filter. After filtering, likely 50,000-100,000 rows. **Mitigation**: The 100-ride minimum threshold significantly reduces the output. The `materialized='table'` strategy is appropriate.

### Challenge 5: Mapbox token requirement
`px.scatter_mapbox` with `open-street-map` style does NOT require a Mapbox token. **Mitigation**: Already handled by using `mapbox_style='open-street-map'`.

### Challenge 6: Dashboard performance
The station weather queries join two Parquet files. **Mitigation**: Both files are small enough to be read entirely into memory. DuckDB handles Parquet joins efficiently. The `LIMIT 20` on the resilience query ensures the result set is small.

---

## 5. Foundation for Future "Near Me" Feature

The `mart_station_directory` with lat/lng coordinates enables future proximity calculations:

- **Haversine distance**: DuckDB supports custom SQL functions. A future phase could add a macro like `{{ haversine(lat1, lng1, lat2, lng2) }}` to compute distances.
- **Spatial indexing**: DuckDB has a `spatial` extension that supports ST_Point, ST_Distance, and spatial indexes for efficient nearest-neighbor queries.
- **Dashboard integration**: A future "near me" feature would take user coordinates, query `mart_station_directory` for the nearest N stations, then look up their `mart_station_weather_performance` data to show current-condition availability predictions.

---

### Critical Files for Implementation

- `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_station_growth.sql` - Pattern reference for how existing mart models query unified_rides and join seed/dimension data
- `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/unified/unified_rides.sql` - Core source table; must understand available columns (start_station_id, start_latitude, start_longitude, hour_of_day) to build station aggregations
- `/Users/chris/Projects/city-cycles/dashboard/app.py` - Primary file to modify for the dashboard station weather component; must integrate with existing page/filter architecture (session state, parameterized queries, Plotly patterns)
- `/Users/chris/Projects/city-cycles/db_duckdb/operations.py` - Contains the MART_TABLES export list (line 449) that must be updated to include the two new marts in the S3 export pipeline
- `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py` - Contains the MARTS download list (line 13) that must be updated so the dashboard can access the new mart Parquet files locally