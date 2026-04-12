# City Cycles Data Model Analysis: "Similar Day" Analysis Support

**Analysis Date:** April 9, 2026  
**Focus:** Does the current data model support the user story "show me how rides looked on days like today (similar weather, time, city, season)"?

---

## Executive Summary

**YES, the data model FULLY SUPPORTS similar day analysis.** The system is architecturally well-designed for this use case:

1. **mart_similar_day_stats** — A purpose-built mart table specifically designed for "days like today" queries
2. **mart_weather_ride_correlation** — Provides the foundational hourly ride + weather data
3. **Weather dimensions** — Temperature bands, precipitation intensity, weather conditions all pre-computed
4. **Seasonal/temporal dimensions** — Month, day_type (weekday/weekend), hour_of_day all available
5. **Dashboard integration** — recommendation_engine.py already uses these marts to generate insights

The data pipeline flows from raw sources → staging (weather + rides) → hourly metrics → correlation table → similar day stats, all designed to enable this exact user story.

---

## Part 1: Mart Models Architecture

### Overview of All Marts

```
Data Flow:
  unified_rides (NYC + London combined)
    ↓
  mart_hourly_rides (hourly aggregation by location/date/hour)
    ↓
  mart_weather_ride_correlation (joined with stg_weather_hourly)
    ↓
  mart_similar_day_stats ← Primary table for "similar day" queries
  mart_weather_impact_summary ← Secondary for weather-only analysis
  
Other marts:
  mart_daily_metrics (daily summary)
  mart_hourly_patterns_summary (time-of-day patterns)
  mart_station_* (station-level analysis)
```

### 1. mart_similar_day_stats (THE KEY TABLE)

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_similar_day_stats.sql`

**Purpose:** Pre-computed ride statistics grouped by weather similarity dimensions. Specifically designed to answer "On days like today, what does bike activity look like?"

**Granularity:** Two grain levels in one table:

| Grain | Dimensions | Rows per Location | Use Case |
|-------|-----------|------------------|----------|
| **daily** | location, month_num, day_type, temperature_band, precipitation_intensity | ~125 rows | Overall daily patterns by season/weather |
| **hourly** | daily dimensions + hour_of_day (0-23) | ~3,000 rows | Hourly patterns within similar days |

**Columns Available:**

```
Core Dimensions:
  - grain (STRING): 'daily' or 'hourly' to distinguish row type
  - location (STRING): 'nyc' or 'london'
  - month_num (INTEGER): 1-12 for seasonal matching
  - day_type (STRING): 'weekday' or 'weekend'
  - temperature_band (STRING): 'freezing', 'cold', 'mild', 'warm', 'hot'
  - precipitation_intensity (STRING): 'none', 'light', 'moderate', 'heavy', 'extreme'
  - hour_of_day (INTEGER): 0-23 (NULL for daily grain)

Ride Metrics:
  - sample_days (INTEGER): Number of historical days matching this dimension combo
  - avg_daily_rides (FLOAT): Average total rides (daily) or per-hour rides (hourly)
  - avg_duration_minutes (FLOAT): Average ride length
  - avg_member_rides (FLOAT): Member ride average
  - avg_casual_rides (FLOAT): Casual rider average

Comparative Metrics (daily grain only):
  - pct_change_vs_overall (FLOAT): % difference from location's overall avg (e.g., -23 = 23% below avg)
  - duration_pct_change_vs_overall (FLOAT): Duration % vs location overall
  - peak_hour_start (INTEGER): Hour with highest avg rides (0-23)
  - peak_hour_end (INTEGER): peak_hour_start + 2, capped at 23
```

**Query Pattern for "Similar Day" Analysis:**

```sql
SELECT * FROM mart_similar_day_stats
WHERE grain = 'daily'
  AND location = 'nyc'
  AND month_num = 4                           -- April (current month)
  AND day_type = 'weekday'                    -- Today is a weekday
  AND temperature_band = 'mild'               -- Today is 10-20°C
  AND precipitation_intensity = 'light'       -- Light rain today
```

Result: A single row with aggregated stats for all historical days matching this exact condition combo. If 47 sample_days match, those 47 days' ride patterns are averaged into this row.

**Strengths:**
- Pre-aggregated (fast queries, no on-the-fly calculations)
- Matches today's conditions directly without custom binning
- Includes sample_days count (confidence metric)
- Peak hour inference built-in for intra-day recommendations
- Both daily and hourly grain in one table (flexible analysis)

**Limitations:**
- Fixed dimension granularity (can't ask "mild OR warm" without a UNION)
- Month is coarse for seasonal matching (April 1 ≠ April 30 meteorologically)
- No inter-city comparisons (separate rows for nyc/london)

---

### 2. mart_weather_ride_correlation

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_weather_ride_correlation.sql`

**Purpose:** Hourly ride metrics joined with weather data (single table, no aggregation). Upstream to mart_similar_day_stats.

**Granularity:** One row per (location, date, hour_of_day) — not aggregated.

**Columns (subset relevant to similar day matching):**

```
Ride Metrics:
  - ride_count (INTEGER)
  - avg_duration_seconds (FLOAT)
  - member_rides (INTEGER)
  - casual_rides (INTEGER)

Weather Metrics (raw):
  - temperature_celsius (FLOAT)
  - apparent_temperature_celsius (FLOAT)
  - relative_humidity_pct (FLOAT)
  - precipitation_mm (FLOAT)
  - rain_mm, snowfall_cm, snow_depth_m (FLOAT)
  - weather_code (INTEGER): WMO code (0-99)
  - cloud_cover_pct (FLOAT)
  - wind_speed_kmh, wind_gusts_kmh (FLOAT)

Weather Metrics (pre-categorized):
  - weather_condition (STRING): 'clear', 'rain', 'snow', 'fog', 'thunderstorm', etc.
  - is_precipitation (BOOLEAN)
  - precipitation_intensity (STRING): 'none', 'light', 'moderate', 'heavy', 'extreme'
  - temperature_band (STRING): 'freezing', 'cold', 'mild', 'warm', 'hot'
  - wind_category (STRING): 'calm', 'light', 'moderate', 'strong', 'severe'
```

**Use:** This is the raw material for mart_similar_day_stats. All the pre-categorized weather fields flow from here into the aggregated similar day stats.

**Key Insight:** Weather dimensions are computed in stg_weather_hourly and passed through unchanged to this mart, ensuring consistency.

---

### 3. mart_weather_impact_summary

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_weather_impact_summary.sql`

**Purpose:** Weather-only impact on rides (no seasonal/temporal grouping). Shows "how much does rain reduce rides at 9am?"

**Granularity:** One row per (location, hour_of_day, dimension_type, dimension_value/is_precipitation/temperature_band)

**Columns:**

```
Dimensions:
  - location (STRING)
  - hour_of_day (INTEGER)
  - dimension_type (STRING): 'weather_condition' OR 'precip_temp'
  - dimension_value (STRING): weather condition name (e.g., 'rain')
  - is_precipitation (BOOLEAN): for precip_temp grain
  - temperature_band (STRING): for precip_temp grain

Metrics:
  - observation_count (INTEGER)
  - avg_rides, avg_duration_seconds
  - avg_member_rides, avg_casual_rides
  - baseline_avg_rides (clear weather baseline for same hour)
  - baseline_avg_duration_seconds
  - pct_change_rides_vs_clear (FLOAT): e.g., -34.0 = 34% fewer rides
  - pct_change_duration_vs_clear (FLOAT)
```

**Use:** Good for answering "At 9am, what's the rain impact?" but NOT for "days like today" because it ignores month/day_type/seasonal context. Secondary to mart_similar_day_stats.

---

### 4. mart_daily_metrics

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_daily_metrics.sql`

**Purpose:** Simple daily summary (no weather joins). Good for trend analysis, not weather matching.

**Granularity:** One row per (location, date)

**Columns:** location, date, year, day_type, total_rides, avg_duration_minutes, member_rides, casual_rides, total_minutes_biked, population, rides_per_1000

**Use:** Time-series baseline. Not directly used for similar day matching (uses mart_similar_day_stats instead).

---

### 5. mart_hourly_rides

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_hourly_rides.sql`

**Purpose:** Hourly ride aggregation without weather (intermediate table).

**Granularity:** One row per (location, date, hour_of_day)

**Columns:** location, date, hour_of_day, ride_count, avg_duration_seconds, member_rides, casual_rides

**Use:** Upstream to mart_weather_ride_correlation. Not used directly for similar day queries.

---

### 6. mart_hourly_patterns_summary

**File:** `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_hourly_patterns_summary.sql`

**Purpose:** Time-of-day patterns (aggregated across all dates). "What time is busiest?"

**Granularity:** One row per (location, hour_of_day)

**Columns:** location, hour_of_day, ride_count (total across all dates)

**Use:** Dashboard time-of-day charts. Not for similar day matching.

---

## Part 2: Staging & Intermediate Models (Data Flow)

### Weather Data Pipeline

**Source → stg_weather_hourly → mart_weather_ride_correlation → mart_similar_day_stats**

**stg_weather_hourly** (`/Users/chris/Projects/city-cycles/dbt_city_cycles/models/staging/stg_weather_hourly.sql`)

Raw weather data from `raw_weather_hourly` is transformed here:

```sql
Key Transformations:
  - Deduplication by (city, hour) in case parquet overlaps
  - Type casting (temps as DOUBLE, codes as INTEGER)
  
  Raw Columns → Staging Columns:
    timestamp → timestamp, date, hour, month, year, day_of_week, hour_of_day
    temperature_2m → temperature_celsius
    apparent_temperature → apparent_temperature_celsius
    relative_humidity_2m → relative_humidity_pct
    precipitation, rain, snowfall, snow_depth → precipitation_mm, rain_mm, snowfall_cm, snow_depth_m
    weather_code → weather_code + derived weather_condition
    cloud_cover → cloud_cover_pct
    wind_speed_10m, wind_gusts_10m → wind_speed_kmh, wind_gusts_kmh
  
  Derived Categorization Columns (CRITICAL for similar day matching):
    weather_condition = CASE WHEN weather_code IN (0) THEN 'clear'
                            WHEN weather_code IN (1,2,3) THEN 'partly_cloudy'
                            WHEN weather_code IN (61,63,65,...) THEN 'rain'
                            ... (12+ weather categories mapped)
    
    is_precipitation = CASE WHEN precipitation > 0 OR rain > 0 OR snowfall > 0 THEN true ELSE false
    
    precipitation_intensity = CASE WHEN precipitation = 0 THEN 'none'
                                   WHEN precipitation < 2.5 THEN 'light'
                                   WHEN precipitation < 7.5 THEN 'moderate'
                                   WHEN precipitation < 50 THEN 'heavy'
                                   ELSE 'extreme'
    
    temperature_band = CASE WHEN temperature_2m < 0 THEN 'freezing'
                            WHEN temperature_2m < 10 THEN 'cold'
                            WHEN temperature_2m < 20 THEN 'mild'
                            WHEN temperature_2m < 30 THEN 'warm'
                            ELSE 'hot'
    
    wind_category = CASE WHEN wind_speed_10m < 12 THEN 'calm'
                         WHEN wind_speed_10m < 30 THEN 'light'
                         ... (5 wind categories)
    
    day_type = day_type macro (weekday vs weekend)
```

**Weather Categories Defined:**

| Category | WMO Codes | Examples |
|----------|-----------|----------|
| clear | 0 | Clear sky |
| partly_cloudy | 1,2,3 | Mainly clear, partly cloudy, overcast |
| fog | 45,48 | Fog, rime fog |
| drizzle | 51,53,55,56,57 | Light to dense drizzle |
| rain | 61,63,65,66,67,80,81,82 | All rain intensities |
| snow | 71,73,75,77,85,86 | Snow & snow showers |
| thunderstorm | 95,96,99 | Thunderstorms with/without hail |

**Ride Data Pipeline**

**NYC/London sources → stg_nyc_modern + stg_nyc_legacy + stg_london_modern + stg_london_legacy → int_nyc_rides + int_london_rides → unified_rides → mart_hourly_rides → mart_weather_ride_correlation → mart_similar_day_stats**

Key fields in staging:

```
Core: ride_id, start_time, stop_time, duration_seconds, user_type, location
Stations: start_station_id, start_station_name, end_station_id, end_station_name
Coordinates: start_lat, start_lng, end_lat, end_lng (NYC only)
Derived: date, month, year, day_type, day_of_week, hour_of_day
```

All ride staging models extract hour_of_day and day_type, enabling grouping for similar day analysis.

---

## Part 3: Dashboard Integration (How It's Used)

### recommendation_engine.py

**File:** `/Users/chris/Projects/city-cycles/dashboard/recommendation_engine.py`

This is the consumer of the marts. It:

1. **Fetches current weather** via weather_service.py (Open-Meteo API)
2. **Classifies conditions** using the same bands as the data model:
   - Temperature: freezing, cold, cool, mild, warm, hot, very_hot
   - Precipitation: none, light, moderate, heavy
   - Wind: calm, light, moderate, strong, very_strong
   - Weather category: clear, partly_cloudy, cloudy, fog, rain, heavy_rain, snow, thunderstorm

3. **Maps to mart dimensions** (note: some mapping needed for fine-grained classes):
   ```python
   # Example mappings from recommendation_engine.py:
   "cool" (10-15°C) → "mild" (10-20°C in mart)
   "hot" (25-30°C) → "warm" (20-30°C in mart)
   "cloudy" → "partly_cloudy" (in mart)
   ```

4. **Queries marts** using DuckDB:
   ```python
   _SIMILAR_DAY_PARQUET = "mart_similar_day_stats.parquet"
   _WEATHER_IMPACT_PARQUET = "mart_weather_impact_summary.parquet"
   
   # Queries (pseudocode):
   SELECT * FROM mart_similar_day_stats
   WHERE grain = 'daily'
     AND location = ?
     AND month_num = MONTH(TODAY())
     AND day_type = ? (weekday/weekend)
     AND temperature_band = ?
     AND precipitation_intensity = ?
   ```

5. **Generates insights** like:
   - "Rides on mild days with light rain: avg 847 rides (15% below typical)"
   - "Peak hour today: 17-19 (5-7pm)"
   - "Biking score: 62/100"

### weather_service.py

**File:** `/Users/chris/Projects/city-cycles/dashboard/weather_service.py`

Fetches real-time weather:

```python
CURRENT_PARAMS = (
    "temperature_2m, relative_humidity_2m, precipitation, rain, snowfall, "
    "weather_code, cloud_cover, wind_speed_10m, wind_gusts_10m, apparent_temperature"
)

HOURLY_PARAMS = (
    "temperature_2m, relative_humidity_2m, precipitation_probability, "
    "precipitation, weather_code, wind_speed_10m, apparent_temperature"
)
```

Returns `CurrentWeather` dataclass with fields that map to data model dimensions.

---

## Part 4: Pydantic Data Models

### weather.py

**File:** `/Users/chris/Projects/city-cycles/data_models/weather.py`

```python
@dataclass
class HourlyWeatherRecord:
    timestamp, city, temperature_2m, relative_humidity_2m,
    apparent_temperature, precipitation, rain, snowfall, snow_depth,
    weather_code, cloud_cover, wind_speed_10m, wind_gusts_10m, source_file
```

Matches the staging model's raw input columns. Ensures type consistency.

### nyc_bike.py & london_bike.py

**Files:** `/Users/chris/Projects/city-cycles/data_models/{nyc,london}_bike.py`

Define modern and legacy schemas for both cities, all converging to the same core fields: ride_id, duration, user_type, start/end station, timestamps.

---

## Part 5: What "Similar Day" Dimensions Are Available?

### Temporal Dimensions

| Dimension | Values | Granularity | Notes |
|-----------|--------|------------|-------|
| **month_num** | 1-12 | Monthly | Proxy for season (April 1 = April 30) |
| **day_type** | weekday, weekend | Daily | Behavioral difference (commute vs leisure) |
| **hour_of_day** | 0-23 | Hourly | Intra-day peak patterns |
| **year** | Present in marts | Annual | Not used for similar day matching in mart_similar_day_stats |

### Weather Dimensions

| Dimension | Values | Breakdown | Matching Granularity |
|-----------|--------|-----------|----------------------|
| **temperature_band** | freezing (-∞,0), cold [0,10), mild [10,20), warm [20,30), hot [30,∞) | 5 bands | ~5°C per band (coarse) |
| **precipitation_intensity** | none, light [0,2.5), moderate [2.5,7.5), heavy [7.5,50), extreme [50,∞) | 5 levels | 0-2.5mm = light, etc. |
| **weather_condition** (in mart_weather_impact_summary) | clear, partly_cloudy, rain, snow, fog, thunderstorm, drizzle, freezing_rain, snow_grains, rain_showers, snow_showers | 11+ categories | WMO code-based |
| **is_precipitation** | boolean | 2 values | Simple wet/dry flag |
| **wind_category** | calm, light, moderate, strong, severe | 5 categories | km/h based |

### Location Dimension

| Dimension | Values |
|-----------|--------|
| **location** | 'nyc', 'london' |

---

## Part 6: Analysis of Gaps & Limitations

### What IS Well-Supported

✅ **Core user story:** "Show me how rides looked on days like today"

- Date (via month_num)
- Season/weather pattern (via temperature_band, precipitation_intensity, month_num)
- Time of day (via hour_of_day, day_type)
- City (via location)

✅ **Fast queries:** Pre-aggregated marts (no on-the-fly calculations)

✅ **Confidence metrics:** sample_days shows how many historical days match

✅ **Comparative insights:** pct_change_vs_overall, peak_hour_start/end

✅ **Hourly granularity:** Both daily and hourly grain available

---

### What is MISSING or WEAK

#### 1. Fine-Grained Seasonal Matching (MEDIUM GAP)

**Problem:** month_num is coarse. April 1 (spring) is grouped with April 30 (late spring).

**Example:** User on April 9 (today) queries mart_similar_day_stats with month_num=4. Result includes Apr 1, 5, 15, 20, 25, 30. But Apr 1 weather/daylight is very different from Apr 30.

**Current Workaround:** None in data model. Dashboard could filter by date range AFTER querying mart, but marts are aggregated (lose row-level dates).

**Missing Column:** `day_of_year` (1-366) or `season` (spring, summer, fall, winter) would improve seasonal matching.

**Impact:** Low (month is still reasonable proxy, and weather band captures actual conditions)

---

#### 2. Location-Aware Comparisons (LOW GAP)

**Problem:** NYC and London are separate rows. Dashboard can't easily compare "NYC mild days vs London mild days" in a single query.

**Current State:** mart_similar_day_stats has location as a dimension. Queries filter by location='nyc' OR location='london', separate results.

**Missing Column:** N/A - this is an architectural choice, not a data modeling gap.

**Impact:** Low (most use cases are single-city)

---

#### 3. Confidence in Historical Sample Size (LOW GAP)

**Problem:** A condition combo might match only 2 historical days. The averages may not be reliable.

**Current:** sample_days column exists. Dashboard should communicate confidence.

**Missing:** No percentile/confidence interval columns. Just point estimates.

**Impact:** Low (sample_days is present; dashboard responsibility to use it)

---

#### 4. Real-Time Weather to Mart Dimension Mapping (MEDIUM GAP)

**Problem:** Current weather from API (e.g., 18.3°C) must be mapped to mart temperature_band ('mild'). The mapping is in recommendation_engine.py, not the data model itself.

**Current:** recommendation_engine.py does the mapping:
```python
def classify_temperature(temp_celsius: float) -> TemperatureBand:
    if temp_celsius < 10: return TemperatureBand.COLD
    elif temp_celsius < 20: return TemperatureBand.MILD
    ...
```

**Issue:** If the classification logic in recommendation_engine.py differs from stg_weather_hourly, mismatches occur. No single source of truth in the data layer.

**Missing:** A reference table or view that documents the exact boundaries and mapping logic.

**Example Mismatch:**
- recommendation_engine.py: COOL = [10-15), MILD = [15-20), WARM = [20-25)
- stg_weather_hourly: MILD = [10-20), WARM = [20-30)
- Result: Engine's "cool" (12°C) is mapped to mart's "mild", slightly incorrect.

**Impact:** Medium (can cause off-by-one-band mismatches, but weather bands are broad enough to limit real-world impact)

---

#### 5. Weather Condition Hierarchy (LOW GAP)

**Problem:** mart_weather_ride_correlation includes detailed weather_condition (clear, rain, snow, etc.), but mart_similar_day_stats does NOT include weather_condition, only precipitation_intensity + temperature_band.

**Current:** Two separate marts:
- mart_weather_ride_correlation: weather_condition available
- mart_similar_day_stats: only precipitation_intensity (none/light/moderate/heavy/extreme)

**Missing:** A version of mart_similar_day_stats that includes weather_condition as a dimension would enable queries like "Days with rain (any intensity) vs light rain only".

**Impact:** Low (precipitation_intensity + temperature_band is usually sufficient; weather_condition is in other marts if needed)

---

#### 6. Multi-City Aggregation (LOW GAP)

**Problem:** User might ask "Compare NYC and London on similar days". Current marts have location=nyc OR location=london as separate rows.

**Current:** Query must UNION two separate result sets.

**Missing:** A "comparison" mart or view that makes cross-city queries natural.

**Impact:** Very Low (most use cases are single-city)

---

#### 7. Trend Adjustments (NO GAP, BY DESIGN)

**Problem:** Is bike usage declining YoY? Should this be factored into "similar day" expectations?

**Current:** mart_similar_day_stats aggregates across all years. 2021 + 2022 + 2023 + 2024 = same average.

**Missing:** A trend-adjusted version or year-over-year decomposition.

**Impact:** Low (this is a design choice; adding year dimension would be straightforward if needed)

---

## Part 7: Summary & Recommendations

### Does the Data Model Support "Similar Day" Analysis?

**YES, FULLY.**

The system is architecturally sound:

1. **mart_similar_day_stats** exists and is purpose-built for this exact use case
2. **All required dimensions** are available: city, season (month), time (hour/day_type), weather (temp_band, precip_intensity)
3. **Pre-aggregated metrics** are fast to query
4. **Dashboard integration** is already implemented (recommendation_engine.py)
5. **Confidence metrics** (sample_days) are included

### Identified Gaps (Minor)

| Gap | Severity | Recommendation |
|-----|----------|-----------------|
| Coarse seasonal matching (month_num only) | Low | Add day_of_year (1-366) to mart_similar_day_stats for finer seasonal matching |
| Weather dimension mapping mismatches | Medium | Create a reference table documenting exact classification boundaries; ensure recommendation_engine.py uses data model definitions |
| No weather_condition in mart_similar_day_stats | Low | Add weather_condition column (clear/rain/snow/etc.) to enable "rainy days" style queries |
| No trend/YoY adjustments | Low | Consider adding year-over-year % change columns if product needs account for usage trends |
| Limited confidence communication | Low | Add percentile/CI columns to sample_days (e.g., 95th percentile rides) |

### Recommended Next Steps (for Product Enhancement)

1. **Validate mapping consistency:** Ensure recommendation_engine.py's temperature/precip classification matches stg_weather_hourly exactly. Consider moving classification logic into staging.

2. **Add day_of_year to mart_similar_day_stats:** For finer seasonal matching (April 1 vs April 30). Cost: minimal (one more GROUP BY dimension).

3. **Document dimension boundaries:** Create a reference table or YAML file in dbt that documents exact thresholds for all categories (temperature_band, precipitation_intensity, etc.). Use this as the source of truth for both staging and recommendation engine.

4. **Consider weather_condition in mart_similar_day_stats:** Low-cardinality (11 values) addition that would enable "rain" style queries directly on the main mart.

5. **Add confidence intervals:** If needed, add percentile columns (p25_rides, p75_rides, etc.) to sample_days rows for uncertainty quantification.

---

## Appendix A: Column Inventory

### mart_similar_day_stats Columns (Complete List)

```
Dimension Columns:
  grain (STRING): 'daily' | 'hourly'
  location (STRING): 'nyc' | 'london'
  month_num (INTEGER): 1-12
  day_type (STRING): 'weekday' | 'weekend'
  temperature_band (STRING): 'freezing' | 'cold' | 'mild' | 'warm' | 'hot'
  precipitation_intensity (STRING): 'none' | 'light' | 'moderate' | 'heavy' | 'extreme'
  hour_of_day (INTEGER): 0-23 (NULL for daily grain)

Metric Columns:
  sample_days (INTEGER)
  avg_daily_rides (FLOAT)
  avg_duration_minutes (FLOAT)
  avg_member_rides (FLOAT)
  avg_casual_rides (FLOAT)

Comparative Columns (daily grain only):
  pct_change_vs_overall (FLOAT)
  duration_pct_change_vs_overall (FLOAT)
  peak_hour_start (INTEGER)
  peak_hour_end (INTEGER)
```

### stg_weather_hourly Columns (Complete List)

```
Core:
  weather_record_id (STRING)
  timestamp (TIMESTAMP)
  city (STRING): 'nyc' | 'london'
  date (DATE)
  hour (TIMESTAMP)
  month (INTEGER): 1-12
  year (INTEGER)
  day_type (STRING): 'weekday' | 'weekend'
  day_of_week (INTEGER): 0-6 (Monday-Sunday)
  hour_of_day (INTEGER): 0-23

Raw Weather:
  temperature_celsius (DOUBLE)
  apparent_temperature_celsius (DOUBLE)
  relative_humidity_pct (DOUBLE)
  precipitation_mm (DOUBLE)
  rain_mm (DOUBLE)
  snowfall_cm (DOUBLE)
  snow_depth_m (DOUBLE)
  weather_code (INTEGER): WMO code (0-99)
  cloud_cover_pct (DOUBLE)
  wind_speed_kmh (DOUBLE)
  wind_gusts_kmh (DOUBLE)

Categorized Weather:
  weather_condition (STRING): 'clear' | 'rain' | 'snow' | 'fog' | 'drizzle' | 'thunderstorm' | ...
  is_precipitation (BOOLEAN)
  precipitation_intensity (STRING): 'none' | 'light' | 'moderate' | 'heavy' | 'extreme'
  temperature_band (STRING): 'freezing' | 'cold' | 'mild' | 'warm' | 'hot'
  wind_category (STRING): 'calm' | 'light' | 'moderate' | 'strong' | 'severe'

Metadata:
  dbt_updated_at (TIMESTAMP)
```

---

## Appendix B: Sample Query for Dashboard

```sql
-- "On days like today (April 9, 2026, weekday, mild, light rain in NYC), how many rides?"

SELECT
    avg_daily_rides,
    avg_duration_minutes,
    sample_days,
    pct_change_vs_overall,
    peak_hour_start,
    peak_hour_end
FROM mart_similar_day_stats
WHERE grain = 'daily'
  AND location = 'nyc'
  AND month_num = 4
  AND day_type = 'weekday'
  AND temperature_band = 'mild'
  AND precipitation_intensity = 'light'

-- Result (example):
-- avg_daily_rides: 847.3
-- avg_duration_minutes: 15.2
-- sample_days: 47
-- pct_change_vs_overall: -15.0  (15% below NYC average)
-- peak_hour_start: 17
-- peak_hour_end: 19
```

---

**End of Analysis**
