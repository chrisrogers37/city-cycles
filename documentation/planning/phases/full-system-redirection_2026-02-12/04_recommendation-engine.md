# Phase 04: Recommendation Engine

**Status:** 🔧 IN PROGRESS
**Started:** 2026-02-12

## PR Title
feat: add weather-informed biking recommendation engine

## Risk Level: Low
## Estimated Effort: 1-2 days
## Dependencies: Phases 02 (Analytics Marts), 03 (Real-time Weather)
## Unlocks: Phase 05 (Atmospheric UI)

## Files Impact
| Action | File |
|--------|------|
| CREATE | dashboard/recommendation_engine.py |
| CREATE | tests/test_recommendation_engine.py |
| MODIFY | streamlit_data_manager/parquet_file_manager.py |
| MODIFY | dashboard/app.py |

## Context
This phase builds the recommendation engine that bridges real-time weather (Phase 03) with historical ride-weather correlations (Phase 02). Given current weather conditions + time, it looks up historical patterns and generates user-facing insights like "Rainy mornings see 34% fewer rides and 22% shorter durations." It includes a condition classifier, biking score (0-100), and template-based natural language insight generator.

---

# Phase 04: Recommendation Engine -- Implementation Plan

## 1. Architecture Overview

The recommendation engine sits as a standalone module at `dashboard/recommendation_engine.py`. It has zero Streamlit imports of its own -- it is a pure Python module that takes structured inputs and returns structured outputs. The dashboard (`app.py`) calls it and renders results. This separation keeps the engine unit-testable without Streamlit import-time side effects (the same problem the existing `test_dashboard.py` documents and works around).

**Data flow:**

```
Phase 03 weather service  -->  current conditions (WeatherConditions dataclass)
                                      |
                                      v
               recommendation_engine.py
                  |         |         |
          classify()  compute_score()  generate_insights()
                  |         |         |
                  v         v         v
             Parquet lookup from mart_weather_impact_summary.parquet
                                      |
                                      v
                          List[Recommendation]  +  BikingScore
                                      |
                                      v
                        dashboard/app.py renders results
```

## 2. Data Structures

All data structures use `dataclass` (not pydantic), matching the existing project convention in `data_models/base.py` and `data_models/nyc_bike.py`. The project uses dataclasses for its model layer.

### 2.1 Input: `WeatherConditions`

This dataclass represents what Phase 03's weather service returns. The recommendation engine accepts it as input.

```python
# dashboard/recommendation_engine.py

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum
import logging
import os
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WeatherConditions:
    """Current weather conditions from Phase 03 weather service."""
    temperature_celsius: float
    wind_speed_kmh: float
    precipitation_mm: float
    weather_code: int          # WMO weather code (0-99)
    location: str              # "nyc" or "london"
    hour: int                  # 0-23, current hour of day
    humidity_percent: Optional[float] = None
    feels_like_celsius: Optional[float] = None
```

### 2.2 Classification Enums

```python
class WeatherCategory(Enum):
    CLEAR = "clear"
    PARTLY_CLOUDY = "partly_cloudy"
    CLOUDY = "cloudy"
    FOG = "fog"
    DRIZZLE = "drizzle"
    RAIN = "rain"
    HEAVY_RAIN = "heavy_rain"
    SNOW = "snow"
    HEAVY_SNOW = "heavy_snow"
    THUNDERSTORM = "thunderstorm"


class TemperatureBand(Enum):
    FREEZING = "freezing"       # < 0
    COLD = "cold"               # 0-10
    COOL = "cool"               # 10-15
    MILD = "mild"               # 15-20
    WARM = "warm"               # 20-25
    HOT = "hot"                 # 25-30
    VERY_HOT = "very_hot"       # > 30


class WindCategory(Enum):
    CALM = "calm"               # < 10 km/h
    LIGHT = "light"             # 10-20
    MODERATE = "moderate"       # 20-30
    STRONG = "strong"           # 30-50
    VERY_STRONG = "very_strong" # > 50


class PrecipitationIntensity(Enum):
    NONE = "none"               # 0 mm
    LIGHT = "light"             # 0-2.5 mm
    MODERATE = "moderate"       # 2.5-7.5 mm
    HEAVY = "heavy"             # > 7.5 mm


class Severity(Enum):
    POSITIVE = "positive"       # green  -- great conditions
    NEUTRAL = "neutral"         # gray   -- normal conditions
    CAUTION = "caution"         # yellow -- somewhat adverse
    WARNING = "warning"         # red    -- poor conditions
```

### 2.3 Classified Conditions

```python
@dataclass
class ClassifiedConditions:
    """Weather conditions after classification into categories."""
    weather_category: WeatherCategory
    temperature_band: TemperatureBand
    wind_category: WindCategory
    precipitation_intensity: PrecipitationIntensity
    raw: WeatherConditions  # keep original for template rendering
```

### 2.4 Output: `Recommendation` and `BikingScore`

```python
@dataclass
class Recommendation:
    """A single recommendation/insight to display to the user."""
    text: str
    severity: Severity
    metric: str                 # e.g. "rides_impact_pct", "duration_impact_pct", "biking_score"
    value: Optional[float] = None  # the numeric value behind the insight


@dataclass
class BikingScore:
    """Composite biking score 0-100."""
    score: int                  # 0-100
    label: str                  # "Excellent", "Good", "Fair", "Poor"
    color: str                  # hex color for display: green/yellow/orange/red


@dataclass
class RecommendationResult:
    """Complete result returned by the engine."""
    biking_score: BikingScore
    recommendations: List[Recommendation]
    classified: ClassifiedConditions
```

## 3. Condition Classifier

### 3.1 `classify_weather_code` -- WMO Code to WeatherCategory

The WMO weather interpretation codes (used by Open-Meteo API) map as follows:

```python
# ---------------------------------------------------------------------------
# WMO weather code mapping
# https://open-meteo.com/en/docs (WMO Weather interpretation codes)
# ---------------------------------------------------------------------------

WMO_CODE_MAP: dict[int, WeatherCategory] = {
    0: WeatherCategory.CLEAR,
    1: WeatherCategory.CLEAR,
    2: WeatherCategory.PARTLY_CLOUDY,
    3: WeatherCategory.CLOUDY,
    45: WeatherCategory.FOG,
    48: WeatherCategory.FOG,
    51: WeatherCategory.DRIZZLE,
    53: WeatherCategory.DRIZZLE,
    55: WeatherCategory.DRIZZLE,
    56: WeatherCategory.DRIZZLE,      # freezing drizzle light
    57: WeatherCategory.DRIZZLE,      # freezing drizzle dense
    61: WeatherCategory.RAIN,
    63: WeatherCategory.RAIN,
    65: WeatherCategory.HEAVY_RAIN,
    66: WeatherCategory.RAIN,         # freezing rain light
    67: WeatherCategory.HEAVY_RAIN,   # freezing rain heavy
    71: WeatherCategory.SNOW,
    73: WeatherCategory.SNOW,
    75: WeatherCategory.HEAVY_SNOW,
    77: WeatherCategory.SNOW,         # snow grains
    80: WeatherCategory.RAIN,         # rain showers slight
    81: WeatherCategory.RAIN,         # rain showers moderate
    82: WeatherCategory.HEAVY_RAIN,   # rain showers violent
    85: WeatherCategory.SNOW,         # snow showers slight
    86: WeatherCategory.HEAVY_SNOW,   # snow showers heavy
    95: WeatherCategory.THUNDERSTORM,
    96: WeatherCategory.THUNDERSTORM, # thunderstorm with slight hail
    99: WeatherCategory.THUNDERSTORM, # thunderstorm with heavy hail
}


def classify_weather_code(code: int) -> WeatherCategory:
    """Map a WMO weather code to a broad WeatherCategory.

    Args:
        code: WMO weather interpretation code (0-99).

    Returns:
        WeatherCategory enum value. Defaults to CLOUDY for unknown codes.
    """
    return WMO_CODE_MAP.get(code, WeatherCategory.CLOUDY)
```

### 3.2 Scalar classifiers

```python
def classify_temperature(temp_celsius: float) -> TemperatureBand:
    """Classify temperature into a band.

    Args:
        temp_celsius: Temperature in degrees Celsius.

    Returns:
        TemperatureBand enum value.
    """
    if temp_celsius < 0:
        return TemperatureBand.FREEZING
    elif temp_celsius < 10:
        return TemperatureBand.COLD
    elif temp_celsius < 15:
        return TemperatureBand.COOL
    elif temp_celsius < 20:
        return TemperatureBand.MILD
    elif temp_celsius < 25:
        return TemperatureBand.WARM
    elif temp_celsius < 30:
        return TemperatureBand.HOT
    else:
        return TemperatureBand.VERY_HOT


def classify_wind(wind_speed_kmh: float) -> WindCategory:
    """Classify wind speed into a category.

    Args:
        wind_speed_kmh: Wind speed in kilometers per hour.

    Returns:
        WindCategory enum value.
    """
    if wind_speed_kmh < 10:
        return WindCategory.CALM
    elif wind_speed_kmh < 20:
        return WindCategory.LIGHT
    elif wind_speed_kmh < 30:
        return WindCategory.MODERATE
    elif wind_speed_kmh < 50:
        return WindCategory.STRONG
    else:
        return WindCategory.VERY_STRONG


def classify_precipitation(precip_mm: float) -> PrecipitationIntensity:
    """Classify precipitation intensity.

    Args:
        precip_mm: Precipitation in millimeters.

    Returns:
        PrecipitationIntensity enum value.
    """
    if precip_mm <= 0:
        return PrecipitationIntensity.NONE
    elif precip_mm <= 2.5:
        return PrecipitationIntensity.LIGHT
    elif precip_mm <= 7.5:
        return PrecipitationIntensity.MODERATE
    else:
        return PrecipitationIntensity.HEAVY


def classify_conditions(conditions: WeatherConditions) -> ClassifiedConditions:
    """Classify all weather conditions into categories.

    Args:
        conditions: Raw weather conditions from the weather service.

    Returns:
        ClassifiedConditions with all dimensions classified.
    """
    return ClassifiedConditions(
        weather_category=classify_weather_code(conditions.weather_code),
        temperature_band=classify_temperature(conditions.temperature_celsius),
        wind_category=classify_wind(conditions.wind_speed_kmh),
        precipitation_intensity=classify_precipitation(conditions.precipitation_mm),
        raw=conditions,
    )
```

## 4. Biking Score Calculator

A simple weighted formula. No ML. Deterministic and easy to reason about.

```python
# ---------------------------------------------------------------------------
# Biking score weights and lookup tables
# ---------------------------------------------------------------------------

# Temperature score: ideal is mild/warm (15-25C), drops off in both directions
_TEMP_SCORES: dict[TemperatureBand, int] = {
    TemperatureBand.FREEZING: 10,
    TemperatureBand.COLD: 40,
    TemperatureBand.COOL: 65,
    TemperatureBand.MILD: 90,
    TemperatureBand.WARM: 100,
    TemperatureBand.HOT: 75,
    TemperatureBand.VERY_HOT: 45,
}

# Precipitation score: none is best, heavy is worst
_PRECIP_SCORES: dict[PrecipitationIntensity, int] = {
    PrecipitationIntensity.NONE: 100,
    PrecipitationIntensity.LIGHT: 60,
    PrecipitationIntensity.MODERATE: 30,
    PrecipitationIntensity.HEAVY: 5,
}

# Wind score: calm is best, very strong is worst
_WIND_SCORES: dict[WindCategory, int] = {
    WindCategory.CALM: 100,
    WindCategory.LIGHT: 85,
    WindCategory.MODERATE: 60,
    WindCategory.STRONG: 30,
    WindCategory.VERY_STRONG: 10,
}

# Weather category score: clear is best, thunderstorm worst
_WEATHER_SCORES: dict[WeatherCategory, int] = {
    WeatherCategory.CLEAR: 100,
    WeatherCategory.PARTLY_CLOUDY: 95,
    WeatherCategory.CLOUDY: 80,
    WeatherCategory.FOG: 55,
    WeatherCategory.DRIZZLE: 45,
    WeatherCategory.RAIN: 25,
    WeatherCategory.HEAVY_RAIN: 10,
    WeatherCategory.SNOW: 15,
    WeatherCategory.HEAVY_SNOW: 5,
    WeatherCategory.THUNDERSTORM: 5,
}

# Weights for combining sub-scores (must sum to 1.0)
_SCORE_WEIGHTS = {
    "temperature": 0.25,
    "precipitation": 0.30,
    "wind": 0.15,
    "weather": 0.30,
}


def compute_biking_score(classified: ClassifiedConditions) -> BikingScore:
    """Compute a composite biking score from 0-100.

    Uses a weighted combination of temperature, precipitation, wind,
    and overall weather condition sub-scores.

    Args:
        classified: Classified weather conditions.

    Returns:
        BikingScore with numeric score, human label, and display color.
    """
    temp_score = _TEMP_SCORES[classified.temperature_band]
    precip_score = _PRECIP_SCORES[classified.precipitation_intensity]
    wind_score = _WIND_SCORES[classified.wind_category]
    weather_score = _WEATHER_SCORES[classified.weather_category]

    raw_score = (
        _SCORE_WEIGHTS["temperature"] * temp_score
        + _SCORE_WEIGHTS["precipitation"] * precip_score
        + _SCORE_WEIGHTS["wind"] * wind_score
        + _SCORE_WEIGHTS["weather"] * weather_score
    )

    score = max(0, min(100, round(raw_score)))

    if score >= 80:
        label = "Excellent"
        color = "#2ecc71"   # green
    elif score >= 60:
        label = "Good"
        color = "#f1c40f"   # yellow
    elif score >= 40:
        label = "Fair"
        color = "#e67e22"   # orange
    else:
        label = "Poor"
        color = "#e74c3c"   # red

    return BikingScore(score=score, label=label, color=color)
```

## 5. Historical Data Lookup

This function queries the `mart_weather_impact_summary.parquet` file that Phase 02 creates. It follows the exact same pattern as `dashboard/app.py` -- reading parquet files directly via DuckDB.

```python
# ---------------------------------------------------------------------------
# Data directory resolution (same pattern as app.py and parquet_file_manager.py)
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

_WEATHER_IMPACT_PARQUET = "mart_weather_impact_summary.parquet"


@dataclass
class HistoricalImpact:
    """Pre-computed historical impact stats for a given condition slice."""
    avg_rides: Optional[float] = None
    pct_change_vs_baseline: Optional[float] = None     # e.g. -34.0 means 34% fewer
    avg_duration_minutes: Optional[float] = None
    duration_pct_change: Optional[float] = None         # e.g. -22.0 means 22% shorter
    sample_days: Optional[int] = None                   # how many historical days match


def lookup_historical_impact(
    location: str,
    hour: int,
    weather_category: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> HistoricalImpact:
    """Query mart_weather_impact_summary for historical stats matching conditions.

    This follows the dashboard's pattern of querying parquet files directly
    with DuckDB. If the mart parquet file does not exist or the query returns
    no rows, returns an empty HistoricalImpact (all None fields).

    Args:
        location: "nyc" or "london".
        hour: Hour of day (0-23).
        weather_category: WeatherCategory.value string (e.g. "rain").
        conn: Optional DuckDB connection. If None, creates an in-memory one.

    Returns:
        HistoricalImpact with stats, or empty if no data found.
    """
    parquet_path = os.path.join(DATA_DIR, _WEATHER_IMPACT_PARQUET)

    if not os.path.exists(parquet_path):
        logger.warning(
            "Weather impact parquet not found at %s. "
            "Returning empty historical impact.",
            parquet_path,
        )
        return HistoricalImpact()

    if conn is None:
        conn = duckdb.connect(":memory:")

    # Map engine WeatherCategory values to mart dimension_value
    # (stg_weather_hourly groups codes differently than the engine)
    mart_weather = _CATEGORY_TO_MART_WEATHER.get(weather_category, weather_category)

    query = f"""
        SELECT
            avg_rides,
            pct_change_rides_vs_clear AS pct_change_vs_baseline,
            avg_duration_seconds / 60.0 AS avg_duration_minutes,
            pct_change_duration_vs_clear AS duration_pct_change,
            observation_count AS sample_days
        FROM '{parquet_path}'
        WHERE location = $1
          AND hour_of_day = $2
          AND dimension_type = 'weather_condition'
          AND dimension_value = $3
        LIMIT 1
    """

    try:
        result = conn.execute(query, [location, hour, mart_weather]).fetchdf()
        if result.empty:
            logger.info(
                "No historical data for location=%s hour=%d weather=%s",
                location, hour, weather_category,
            )
            return HistoricalImpact()

        row = result.iloc[0]
        return HistoricalImpact(
            avg_rides=_safe_float(row.get("avg_rides")),
            pct_change_vs_baseline=_safe_float(row.get("pct_change_vs_baseline")),
            avg_duration_minutes=_safe_float(row.get("avg_duration_minutes")),
            duration_pct_change=_safe_float(row.get("duration_pct_change")),
            sample_days=_safe_int(row.get("sample_days")),
        )
    except Exception:
        logger.exception("Error querying weather impact summary")
        return HistoricalImpact()


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None if NaN or conversion fails."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convert a value to int, returning None if conversion fails."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
```

## 6. Insight Generator

Template-based natural language generation. Each insight type has a function that returns an `Optional[Recommendation]` -- returning `None` if the data is insufficient. The generator collects all non-None insights, ranks them, and returns the top 3-5.

```python
# ---------------------------------------------------------------------------
# Insight templates
# ---------------------------------------------------------------------------

_LOCATION_DISPLAY = {"nyc": "NYC", "london": "London"}


def _location_name(location: str) -> str:
    """Return display name for a location."""
    return _LOCATION_DISPLAY.get(location, location.upper())


def _insight_ride_volume(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
) -> Optional[Recommendation]:
    """Generate insight about ride volume impact.

    Example: "Rainy mornings see 34% fewer rides in NYC"
    """
    pct = impact.pct_change_vs_baseline
    if pct is None:
        return None

    location_name = _location_name(classified.raw.location)
    weather_label = classified.weather_category.value.replace("_", " ")
    hour = classified.raw.hour

    # Determine time-of-day label
    if 5 <= hour < 12:
        time_label = "mornings"
    elif 12 <= hour < 17:
        time_label = "afternoons"
    elif 17 <= hour < 21:
        time_label = "evenings"
    else:
        time_label = "nights"

    abs_pct = abs(round(pct))

    if pct <= -50:
        text = (
            f"{weather_label.capitalize()} {time_label} see {abs_pct}% fewer rides "
            f"in {location_name} — expect quieter stations"
        )
        severity = Severity.WARNING
    elif pct <= -20:
        text = (
            f"{weather_label.capitalize()} {time_label} see {abs_pct}% fewer rides "
            f"in {location_name}"
        )
        severity = Severity.CAUTION
    elif pct >= 10:
        text = (
            f"Great conditions! {weather_label.capitalize()} {time_label} historically "
            f"see {abs_pct}% more rides in {location_name}"
        )
        severity = Severity.POSITIVE
    else:
        text = (
            f"Ridership is near typical levels for {weather_label} "
            f"{time_label} in {location_name}"
        )
        severity = Severity.NEUTRAL

    return Recommendation(
        text=text,
        severity=severity,
        metric="rides_impact_pct",
        value=round(pct, 1),
    )


def _insight_duration(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
) -> Optional[Recommendation]:
    """Generate insight about ride duration impact.

    Example: "Rides tend to be 15% shorter in moderate wind"
    """
    pct = impact.duration_pct_change
    if pct is None:
        return None

    abs_pct = abs(round(pct))
    if abs_pct < 3:
        return None  # Not significant enough to mention

    if pct < 0:
        direction = "shorter"
    else:
        direction = "longer"

    # Identify the likely cause
    weather_label = classified.weather_category.value.replace("_", " ")
    wind_label = classified.wind_category.value.replace("_", " ")

    # If wind is the dominant factor (moderate or above), mention wind
    if classified.wind_category in (WindCategory.MODERATE, WindCategory.STRONG, WindCategory.VERY_STRONG):
        cause = f"{wind_label} wind ({round(classified.raw.wind_speed_kmh)} km/h)"
    else:
        cause = f"{weather_label} conditions"

    text = f"Rides tend to be {abs_pct}% {direction} in {cause}"
    severity = Severity.CAUTION if abs_pct >= 15 else Severity.NEUTRAL

    return Recommendation(
        text=text,
        severity=severity,
        metric="duration_impact_pct",
        value=round(pct, 1),
    )


def _insight_biking_score(
    biking_score: BikingScore,
    classified: ClassifiedConditions,
) -> Recommendation:
    """Generate top-level insight from the biking score.

    Example: "Great day to bike! Clear skies at 9am historically mean peak ridership"
    """
    hour = classified.raw.hour
    weather_label = classified.weather_category.value.replace("_", " ")

    if biking_score.score >= 80:
        text = (
            f"Great day to bike! {weather_label.capitalize()} skies at {hour}:00 "
            f"historically mean peak ridership"
        )
        severity = Severity.POSITIVE
    elif biking_score.score >= 60:
        text = (
            f"Decent conditions for cycling — {weather_label} at {hour}:00, "
            f"biking score: {biking_score.score}/100"
        )
        severity = Severity.NEUTRAL
    elif biking_score.score >= 40:
        text = (
            f"Conditions are below average for cycling — "
            f"biking score: {biking_score.score}/100"
        )
        severity = Severity.CAUTION
    else:
        text = (
            f"Challenging conditions for cycling — "
            f"biking score: {biking_score.score}/100. Consider alternative transport"
        )
        severity = Severity.WARNING

    return Recommendation(
        text=text,
        severity=severity,
        metric="biking_score",
        value=float(biking_score.score),
    )


def _insight_comparison_to_best(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
    biking_score: BikingScore,
) -> Optional[Recommendation]:
    """Generate insight comparing current conditions to the best historical days.

    Example: "Current conditions are similar to the top 10% best biking days historically"
    """
    if biking_score.score >= 90 and impact.pct_change_vs_baseline is not None:
        if impact.pct_change_vs_baseline >= 5:
            text = (
                "Current conditions are similar to the top 10% best biking days "
                "historically"
            )
            return Recommendation(
                text=text,
                severity=Severity.POSITIVE,
                metric="comparison_to_best",
                value=float(biking_score.score),
            )
    return None


def _insight_missing_data(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
) -> Optional[Recommendation]:
    """Generate a notice when historical data is sparse or missing.

    Example: "Limited historical data for thunderstorm conditions at this hour"
    """
    if impact.sample_days is not None and impact.sample_days < 5:
        weather_label = classified.weather_category.value.replace("_", " ")
        text = (
            f"Limited historical data for {weather_label} conditions "
            f"at this hour ({impact.sample_days} days in dataset)"
        )
        return Recommendation(
            text=text,
            severity=Severity.NEUTRAL,
            metric="data_quality",
            value=float(impact.sample_days),
        )

    # Completely missing data
    if impact.avg_rides is None and impact.pct_change_vs_baseline is None:
        weather_label = classified.weather_category.value.replace("_", " ")
        text = (
            f"No historical riding data available for {weather_label} conditions "
            f"at this hour — this is a rare combination"
        )
        return Recommendation(
            text=text,
            severity=Severity.NEUTRAL,
            metric="data_quality",
            value=None,
        )

    return None
```

### 6.1 Insight Ranking and Assembly

```python
# Severity priority for ranking (higher = shown first)
_SEVERITY_PRIORITY = {
    Severity.WARNING: 4,
    Severity.POSITIVE: 3,
    Severity.CAUTION: 2,
    Severity.NEUTRAL: 1,
}


def generate_insights(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
    biking_score: BikingScore,
    max_insights: int = 5,
) -> List[Recommendation]:
    """Generate ranked list of insights from classified conditions and historical data.

    Collects insights from all generators, filters out None values,
    ranks by severity (warnings first, then positive, then caution, then neutral),
    and returns the top max_insights.

    Args:
        classified: Classified weather conditions.
        impact: Historical impact data from mart lookup.
        biking_score: Computed biking score.
        max_insights: Maximum number of insights to return. Defaults to 5.

    Returns:
        List of Recommendation objects, ordered by severity priority. Always
        returns at least 1 item (the biking score insight).
    """
    candidates: List[Optional[Recommendation]] = [
        _insight_biking_score(biking_score, classified),
        _insight_ride_volume(classified, impact),
        _insight_duration(classified, impact),
        _insight_comparison_to_best(classified, impact, biking_score),
        _insight_missing_data(classified, impact),
    ]

    # Filter out None values
    insights = [r for r in candidates if r is not None]

    # Sort by severity priority (descending), then by absolute value (descending)
    insights.sort(
        key=lambda r: (
            _SEVERITY_PRIORITY.get(r.severity, 0),
            abs(r.value) if r.value is not None else 0,
        ),
        reverse=True,
    )

    return insights[:max_insights]
```

## 7. Main Entry Point

```python
def get_recommendations(
    conditions: WeatherConditions,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> RecommendationResult:
    """Generate recommendations for current weather conditions.

    This is the main entry point for the recommendation engine. It:
    1. Classifies raw weather conditions into categories
    2. Computes a composite biking score (0-100)
    3. Looks up historical impact from mart_weather_impact_summary
    4. Generates 3-5 ranked natural language insights

    Args:
        conditions: Current weather conditions from Phase 03 weather service.
        conn: Optional DuckDB connection for parquet queries.
              If None, creates an in-memory connection.

    Returns:
        RecommendationResult containing biking score, recommendations list,
        and classified conditions.
    """
    # Step 1: Classify
    classified = classify_conditions(conditions)

    # Step 2: Score
    biking_score = compute_biking_score(classified)

    # Step 3: Historical lookup
    impact = lookup_historical_impact(
        location=conditions.location,
        hour=conditions.hour,
        weather_category=classified.weather_category.value,
        conn=conn,
    )

    # Step 4: Generate insights
    recommendations = generate_insights(classified, impact, biking_score)

    return RecommendationResult(
        biking_score=biking_score,
        recommendations=recommendations,
        classified=classified,
    )
```

## 8. Test Plan

File: `tests/test_recommendation_engine.py`

This follows the project's existing test conventions:
- Class-based test organization (like `TestExtractionUtils`, `TestRunQueryLogic`)
- pytest fixtures
- Descriptive docstrings on every test method
- No import-time side effects (the recommendation engine has no Streamlit imports)

### 8.1 Test Classes and Cases

```python
"""
Tests for dashboard/recommendation_engine.py.

Tests the condition classifier, biking score calculator, insight generator,
and historical data lookup. No Streamlit imports, no S3 calls, no side effects.
"""

import pytest
import duckdb
import pandas as pd
import os

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
    Recommendation,
    RecommendationResult,
    classify_weather_code,
    classify_temperature,
    classify_wind,
    classify_precipitation,
    classify_conditions,
    compute_biking_score,
    lookup_historical_impact,
    generate_insights,
    get_recommendations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def clear_morning_nyc() -> WeatherConditions:
    """Clear, warm morning conditions in NYC."""
    return WeatherConditions(
        temperature_celsius=22.0,
        wind_speed_kmh=8.0,
        precipitation_mm=0.0,
        weather_code=0,
        location="nyc",
        hour=9,
    )


@pytest.fixture
def rainy_afternoon_london() -> WeatherConditions:
    """Rainy, cool afternoon conditions in London."""
    return WeatherConditions(
        temperature_celsius=12.0,
        wind_speed_kmh=25.0,
        precipitation_mm=5.0,
        weather_code=63,
        location="london",
        hour=14,
    )


@pytest.fixture
def snowy_morning_nyc() -> WeatherConditions:
    """Snowy, freezing morning conditions in NYC."""
    return WeatherConditions(
        temperature_celsius=-3.0,
        wind_speed_kmh=35.0,
        precipitation_mm=8.0,
        weather_code=75,
        location="nyc",
        hour=8,
    )


@pytest.fixture
def weather_impact_parquet(tmp_path) -> str:
    """Create a temporary mart_weather_impact_summary.parquet for testing."""
    data = pd.DataFrame({
        "location": ["nyc", "nyc", "london", "nyc"],
        "hour_of_day": [9, 9, 14, 8],
        "dimension_type": ["weather_condition"] * 4,
        "dimension_value": ["clear", "rain", "rain", "snow"],
        "is_precipitation": [False, True, True, True],
        "temperature_band": [None, None, None, None],
        "observation_count": [120, 45, 60, 3],
        "avg_rides": [1500.0, 990.0, 800.0, 525.0],
        "avg_duration_seconds": [870.0, 678.0, 1080.0, 492.0],
        "avg_member_rides": [1000.0, 660.0, 500.0, 350.0],
        "avg_casual_rides": [500.0, 330.0, 300.0, 175.0],
        "baseline_avg_rides": [1339.3, 1339.3, 1111.1, 1500.0],
        "baseline_avg_duration_seconds": [828.6, 828.6, 1200.0, 862.0],
        "pct_change_rides_vs_clear": [12.0, -34.0, -28.0, -65.0],
        "pct_change_duration_vs_clear": [5.0, -22.0, -10.0, -43.0],
    })
    path = tmp_path / "mart_weather_impact_summary.parquet"
    data.to_parquet(str(path))
    return str(tmp_path)


# ---------------------------------------------------------------------------
# TestClassifier
# ---------------------------------------------------------------------------

class TestWeatherCodeClassifier:
    """Tests for WMO weather code classification."""

    def test_clear_codes(self):
        """WMO codes 0 and 1 should classify as CLEAR."""
        assert classify_weather_code(0) == WeatherCategory.CLEAR
        assert classify_weather_code(1) == WeatherCategory.CLEAR

    def test_partly_cloudy(self):
        """WMO code 2 should classify as PARTLY_CLOUDY."""
        assert classify_weather_code(2) == WeatherCategory.PARTLY_CLOUDY

    def test_rain_codes(self):
        """WMO codes 61 and 63 should classify as RAIN."""
        assert classify_weather_code(61) == WeatherCategory.RAIN
        assert classify_weather_code(63) == WeatherCategory.RAIN

    def test_heavy_rain_codes(self):
        """WMO codes 65, 67, 82 should classify as HEAVY_RAIN."""
        assert classify_weather_code(65) == WeatherCategory.HEAVY_RAIN
        assert classify_weather_code(67) == WeatherCategory.HEAVY_RAIN
        assert classify_weather_code(82) == WeatherCategory.HEAVY_RAIN

    def test_snow_codes(self):
        """WMO codes 71, 73, 77 should classify as SNOW."""
        assert classify_weather_code(71) == WeatherCategory.SNOW
        assert classify_weather_code(73) == WeatherCategory.SNOW
        assert classify_weather_code(77) == WeatherCategory.SNOW

    def test_heavy_snow_codes(self):
        """WMO codes 75 and 86 should classify as HEAVY_SNOW."""
        assert classify_weather_code(75) == WeatherCategory.HEAVY_SNOW
        assert classify_weather_code(86) == WeatherCategory.HEAVY_SNOW

    def test_thunderstorm_codes(self):
        """WMO codes 95, 96, 99 should classify as THUNDERSTORM."""
        assert classify_weather_code(95) == WeatherCategory.THUNDERSTORM
        assert classify_weather_code(96) == WeatherCategory.THUNDERSTORM
        assert classify_weather_code(99) == WeatherCategory.THUNDERSTORM

    def test_unknown_code_defaults_to_cloudy(self):
        """Unknown WMO codes should default to CLOUDY."""
        assert classify_weather_code(999) == WeatherCategory.CLOUDY
        assert classify_weather_code(-1) == WeatherCategory.CLOUDY


class TestTemperatureClassifier:
    """Tests for temperature band classification."""

    def test_freezing(self):
        """Temperatures below 0 should classify as FREEZING."""
        assert classify_temperature(-5.0) == TemperatureBand.FREEZING
        assert classify_temperature(-0.1) == TemperatureBand.FREEZING

    def test_cold(self):
        """Temperatures 0-10 should classify as COLD."""
        assert classify_temperature(0.0) == TemperatureBand.COLD
        assert classify_temperature(9.9) == TemperatureBand.COLD

    def test_cool(self):
        """Temperatures 10-15 should classify as COOL."""
        assert classify_temperature(10.0) == TemperatureBand.COOL
        assert classify_temperature(14.9) == TemperatureBand.COOL

    def test_mild(self):
        """Temperatures 15-20 should classify as MILD."""
        assert classify_temperature(15.0) == TemperatureBand.MILD
        assert classify_temperature(19.9) == TemperatureBand.MILD

    def test_warm(self):
        """Temperatures 20-25 should classify as WARM."""
        assert classify_temperature(20.0) == TemperatureBand.WARM
        assert classify_temperature(24.9) == TemperatureBand.WARM

    def test_hot(self):
        """Temperatures 25-30 should classify as HOT."""
        assert classify_temperature(25.0) == TemperatureBand.HOT
        assert classify_temperature(29.9) == TemperatureBand.HOT

    def test_very_hot(self):
        """Temperatures above 30 should classify as VERY_HOT."""
        assert classify_temperature(30.0) == TemperatureBand.VERY_HOT
        assert classify_temperature(40.0) == TemperatureBand.VERY_HOT

    def test_boundary_values(self):
        """Boundary values should fall into the correct band."""
        assert classify_temperature(0.0) == TemperatureBand.COLD      # not FREEZING
        assert classify_temperature(10.0) == TemperatureBand.COOL     # not COLD
        assert classify_temperature(15.0) == TemperatureBand.MILD     # not COOL
        assert classify_temperature(20.0) == TemperatureBand.WARM     # not MILD
        assert classify_temperature(25.0) == TemperatureBand.HOT      # not WARM
        assert classify_temperature(30.0) == TemperatureBand.VERY_HOT # not HOT


class TestWindClassifier:
    """Tests for wind speed classification."""

    def test_calm(self):
        """Wind below 10 km/h should classify as CALM."""
        assert classify_wind(0.0) == WindCategory.CALM
        assert classify_wind(9.9) == WindCategory.CALM

    def test_light(self):
        """Wind 10-20 km/h should classify as LIGHT."""
        assert classify_wind(10.0) == WindCategory.LIGHT
        assert classify_wind(19.9) == WindCategory.LIGHT

    def test_moderate(self):
        """Wind 20-30 km/h should classify as MODERATE."""
        assert classify_wind(20.0) == WindCategory.MODERATE
        assert classify_wind(29.9) == WindCategory.MODERATE

    def test_strong(self):
        """Wind 30-50 km/h should classify as STRONG."""
        assert classify_wind(30.0) == WindCategory.STRONG
        assert classify_wind(49.9) == WindCategory.STRONG

    def test_very_strong(self):
        """Wind above 50 km/h should classify as VERY_STRONG."""
        assert classify_wind(50.0) == WindCategory.VERY_STRONG
        assert classify_wind(100.0) == WindCategory.VERY_STRONG


class TestPrecipitationClassifier:
    """Tests for precipitation intensity classification."""

    def test_none(self):
        """Zero or negative precipitation should classify as NONE."""
        assert classify_precipitation(0.0) == PrecipitationIntensity.NONE
        assert classify_precipitation(-0.1) == PrecipitationIntensity.NONE

    def test_light(self):
        """Precipitation 0-2.5 mm should classify as LIGHT."""
        assert classify_precipitation(0.1) == PrecipitationIntensity.LIGHT
        assert classify_precipitation(2.5) == PrecipitationIntensity.LIGHT

    def test_moderate(self):
        """Precipitation 2.5-7.5 mm should classify as MODERATE."""
        assert classify_precipitation(2.6) == PrecipitationIntensity.MODERATE
        assert classify_precipitation(7.5) == PrecipitationIntensity.MODERATE

    def test_heavy(self):
        """Precipitation above 7.5 mm should classify as HEAVY."""
        assert classify_precipitation(7.6) == PrecipitationIntensity.HEAVY
        assert classify_precipitation(50.0) == PrecipitationIntensity.HEAVY


class TestClassifyConditions:
    """Tests for the combined classify_conditions function."""

    def test_returns_classified_conditions(self, clear_morning_nyc):
        """classify_conditions should return a ClassifiedConditions dataclass."""
        result = classify_conditions(clear_morning_nyc)
        assert isinstance(result, ClassifiedConditions)

    def test_clear_warm_calm(self, clear_morning_nyc):
        """Clear, warm, calm conditions should classify correctly in all dimensions."""
        result = classify_conditions(clear_morning_nyc)
        assert result.weather_category == WeatherCategory.CLEAR
        assert result.temperature_band == TemperatureBand.WARM
        assert result.wind_category == WindCategory.CALM
        assert result.precipitation_intensity == PrecipitationIntensity.NONE

    def test_rainy_cool_moderate(self, rainy_afternoon_london):
        """Rainy, cool, moderate-wind conditions should classify correctly."""
        result = classify_conditions(rainy_afternoon_london)
        assert result.weather_category == WeatherCategory.RAIN
        assert result.temperature_band == TemperatureBand.COOL
        assert result.wind_category == WindCategory.MODERATE
        assert result.precipitation_intensity == PrecipitationIntensity.MODERATE

    def test_preserves_raw_conditions(self, clear_morning_nyc):
        """classify_conditions should preserve the original WeatherConditions."""
        result = classify_conditions(clear_morning_nyc)
        assert result.raw is clear_morning_nyc
        assert result.raw.location == "nyc"
        assert result.raw.hour == 9


# ---------------------------------------------------------------------------
# TestBikingScore
# ---------------------------------------------------------------------------

class TestBikingScore:
    """Tests for biking score computation."""

    def test_perfect_conditions_score_high(self, clear_morning_nyc):
        """Clear, warm, calm, dry conditions should produce a high score."""
        classified = classify_conditions(clear_morning_nyc)
        score = compute_biking_score(classified)
        assert score.score >= 80
        assert score.label == "Excellent"
        assert score.color == "#2ecc71"

    def test_terrible_conditions_score_low(self, snowy_morning_nyc):
        """Snowy, freezing, windy conditions should produce a low score."""
        classified = classify_conditions(snowy_morning_nyc)
        score = compute_biking_score(classified)
        assert score.score < 40
        assert score.label == "Poor"
        assert score.color == "#e74c3c"

    def test_score_range(self, clear_morning_nyc):
        """Biking score should always be between 0 and 100."""
        classified = classify_conditions(clear_morning_nyc)
        score = compute_biking_score(classified)
        assert 0 <= score.score <= 100

    def test_moderate_conditions_score_mid(self, rainy_afternoon_london):
        """Rainy but not extreme conditions should produce a mid-range score."""
        classified = classify_conditions(rainy_afternoon_london)
        score = compute_biking_score(classified)
        assert 20 <= score.score <= 60

    def test_score_labels_match_ranges(self):
        """Score labels should correspond to the correct score ranges."""
        # Build conditions that produce each label
        conditions_excellent = WeatherConditions(
            temperature_celsius=22, wind_speed_kmh=5,
            precipitation_mm=0, weather_code=0, location="nyc", hour=10,
        )
        conditions_poor = WeatherConditions(
            temperature_celsius=-5, wind_speed_kmh=60,
            precipitation_mm=15, weather_code=75, location="nyc", hour=10,
        )

        excellent = compute_biking_score(classify_conditions(conditions_excellent))
        poor = compute_biking_score(classify_conditions(conditions_poor))

        assert excellent.score > poor.score
        assert excellent.label == "Excellent"
        assert poor.label == "Poor"


# ---------------------------------------------------------------------------
# TestHistoricalLookup
# ---------------------------------------------------------------------------

class TestHistoricalLookup:
    """Tests for mart_weather_impact_summary parquet lookup."""

    def test_returns_data_when_match_found(self, weather_impact_parquet):
        """lookup_historical_impact should return populated HistoricalImpact when data matches."""
        from unittest.mock import patch
        with patch("dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet):
            result = lookup_historical_impact("nyc", 9, "clear")

        assert result.avg_rides == 1500.0
        assert result.pct_change_vs_baseline == 12.0
        assert result.sample_days == 120

    def test_returns_empty_when_no_match(self, weather_impact_parquet):
        """lookup_historical_impact should return empty HistoricalImpact when no data matches."""
        from unittest.mock import patch
        with patch("dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet):
            result = lookup_historical_impact("nyc", 23, "thunderstorm")

        assert result.avg_rides is None
        assert result.pct_change_vs_baseline is None

    def test_returns_empty_when_parquet_missing(self, tmp_path):
        """lookup_historical_impact should return empty when parquet file does not exist."""
        from unittest.mock import patch
        with patch("dashboard.recommendation_engine.DATA_DIR", str(tmp_path)):
            result = lookup_historical_impact("nyc", 9, "clear")

        assert result.avg_rides is None

    def test_accepts_external_connection(self, weather_impact_parquet):
        """lookup_historical_impact should work with an externally provided DuckDB connection."""
        from unittest.mock import patch
        conn = duckdb.connect(":memory:")
        with patch("dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet):
            result = lookup_historical_impact("london", 14, "rain", conn=conn)
        conn.close()

        assert result.pct_change_vs_baseline == -28.0


# ---------------------------------------------------------------------------
# TestInsightGenerator
# ---------------------------------------------------------------------------

class TestInsightGenerator:
    """Tests for insight generation and ranking."""

    def test_always_returns_at_least_one_insight(self, clear_morning_nyc):
        """generate_insights should always return at least the biking score insight."""
        classified = classify_conditions(clear_morning_nyc)
        score = compute_biking_score(classified)
        empty_impact = HistoricalImpact()

        insights = generate_insights(classified, empty_impact, score)
        assert len(insights) >= 1
        assert any(r.metric == "biking_score" for r in insights)

    def test_returns_at_most_max_insights(self, clear_morning_nyc):
        """generate_insights should not return more than max_insights."""
        classified = classify_conditions(clear_morning_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=1500, pct_change_vs_baseline=12.0,
            avg_duration_minutes=14.5, duration_pct_change=5.0, sample_days=120,
        )

        insights = generate_insights(classified, impact, score, max_insights=2)
        assert len(insights) <= 2

    def test_warnings_ranked_first(self, snowy_morning_nyc):
        """Insights with WARNING severity should appear before NEUTRAL ones."""
        classified = classify_conditions(snowy_morning_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=525, pct_change_vs_baseline=-65.0,
            avg_duration_minutes=8.2, duration_pct_change=-43.0, sample_days=3,
        )

        insights = generate_insights(classified, impact, score)
        severities = [r.severity for r in insights]

        # WARNING items should come before NEUTRAL items
        warning_indices = [i for i, s in enumerate(severities) if s == Severity.WARNING]
        neutral_indices = [i for i, s in enumerate(severities) if s == Severity.NEUTRAL]

        if warning_indices and neutral_indices:
            assert max(warning_indices) < min(neutral_indices)

    def test_ride_volume_insight_text_for_negative(self):
        """Negative pct_change should produce 'fewer rides' text."""
        conditions = WeatherConditions(
            temperature_celsius=12, wind_speed_kmh=25,
            precipitation_mm=5, weather_code=63, location="nyc", hour=9,
        )
        classified = classify_conditions(conditions)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=990, pct_change_vs_baseline=-34.0,
            avg_duration_minutes=11.3, duration_pct_change=-22.0, sample_days=45,
        )

        insights = generate_insights(classified, impact, score)
        ride_insights = [r for r in insights if r.metric == "rides_impact_pct"]
        assert len(ride_insights) == 1
        assert "fewer rides" in ride_insights[0].text
        assert "34%" in ride_insights[0].text

    def test_missing_data_insight_for_sparse_data(self, snowy_morning_nyc):
        """generate_insights should include a data quality notice when sample_days < 5."""
        classified = classify_conditions(snowy_morning_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=525, pct_change_vs_baseline=-65.0,
            avg_duration_minutes=8.2, duration_pct_change=-43.0, sample_days=3,
        )

        insights = generate_insights(classified, impact, score)
        data_quality = [r for r in insights if r.metric == "data_quality"]
        assert len(data_quality) == 1
        assert "3 days" in data_quality[0].text

    def test_positive_conditions_produce_positive_severity(self, clear_morning_nyc):
        """Excellent conditions should produce POSITIVE severity insights."""
        classified = classify_conditions(clear_morning_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=1500, pct_change_vs_baseline=12.0,
            avg_duration_minutes=14.5, duration_pct_change=5.0, sample_days=120,
        )

        insights = generate_insights(classified, impact, score)
        assert any(r.severity == Severity.POSITIVE for r in insights)


# ---------------------------------------------------------------------------
# TestGetRecommendations (integration)
# ---------------------------------------------------------------------------

class TestGetRecommendations:
    """Integration tests for the main get_recommendations entry point."""

    def test_returns_recommendation_result(self, clear_morning_nyc, weather_impact_parquet):
        """get_recommendations should return a RecommendationResult."""
        from unittest.mock import patch
        with patch("dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet):
            result = get_recommendations(clear_morning_nyc)

        assert isinstance(result, RecommendationResult)
        assert isinstance(result.biking_score, BikingScore)
        assert isinstance(result.recommendations, list)
        assert isinstance(result.classified, ClassifiedConditions)

    def test_works_without_parquet_file(self, clear_morning_nyc, tmp_path):
        """get_recommendations should work gracefully when parquet file is missing."""
        from unittest.mock import patch
        with patch("dashboard.recommendation_engine.DATA_DIR", str(tmp_path)):
            result = get_recommendations(clear_morning_nyc)

        assert result.biking_score.score >= 80
        assert len(result.recommendations) >= 1

    def test_end_to_end_rainy(self, rainy_afternoon_london, weather_impact_parquet):
        """Full pipeline for rainy London afternoon should produce caution/warning insights."""
        from unittest.mock import patch
        with patch("dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet):
            result = get_recommendations(rainy_afternoon_london)

        assert result.biking_score.score < 60
        severities = {r.severity for r in result.recommendations}
        assert Severity.CAUTION in severities or Severity.WARNING in severities
```

## 9. Integration with `dashboard/app.py`

This section describes the changes needed to `app.py` to render the recommendations. This is the only file outside `dashboard/recommendation_engine.py` that needs modification. The integration follows the existing patterns in `app.py`:

### 9.1 Changes to `streamlit_data_manager/parquet_file_manager.py`

~~Add `"mart_weather_impact_summary.parquet"` to the `MARTS` list.~~
**ALREADY DONE** in Phase 02. Current MARTS list has 8 entries including `mart_weather_impact_summary.parquet`. No changes needed.

### 9.2 Changes to `dashboard/app.py`

Add a new section after the sidebar is set up. This section imports the recommendation engine, calls Phase 03's weather service (assumed to provide a `get_current_weather(location: str) -> WeatherConditions` function), and renders the results.

The rendering code would go inside the `if st.session_state.get('date_filter_applied', False)` block, in the per-city page sections (NYC and London), immediately after the KPI metrics cards. The rendering uses `st.container`, `st.metric`, and `st.markdown` with inline CSS for the severity color coding.

```python
# At top of app.py, add import:
from dashboard.recommendation_engine import (
    get_recommendations,
    WeatherConditions,
    Severity,
    RecommendationResult,
)

# In the city-specific sections (NYC / London), after the metrics cards:
# This assumes Phase 03 provides: from weather_service import get_current_weather

_SEVERITY_EMOJI = {
    Severity.POSITIVE: "&#9989;",    # green checkmark
    Severity.NEUTRAL: "&#8505;",     # info
    Severity.CAUTION: "&#9888;",     # warning triangle
    Severity.WARNING: "&#128721;",   # stop sign
}

_SEVERITY_BG_COLOR = {
    Severity.POSITIVE: "#d4edda",
    Severity.NEUTRAL: "#e2e3e5",
    Severity.CAUTION: "#fff3cd",
    Severity.WARNING: "#f8d7da",
}


def render_recommendations(result: RecommendationResult):
    """Render recommendation engine results in the dashboard."""
    # Biking Score gauge
    score = result.biking_score
    st.markdown(
        f"<div style='text-align:center; padding:1em; "
        f"background:{score.color}22; border-radius:8px; margin-bottom:1em;'>"
        f"<h2 style='margin:0; color:{score.color};'>"
        f"Biking Score: {score.score}/100</h2>"
        f"<p style='margin:0; font-size:1.2em;'>{score.label}</p>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Individual recommendations
    for rec in result.recommendations:
        emoji = _SEVERITY_EMOJI[rec.severity]
        bg = _SEVERITY_BG_COLOR[rec.severity]
        st.markdown(
            f"<div style='padding:0.75em 1em; background:{bg}; "
            f"border-radius:6px; margin-bottom:0.5em;'>"
            f"{emoji} {rec.text}</div>",
            unsafe_allow_html=True,
        )
```

## 10. File Summary and Sequencing

### Files to create:
1. **`/Users/chris/Projects/city-cycles/dashboard/recommendation_engine.py`** -- The complete recommendation engine module (all dataclasses, enums, classifiers, scorer, lookup, insight generator, entry point)
2. **`/Users/chris/Projects/city-cycles/tests/test_recommendation_engine.py`** -- Comprehensive unit and integration tests

### Files to modify:
3. **`/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py`** -- Add `"mart_weather_impact_summary.parquet"` to `MARTS` list
4. **`/Users/chris/Projects/city-cycles/dashboard/app.py`** -- Add import and render section for recommendations (after Phase 03 weather service is integrated)
5. **`/Users/chris/Projects/city-cycles/CHANGELOG.md`** -- Add entry under `[Unreleased]`

### Implementation order:
1. Create `dashboard/recommendation_engine.py` (standalone, no dependencies on Phase 03)
2. Create `tests/test_recommendation_engine.py`
3. Run tests: `/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/test_recommendation_engine.py -v`
4. Update `streamlit_data_manager/parquet_file_manager.py` (minor one-line addition)
5. Update `CHANGELOG.md`
6. Integration with `dashboard/app.py` happens when Phase 03 (weather service) is complete. The recommendation engine itself can be built and tested fully independently.

### Dependencies on other phases:
- **Phase 02** (mart_weather_impact_summary): The engine gracefully handles a missing parquet file. During development, the test suite creates its own test parquet. Once Phase 02 is deployed, the real data flows in automatically.
- **Phase 03** (weather service): Only needed for the `dashboard/app.py` integration. The engine accepts a `WeatherConditions` dataclass, which can be constructed manually for testing without the live weather service.

### Potential challenges:
1. **Column name mismatch with Phase 02 mart**: The `lookup_historical_impact` query assumes specific column names (`weather_condition`, `hour_of_day`, `avg_rides`, `pct_change_vs_baseline`, `avg_duration_minutes`, `duration_pct_change`, `sample_days`). Phase 02's mart must use these exact names. If they differ, only the SQL query in `lookup_historical_impact` needs updating.
2. **Weather category mapping alignment**: The `WeatherCategory.value` strings (e.g., "rain", "heavy_rain") must match what Phase 02 stores in the `weather_condition` column. This needs coordination with Phase 02.
3. **`__init__.py` in `dashboard/`**: ~~The project does not use `__init__.py`.~~ **ALREADY EXISTS** — created in Phase 03. No additional changes needed.

### Critical Files for Implementation
- `/Users/chris/Projects/city-cycles/dashboard/recommendation_engine.py` - Core module to create: all engine logic, classifiers, scoring, insights
- `/Users/chris/Projects/city-cycles/tests/test_recommendation_engine.py` - Test suite to create: ~35 test cases covering all components
- `/Users/chris/Projects/city-cycles/dashboard/app.py` - Dashboard to modify: add recommendation rendering section
- `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py` - Add new mart parquet to the download list
- `/Users/chris/Projects/city-cycles/tests/test_dashboard.py` - Pattern to follow: test structure avoiding Streamlit import-time side effects