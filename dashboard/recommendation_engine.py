"""
Weather-informed biking recommendation engine.

Bridges real-time weather (Phase 03) with historical ride-weather correlations
(Phase 02). Given current conditions + time, it looks up historical patterns
and generates user-facing insights like "Rainy mornings see 34% fewer rides."

Pure Python module — no Streamlit imports. Fully unit-testable.
"""

import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data directory resolution (same pattern as app.py and parquet_file_manager)
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

_WEATHER_IMPACT_PARQUET = "mart_weather_impact_summary.parquet"


# ---------------------------------------------------------------------------
# Input dataclass
# ---------------------------------------------------------------------------


@dataclass
class WeatherConditions:
    """Current weather conditions from Phase 03 weather service."""

    temperature_celsius: float
    wind_speed_kmh: float
    precipitation_mm: float
    weather_code: int  # WMO weather code (0-99)
    location: str  # "nyc" or "london"
    hour: int  # 0-23, current hour of day
    humidity_percent: Optional[float] = None
    feels_like_celsius: Optional[float] = None


# ---------------------------------------------------------------------------
# Classification enums
# ---------------------------------------------------------------------------


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
    FREEZING = "freezing"  # < 0
    COLD = "cold"  # 0-10
    COOL = "cool"  # 10-15
    MILD = "mild"  # 15-20
    WARM = "warm"  # 20-25
    HOT = "hot"  # 25-30
    VERY_HOT = "very_hot"  # > 30


class WindCategory(Enum):
    CALM = "calm"  # < 10 km/h
    LIGHT = "light"  # 10-20
    MODERATE = "moderate"  # 20-30
    STRONG = "strong"  # 30-50
    VERY_STRONG = "very_strong"  # > 50


class PrecipitationIntensity(Enum):
    NONE = "none"  # 0 mm
    LIGHT = "light"  # 0-2.5 mm
    MODERATE = "moderate"  # 2.5-7.5 mm
    HEAVY = "heavy"  # > 7.5 mm


class Severity(Enum):
    POSITIVE = "positive"  # green — great conditions
    NEUTRAL = "neutral"  # gray — normal conditions
    CAUTION = "caution"  # yellow — somewhat adverse
    WARNING = "warning"  # red — poor conditions


# ---------------------------------------------------------------------------
# Classified conditions and output dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ClassifiedConditions:
    """Weather conditions after classification into categories."""

    weather_category: WeatherCategory
    temperature_band: TemperatureBand
    wind_category: WindCategory
    precipitation_intensity: PrecipitationIntensity
    raw: WeatherConditions


@dataclass
class Recommendation:
    """A single recommendation/insight to display to the user."""

    text: str
    severity: Severity
    metric: str  # e.g. "rides_impact_pct", "duration_impact_pct", "biking_score"
    value: Optional[float] = None


@dataclass
class BikingScore:
    """Composite biking score 0-100."""

    score: int  # 0-100
    label: str  # "Excellent", "Good", "Fair", "Poor"
    color: str  # hex color for display


@dataclass
class HistoricalImpact:
    """Pre-computed historical impact stats for a given condition slice."""

    avg_rides: Optional[float] = None
    pct_change_vs_baseline: Optional[float] = None  # e.g. -34.0 means 34% fewer
    avg_duration_minutes: Optional[float] = None
    duration_pct_change: Optional[float] = None  # e.g. -22.0 means 22% shorter
    sample_days: Optional[int] = None  # how many historical observations match


@dataclass
class RecommendationResult:
    """Complete result returned by the engine."""

    biking_score: BikingScore
    recommendations: List[Recommendation]
    classified: ClassifiedConditions


# ---------------------------------------------------------------------------
# WMO weather code mapping
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
    56: WeatherCategory.DRIZZLE,
    57: WeatherCategory.DRIZZLE,
    61: WeatherCategory.RAIN,
    63: WeatherCategory.RAIN,
    65: WeatherCategory.HEAVY_RAIN,
    66: WeatherCategory.RAIN,
    67: WeatherCategory.HEAVY_RAIN,
    71: WeatherCategory.SNOW,
    73: WeatherCategory.SNOW,
    75: WeatherCategory.HEAVY_SNOW,
    77: WeatherCategory.SNOW,
    80: WeatherCategory.RAIN,
    81: WeatherCategory.RAIN,
    82: WeatherCategory.HEAVY_RAIN,
    85: WeatherCategory.SNOW,
    86: WeatherCategory.HEAVY_SNOW,
    95: WeatherCategory.THUNDERSTORM,
    96: WeatherCategory.THUNDERSTORM,
    99: WeatherCategory.THUNDERSTORM,
}

# Maps engine WeatherCategory values to the mart's dimension_value.
# stg_weather_hourly groups codes differently (e.g., all rain intensities → "rain").
_CATEGORY_TO_MART_WEATHER: dict[str, str] = {
    "clear": "clear",
    "partly_cloudy": "partly_cloudy",
    "cloudy": "partly_cloudy",
    "fog": "fog",
    "drizzle": "drizzle",
    "rain": "rain",
    "heavy_rain": "rain",
    "snow": "snow",
    "heavy_snow": "snow",
    "thunderstorm": "thunderstorm",
}


# ---------------------------------------------------------------------------
# Classifiers
# ---------------------------------------------------------------------------


def classify_weather_code(code: int) -> WeatherCategory:
    """Map a WMO weather code to a broad WeatherCategory."""
    return WMO_CODE_MAP.get(code, WeatherCategory.CLOUDY)


def classify_temperature(temp_celsius: float) -> TemperatureBand:
    """Classify temperature into a band."""
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
    """Classify wind speed into a category."""
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
    """Classify precipitation intensity."""
    if precip_mm <= 0:
        return PrecipitationIntensity.NONE
    elif precip_mm <= 2.5:
        return PrecipitationIntensity.LIGHT
    elif precip_mm <= 7.5:
        return PrecipitationIntensity.MODERATE
    else:
        return PrecipitationIntensity.HEAVY


def classify_conditions(conditions: WeatherConditions) -> ClassifiedConditions:
    """Classify all weather conditions into categories."""
    return ClassifiedConditions(
        weather_category=classify_weather_code(conditions.weather_code),
        temperature_band=classify_temperature(conditions.temperature_celsius),
        wind_category=classify_wind(conditions.wind_speed_kmh),
        precipitation_intensity=classify_precipitation(conditions.precipitation_mm),
        raw=conditions,
    )


# ---------------------------------------------------------------------------
# Biking score weights and lookup tables
# ---------------------------------------------------------------------------

_TEMP_SCORES: dict[TemperatureBand, int] = {
    TemperatureBand.FREEZING: 10,
    TemperatureBand.COLD: 40,
    TemperatureBand.COOL: 65,
    TemperatureBand.MILD: 90,
    TemperatureBand.WARM: 100,
    TemperatureBand.HOT: 75,
    TemperatureBand.VERY_HOT: 45,
}

_PRECIP_SCORES: dict[PrecipitationIntensity, int] = {
    PrecipitationIntensity.NONE: 100,
    PrecipitationIntensity.LIGHT: 60,
    PrecipitationIntensity.MODERATE: 30,
    PrecipitationIntensity.HEAVY: 5,
}

_WIND_SCORES: dict[WindCategory, int] = {
    WindCategory.CALM: 100,
    WindCategory.LIGHT: 85,
    WindCategory.MODERATE: 60,
    WindCategory.STRONG: 30,
    WindCategory.VERY_STRONG: 10,
}

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
    """
    raw_score = (
        _SCORE_WEIGHTS["temperature"] * _TEMP_SCORES[classified.temperature_band]
        + _SCORE_WEIGHTS["precipitation"]
        * _PRECIP_SCORES[classified.precipitation_intensity]
        + _SCORE_WEIGHTS["wind"] * _WIND_SCORES[classified.wind_category]
        + _SCORE_WEIGHTS["weather"] * _WEATHER_SCORES[classified.weather_category]
    )

    score = max(0, min(100, round(raw_score)))

    if score >= 80:
        label, color = "Excellent", "#2ecc71"
    elif score >= 60:
        label, color = "Good", "#f1c40f"
    elif score >= 40:
        label, color = "Fair", "#e67e22"
    else:
        label, color = "Poor", "#e74c3c"

    return BikingScore(score=score, label=label, color=color)


# ---------------------------------------------------------------------------
# Historical data lookup
# ---------------------------------------------------------------------------


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


def lookup_historical_impact(
    location: str,
    hour: int,
    weather_category: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> HistoricalImpact:
    """Query mart_weather_impact_summary for historical stats matching conditions.

    Args:
        location: "nyc" or "london".
        hour: Hour of day (0-23).
        weather_category: WeatherCategory.value string (e.g. "rain", "heavy_rain").
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

    # Map engine category to mart dimension_value
    mart_weather = _CATEGORY_TO_MART_WEATHER.get(
        weather_category, weather_category
    )

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
                location,
                hour,
                mart_weather,
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


# ---------------------------------------------------------------------------
# Insight generators
# ---------------------------------------------------------------------------

_LOCATION_DISPLAY = {"nyc": "NYC", "london": "London"}


def _location_name(location: str) -> str:
    """Return display name for a location."""
    return _LOCATION_DISPLAY.get(location, location.upper())


def _insight_ride_volume(
    classified: ClassifiedConditions,
    impact: HistoricalImpact,
) -> Optional[Recommendation]:
    """Generate insight about ride volume impact."""
    pct = impact.pct_change_vs_baseline
    if pct is None:
        return None

    location_name = _location_name(classified.raw.location)
    weather_label = classified.weather_category.value.replace("_", " ")
    hour = classified.raw.hour

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
            f"in {location_name} \u2014 expect quieter stations"
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
    """Generate insight about ride duration impact."""
    pct = impact.duration_pct_change
    if pct is None:
        return None

    abs_pct = abs(round(pct))
    if abs_pct < 3:
        return None  # Not significant enough to mention

    direction = "shorter" if pct < 0 else "longer"

    weather_label = classified.weather_category.value.replace("_", " ")
    wind_label = classified.wind_category.value.replace("_", " ")

    if classified.wind_category in (
        WindCategory.MODERATE,
        WindCategory.STRONG,
        WindCategory.VERY_STRONG,
    ):
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
    """Generate top-level insight from the biking score."""
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
            f"Decent conditions for cycling \u2014 {weather_label} at {hour}:00, "
            f"biking score: {biking_score.score}/100"
        )
        severity = Severity.NEUTRAL
    elif biking_score.score >= 40:
        text = (
            f"Conditions are below average for cycling \u2014 "
            f"biking score: {biking_score.score}/100"
        )
        severity = Severity.CAUTION
    else:
        text = (
            f"Challenging conditions for cycling \u2014 "
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
    """Generate insight comparing current conditions to the best historical days."""
    if biking_score.score >= 90 and impact.pct_change_vs_baseline is not None:
        if impact.pct_change_vs_baseline >= 5:
            return Recommendation(
                text=(
                    "Current conditions are similar to the top 10% best biking "
                    "days historically"
                ),
                severity=Severity.POSITIVE,
                metric="comparison_to_best",
                value=float(biking_score.score),
            )
    return None


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


# ---------------------------------------------------------------------------
# Insight ranking and assembly
# ---------------------------------------------------------------------------

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

    Always returns at least 1 item (the biking score insight).
    """
    candidates: List[Optional[Recommendation]] = [
        _insight_biking_score(biking_score, classified),
        _insight_ride_volume(classified, impact),
        _insight_duration(classified, impact),
        _insight_comparison_to_best(classified, impact, biking_score),
        _insight_missing_data(classified, impact),
    ]

    insights = [r for r in candidates if r is not None]

    insights.sort(
        key=lambda r: (
            _SEVERITY_PRIORITY.get(r.severity, 0),
            abs(r.value) if r.value is not None else 0,
        ),
        reverse=True,
    )

    return insights[:max_insights]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def get_recommendations(
    conditions: WeatherConditions,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> RecommendationResult:
    """Generate recommendations for current weather conditions.

    This is the main entry point. It classifies conditions, computes a biking
    score, looks up historical impact, and generates ranked insights.
    """
    classified = classify_conditions(conditions)
    biking_score = compute_biking_score(classified)

    impact = lookup_historical_impact(
        location=conditions.location,
        hour=conditions.hour,
        weather_category=classified.weather_category.value,
        conn=conn,
    )

    recommendations = generate_insights(classified, impact, biking_score)

    return RecommendationResult(
        biking_score=biking_score,
        recommendations=recommendations,
        classified=classified,
    )
