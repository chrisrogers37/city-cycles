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
_SIMILAR_DAY_PARQUET = "mart_similar_day_stats.parquet"


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
class SimilarDayInsight:
    """Pre-computed stats for days similar to today from mart_similar_day_stats."""

    avg_daily_rides: Optional[float] = None
    pct_change_vs_overall: Optional[float] = None  # e.g. -23.0 means 23% below typical
    avg_duration_minutes: Optional[float] = None
    duration_pct_change_vs_overall: Optional[float] = None
    peak_hour_start: Optional[int] = None  # e.g. 17 means peak at 5 PM
    peak_hour_end: Optional[int] = None  # e.g. 19 means peak ends at 7 PM
    sample_days: Optional[int] = None
    month: Optional[int] = None
    day_type: Optional[str] = None
    temperature_band: Optional[str] = None
    precipitation_intensity: Optional[str] = None


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

# Maps engine TemperatureBand values to stg_weather_hourly temperature_band values.
# The staging model uses 5 bands vs the engine's 7.
_TEMP_BAND_TO_MART: dict[str, str] = {
    "freezing": "freezing",
    "cold": "cold",
    "cool": "mild",       # engine's "cool" (10-15C) maps to mart's "mild" (10-20C)
    "mild": "mild",       # engine's "mild" (15-20C) maps to mart's "mild" (10-20C)
    "warm": "warm",       # engine's "warm" (20-25C) maps to mart's "warm" (20-30C)
    "hot": "warm",        # engine's "hot" (25-30C) maps to mart's "warm" (20-30C)
    "very_hot": "hot",    # engine's "very_hot" (>30C) maps to mart's "hot" (>30C)
}

# Maps engine PrecipitationIntensity values to stg_weather_hourly values.
# Mart uses: none, light, moderate, heavy, extreme. Engine uses: none, light, moderate, heavy.
_PRECIP_TO_MART: dict[str, str] = {
    "none": "none",
    "light": "light",
    "moderate": "moderate",
    "heavy": "heavy",
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


def lookup_similar_day_stats(
    location: str,
    month: int,
    day_type: str,
    temperature_band: str,
    precipitation_intensity: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> SimilarDayInsight:
    """Query mart_similar_day_stats for historical stats on days like today.

    Args:
        location: "nyc" or "london".
        month: Month number (1-12).
        day_type: "weekday" or "weekend".
        temperature_band: From stg_weather_hourly (freezing/cold/mild/warm/hot).
        precipitation_intensity: From stg_weather_hourly (none/light/moderate/heavy/extreme).
        conn: Optional DuckDB connection. If None, creates an in-memory one.

    Returns:
        SimilarDayInsight with stats, or empty if no data found.
    """
    parquet_path = os.path.join(DATA_DIR, _SIMILAR_DAY_PARQUET)

    if not os.path.exists(parquet_path):
        logger.warning(
            "Similar day stats parquet not found at %s. "
            "Returning empty similar day insight.",
            parquet_path,
        )
        return SimilarDayInsight()

    if conn is None:
        conn = duckdb.connect(":memory:")

    query = f"""
        SELECT
            avg_daily_rides,
            pct_change_vs_overall,
            avg_duration_minutes,
            duration_pct_change_vs_overall,
            peak_hour_start,
            peak_hour_end,
            sample_days
        FROM '{parquet_path}'
        WHERE grain = 'daily'
          AND location = $1
          AND month_num = $2
          AND day_type = $3
          AND temperature_band = $4
          AND precipitation_intensity = $5
        LIMIT 1
    """

    try:
        result = conn.execute(
            query,
            [location, month, day_type, temperature_band, precipitation_intensity],
        ).fetchdf()
        if result.empty:
            logger.info(
                "No similar day data for location=%s month=%d day_type=%s "
                "temp_band=%s precip=%s",
                location,
                month,
                day_type,
                temperature_band,
                precipitation_intensity,
            )
            return SimilarDayInsight()

        row = result.iloc[0]
        return SimilarDayInsight(
            avg_daily_rides=_safe_float(row.get("avg_daily_rides")),
            pct_change_vs_overall=_safe_float(row.get("pct_change_vs_overall")),
            avg_duration_minutes=_safe_float(row.get("avg_duration_minutes")),
            duration_pct_change_vs_overall=_safe_float(
                row.get("duration_pct_change_vs_overall")
            ),
            peak_hour_start=_safe_int(row.get("peak_hour_start")),
            peak_hour_end=_safe_int(row.get("peak_hour_end")),
            sample_days=_safe_int(row.get("sample_days")),
            month=month,
            day_type=day_type,
            temperature_band=temperature_band,
            precipitation_intensity=precipitation_intensity,
        )
    except Exception:
        logger.exception("Error querying similar day stats")
        return SimilarDayInsight()


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


# ---------------------------------------------------------------------------
# "Days Like Today" insight generators
# ---------------------------------------------------------------------------

_MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


def _format_ride_count(count: float) -> str:
    """Format a ride count for display (e.g. 12400 -> '12,400')."""
    return f"{round(count):,}"


def _insight_similar_day_volume(
    classified: ClassifiedConditions,
    similar: SimilarDayInsight,
) -> Optional[Recommendation]:
    """Generate insight about ride volume on similar days.

    Produces sentences like:
    "On similar February weekdays with light rain, NYC averaged 12,400 rides
     -- 23% below typical"
    """
    if similar.avg_daily_rides is None:
        return None

    location_name = _location_name(classified.raw.location)
    month_name = _MONTH_NAMES.get(similar.month, f"month {similar.month}")
    day_label = similar.day_type or "days"
    precip_label = (similar.precipitation_intensity or "").replace("_", " ")

    # Build the condition descriptor
    if precip_label and precip_label != "none":
        condition_desc = f"with {precip_label} precipitation"
    else:
        temp_label = (similar.temperature_band or "").replace("_", " ")
        condition_desc = f"with {temp_label} temperatures" if temp_label else ""

    rides_str = _format_ride_count(similar.avg_daily_rides)

    pct = similar.pct_change_vs_overall
    if pct is not None and abs(pct) >= 3:
        abs_pct = abs(round(pct))
        direction = "below" if pct < 0 else "above"
        pct_clause = f" \u2014 {abs_pct}% {direction} typical"
    else:
        pct_clause = ""

    text = (
        f"On similar {month_name} {day_label}s {condition_desc}, "
        f"{location_name} averaged {rides_str} rides{pct_clause}"
    )

    # Determine severity based on deviation
    if pct is not None:
        if pct <= -30:
            severity = Severity.WARNING
        elif pct <= -10:
            severity = Severity.CAUTION
        elif pct >= 10:
            severity = Severity.POSITIVE
        else:
            severity = Severity.NEUTRAL
    else:
        severity = Severity.NEUTRAL

    return Recommendation(
        text=text,
        severity=severity,
        metric="similar_day_rides",
        value=round(pct, 1) if pct is not None else None,
    )


def _insight_similar_day_duration(
    classified: ClassifiedConditions,
    similar: SimilarDayInsight,
) -> Optional[Recommendation]:
    """Generate insight about trip duration on similar days.

    Produces sentences like:
    "Riders took 8% shorter trips on days like today"
    """
    pct = similar.duration_pct_change_vs_overall
    if pct is None or abs(pct) < 3:
        return None  # Not significant enough to mention

    abs_pct = abs(round(pct))
    direction = "shorter" if pct < 0 else "longer"

    text = f"Riders took {abs_pct}% {direction} trips on days like today"

    severity = Severity.CAUTION if abs_pct >= 15 else Severity.NEUTRAL

    return Recommendation(
        text=text,
        severity=severity,
        metric="similar_day_duration",
        value=round(pct, 1),
    )


def _insight_similar_day_peak(
    classified: ClassifiedConditions,
    similar: SimilarDayInsight,
) -> Optional[Recommendation]:
    """Generate insight about peak activity hours on similar days.

    Produces sentences like:
    "Similar conditions saw peak activity between 5-7 PM"
    """
    if similar.peak_hour_start is None or similar.peak_hour_end is None:
        return None

    def _format_hour(h: int) -> str:
        """Format 24h hour to 12h display (e.g. 17 -> '5 PM')."""
        if h == 0:
            return "12 AM"
        elif h < 12:
            return f"{h} AM"
        elif h == 12:
            return "12 PM"
        else:
            return f"{h - 12} PM"

    start_str = _format_hour(similar.peak_hour_start)
    end_str = _format_hour(similar.peak_hour_end)

    text = f"Similar conditions saw peak activity between {start_str}\u2013{end_str}"

    return Recommendation(
        text=text,
        severity=Severity.NEUTRAL,
        metric="similar_day_peak",
        value=float(similar.peak_hour_start),
    )


def _insight_similar_day_no_data(
    classified: ClassifiedConditions,
    similar: SimilarDayInsight,
) -> Optional[Recommendation]:
    """Generate a fallback message when no similar-day data is available.

    Only fires when ALL similar-day fields are None (mart missing or no
    matching rows). Returns None if any data exists, so this never
    conflicts with the real insights above.
    """
    if similar.avg_daily_rides is not None:
        return None  # We have data, no need for fallback

    if similar.sample_days is not None and similar.sample_days > 0:
        return None  # We have some data, no need for fallback

    return Recommendation(
        text=(
            "Historical comparison data for conditions like today "
            "is being built \u2014 check back soon for personalized insights"
        ),
        severity=Severity.NEUTRAL,
        metric="similar_day_no_data",
        value=None,
    )


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
    similar: Optional[SimilarDayInsight] = None,
    max_insights: int = 5,
) -> List[Recommendation]:
    """Generate ranked list of insights from classified conditions and historical data.

    Always returns at least 1 item (the biking score insight).
    Includes "days like today" contextual insights when similar-day data
    is available.
    """
    candidates: List[Optional[Recommendation]] = [
        _insight_biking_score(biking_score, classified),
        _insight_ride_volume(classified, impact),
        _insight_duration(classified, impact),
        _insight_comparison_to_best(classified, impact, biking_score),
        _insight_missing_data(classified, impact),
    ]

    # Add "days like today" insights when available
    if similar is not None:
        candidates.extend([
            _insight_similar_day_volume(classified, similar),
            _insight_similar_day_duration(classified, similar),
            _insight_similar_day_peak(classified, similar),
            _insight_similar_day_no_data(classified, similar),
        ])

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


def _current_month_and_day_type() -> tuple[int, str]:
    """Return current month (1-12) and day type ('weekday' or 'weekend').

    Extracted to a function for testability — can be mocked in tests.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    month = now.month
    day_type = "weekend" if now.weekday() >= 5 else "weekday"
    return month, day_type


def get_recommendations(
    conditions: WeatherConditions,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> RecommendationResult:
    """Generate recommendations for current weather conditions.

    This is the main entry point. It classifies conditions, computes a biking
    score, looks up historical impact, looks up similar-day stats, and
    generates ranked insights.
    """
    classified = classify_conditions(conditions)
    biking_score = compute_biking_score(classified)

    impact = lookup_historical_impact(
        location=conditions.location,
        hour=conditions.hour,
        weather_category=classified.weather_category.value,
        conn=conn,
    )

    # Look up "days like today" stats
    month, day_type = _current_month_and_day_type()
    mart_temp_band = _TEMP_BAND_TO_MART.get(
        classified.temperature_band.value, classified.temperature_band.value
    )
    mart_precip = _PRECIP_TO_MART.get(
        classified.precipitation_intensity.value,
        classified.precipitation_intensity.value,
    )
    similar = lookup_similar_day_stats(
        location=conditions.location,
        month=month,
        day_type=day_type,
        temperature_band=mart_temp_band,
        precipitation_intensity=mart_precip,
        conn=conn,
    )

    recommendations = generate_insights(
        classified, impact, biking_score, similar=similar
    )

    return RecommendationResult(
        biking_score=biking_score,
        recommendations=recommendations,
        classified=classified,
    )
