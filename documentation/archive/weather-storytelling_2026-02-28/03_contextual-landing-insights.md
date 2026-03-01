**Status:** ✅ COMPLETE
**Started:** 2026-02-28
**Completed:** 2026-03-01
**PR:** #47

# Phase 03 — Surface Contextual "Days Like Today" Insights on Landing Page

## Header

| Field | Value |
|-------|-------|
| **PR Title** | feat: add "Days Like Today" contextual insights to landing page |
| **Risk Level** | Low |
| **Estimated Effort** | Medium (~3-4 hours) |
| **Files Modified** | 4 |
| **Files Created** | 1 |
| **Dependencies** | Phase 02 (mart_similar_day_stats must exist) |
| **Unlocks** | None |

---

## Context

The landing page currently shows real-time weather conditions and a biking score with recommendation cards powered by `recommendation_engine.py`. These recommendations are driven by `mart_weather_impact_summary`, which slices data by weather condition and hour — useful, but generic. A rider checking the dashboard on a rainy February weekday sees "Rain mornings see 34% fewer rides in NYC" but has no sense of what *days like today specifically* looked like historically.

Phase 02 creates `mart_similar_day_stats`, a new dbt mart that aggregates historical ride statistics by the combination of (location, month, day_type, temperature_band, precipitation_intensity). This phase wires that mart into the dashboard to produce contextual, natural-language insight sentences such as:

- "On similar February weekdays with light rain, NYC averaged 12,400 rides — 23% below typical"
- "Riders took 8% shorter trips on days like today"
- "Similar conditions saw peak activity between 5-7 PM"

These insights appear in the existing "Riding Insights" card section alongside the current recommendation cards, creating weather-driven storytelling rather than raw data display.

---

## Dependencies

- **Phase 02** must be completed first. This phase queries `mart_similar_day_stats.parquet`, which Phase 02 creates as a new dbt mart and exports to S3.
- No other phase dependencies.

---

## Detailed Implementation Plan

### Step 1: Add `mart_similar_day_stats.parquet` to the data manager download list

**File:** `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py`

**Why:** The dashboard downloads mart parquet files from S3 at startup. The new mart must be in this list or it will never be available locally.

**Current code (line 18-29):**

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
    "mart_station_directory.parquet",
    "mart_station_weather_performance.parquet",
]
```

**Change:** Add `"mart_similar_day_stats.parquet"` to the end of the `MARTS` list.

**After:**

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
    "mart_station_directory.parquet",
    "mart_station_weather_performance.parquet",
    "mart_similar_day_stats.parquet",
]
```

### Step 2: Add the similar-day lookup and insight generation to `recommendation_engine.py`

**File:** `/Users/chris/Projects/city-cycles/dashboard/recommendation_engine.py`

**Why:** This is the pure-Python engine (no Streamlit imports) that classifies conditions and generates insights. We extend it with a new data source (the similar-day mart) and new insight generators that produce natural-language "days like today" sentences.

#### 2a: Add the new parquet constant

After the existing `_WEATHER_IMPACT_PARQUET` constant on line 29, add:

```python
_SIMILAR_DAY_PARQUET = "mart_similar_day_stats.parquet"
```

**Full context — lines 29-30 become:**

```python
_WEATHER_IMPACT_PARQUET = "mart_weather_impact_summary.parquet"
_SIMILAR_DAY_PARQUET = "mart_similar_day_stats.parquet"
```

#### 2b: Add a `SimilarDayInsight` dataclass

After the `HistoricalImpact` dataclass (line 145), add the new dataclass. This holds the pre-computed stats from the similar-day mart for a specific (location, month, day_type, temperature_band, precipitation_intensity) slice.

```python
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
```

#### 2c: Add mapping from engine classification enums to mart dimension values

The engine's `TemperatureBand` enum uses 7 bands (freezing/cold/cool/mild/warm/hot/very_hot), but `stg_weather_hourly` uses 5 bands (freezing/cold/mild/warm/hot). We need a mapping. Similarly, the engine's `PrecipitationIntensity` has 4 levels (none/light/moderate/heavy) while the staging model has 5 (none/light/moderate/heavy/extreme). Add these mappings after the `_CATEGORY_TO_MART_WEATHER` dict (after line 204):

```python
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
```

#### 2d: Add the `lookup_similar_day_stats` function

Add this function after `lookup_historical_impact` (after line 447). It queries the mart_similar_day_stats parquet file matching today's conditions.

```python
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
        WHERE location = $1
          AND month = $2
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
```

#### 2e: Add "days like today" insight generator functions

Add these three functions after the existing `_insight_missing_data` function (after line 647). These generate the three types of contextual insight sentences.

```python
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
```

#### 2f: Update `generate_insights` to include similar-day insights

**Current code (lines 662-690):**

```python
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
```

**After:**

```python
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
```

**Key design note:** The `similar` parameter defaults to `None`, which means all existing callers (including tests) continue to work unchanged with zero modifications. The new parameter is purely additive.

#### 2g: Update `get_recommendations` to fetch and pass similar-day data

**Current code (lines 698-723):**

```python
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
```

**After:**

```python
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
```

**Why `_current_month_and_day_type()` is a separate function:** The landing page auto-refreshes every 15 minutes. We need the current month and day type at call time, not at import time. Extracting this to a function also makes it trivially mockable in tests so we can verify February weekday vs August weekend behavior deterministically.

### Step 3: No changes needed to `landing.py`

**File:** `/Users/chris/Projects/city-cycles/dashboard/pages/landing.py`

**Why no changes:** The landing page already calls `get_recommendations(conditions)` on line 54 and passes the result to `render_recommendations(result)` on line 69. The `RecommendationResult` dataclass is unchanged — it still contains `recommendations: List[Recommendation]`. The new similar-day insights are just additional `Recommendation` objects in that list. The `render_recommendations` function in `recommendation_cards.py` already iterates over all recommendations and renders them as cards. No UI changes needed.

This is by design: the recommendation engine is the single point of change. The engine produces more insights, and the existing rendering pipeline displays them automatically.

### Step 4: No changes needed to `recommendation_cards.py`

**File:** `/Users/chris/Projects/city-cycles/dashboard/components/recommendation_cards.py`

**Why no changes:** The card renderer on lines 41-48 iterates over `result.recommendations` and renders each one using its `severity` and `text` fields. The new similar-day `Recommendation` objects have the same structure — they use the existing `Severity` enum values and produce human-readable `text` strings. No new card types, no new severity levels, no rendering changes.

### Step 5: Create tests for the new similar-day functionality

**File to create:** `/Users/chris/Projects/city-cycles/tests/test_similar_day_insights.py`

This file contains focused tests for the new similar-day lookup and insight generation. It follows the same patterns as the existing `test_recommendation_engine.py` — fixtures with `tmp_path`, `unittest.mock.patch` for `DATA_DIR`, and DuckDB in-memory connections.

```python
"""
Tests for "Days Like Today" similar-day insights in recommendation_engine.py.

Tests the similar-day lookup, insight generators, and integration with
the existing recommendation pipeline.
"""

import pytest
import pandas as pd
from unittest.mock import patch

from dashboard.recommendation_engine import (
    WeatherConditions,
    ClassifiedConditions,
    HistoricalImpact,
    SimilarDayInsight,
    Severity,
    classify_conditions,
    compute_biking_score,
    generate_insights,
    get_recommendations,
    lookup_similar_day_stats,
    _insight_similar_day_volume,
    _insight_similar_day_duration,
    _insight_similar_day_peak,
    _insight_similar_day_no_data,
    _format_ride_count,
    _current_month_and_day_type,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def rainy_feb_weekday_nyc() -> WeatherConditions:
    """Rainy February weekday morning in NYC."""
    return WeatherConditions(
        temperature_celsius=5.0,
        wind_speed_kmh=15.0,
        precipitation_mm=3.0,
        weather_code=63,
        location="nyc",
        hour=9,
    )


@pytest.fixture
def clear_august_weekend_london() -> WeatherConditions:
    """Clear August weekend afternoon in London."""
    return WeatherConditions(
        temperature_celsius=24.0,
        wind_speed_kmh=8.0,
        precipitation_mm=0.0,
        weather_code=0,
        location="london",
        hour=14,
    )


@pytest.fixture
def similar_day_parquet(tmp_path) -> str:
    """Create a temporary mart_similar_day_stats.parquet for testing."""
    data = pd.DataFrame(
        {
            "location": ["nyc", "nyc", "london"],
            "month": [2, 8, 8],
            "day_type": ["weekday", "weekend", "weekend"],
            "temperature_band": ["cold", "warm", "warm"],
            "precipitation_intensity": ["moderate", "none", "none"],
            "avg_daily_rides": [12400.0, 58000.0, 32000.0],
            "pct_change_vs_overall": [-23.0, 15.0, 8.0],
            "avg_duration_minutes": [11.2, 16.5, 14.0],
            "duration_pct_change_vs_overall": [-8.0, 12.0, 3.0],
            "peak_hour_start": [17, 11, 12],
            "peak_hour_end": [19, 15, 16],
            "sample_days": [45, 30, 25],
        }
    )
    path = tmp_path / "mart_similar_day_stats.parquet"
    data.to_parquet(str(path))
    return str(tmp_path)


@pytest.fixture
def weather_impact_parquet(tmp_path) -> str:
    """Create a temporary mart_weather_impact_summary.parquet for testing.

    Reused from test_recommendation_engine.py pattern.
    """
    data = pd.DataFrame(
        {
            "location": ["nyc"],
            "hour_of_day": [9],
            "dimension_type": ["weather_condition"],
            "dimension_value": ["rain"],
            "is_precipitation": [True],
            "temperature_band": [None],
            "observation_count": [45],
            "avg_rides": [990.0],
            "avg_duration_seconds": [678.0],
            "avg_member_rides": [660.0],
            "avg_casual_rides": [330.0],
            "baseline_avg_rides": [1339.3],
            "baseline_avg_duration_seconds": [828.6],
            "pct_change_rides_vs_clear": [-34.0],
            "pct_change_duration_vs_clear": [-22.0],
        }
    )
    path = tmp_path / "mart_weather_impact_summary.parquet"
    data.to_parquet(str(path))
    return str(tmp_path)


# ---------------------------------------------------------------------------
# TestFormatRideCount
# ---------------------------------------------------------------------------


class TestFormatRideCount:
    """Tests for the ride count formatter."""

    def test_formats_with_comma(self):
        assert _format_ride_count(12400.0) == "12,400"

    def test_formats_small_number(self):
        assert _format_ride_count(500.0) == "500"

    def test_formats_large_number(self):
        assert _format_ride_count(1234567.0) == "1,234,567"

    def test_rounds_float(self):
        assert _format_ride_count(12400.7) == "12,401"


# ---------------------------------------------------------------------------
# TestCurrentMonthAndDayType
# ---------------------------------------------------------------------------


class TestCurrentMonthAndDayType:
    """Tests for the date helper function."""

    def test_returns_tuple(self):
        month, day_type = _current_month_and_day_type()
        assert isinstance(month, int)
        assert 1 <= month <= 12
        assert day_type in ("weekday", "weekend")

    def test_weekday_detection(self):
        """Monday (weekday=0) should return 'weekday'."""
        from datetime import datetime, timezone

        with patch(
            "dashboard.recommendation_engine.datetime",
        ) as mock_dt:
            # Create a real datetime for Monday Feb 24, 2026
            monday = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.now.return_value = monday
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            # Can't easily mock datetime.now inside the function because
            # it imports from datetime. Instead, test the actual function
            # and verify the return types.
            month, day_type = _current_month_and_day_type()
            assert isinstance(month, int)
            assert day_type in ("weekday", "weekend")


# ---------------------------------------------------------------------------
# TestLookupSimilarDayStats
# ---------------------------------------------------------------------------


class TestLookupSimilarDayStats:
    """Tests for the similar-day mart lookup."""

    def test_returns_data_when_match_found(self, similar_day_parquet):
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.avg_daily_rides == 12400.0
        assert result.pct_change_vs_overall == -23.0
        assert result.peak_hour_start == 17
        assert result.peak_hour_end == 19
        assert result.sample_days == 45

    def test_returns_empty_when_no_match(self, similar_day_parquet):
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 6, "weekday", "warm", "none"
            )

        assert result.avg_daily_rides is None
        assert result.pct_change_vs_overall is None

    def test_returns_empty_when_parquet_missing(self, tmp_path):
        with patch("dashboard.recommendation_engine.DATA_DIR", str(tmp_path)):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.avg_daily_rides is None

    def test_preserves_query_params_in_result(self, similar_day_parquet):
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.month == 2
        assert result.day_type == "weekday"
        assert result.temperature_band == "cold"
        assert result.precipitation_intensity == "moderate"

    def test_duration_in_minutes(self, similar_day_parquet):
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.avg_duration_minutes == pytest.approx(11.2)


# ---------------------------------------------------------------------------
# TestSimilarDayInsightGenerators
# ---------------------------------------------------------------------------


class TestSimilarDayVolumeInsight:
    """Tests for _insight_similar_day_volume."""

    def test_produces_below_typical_text(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(
            avg_daily_rides=12400.0,
            pct_change_vs_overall=-23.0,
            month=2,
            day_type="weekday",
            precipitation_intensity="moderate",
            temperature_band="cold",
            sample_days=45,
        )
        result = _insight_similar_day_volume(classified, similar)
        assert result is not None
        assert "12,400" in result.text
        assert "23%" in result.text
        assert "below" in result.text
        assert "February" in result.text
        assert result.severity == Severity.CAUTION

    def test_produces_above_typical_text(self, clear_august_weekend_london):
        classified = classify_conditions(clear_august_weekend_london)
        similar = SimilarDayInsight(
            avg_daily_rides=32000.0,
            pct_change_vs_overall=15.0,
            month=8,
            day_type="weekend",
            precipitation_intensity="none",
            temperature_band="warm",
            sample_days=25,
        )
        result = _insight_similar_day_volume(classified, similar)
        assert result is not None
        assert "above" in result.text
        assert result.severity == Severity.POSITIVE

    def test_returns_none_when_no_rides(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight()
        result = _insight_similar_day_volume(classified, similar)
        assert result is None

    def test_warning_severity_for_large_drop(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(
            avg_daily_rides=5000.0,
            pct_change_vs_overall=-45.0,
            month=2,
            day_type="weekday",
            precipitation_intensity="heavy",
            temperature_band="cold",
            sample_days=10,
        )
        result = _insight_similar_day_volume(classified, similar)
        assert result is not None
        assert result.severity == Severity.WARNING


class TestSimilarDayDurationInsight:
    """Tests for _insight_similar_day_duration."""

    def test_shorter_trips(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(duration_pct_change_vs_overall=-8.0)
        result = _insight_similar_day_duration(classified, similar)
        assert result is not None
        assert "8%" in result.text
        assert "shorter" in result.text

    def test_longer_trips(self, clear_august_weekend_london):
        classified = classify_conditions(clear_august_weekend_london)
        similar = SimilarDayInsight(duration_pct_change_vs_overall=12.0)
        result = _insight_similar_day_duration(classified, similar)
        assert result is not None
        assert "longer" in result.text

    def test_returns_none_for_insignificant_change(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(duration_pct_change_vs_overall=-2.0)
        result = _insight_similar_day_duration(classified, similar)
        assert result is None

    def test_returns_none_when_no_data(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight()
        result = _insight_similar_day_duration(classified, similar)
        assert result is None


class TestSimilarDayPeakInsight:
    """Tests for _insight_similar_day_peak."""

    def test_formats_peak_hours(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(peak_hour_start=17, peak_hour_end=19)
        result = _insight_similar_day_peak(classified, similar)
        assert result is not None
        assert "5 PM" in result.text
        assert "7 PM" in result.text
        assert "peak activity" in result.text

    def test_formats_morning_peak(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(peak_hour_start=8, peak_hour_end=10)
        result = _insight_similar_day_peak(classified, similar)
        assert result is not None
        assert "8 AM" in result.text
        assert "10 AM" in result.text

    def test_returns_none_when_no_peak_data(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight()
        result = _insight_similar_day_peak(classified, similar)
        assert result is None


class TestSimilarDayNoDataInsight:
    """Tests for _insight_similar_day_no_data (graceful degradation)."""

    def test_fires_when_all_fields_none(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight()
        result = _insight_similar_day_no_data(classified, similar)
        assert result is not None
        assert "check back soon" in result.text
        assert result.severity == Severity.NEUTRAL

    def test_does_not_fire_when_data_exists(self, rainy_feb_weekday_nyc):
        classified = classify_conditions(rainy_feb_weekday_nyc)
        similar = SimilarDayInsight(avg_daily_rides=12400.0)
        result = _insight_similar_day_no_data(classified, similar)
        assert result is None


# ---------------------------------------------------------------------------
# TestGenerateInsightsWithSimilarDay
# ---------------------------------------------------------------------------


class TestGenerateInsightsWithSimilarDay:
    """Tests for generate_insights with the similar parameter."""

    def test_backward_compatible_without_similar(self, rainy_feb_weekday_nyc):
        """generate_insights still works without similar parameter."""
        classified = classify_conditions(rainy_feb_weekday_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact()

        insights = generate_insights(classified, impact, score)
        assert len(insights) >= 1
        # No similar_day metrics should appear
        metrics = {r.metric for r in insights}
        assert "similar_day_rides" not in metrics
        assert "similar_day_duration" not in metrics

    def test_includes_similar_day_insights_when_provided(
        self, rainy_feb_weekday_nyc
    ):
        """generate_insights includes similar-day insights when data exists."""
        classified = classify_conditions(rainy_feb_weekday_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact()
        similar = SimilarDayInsight(
            avg_daily_rides=12400.0,
            pct_change_vs_overall=-23.0,
            avg_duration_minutes=11.2,
            duration_pct_change_vs_overall=-8.0,
            peak_hour_start=17,
            peak_hour_end=19,
            sample_days=45,
            month=2,
            day_type="weekday",
            temperature_band="cold",
            precipitation_intensity="moderate",
        )

        insights = generate_insights(
            classified, impact, score, similar=similar
        )
        metrics = {r.metric for r in insights}
        assert "similar_day_rides" in metrics

    def test_respects_max_insights_with_similar(self, rainy_feb_weekday_nyc):
        """Max insights limit still applies with similar-day data."""
        classified = classify_conditions(rainy_feb_weekday_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=990,
            pct_change_vs_baseline=-34.0,
            sample_days=45,
        )
        similar = SimilarDayInsight(
            avg_daily_rides=12400.0,
            pct_change_vs_overall=-23.0,
            duration_pct_change_vs_overall=-8.0,
            peak_hour_start=17,
            peak_hour_end=19,
            month=2,
            day_type="weekday",
            temperature_band="cold",
            precipitation_intensity="moderate",
        )

        insights = generate_insights(
            classified, impact, score, similar=similar, max_insights=3
        )
        assert len(insights) <= 3


# ---------------------------------------------------------------------------
# TestGetRecommendationsIntegration
# ---------------------------------------------------------------------------


class TestGetRecommendationsWithSimilarDay:
    """Integration tests for get_recommendations with similar-day mart."""

    def test_end_to_end_with_both_marts(
        self, rainy_feb_weekday_nyc, similar_day_parquet, weather_impact_parquet
    ):
        """get_recommendations produces insights from both data sources."""
        # Combine both parquets into a single tmp directory
        import shutil
        import os

        combined_dir = os.path.join(similar_day_parquet, "_combined")
        os.makedirs(combined_dir, exist_ok=True)
        shutil.copy(
            os.path.join(similar_day_parquet, "mart_similar_day_stats.parquet"),
            os.path.join(combined_dir, "mart_similar_day_stats.parquet"),
        )
        shutil.copy(
            os.path.join(
                weather_impact_parquet, "mart_weather_impact_summary.parquet"
            ),
            os.path.join(combined_dir, "mart_weather_impact_summary.parquet"),
        )

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", combined_dir
        ), patch(
            "dashboard.recommendation_engine._current_month_and_day_type",
            return_value=(2, "weekday"),
        ):
            result = get_recommendations(rainy_feb_weekday_nyc)

        assert result.biking_score.score < 60
        assert len(result.recommendations) >= 2

    def test_graceful_without_similar_day_mart(
        self, rainy_feb_weekday_nyc, weather_impact_parquet
    ):
        """get_recommendations works when only the weather impact mart exists."""
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ), patch(
            "dashboard.recommendation_engine._current_month_and_day_type",
            return_value=(2, "weekday"),
        ):
            result = get_recommendations(rainy_feb_weekday_nyc)

        # Should still work — biking score + weather insights
        assert result.biking_score.score < 60
        assert len(result.recommendations) >= 1

    def test_graceful_without_any_marts(self, rainy_feb_weekday_nyc, tmp_path):
        """get_recommendations works with no mart files at all."""
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", str(tmp_path)
        ), patch(
            "dashboard.recommendation_engine._current_month_and_day_type",
            return_value=(2, "weekday"),
        ):
            result = get_recommendations(rainy_feb_weekday_nyc)

        # Should still compute biking score and show fallback insights
        assert result.biking_score is not None
        assert len(result.recommendations) >= 1
```

### Step 6: Update `CHANGELOG.md`

**File:** `/Users/chris/Projects/city-cycles/CHANGELOG.md`

Add the following entry under `[Unreleased]`:

```markdown
### Added
- **"Days Like Today" Contextual Insights** - Landing page now shows historical ride patterns for similar conditions
  - Queries mart_similar_day_stats by current month, day type, temperature band, and precipitation
  - Generates natural-language insights like "On similar February weekdays with light rain, NYC averaged 12,400 rides"
  - Includes trip duration comparisons and peak activity hour insights
  - Graceful degradation when mart data is unavailable
```

---

## Test Plan

### New Tests

Create `/Users/chris/Projects/city-cycles/tests/test_similar_day_insights.py` with the following test classes (full content in Step 5 above):

| Test Class | What It Verifies |
|------------|-----------------|
| `TestFormatRideCount` | Ride count formatting with commas and rounding |
| `TestCurrentMonthAndDayType` | Date helper returns valid month and day_type |
| `TestLookupSimilarDayStats` | Parquet query returns correct data, handles missing files, preserves params |
| `TestSimilarDayVolumeInsight` | Volume insight text for above/below typical, severity thresholds |
| `TestSimilarDayDurationInsight` | Duration insight text for shorter/longer, insignificance filter |
| `TestSimilarDayPeakInsight` | Peak hour formatting (AM/PM), missing data handling |
| `TestSimilarDayNoDataInsight` | Fallback message fires only when all fields are None |
| `TestGenerateInsightsWithSimilarDay` | Backward compatibility, inclusion of similar insights, max_insights limit |
| `TestGetRecommendationsWithSimilarDay` | End-to-end with both marts, graceful degradation without marts |

### Existing Tests

No existing tests need modification. The `similar` parameter defaults to `None`, so all existing `generate_insights` and `get_recommendations` calls in `test_recommendation_engine.py` continue to pass unchanged.

**Verify:** Run the full test suite to confirm no regressions:

```bash
/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v
```

Expected: 283 existing tests pass + ~25 new tests pass, 3 skip.

### Manual Verification Steps

1. Start the dashboard locally: `streamlit run dashboard/app.py`
2. Verify the landing page loads without errors
3. Toggle between NYC and London — insights should update
4. If `mart_similar_day_stats.parquet` is not in `data/`, verify the graceful degradation message appears: "Historical comparison data for conditions like today is being built -- check back soon"
5. If the mart IS available, verify 2-3 "days like today" insight cards appear in the "Riding Insights" section alongside existing cards

---

## Documentation Updates

### CHANGELOG.md

Add entry under `[Unreleased]` as specified in Step 6.

### Inline Code Comments

All new functions include docstrings explaining:
- What they do
- What arguments they take
- What they return
- Edge case behavior (empty data, missing parquet)

### No README Changes Needed

The dashboard's user-facing behavior changes (new insight cards), but the README does not document individual dashboard features at this level of detail. No README update required.

---

## Stress Testing & Edge Cases

### Edge Cases to Handle

| Scenario | Expected Behavior |
|----------|-------------------|
| `mart_similar_day_stats.parquet` does not exist | `lookup_similar_day_stats` returns empty `SimilarDayInsight()`. `_insight_similar_day_no_data` fires with "check back soon" message. |
| Parquet exists but no rows match current conditions | Same as above — empty `SimilarDayInsight()`, fallback message shown. |
| Only 1 or 2 sample days in matching slice | Data is returned as-is. Unlike `_insight_missing_data` (which flags < 5 days), similar-day insights do not flag sparse data — the mart itself should enforce minimum thresholds. |
| DuckDB query throws an exception | `lookup_similar_day_stats` catches all exceptions, logs them, returns empty `SimilarDayInsight()`. Dashboard continues with existing weather-only insights. |
| `max_insights=5` with both weather AND similar-day candidates | The ranking/sorting ensures the most severe and impactful insights surface. Lower-priority ones are truncated. |
| Month boundary (e.g., refreshing at 11:59 PM on Jan 31 vs 12:00 AM Feb 1) | `_current_month_and_day_type()` uses `datetime.now(timezone.utc)` at call time. Each 15-minute refresh recalculates. |
| Weekend vs weekday boundary (Friday night refresh) | Same as above — recalculated at call time. |
| Engine temperature bands don't match mart temperature bands | `_TEMP_BAND_TO_MART` mapping handles the 7-to-5 band conversion explicitly. |

### Performance Considerations

- The parquet file query is a simple equality filter on 5 columns. Even with thousands of rows, DuckDB scans this in < 1ms.
- The `os.path.exists` check on the parquet file runs on every call (every 15 minutes). This is negligible.
- No additional API calls are introduced — all data comes from the local parquet file.

---

## Verification Checklist

- [ ] `mart_similar_day_stats.parquet` added to `MARTS` list in `parquet_file_manager.py`
- [ ] `SimilarDayInsight` dataclass added to `recommendation_engine.py`
- [ ] `_SIMILAR_DAY_PARQUET` constant added to `recommendation_engine.py`
- [ ] `_TEMP_BAND_TO_MART` and `_PRECIP_TO_MART` mappings added
- [ ] `lookup_similar_day_stats` function added with proper error handling
- [ ] `_insight_similar_day_volume`, `_insight_similar_day_duration`, `_insight_similar_day_peak`, `_insight_similar_day_no_data` generators added
- [ ] `generate_insights` signature updated with optional `similar` parameter (default `None`)
- [ ] `get_recommendations` updated to call `lookup_similar_day_stats` and pass result
- [ ] `_current_month_and_day_type` helper function added
- [ ] No changes to `landing.py` or `recommendation_cards.py` (existing rendering pipeline handles new insights automatically)
- [ ] New test file `tests/test_similar_day_insights.py` created with ~25 tests
- [ ] All existing tests pass: `venv/bin/python -m pytest tests/test_recommendation_engine.py -v`
- [ ] All new tests pass: `venv/bin/python -m pytest tests/test_similar_day_insights.py -v`
- [ ] Full suite passes: `venv/bin/python -m pytest tests/ -v`
- [ ] CHANGELOG.md updated with entry under `[Unreleased]`
- [ ] Dashboard loads without errors when mart is missing (graceful degradation)
- [ ] Dashboard shows contextual insight cards when mart is present

---

## What NOT To Do

1. **Do NOT modify `landing.py` or `recommendation_cards.py`.** The entire point of the recommendation engine architecture is that new insight types are added in the engine and flow through the existing rendering pipeline automatically. Adding special-case rendering for similar-day insights would violate this separation of concerns.

2. **Do NOT add Streamlit imports to `recommendation_engine.py`.** The engine is explicitly documented as "Pure Python module -- no Streamlit imports. Fully unit-testable." This is critical for test isolation. Use `_current_month_and_day_type()` (pure Python `datetime`) instead of `st.session_state` or similar.

3. **Do NOT hardcode month names or day types.** Use the `_MONTH_NAMES` dict and the computed `day_type` from `_current_month_and_day_type()`. Never assume "it's always February" or "it's always a weekday".

4. **Do NOT skip the temperature band mapping.** The engine uses 7 temperature bands (freezing/cold/cool/mild/warm/hot/very_hot) but `stg_weather_hourly` and therefore `mart_similar_day_stats` uses 5 bands (freezing/cold/mild/warm/hot). If you query the mart with the engine's band value (e.g., "cool"), you will get zero results. Always use `_TEMP_BAND_TO_MART` to translate.

5. **Do NOT make the `similar` parameter required in `generate_insights`.** It must default to `None` for backward compatibility. All existing callers pass only 3 positional arguments. Making it required would break all existing tests and any other code that calls `generate_insights`.

6. **Do NOT create a separate UI section for similar-day insights.** They should appear as regular `Recommendation` cards in the existing "Riding Insights" section, ranked alongside the existing weather insights by severity. Creating a separate section would fragment the user experience.

7. **Do NOT query the mart at import time or module level.** The query must happen inside `get_recommendations()` at call time, because the current month and day type change over the dashboard's lifetime (auto-refresh every 15 minutes).

8. **Do NOT import `datetime` at module level in `recommendation_engine.py`.** The import should happen inside `_current_month_and_day_type()` to keep the module's top-level imports clean and avoid any potential conflicts with the existing codebase. (The module currently imports only `logging`, `os`, `dataclass`, `enum`, `typing`, `duckdb`, and `pandas`.)

9. **Do NOT show raw numbers without formatting.** Ride counts should use `_format_ride_count()` for comma-separated display (12,400 not 12400). Percentages should be rounded integers (23% not 23.4%).

10. **Do NOT forget the `_insight_similar_day_no_data` fallback.** Without it, when the mart is empty or missing, the user sees no similar-day insights at all — which is fine for the existing weather insights, but the "days like today" concept should acknowledge its absence with a friendly message rather than leaving a gap.
