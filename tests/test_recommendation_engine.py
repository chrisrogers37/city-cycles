"""
Tests for dashboard/recommendation_engine.py.

Tests the condition classifier, biking score calculator, insight generator,
and historical data lookup. No Streamlit imports, no S3 calls, no side effects.
"""

import pytest
import duckdb
import pandas as pd

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
    """Create a temporary mart_weather_impact_summary.parquet for testing.

    Uses the actual mart schema with dimension_type/dimension_value columns.
    """
    data = pd.DataFrame(
        {
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
        }
    )
    path = tmp_path / "mart_weather_impact_summary.parquet"
    data.to_parquet(str(path))
    return str(tmp_path)


# ---------------------------------------------------------------------------
# TestWeatherCodeClassifier
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


# ---------------------------------------------------------------------------
# TestTemperatureClassifier
# ---------------------------------------------------------------------------


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
        assert classify_temperature(0.0) == TemperatureBand.COLD
        assert classify_temperature(10.0) == TemperatureBand.COOL
        assert classify_temperature(15.0) == TemperatureBand.MILD
        assert classify_temperature(20.0) == TemperatureBand.WARM
        assert classify_temperature(25.0) == TemperatureBand.HOT
        assert classify_temperature(30.0) == TemperatureBand.VERY_HOT


# ---------------------------------------------------------------------------
# TestWindClassifier
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TestPrecipitationClassifier
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TestClassifyConditions
# ---------------------------------------------------------------------------


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
        conditions_excellent = WeatherConditions(
            temperature_celsius=22,
            wind_speed_kmh=5,
            precipitation_mm=0,
            weather_code=0,
            location="nyc",
            hour=10,
        )
        conditions_poor = WeatherConditions(
            temperature_celsius=-5,
            wind_speed_kmh=60,
            precipitation_mm=15,
            weather_code=75,
            location="nyc",
            hour=10,
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
        """lookup_historical_impact should return populated HistoricalImpact."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = lookup_historical_impact("nyc", 9, "clear")

        assert result.avg_rides == 1500.0
        assert result.pct_change_vs_baseline == 12.0
        assert result.sample_days == 120

    def test_returns_empty_when_no_match(self, weather_impact_parquet):
        """lookup_historical_impact should return empty when no data matches."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = lookup_historical_impact("nyc", 23, "thunderstorm")

        assert result.avg_rides is None
        assert result.pct_change_vs_baseline is None

    def test_returns_empty_when_parquet_missing(self, tmp_path):
        """lookup_historical_impact should return empty when parquet file missing."""
        from unittest.mock import patch

        with patch("dashboard.recommendation_engine.DATA_DIR", str(tmp_path)):
            result = lookup_historical_impact("nyc", 9, "clear")

        assert result.avg_rides is None

    def test_accepts_external_connection(self, weather_impact_parquet):
        """lookup_historical_impact should work with an external DuckDB connection."""
        from unittest.mock import patch

        conn = duckdb.connect(":memory:")
        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = lookup_historical_impact("london", 14, "rain", conn=conn)
        conn.close()

        assert result.pct_change_vs_baseline == -28.0

    def test_heavy_rain_maps_to_rain_in_mart(self, weather_impact_parquet):
        """heavy_rain category should map to 'rain' in mart lookup."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = lookup_historical_impact("nyc", 9, "heavy_rain")

        # "heavy_rain" maps to "rain" via _CATEGORY_TO_MART_WEATHER
        assert result.avg_rides == 990.0
        assert result.pct_change_vs_baseline == -34.0

    def test_heavy_snow_maps_to_snow_in_mart(self, weather_impact_parquet):
        """heavy_snow category should map to 'snow' in mart lookup."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = lookup_historical_impact("nyc", 8, "heavy_snow")

        # "heavy_snow" maps to "snow" via _CATEGORY_TO_MART_WEATHER
        assert result.avg_rides == 525.0
        assert result.pct_change_vs_baseline == -65.0

    def test_duration_converted_to_minutes(self, weather_impact_parquet):
        """avg_duration_minutes should be avg_duration_seconds / 60."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = lookup_historical_impact("nyc", 9, "clear")

        # 870 seconds / 60 = 14.5 minutes
        assert result.avg_duration_minutes == pytest.approx(14.5)


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
            avg_rides=1500,
            pct_change_vs_baseline=12.0,
            avg_duration_minutes=14.5,
            duration_pct_change=5.0,
            sample_days=120,
        )

        insights = generate_insights(classified, impact, score, max_insights=2)
        assert len(insights) <= 2

    def test_warnings_ranked_first(self, snowy_morning_nyc):
        """Insights with WARNING severity should appear before NEUTRAL ones."""
        classified = classify_conditions(snowy_morning_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=525,
            pct_change_vs_baseline=-65.0,
            avg_duration_minutes=8.2,
            duration_pct_change=-43.0,
            sample_days=3,
        )

        insights = generate_insights(classified, impact, score)
        severities = [r.severity for r in insights]

        warning_indices = [
            i for i, s in enumerate(severities) if s == Severity.WARNING
        ]
        neutral_indices = [
            i for i, s in enumerate(severities) if s == Severity.NEUTRAL
        ]

        if warning_indices and neutral_indices:
            assert max(warning_indices) < min(neutral_indices)

    def test_ride_volume_insight_text_for_negative(self):
        """Negative pct_change should produce 'fewer rides' text."""
        conditions = WeatherConditions(
            temperature_celsius=12,
            wind_speed_kmh=25,
            precipitation_mm=5,
            weather_code=63,
            location="nyc",
            hour=9,
        )
        classified = classify_conditions(conditions)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=990,
            pct_change_vs_baseline=-34.0,
            avg_duration_minutes=11.3,
            duration_pct_change=-22.0,
            sample_days=45,
        )

        insights = generate_insights(classified, impact, score)
        ride_insights = [r for r in insights if r.metric == "rides_impact_pct"]
        assert len(ride_insights) == 1
        assert "fewer rides" in ride_insights[0].text
        assert "34%" in ride_insights[0].text

    def test_missing_data_insight_for_sparse_data(self, snowy_morning_nyc):
        """generate_insights should include data quality notice when sample_days < 5."""
        classified = classify_conditions(snowy_morning_nyc)
        score = compute_biking_score(classified)
        impact = HistoricalImpact(
            avg_rides=525,
            pct_change_vs_baseline=-65.0,
            avg_duration_minutes=8.2,
            duration_pct_change=-43.0,
            sample_days=3,
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
            avg_rides=1500,
            pct_change_vs_baseline=12.0,
            avg_duration_minutes=14.5,
            duration_pct_change=5.0,
            sample_days=120,
        )

        insights = generate_insights(classified, impact, score)
        assert any(r.severity == Severity.POSITIVE for r in insights)


# ---------------------------------------------------------------------------
# TestGetRecommendations (integration)
# ---------------------------------------------------------------------------


class TestGetRecommendations:
    """Integration tests for the main get_recommendations entry point."""

    def test_returns_recommendation_result(
        self, clear_morning_nyc, weather_impact_parquet
    ):
        """get_recommendations should return a RecommendationResult."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = get_recommendations(clear_morning_nyc)

        assert isinstance(result, RecommendationResult)
        assert isinstance(result.biking_score, BikingScore)
        assert isinstance(result.recommendations, list)
        assert isinstance(result.classified, ClassifiedConditions)

    def test_works_without_parquet_file(self, clear_morning_nyc, tmp_path):
        """get_recommendations should work gracefully when parquet file missing."""
        from unittest.mock import patch

        with patch("dashboard.recommendation_engine.DATA_DIR", str(tmp_path)):
            result = get_recommendations(clear_morning_nyc)

        assert result.biking_score.score >= 80
        assert len(result.recommendations) >= 1

    def test_end_to_end_rainy(
        self, rainy_afternoon_london, weather_impact_parquet
    ):
        """Full pipeline for rainy London afternoon should produce adverse insights."""
        from unittest.mock import patch

        with patch(
            "dashboard.recommendation_engine.DATA_DIR", weather_impact_parquet
        ):
            result = get_recommendations(rainy_afternoon_london)

        assert result.biking_score.score < 60
        severities = {r.severity for r in result.recommendations}
        assert Severity.CAUTION in severities or Severity.WARNING in severities
