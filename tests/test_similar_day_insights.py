"""
Tests for "Days Like Today" similar-day insights in recommendation_engine.py.

Tests the similar-day lookup, insight generators, and integration with
the existing recommendation pipeline.
"""

import pytest
import pandas as pd
from unittest.mock import patch

from api.services.recommendation_engine import (
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
            "grain": ["daily", "daily", "daily"],
            "location": ["nyc", "nyc", "london"],
            "month_num": [2, 8, 8],
            "day_type": ["weekday", "weekend", "weekend"],
            "temperature_band": ["cold", "warm", "warm"],
            "precipitation_intensity": ["moderate", "none", "none"],
            "hour_of_day": [None, None, None],
            "sample_days": [45, 30, 25],
            "avg_daily_rides": [12400.0, 58000.0, 32000.0],
            "avg_duration_minutes": [11.2, 16.5, 14.0],
            "avg_member_rides": [8000.0, 35000.0, 0.0],
            "avg_casual_rides": [4400.0, 23000.0, 0.0],
            "pct_change_vs_overall": [-23.0, 15.0, 8.0],
            "duration_pct_change_vs_overall": [-8.0, 12.0, 3.0],
            "peak_hour_start": [17, 11, 12],
            "peak_hour_end": [19, 13, 14],
        }
    )
    path = tmp_path / "mart_similar_day_stats.parquet"
    data.to_parquet(str(path))
    return str(tmp_path)


@pytest.fixture
def weather_impact_parquet(tmp_path) -> str:
    """Create a temporary mart_weather_impact_summary.parquet for testing."""
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


# ---------------------------------------------------------------------------
# TestLookupSimilarDayStats
# ---------------------------------------------------------------------------


class TestLookupSimilarDayStats:
    """Tests for the similar-day mart lookup."""

    def test_returns_data_when_match_found(self, similar_day_parquet):
        with patch(
            "api.services.recommendation_engine.DATA_DIR", similar_day_parquet
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
            "api.services.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 6, "weekday", "warm", "none"
            )

        assert result.avg_daily_rides is None
        assert result.pct_change_vs_overall is None

    def test_returns_empty_when_parquet_missing(self, tmp_path):
        with patch("api.services.recommendation_engine.DATA_DIR", str(tmp_path)):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.avg_daily_rides is None

    def test_preserves_query_params_in_result(self, similar_day_parquet):
        with patch(
            "api.services.recommendation_engine.DATA_DIR", similar_day_parquet
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
            "api.services.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.avg_duration_minutes == pytest.approx(11.2)

    def test_duration_pct_change(self, similar_day_parquet):
        with patch(
            "api.services.recommendation_engine.DATA_DIR", similar_day_parquet
        ):
            result = lookup_similar_day_stats(
                "nyc", 2, "weekday", "cold", "moderate"
            )

        assert result.duration_pct_change_vs_overall == -8.0


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
            "api.services.recommendation_engine.DATA_DIR", combined_dir
        ), patch(
            "api.services.recommendation_engine._current_month_and_day_type",
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
            "api.services.recommendation_engine.DATA_DIR", weather_impact_parquet
        ), patch(
            "api.services.recommendation_engine._current_month_and_day_type",
            return_value=(2, "weekday"),
        ):
            result = get_recommendations(rainy_feb_weekday_nyc)

        # Should still work — biking score + weather insights
        assert result.biking_score.score < 60
        assert len(result.recommendations) >= 1

    def test_graceful_without_any_marts(self, rainy_feb_weekday_nyc, tmp_path):
        """get_recommendations works with no mart files at all."""
        with patch(
            "api.services.recommendation_engine.DATA_DIR", str(tmp_path)
        ), patch(
            "api.services.recommendation_engine._current_month_and_day_type",
            return_value=(2, "weekday"),
        ):
            result = get_recommendations(rainy_feb_weekday_nyc)

        # Should still compute biking score and show fallback insights
        assert result.biking_score is not None
        assert len(result.recommendations) >= 1
