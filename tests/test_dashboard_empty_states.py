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
