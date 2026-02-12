"""
Tests for the weather extraction module.

Tests extraction/weather.py functions.

Uses mocked HTTP requests and S3 operations -- no real API calls or AWS access.
Because extraction.utils has module-level side effects, imports are done
inside test functions using the same pattern as test_extraction.py.
"""

import pytest
import os
import sys
import json
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _import_weather(mock_boto_client):
    """
    Import extraction.weather with mocked environment and boto3.

    Args:
        mock_boto_client: The MagicMock to return from boto3.client()

    Returns:
        The extraction.weather module (freshly imported)
    """
    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("extraction"):
            del sys.modules[mod_name]

    with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
         patch("boto3.client", return_value=mock_boto_client):
        import extraction.weather as weather_mod
        return weather_mod


def _make_api_response(num_hours: int = 24, city: str = "nyc") -> dict:
    """Create a mock Open-Meteo API response JSON."""
    times = [f"2023-06-{15 + h // 24:02d}T{h % 24:02d}:00" for h in range(num_hours)]

    return {
        "latitude": 40.7 if city == "nyc" else 51.5,
        "longitude": -74.0 if city == "nyc" else -0.13,
        "timezone": "America/New_York" if city == "nyc" else "Europe/London",
        "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
        "hourly": {
            "time": times,
            "temperature_2m": [20.0 + i * 0.5 for i in range(num_hours)],
            "relative_humidity_2m": [65.0] * num_hours,
            "apparent_temperature": [19.0 + i * 0.5 for i in range(num_hours)],
            "precipitation": [0.0] * num_hours,
            "rain": [0.0] * num_hours,
            "snowfall": [0.0] * num_hours,
            "snow_depth": [0.0] * num_hours,
            "weather_code": [0] * num_hours,
            "cloud_cover": [25.0] * num_hours,
            "wind_speed_10m": [10.0] * num_hours,
            "wind_gusts_10m": [15.0] * num_hours,
        },
    }


# ---------------------------------------------------------------------------
# Tests for fetch_historical_weather
# ---------------------------------------------------------------------------

class TestFetchHistoricalWeather:
    """Tests for extraction/weather.py fetch_historical_weather()."""

    def test_fetch_returns_dataframe_with_correct_columns(self):
        """fetch_historical_weather() should return a DataFrame with expected columns."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(24, "nyc")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            df = weather.fetch_historical_weather("nyc", "2023-06-15", "2023-06-15")

        assert len(df) == 24
        assert "timestamp" in df.columns
        assert "city" in df.columns
        assert "temperature_2m" in df.columns
        assert "wind_speed_10m" in df.columns
        assert df["city"].iloc[0] == "nyc"

    def test_fetch_raises_on_invalid_city(self):
        """fetch_historical_weather() should raise ValueError for unknown city."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        with pytest.raises(ValueError, match="Unknown city"):
            weather.fetch_historical_weather("paris", "2023-01-01", "2023-01-01")

    def test_fetch_returns_empty_df_when_no_hourly_data(self):
        """fetch_historical_weather() should return empty DataFrame when API returns no hourly key."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = {"latitude": 40.7, "longitude": -74.0}
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            df = weather.fetch_historical_weather("nyc", "2023-06-15", "2023-06-15")

        assert df.empty

    def test_fetch_sends_correct_api_parameters(self):
        """fetch_historical_weather() should send correct parameters to the API."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(1, "london")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response) as mock_get:
            weather.fetch_historical_weather("london", "2023-01-01", "2023-01-31")

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["latitude"] == 51.5074
        assert params["longitude"] == -0.1278
        assert params["timezone"] == "Europe/London"
        assert params["start_date"] == "2023-01-01"
        assert params["end_date"] == "2023-01-31"


# ---------------------------------------------------------------------------
# Tests for backfill_city
# ---------------------------------------------------------------------------

class TestBackfillCity:
    """Tests for extraction/weather.py backfill_city()."""

    def test_backfill_skips_existing_years(self):
        """backfill_city() should skip years that already exist in S3."""
        mock_s3 = MagicMock()
        # head_object succeeds = file exists
        mock_s3.head_object.return_value = {"ContentLength": 1234}
        mock_s3.exceptions.ClientError = Exception

        weather = _import_weather(mock_s3)

        with patch("extraction.weather.requests.get") as mock_get:
            results = weather.backfill_city("nyc", start_year=2023, end_year=2023)

        # Should NOT make any API calls
        mock_get.assert_not_called()
        assert results["2023"] is False

    def test_backfill_fetches_and_uploads_missing_years(self):
        """backfill_city() should fetch and upload years that do not exist in S3."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(24, "nyc")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response), \
             patch("extraction.weather.time.sleep"):  # Skip rate limiting in tests
            results = weather.backfill_city("nyc", start_year=2023, end_year=2023)

        assert results["2023"] is True
        mock_s3.upload_file.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for incremental_update
# ---------------------------------------------------------------------------

class TestIncrementalUpdate:
    """Tests for extraction/weather.py incremental_update()."""

    def test_incremental_returns_true_on_success(self):
        """incremental_update() should return True when data is fetched and uploaded."""
        mock_s3 = MagicMock()
        mock_s3.exceptions.ClientError = Exception

        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(48, "london")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            result = weather.incremental_update("london", days_back=5)

        assert result is True
        mock_s3.upload_file.assert_called_once()

    def test_incremental_returns_false_on_empty_response(self):
        """incremental_update() should return False when API returns no data."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = {"latitude": 40.7}
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            result = weather.incremental_update("nyc")

        assert result is False


# ---------------------------------------------------------------------------
# Tests for HourlyWeatherRecord data model
# ---------------------------------------------------------------------------

class TestHourlyWeatherRecord:
    """Tests for data_models/weather.py HourlyWeatherRecord."""

    def test_validate_schema_passes_with_all_columns(self):
        """HourlyWeatherRecord.validate_schema() should pass with all required columns."""
        from data_models.weather import HourlyWeatherRecord

        df = pd.DataFrame({
            "timestamp": ["2023-06-15T12:00"],
            "city": ["nyc"],
            "temperature_2m": [25.0],
            "relative_humidity_2m": [65.0],
            "apparent_temperature": [24.0],
            "precipitation": [0.0],
            "rain": [0.0],
            "snowfall": [0.0],
            "weather_code": [0],
            "cloud_cover": [25.0],
            "wind_speed_10m": [10.0],
            "wind_gusts_10m": [15.0],
        })

        assert HourlyWeatherRecord.validate_schema(df) is True

    def test_validate_schema_fails_with_missing_columns(self):
        """HourlyWeatherRecord.validate_schema() should fail when required columns are missing."""
        from data_models.weather import HourlyWeatherRecord

        df = pd.DataFrame({
            "timestamp": ["2023-06-15T12:00"],
            "city": ["nyc"],
            # Missing most required columns
        })

        assert HourlyWeatherRecord.validate_schema(df) is False

    def test_to_dataframe_adds_source_file(self):
        """HourlyWeatherRecord.to_dataframe() should add source_file column."""
        from data_models.weather import HourlyWeatherRecord

        df = pd.DataFrame({
            "timestamp": ["2023-06-15T12:00"],
            "city": ["nyc"],
            "temperature_2m": [25.0],
            "relative_humidity_2m": [65.0],
            "apparent_temperature": [24.0],
            "precipitation": [0.0],
            "rain": [0.0],
            "snowfall": [0.0],
            "snow_depth": [0.0],
            "weather_code": [0],
            "cloud_cover": [25.0],
            "wind_speed_10m": [10.0],
            "wind_gusts_10m": [15.0],
        })

        result = HourlyWeatherRecord.to_dataframe(df, "weather_nyc_2023.parquet")
        assert "source_file" in result.columns
        assert result["source_file"].iloc[0] == "weather_nyc_2023.parquet"
