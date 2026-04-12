"""
Tests for dashboard/weather_service.py.

Tests the weather service module that fetches current conditions and
48-hour forecasts from Open-Meteo for NYC and London.

All HTTP calls are mocked. No real API interactions.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from api.services.weather_service import (
    fetch_city_weather,
    get_weather_description,
    get_weather_category,
    get_weather_emoji,
    _parse_current_weather,
    _parse_hourly_forecast,
    WeatherAPIError,
    WMO_CODE_DESCRIPTIONS,
    WMO_CODE_CATEGORIES,
    CITY_COORDINATES,
    CurrentWeather,
    HourlyForecastEntry,
    CityWeather,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_api_response():
    """A realistic Open-Meteo API response for NYC."""
    return {
        "latitude": 40.71,
        "longitude": -74.01,
        "generationtime_ms": 0.05,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 51.0,
        "current_units": {
            "time": "iso8601",
            "interval": "seconds",
            "temperature_2m": "\u00b0C",
            "relative_humidity_2m": "%",
            "precipitation": "mm",
            "rain": "mm",
            "snowfall": "cm",
            "weather_code": "wmo code",
            "cloud_cover": "%",
            "wind_speed_10m": "km/h",
            "wind_gusts_10m": "km/h",
            "apparent_temperature": "\u00b0C",
        },
        "current": {
            "time": "2026-02-12T15:00",
            "interval": 900,
            "temperature_2m": 5.0,
            "apparent_temperature": 2.0,
            "relative_humidity_2m": 65,
            "precipitation": 0.0,
            "rain": 0.0,
            "snowfall": 0.0,
            "weather_code": 3,
            "cloud_cover": 75,
            "wind_speed_10m": 15.0,
            "wind_gusts_10m": 25.0,
        },
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "\u00b0C",
            "relative_humidity_2m": "%",
            "precipitation_probability": "%",
            "precipitation": "mm",
            "weather_code": "wmo code",
            "wind_speed_10m": "km/h",
            "apparent_temperature": "\u00b0C",
        },
        "hourly": {
            "time": [
                "2026-02-12T00:00",
                "2026-02-12T01:00",
                "2026-02-12T02:00",
            ],
            "temperature_2m": [3.0, 2.5, 2.0],
            "apparent_temperature": [0.5, 0.0, -0.5],
            "relative_humidity_2m": [70, 72, 74],
            "precipitation_probability": [10, 20, 30],
            "precipitation": [0.0, 0.1, 0.2],
            "weather_code": [2, 3, 61],
            "wind_speed_10m": [10.0, 12.0, 14.0],
        },
    }


# ---------------------------------------------------------------------------
# WMO Code Mapping Tests
# ---------------------------------------------------------------------------

class TestWMOCodeMapping:
    """Tests for WMO weather code interpretation."""

    def test_clear_sky_description(self):
        assert get_weather_description(0) == "Clear sky"

    def test_overcast_description(self):
        assert get_weather_description(3) == "Overcast"

    def test_heavy_rain_description(self):
        assert get_weather_description(65) == "Heavy rain"

    def test_thunderstorm_description(self):
        assert get_weather_description(95) == "Thunderstorm"

    def test_unknown_code_description(self):
        result = get_weather_description(999)
        assert "Unknown" in result
        assert "999" in result

    def test_clear_category(self):
        assert get_weather_category(0) == "clear"
        assert get_weather_category(1) == "clear"

    def test_rain_category(self):
        assert get_weather_category(61) == "rain"
        assert get_weather_category(63) == "rain"
        assert get_weather_category(80) == "rain"

    def test_snow_category(self):
        assert get_weather_category(71) == "snow"
        assert get_weather_category(77) == "snow"

    def test_unknown_category(self):
        assert get_weather_category(999) == "unknown"

    def test_all_codes_have_descriptions(self):
        """Every code in CATEGORIES should also be in DESCRIPTIONS."""
        for code in WMO_CODE_CATEGORIES:
            assert code in WMO_CODE_DESCRIPTIONS

    def test_emoji_returns_string_for_all_categories(self):
        categories_seen = set()
        for code in WMO_CODE_CATEGORIES:
            emoji = get_weather_emoji(code)
            assert isinstance(emoji, str)
            assert len(emoji) > 0
            categories_seen.add(get_weather_category(code))
        assert "clear" in categories_seen
        assert "rain" in categories_seen
        assert "snow" in categories_seen


# ---------------------------------------------------------------------------
# Parse Current Weather Tests
# ---------------------------------------------------------------------------

class TestParseCurrentWeather:
    """Tests for _parse_current_weather()."""

    def test_parses_all_fields(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        assert isinstance(result, CurrentWeather)
        assert result.city == "nyc"
        assert result.temperature_c == 5.0
        assert result.apparent_temperature_c == 2.0
        assert result.relative_humidity == 65
        assert result.precipitation_mm == 0.0
        assert result.weather_code == 3
        assert result.weather_description == "Overcast"
        assert result.weather_category == "cloudy"
        assert result.cloud_cover == 75
        assert result.wind_speed_kmh == 15.0
        assert result.wind_gusts_kmh == 25.0

    def test_temperature_fahrenheit_conversion(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        # 5.0 C = 41.0 F
        assert abs(result.temperature_f - 41.0) < 0.1

    def test_wind_speed_mph_conversion(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        # 15.0 km/h * 0.621371 = 9.32 mph
        assert abs(result.wind_speed_mph - 9.32) < 0.1

    def test_time_parsing(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        assert result.time == datetime(2026, 2, 12, 15, 0)

    def test_raises_on_missing_key(self):
        bad_data = {"current": {"time": "2026-02-12T15:00"}}
        with pytest.raises(KeyError):
            _parse_current_weather("nyc", bad_data)


# ---------------------------------------------------------------------------
# Parse Hourly Forecast Tests
# ---------------------------------------------------------------------------

class TestParseHourlyForecast:
    """Tests for _parse_hourly_forecast()."""

    def test_parses_correct_count(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        assert len(result) == 3

    def test_each_entry_is_correct_type(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        for entry in result:
            assert isinstance(entry, HourlyForecastEntry)

    def test_first_entry_values(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        first = result[0]
        assert first.temperature_c == 3.0
        assert first.precipitation_probability == 10
        assert first.weather_code == 2
        assert first.weather_description == "Partly cloudy"

    def test_last_entry_has_rain(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        last = result[2]
        assert last.weather_code == 61
        assert last.weather_category == "rain"
        assert last.precipitation_mm == 0.2

    def test_raises_on_missing_hourly_key(self):
        bad_data = {"hourly": {"time": ["2026-02-12T00:00"]}}
        with pytest.raises(KeyError):
            _parse_hourly_forecast(bad_data)


# ---------------------------------------------------------------------------
# Fetch City Weather Tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchCityWeather:
    """Tests for fetch_city_weather() with mocked HTTP."""

    def test_successful_fetch(self, mock_api_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("api.services.weather_service.requests.get",
                   return_value=mock_response):
            result = fetch_city_weather("nyc")

        assert isinstance(result, CityWeather)
        assert result.current.city == "nyc"
        assert len(result.forecast) == 3
        assert isinstance(result.fetched_at, datetime)

    def test_raises_on_invalid_city(self):
        with pytest.raises(ValueError, match="Unknown city"):
            fetch_city_weather("tokyo")

    def test_raises_on_http_error(self):
        from requests.exceptions import RequestException
        with patch("api.services.weather_service.requests.get",
                   side_effect=RequestException("Connection refused")):
            with pytest.raises(WeatherAPIError, match="Connection refused"):
                fetch_city_weather("nyc")

    def test_raises_on_api_error_response(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "error": True,
            "reason": "Invalid parameter"
        }
        mock_response.raise_for_status = MagicMock()

        with patch("api.services.weather_service.requests.get",
                   return_value=mock_response):
            with pytest.raises(WeatherAPIError, match="Invalid parameter"):
                fetch_city_weather("london")

    def test_raises_on_malformed_json(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"current": {}, "hourly": {}}
        mock_response.raise_for_status = MagicMock()

        with patch("api.services.weather_service.requests.get",
                   return_value=mock_response):
            with pytest.raises(WeatherAPIError):
                fetch_city_weather("nyc")

    def test_london_fetch_uses_correct_coordinates(self, mock_api_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("api.services.weather_service.requests.get",
                   return_value=mock_response) as mock_get:
            fetch_city_weather("london")

        called_url = mock_get.call_args[0][0]
        assert "51.5074" in called_url
        assert "-0.1278" in called_url


# ---------------------------------------------------------------------------
# City Coordinates Tests
# ---------------------------------------------------------------------------

class TestCityCoordinates:
    """Tests for the CITY_COORDINATES constant."""

    def test_nyc_coordinates(self):
        assert CITY_COORDINATES["nyc"]["latitude"] == 40.7128
        assert CITY_COORDINATES["nyc"]["longitude"] == -74.0060

    def test_london_coordinates(self):
        assert CITY_COORDINATES["london"]["latitude"] == 51.5074
        assert CITY_COORDINATES["london"]["longitude"] == -0.1278

    def test_both_cities_have_labels(self):
        for city in CITY_COORDINATES:
            assert "label" in CITY_COORDINATES[city]
