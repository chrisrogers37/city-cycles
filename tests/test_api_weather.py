"""Tests for weather API endpoints."""

from unittest.mock import patch

from tests.conftest_api import *  # noqa: F401,F403


class TestWeatherEndpoint:
    def test_get_weather_nyc(self, client, mock_weather):
        with patch("api.services.weather_bridge.fetch_city_weather", return_value=mock_weather):
            resp = client.get("/api/weather/nyc")
        assert resp.status_code == 200
        data = resp.json()
        assert data["city"] == "nyc"
        assert data["temperature_c"] == 18.5
        assert data["humidity"] == 62
        assert data["apparent_temperature"] == 17.2
        assert data["weather_category"] == "clear"

    def test_get_weather_london(self, client, mock_weather):
        # Reuse mock but note city in the CityWeather is "nyc" — that's fine,
        # we're testing the HTTP layer, not the weather service
        with patch("api.services.weather_bridge.fetch_city_weather", return_value=mock_weather):
            resp = client.get("/api/weather/london")
        assert resp.status_code == 200

    def test_invalid_city_returns_422(self, client):
        resp = client.get("/api/weather/paris")
        assert resp.status_code == 422

    def test_weather_api_failure_returns_502(self, client):
        from dashboard.weather_service import WeatherAPIError
        with patch("api.services.weather_bridge.fetch_city_weather", side_effect=WeatherAPIError("timeout")):
            resp = client.get("/api/weather/nyc")
        assert resp.status_code == 502


class TestForecastEndpoint:
    def test_get_forecast(self, client, mock_weather):
        with patch("api.services.weather_bridge.fetch_city_weather", return_value=mock_weather):
            resp = client.get("/api/weather/nyc/forecast")
        assert resp.status_code == 200
        data = resp.json()
        assert data["city"] == "nyc"
        assert len(data["hours"]) == 24
        assert data["hours"][0]["temperature_c"] == 19.0
        assert data["hours"][0]["humidity"] == 60

    def test_forecast_invalid_city(self, client):
        resp = client.get("/api/weather/tokyo/forecast")
        assert resp.status_code == 422
