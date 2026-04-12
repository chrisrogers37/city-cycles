"""Tests for similar-day API endpoints."""

from unittest.mock import patch

from api.services.recommendation_engine import SimilarDayInsight

from tests.conftest_api import *  # noqa: F401,F403


def _mock_similar():
    return SimilarDayInsight(
        avg_daily_rides=14200.0,
        pct_change_vs_overall=-12.3,
        avg_duration_minutes=14.8,
        duration_pct_change_vs_overall=-2.1,
        peak_hour_start=17,
        peak_hour_end=19,
        sample_days=47,
        month=4,
        day_type="weekday",
        temperature_band="mild",
        precipitation_intensity="none",
    )


class TestSimilarDayEndpoint:
    def test_get_similar_day(self, client, mock_weather):
        with patch("api.routes.similar_day.get_city_weather", return_value=mock_weather), \
             patch("api.routes.similar_day.lookup_similar_day_stats", return_value=_mock_similar()), \
             patch("api.routes.similar_day._current_month_and_day_type", return_value=(4, "weekday")):
            resp = client.get("/api/similar-day/nyc")
        assert resp.status_code == 200
        data = resp.json()
        assert data["avg_daily_rides"] == 14200.0
        assert data["pct_change_vs_overall"] == -12.3
        assert data["sample_days"] == 47

    def test_invalid_city(self, client):
        resp = client.get("/api/similar-day/paris")
        assert resp.status_code == 422

    def test_weather_failure_502(self, client, mock_weather):
        from api.services.weather_service import WeatherAPIError
        with patch("api.routes.similar_day.get_city_weather", side_effect=WeatherAPIError("fail")):
            resp = client.get("/api/similar-day/nyc")
        assert resp.status_code == 502


class TestSimilarDayHourlyEndpoint:
    def test_invalid_city(self, client):
        resp = client.get("/api/similar-day/mars/hourly")
        assert resp.status_code == 422
