"""Tests for insights API endpoint."""

from unittest.mock import patch, MagicMock

from dashboard.recommendation_engine import (
    BikingScore,
    ClassifiedConditions,
    Recommendation,
    RecommendationResult,
    Severity,
    TemperatureBand,
    WeatherCategory,
    WindCategory,
    PrecipitationIntensity,
    WeatherConditions,
)

from tests.conftest_api import *  # noqa: F401,F403


def _mock_result():
    """Build a mock RecommendationResult."""
    conditions = WeatherConditions(
        temperature_celsius=18.5, wind_speed_kmh=12.3,
        precipitation_mm=0.0, weather_code=1, location="nyc", hour=14,
    )
    classified = ClassifiedConditions(
        weather_category=WeatherCategory.CLEAR,
        temperature_band=TemperatureBand.MILD,
        wind_category=WindCategory.LIGHT,
        precipitation_intensity=PrecipitationIntensity.NONE,
        raw=conditions,
    )
    return RecommendationResult(
        biking_score=BikingScore(score=88, label="Excellent", color="#2ecc71"),
        recommendations=[
            Recommendation(text="Great day to bike!", severity=Severity.POSITIVE, metric="biking_score", value=88.0),
        ],
        classified=classified,
    )


class TestInsightsEndpoint:
    def test_get_insights_nyc(self, client, mock_weather):
        with patch("api.services.weather_bridge.fetch_city_weather", return_value=mock_weather), \
             patch("api.routes.insights.get_recommendations", return_value=_mock_result()):
            resp = client.get("/api/insights/nyc")
        assert resp.status_code == 200
        data = resp.json()
        assert data["biking_score"]["score"] == 88
        assert data["classified"]["weather_category"] == "clear"
        assert len(data["recommendations"]) == 1
        assert data["recommendations"][0]["severity"] == "positive"

    def test_invalid_city(self, client):
        resp = client.get("/api/insights/berlin")
        assert resp.status_code == 422

    def test_weather_failure_502(self, client):
        from dashboard.weather_service import WeatherAPIError
        with patch("api.services.weather_bridge.fetch_city_weather", side_effect=WeatherAPIError("fail")):
            resp = client.get("/api/insights/nyc")
        assert resp.status_code == 502
