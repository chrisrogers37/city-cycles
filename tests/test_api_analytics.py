"""Tests for analytics API endpoints."""

import os
from unittest.mock import patch

import pandas as pd

from tests.conftest_api import *  # noqa: F401,F403


class TestDailyMetrics:
    def test_requires_date_params(self, client):
        resp = client.get("/api/analytics/nyc/daily-metrics")
        assert resp.status_code == 422  # missing required query params

    def test_returns_metrics(self, client):
        mock_total = pd.DataFrame({"total_rides": [1000000.0]})
        mock_avg = pd.DataFrame({"avg_daily_rides": [2740.0]})
        mock_dur = pd.DataFrame({"avg_ride_duration_minutes": [14.5]})

        with patch("api.routes.analytics.run_query_params", side_effect=[mock_total, mock_avg, mock_dur]):
            resp = client.get("/api/analytics/nyc/daily-metrics?start_date=2024-01-01&end_date=2024-12-31")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_rides"] == 1000000.0
        assert data["avg_daily_rides"] == 2740.0
        assert data["avg_ride_duration_minutes"] == 14.5

    def test_invalid_city(self, client):
        resp = client.get("/api/analytics/paris/daily-metrics?start_date=2024-01-01&end_date=2024-12-31")
        assert resp.status_code == 422


class TestHourlyPatterns:
    def test_returns_empty_when_no_parquet(self, client):
        with patch("api.routes.analytics.parquet_exists", return_value=False):
            resp = client.get("/api/analytics/nyc/hourly-patterns")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_data(self, client):
        mock_df = pd.DataFrame({"hour_of_day": [0, 1, 2], "ride_count": [50.0, 30.0, 20.0]})
        with patch("api.routes.analytics.parquet_exists", return_value=True), \
             patch("api.routes.analytics.run_query_params", return_value=mock_df):
            resp = client.get("/api/analytics/nyc/hourly-patterns")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["hour_of_day"] == 0


class TestMemberAnalysis:
    def test_london_returns_404(self, client):
        resp = client.get("/api/analytics/london/member-analysis")
        assert resp.status_code == 404


class TestStationGrowth:
    def test_returns_empty_when_no_parquet(self, client):
        with patch("api.routes.analytics.parquet_exists", return_value=False):
            resp = client.get("/api/analytics/nyc/station-growth")
        assert resp.status_code == 200
        assert resp.json() == []


class TestWeatherImpact:
    def test_returns_empty_when_no_parquet(self, client):
        with patch("api.routes.analytics.parquet_exists", return_value=False):
            resp = client.get("/api/analytics/nyc/weather-impact")
        assert resp.status_code == 200
        assert resp.json() == []


class TestStationPerformance:
    def test_requires_weather_condition(self, client):
        resp = client.get("/api/analytics/nyc/station-performance")
        assert resp.status_code == 422  # missing required param
