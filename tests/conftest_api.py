"""Shared fixtures for API tests."""

import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from httpx import AsyncClient, ASGITransport

from api.main import app
from api import cache


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear API cache before each test."""
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def client():
    """Synchronous test client using httpx."""
    from fastapi.testclient import TestClient
    # Skip S3 download during tests
    with patch("api.services.data_loader.ensure_local_parquet_files"):
        with TestClient(app) as c:
            yield c


@pytest.fixture
def mock_weather():
    """Create a mock CityWeather object."""
    from api.services.weather_service import CityWeather, CurrentWeather, HourlyForecastEntry

    current = CurrentWeather(
        city="nyc",
        time=datetime(2026, 4, 9, 14, 0, tzinfo=timezone.utc),
        temperature_c=18.5,
        apparent_temperature_c=17.2,
        relative_humidity=62,
        precipitation_mm=0.0,
        rain_mm=0.0,
        snowfall_cm=0.0,
        weather_code=1,
        weather_description="Mainly clear",
        weather_category="clear",
        weather_emoji="\u2600\ufe0f",
        cloud_cover=15,
        wind_speed_kmh=12.3,
        wind_gusts_kmh=22.1,
    )

    forecast_entry = HourlyForecastEntry(
        time=datetime(2026, 4, 9, 15, 0, tzinfo=timezone.utc),
        temperature_c=19.0,
        apparent_temperature_c=18.0,
        relative_humidity=60,
        precipitation_probability=5,
        precipitation_mm=0.0,
        weather_code=1,
        weather_description="Mainly clear",
        weather_category="clear",
        weather_emoji="\u2600\ufe0f",
        wind_speed_kmh=11.0,
    )

    return CityWeather(
        current=current,
        forecast=[forecast_entry] * 24,
        fetched_at=datetime.now(timezone.utc),
    )
