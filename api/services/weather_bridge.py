"""
Weather service bridge — wraps dashboard.weather_service.fetch_city_weather()
with API-level caching (replaces Streamlit's @st.cache_data).
"""

import logging

from api.services.weather_service import (
    CityWeather,
    WeatherAPIError,
    fetch_city_weather,
)
from api.services.recommendation_engine import WeatherConditions

from api import cache

logger = logging.getLogger(__name__)

WEATHER_TTL = 900  # 15 minutes, matches existing Streamlit cache


def get_city_weather(city: str) -> CityWeather:
    """Fetch weather with TTL cache. Raises WeatherAPIError on failure."""
    cache_key = f"weather:{city}"
    cached = cache.get(cache_key, WEATHER_TTL)
    if cached is not None:
        return cached

    weather = fetch_city_weather(city)
    cache.set(cache_key, weather)
    return weather


def weather_to_conditions(weather: CityWeather) -> WeatherConditions:
    """Bridge CityWeather to WeatherConditions (same as landing.py:22-34)."""
    current = weather.current
    return WeatherConditions(
        temperature_celsius=current.temperature_c,
        wind_speed_kmh=current.wind_speed_kmh,
        precipitation_mm=current.precipitation_mm,
        weather_code=current.weather_code,
        location=current.city,
        hour=current.time.hour,
        humidity_percent=float(current.relative_humidity),
        feels_like_celsius=current.apparent_temperature_c,
    )
