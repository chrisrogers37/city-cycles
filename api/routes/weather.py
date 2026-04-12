"""Weather API routes — current conditions and forecast."""

import logging

from fastapi import APIRouter, HTTPException

from api.dependencies import CityParam
from api.models.weather import (
    CurrentWeatherResponse,
    ForecastResponse,
    HourlyForecastResponse,
)
from api.services.weather_bridge import get_city_weather
from api.services.weather_service import WeatherAPIError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/weather", tags=["weather"])


def _current_to_response(weather) -> CurrentWeatherResponse:
    """Map CityWeather.current to response model with friendlier names."""
    c = weather.current
    return CurrentWeatherResponse(
        city=c.city,
        time=c.time,
        temperature_c=c.temperature_c,
        temperature_f=c.temperature_f,
        apparent_temperature=c.apparent_temperature_c,
        humidity=c.relative_humidity,
        precipitation_mm=c.precipitation_mm,
        rain_mm=c.rain_mm,
        snowfall_cm=c.snowfall_cm,
        weather_code=c.weather_code,
        weather_description=c.weather_description,
        weather_category=c.weather_category,
        weather_emoji=c.weather_emoji,
        cloud_cover=c.cloud_cover,
        wind_speed_kmh=c.wind_speed_kmh,
        wind_gusts_kmh=c.wind_gusts_kmh,
    )


def _forecast_entry_to_response(entry) -> HourlyForecastResponse:
    return HourlyForecastResponse(
        time=entry.time,
        temperature_c=entry.temperature_c,
        temperature_f=entry.temperature_f,
        apparent_temperature=entry.apparent_temperature_c,
        humidity=entry.relative_humidity,
        precipitation_probability=entry.precipitation_probability,
        precipitation_mm=entry.precipitation_mm,
        weather_code=entry.weather_code,
        weather_description=entry.weather_description,
        weather_category=entry.weather_category,
        weather_emoji=entry.weather_emoji,
        wind_speed_kmh=entry.wind_speed_kmh,
    )


@router.get("/{city}", response_model=CurrentWeatherResponse)
def get_weather(city: CityParam):
    """Current weather conditions for a city."""
    try:
        weather = get_city_weather(city.value)
    except WeatherAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))
    return _current_to_response(weather)


@router.get("/{city}/forecast", response_model=ForecastResponse)
def get_forecast(city: CityParam):
    """24-hour hourly forecast for a city."""
    try:
        weather = get_city_weather(city.value)
    except WeatherAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))
    hours = [_forecast_entry_to_response(e) for e in weather.forecast[:24]]
    return ForecastResponse(city=city.value, hours=hours)
