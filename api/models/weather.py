"""Pydantic response models for weather endpoints."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CurrentWeatherResponse(BaseModel):
    city: str
    time: datetime
    temperature_c: float
    temperature_f: float
    apparent_temperature: float
    humidity: int
    precipitation_mm: float
    rain_mm: float
    snowfall_cm: float
    weather_code: int
    weather_description: str
    weather_category: str
    weather_emoji: str
    cloud_cover: int
    wind_speed_kmh: float
    wind_gusts_kmh: float


class HourlyForecastResponse(BaseModel):
    time: datetime
    temperature_c: float
    temperature_f: float
    apparent_temperature: float
    humidity: int
    precipitation_probability: int
    precipitation_mm: float
    weather_code: int
    weather_description: str
    weather_category: str
    weather_emoji: str
    wind_speed_kmh: float


class ForecastResponse(BaseModel):
    city: str
    hours: list[HourlyForecastResponse]
