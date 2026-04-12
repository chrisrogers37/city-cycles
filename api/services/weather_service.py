"""
Real-time weather service for the City Cycles API.

Fetches current conditions and 48-hour forecasts from Open-Meteo API
for NYC and London. Designed to run independently of the batch pipeline.
"""

import logging
import requests
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

CITY_COORDINATES = {
    "nyc": {"latitude": 40.7128, "longitude": -74.0060, "label": "New York City"},
    "london": {"latitude": 51.5074, "longitude": -0.1278, "label": "London"},
}

CURRENT_PARAMS = (
    "temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,"
    "weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m,apparent_temperature"
)

HOURLY_PARAMS = (
    "temperature_2m,relative_humidity_2m,precipitation_probability,"
    "precipitation,weather_code,wind_speed_10m,apparent_temperature"
)

REQUEST_TIMEOUT_SECONDS = 10

# ---------------------------------------------------------------------------
# WMO Weather Code Mappings
# ---------------------------------------------------------------------------

WMO_CODE_DESCRIPTIONS: dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snowfall",
    73: "Moderate snowfall",
    75: "Heavy snowfall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}

WMO_CODE_CATEGORIES: dict[int, str] = {
    0: "clear", 1: "clear", 2: "cloudy", 3: "cloudy",
    45: "fog", 48: "fog",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    56: "drizzle", 57: "drizzle",
    61: "rain", 63: "rain", 65: "rain",
    66: "rain", 67: "rain",
    71: "snow", 73: "snow", 75: "snow", 77: "snow",
    80: "rain", 81: "rain", 82: "rain",
    85: "snow", 86: "snow",
    95: "thunderstorm", 96: "thunderstorm", 99: "thunderstorm",
}

WMO_CODE_EMOJIS: dict[str, str] = {
    "clear": "\u2600\ufe0f",
    "cloudy": "\u2601\ufe0f",
    "fog": "\U0001f32b\ufe0f",
    "drizzle": "\U0001f326\ufe0f",
    "rain": "\U0001f327\ufe0f",
    "snow": "\u2744\ufe0f",
    "thunderstorm": "\u26c8\ufe0f",
}

# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CurrentWeather:
    """Current weather conditions for a single city."""
    city: str
    time: datetime
    temperature_c: float
    apparent_temperature_c: float
    relative_humidity: int
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

    @property
    def temperature_f(self) -> float:
        return self.temperature_c * 9 / 5 + 32

    @property
    def apparent_temperature_f(self) -> float:
        return self.apparent_temperature_c * 9 / 5 + 32

    @property
    def wind_speed_mph(self) -> float:
        return self.wind_speed_kmh * 0.621371

    @property
    def wind_gusts_mph(self) -> float:
        return self.wind_gusts_kmh * 0.621371


@dataclass(frozen=True)
class HourlyForecastEntry:
    """A single hour's forecast data."""
    time: datetime
    temperature_c: float
    apparent_temperature_c: float
    relative_humidity: int
    precipitation_probability: int
    precipitation_mm: float
    weather_code: int
    weather_description: str
    weather_category: str
    weather_emoji: str
    wind_speed_kmh: float

    @property
    def temperature_f(self) -> float:
        return self.temperature_c * 9 / 5 + 32

    @property
    def apparent_temperature_f(self) -> float:
        return self.apparent_temperature_c * 9 / 5 + 32


@dataclass(frozen=True)
class CityWeather:
    """Complete weather data for a city: current + forecast."""
    current: CurrentWeather
    forecast: list[HourlyForecastEntry]
    fetched_at: datetime

# ---------------------------------------------------------------------------
# WMO Mapping Functions
# ---------------------------------------------------------------------------


def get_weather_description(code: int) -> str:
    return WMO_CODE_DESCRIPTIONS.get(code, f"Unknown ({code})")


def get_weather_category(code: int) -> str:
    return WMO_CODE_CATEGORIES.get(code, "unknown")


def get_weather_emoji(code: int) -> str:
    category = get_weather_category(code)
    return WMO_CODE_EMOJIS.get(category, "\u2753")

# ---------------------------------------------------------------------------
# API Error
# ---------------------------------------------------------------------------


class WeatherAPIError(Exception):
    """Raised when the weather API request fails or returns invalid data."""
    pass

# ---------------------------------------------------------------------------
# API Fetch Functions (pure, no Streamlit)
# ---------------------------------------------------------------------------


def _build_api_url(city: str) -> str:
    """Build the Open-Meteo API URL for a city."""
    coords = CITY_COORDINATES[city]
    return (
        f"{OPEN_METEO_BASE_URL}"
        f"?latitude={coords['latitude']}"
        f"&longitude={coords['longitude']}"
        f"&current={CURRENT_PARAMS}"
        f"&hourly={HOURLY_PARAMS}"
        f"&forecast_days=2"
    )


def fetch_city_weather(city: str) -> CityWeather:
    """Fetch current weather and 48-hour forecast for a city.

    Args:
        city: 'nyc' or 'london'

    Returns:
        CityWeather dataclass with current conditions and hourly forecast.

    Raises:
        WeatherAPIError: If the API request fails or returns invalid data.
    """
    if city not in CITY_COORDINATES:
        raise ValueError(f"Unknown city: {city}. Must be one of {list(CITY_COORDINATES.keys())}")

    url = _build_api_url(city)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch weather for %s: %s", city, e)
        raise WeatherAPIError(f"Failed to fetch weather for {city}: {e}") from e

    if "error" in data and data["error"]:
        reason = data.get("reason", "Unknown error")
        logger.error("Open-Meteo API error for %s: %s", city, reason)
        raise WeatherAPIError(f"Open-Meteo API error: {reason}")

    try:
        current = _parse_current_weather(city, data)
        forecast = _parse_hourly_forecast(data)
    except (KeyError, TypeError, IndexError) as e:
        logger.error("Failed to parse weather data for %s: %s", city, e)
        raise WeatherAPIError(f"Failed to parse weather data for {city}: {e}") from e

    return CityWeather(
        current=current,
        forecast=forecast,
        fetched_at=datetime.now(timezone.utc),
    )

# ---------------------------------------------------------------------------
# Parsing Functions
# ---------------------------------------------------------------------------


def _parse_current_weather(city: str, data: dict) -> CurrentWeather:
    """Parse the 'current' section of the Open-Meteo response."""
    current = data["current"]
    code = int(current["weather_code"])
    return CurrentWeather(
        city=city,
        time=datetime.fromisoformat(current["time"]),
        temperature_c=float(current["temperature_2m"]),
        apparent_temperature_c=float(current["apparent_temperature"]),
        relative_humidity=int(current["relative_humidity_2m"]),
        precipitation_mm=float(current["precipitation"]),
        rain_mm=float(current["rain"]),
        snowfall_cm=float(current["snowfall"]),
        weather_code=code,
        weather_description=get_weather_description(code),
        weather_category=get_weather_category(code),
        weather_emoji=get_weather_emoji(code),
        cloud_cover=int(current["cloud_cover"]),
        wind_speed_kmh=float(current["wind_speed_10m"]),
        wind_gusts_kmh=float(current["wind_gusts_10m"]),
    )


def _parse_hourly_forecast(data: dict) -> list[HourlyForecastEntry]:
    """Parse the 'hourly' section of the Open-Meteo response into a list of entries."""
    hourly = data["hourly"]
    times = hourly["time"]
    entries = []
    for i in range(len(times)):
        code = int(hourly["weather_code"][i])
        entries.append(HourlyForecastEntry(
            time=datetime.fromisoformat(times[i]),
            temperature_c=float(hourly["temperature_2m"][i]),
            apparent_temperature_c=float(hourly["apparent_temperature"][i]),
            relative_humidity=int(hourly["relative_humidity_2m"][i]),
            precipitation_probability=int(hourly["precipitation_probability"][i]),
            precipitation_mm=float(hourly["precipitation"][i]),
            weather_code=code,
            weather_description=get_weather_description(code),
            weather_category=get_weather_category(code),
            weather_emoji=get_weather_emoji(code),
            wind_speed_kmh=float(hourly["wind_speed_10m"][i]),
        ))
    return entries
