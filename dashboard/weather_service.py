"""
Real-time weather service for the City Cycles dashboard.

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

# ---------------------------------------------------------------------------
# Streamlit Caching Wrapper
# ---------------------------------------------------------------------------


def get_city_weather_cached(city: str) -> Optional[CityWeather]:
    """Fetch weather with Streamlit caching and graceful error handling.

    This function wraps fetch_city_weather with @st.cache_data(ttl=900)
    for 15-minute caching and catches errors to return None on failure.

    This function must be imported only from Streamlit contexts.
    """
    import streamlit as st

    @st.cache_data(ttl=900, show_spinner=False)
    def _cached_fetch(city: str) -> Optional[CityWeather]:
        try:
            return fetch_city_weather(city)
        except WeatherAPIError:
            return None

    return _cached_fetch(city)

# ---------------------------------------------------------------------------
# Display Components (Streamlit UI)
# ---------------------------------------------------------------------------


def render_current_weather(weather: CityWeather, use_fahrenheit: bool = False) -> None:
    """Render current weather conditions as Streamlit metrics."""
    import streamlit as st

    current = weather.current
    city_label = CITY_COORDINATES[current.city]["label"]

    if use_fahrenheit:
        temp_str = f"{current.temperature_f:.0f}\u00b0F"
        feels_like_str = f"Feels like {current.apparent_temperature_f:.0f}\u00b0F"
        wind_str = f"{current.wind_speed_mph:.0f} mph"
        gust_str = f"Gusts {current.wind_gusts_mph:.0f} mph"
    else:
        temp_str = f"{current.temperature_c:.0f}\u00b0C"
        feels_like_str = f"Feels like {current.apparent_temperature_c:.0f}\u00b0C"
        wind_str = f"{current.wind_speed_kmh:.0f} km/h"
        gust_str = f"Gusts {current.wind_gusts_kmh:.0f} km/h"

    st.markdown(f"### {current.weather_emoji} {city_label} Now")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Temperature", temp_str, feels_like_str)
    with col2:
        st.metric("Conditions", current.weather_description)
    with col3:
        st.metric("Precipitation", f"{current.precipitation_mm:.1f} mm")
    with col4:
        st.metric("Wind", wind_str, gust_str)

    st.caption(
        f"Humidity {current.relative_humidity}% | "
        f"Cloud cover {current.cloud_cover}% | "
        f"Updated {weather.fetched_at.strftime('%H:%M UTC')}"
    )


def render_forecast_chart(weather: CityWeather, use_fahrenheit: bool = False) -> None:
    """Render a 48-hour forecast as a Plotly chart."""
    import streamlit as st
    import pandas as pd
    import plotly.graph_objects as go

    forecast = weather.forecast
    df = pd.DataFrame([
        {
            "time": entry.time,
            "temperature": entry.temperature_f if use_fahrenheit else entry.temperature_c,
            "precipitation_probability": entry.precipitation_probability,
            "precipitation_mm": entry.precipitation_mm,
            "wind_speed": entry.wind_speed_mph if use_fahrenheit else entry.wind_speed_kmh,
            "conditions": entry.weather_description,
            "emoji": entry.weather_emoji,
        }
        for entry in forecast
    ])

    temp_unit = "\u00b0F" if use_fahrenheit else "\u00b0C"

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["time"], y=df["temperature"],
        mode="lines+markers",
        name=f"Temperature ({temp_unit})",
        line=dict(color="#FF6B6B", width=2),
        marker=dict(size=4),
        hovertemplate="%{y:.0f}" + temp_unit + "<br>%{text}<extra></extra>",
        text=df["conditions"],
    ))

    fig.add_trace(go.Bar(
        x=df["time"], y=df["precipitation_probability"],
        name="Precip. Probability (%)",
        marker=dict(color="rgba(100, 149, 237, 0.4)"),
        yaxis="y2",
        hovertemplate="%{y}%<extra></extra>",
    ))

    fig.update_layout(
        title="48-Hour Forecast",
        xaxis_title="Time",
        yaxis=dict(title=f"Temperature ({temp_unit})", side="left"),
        yaxis2=dict(title="Precipitation Probability (%)", side="right",
                    overlaying="y", range=[0, 100]),
        height=350,
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)


def render_forecast_table(weather: CityWeather, use_fahrenheit: bool = False) -> None:
    """Render a compact forecast summary table (next 12 hours)."""
    import streamlit as st
    import pandas as pd

    forecast = weather.forecast[:12]
    rows = []
    for entry in forecast:
        temp = entry.temperature_f if use_fahrenheit else entry.temperature_c
        temp_unit = "\u00b0F" if use_fahrenheit else "\u00b0C"
        rows.append({
            "Time": entry.time.strftime("%H:%M"),
            "": entry.weather_emoji,
            "Conditions": entry.weather_description,
            f"Temp ({temp_unit})": f"{temp:.0f}",
            "Precip %": f"{entry.precipitation_probability}%",
            "Rain (mm)": f"{entry.precipitation_mm:.1f}",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
