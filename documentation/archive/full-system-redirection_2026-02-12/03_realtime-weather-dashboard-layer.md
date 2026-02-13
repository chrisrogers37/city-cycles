# Phase 03: Real-time Weather Dashboard Layer

## PR Title
feat: add real-time weather service with 15-minute auto-refresh

## Status: ✅ COMPLETE
## Started: 2026-02-12
## Completed: 2026-02-12
## PR: #38
## Risk Level: Medium
## Estimated Effort: 1-2 days
## Dependencies: Phase 01 (Weather Data Pipeline) ✅ MERGED (PR #36)
## Unlocks: Phases 04, 05

## Files Impact
| Action | File |
|--------|------|
| CREATE | dashboard/weather_service.py |
| CREATE | dashboard/__init__.py |
| CREATE | tests/test_weather_service.py |
| MODIFY | dashboard/app.py |

## Context
This phase adds a live weather data layer to the dashboard, fetching current conditions and 48-hour forecasts from Open-Meteo with 15-minute auto-refresh using Streamlit fragments. This is separate from the batch pipeline — it fetches directly from the API at display time. The weather service provides dataclasses, WMO code mapping, unit conversions, and display components used by the recommendation engine (Phase 04) and atmospheric UI (Phase 05).

---

# Phase 03: Real-time Weather Dashboard Layer -- Implementation Plan

---

### 1. Overview and Architectural Decisions

**Objective:** Add a live weather data layer to the Streamlit dashboard that fetches current conditions and 48-hour forecasts from Open-Meteo for NYC and London, auto-refreshing every 15 minutes, completely separate from the batch pipeline.

**Key architectural decisions:**

**Decision 1: Use `st.fragment(run_every="15m")` instead of `streamlit-autorefresh`.**
The project already has Streamlit 1.54.0, which includes the stable `st.fragment` API (GA since 1.37). This is a first-party solution that supports partial rerun (only the weather fragment reruns, not the whole page). This eliminates the need for `streamlit-autorefresh` as a third-party dependency. The requirements.txt changes are therefore limited to adding `requests` usage for the weather API calls -- but `requests==2.32.5` is already in `requirements.txt`.

**Decision 2: No new dependencies required.**
- `requests` -- already in requirements.txt (2.32.5)
- `st.fragment` -- built into Streamlit 1.54.0
- `st.cache_data(ttl=900)` -- built into Streamlit 1.54.0
- `dataclasses` -- Python stdlib

The requirement to "add streamlit-autorefresh to requirements.txt" is superseded by this cleaner approach. The plan should note this to the user.

**Decision 3: Weather service is a standalone module, not tied to the batch pipeline.**
`dashboard/weather_service.py` will use `requests` directly to call Open-Meteo (no S3, no DuckDB, no dbt). It is a pure Python module with no Streamlit imports in its core logic, making it independently testable. Only the caching decorator (`@st.cache_data`) sits at the boundary in a thin wrapper function.

**Decision 4: Separate the weather service logic from Streamlit-specific caching.**
The core fetch/parse logic in `weather_service.py` will be pure functions that accept/return dataclasses. A thin `get_weather_cached()` function will wrap the core logic with `@st.cache_data(ttl=900)`. Tests will target the pure functions directly without needing Streamlit.

---

### 2. Open-Meteo API Response Format

Based on the Open-Meteo documentation and API structure, the response for the endpoints described in the requirements looks like this:

**Current weather response structure:**
```json
{
  "latitude": 40.71,
  "longitude": -74.01,
  "generationtime_ms": 0.05,
  "utc_offset_seconds": 0,
  "timezone": "GMT",
  "timezone_abbreviation": "GMT",
  "elevation": 51.0,
  "current_units": {
    "time": "iso8601",
    "interval": "seconds",
    "temperature_2m": "\u00b0C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "rain": "mm",
    "snowfall": "cm",
    "weather_code": "wmo code",
    "cloud_cover": "%",
    "wind_speed_10m": "km/h",
    "wind_gusts_10m": "km/h",
    "apparent_temperature": "\u00b0C"
  },
  "current": {
    "time": "2026-02-12T15:00",
    "interval": 900,
    "temperature_2m": 2.4,
    "relative_humidity_2m": 65,
    "precipitation": 0.0,
    "rain": 0.0,
    "snowfall": 0.0,
    "weather_code": 3,
    "cloud_cover": 75,
    "wind_speed_10m": 11.9,
    "wind_gusts_10m": 22.3,
    "apparent_temperature": -1.2
  },
  "hourly_units": {
    "time": "iso8601",
    "temperature_2m": "\u00b0C",
    "relative_humidity_2m": "%",
    "precipitation_probability": "%",
    "precipitation": "mm",
    "weather_code": "wmo code",
    "wind_speed_10m": "km/h",
    "apparent_temperature": "\u00b0C"
  },
  "hourly": {
    "time": ["2026-02-12T00:00", "2026-02-12T01:00", ...],
    "temperature_2m": [1.2, 1.0, ...],
    "relative_humidity_2m": [70, 72, ...],
    "precipitation_probability": [10, 15, ...],
    "precipitation": [0.0, 0.0, ...],
    "weather_code": [2, 3, ...],
    "wind_speed_10m": [8.5, 9.1, ...],
    "apparent_temperature": [-2.1, -2.5, ...]
  }
}
```

All temperatures are in Celsius by default. Wind speed in km/h. The module will handle F/C conversion for display.

---

### 3. WMO Weather Code Mapping

The Open-Meteo API uses a simplified subset of WMO code table 4677. The complete mapping for codes the API returns:

```python
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
    "clear": "\u2600\ufe0f",       # sun
    "cloudy": "\u2601\ufe0f",      # cloud
    "fog": "\ud83c\udf2b\ufe0f",   # fog
    "drizzle": "\ud83c\udf26\ufe0f", # sun behind rain cloud
    "rain": "\ud83c\udf27\ufe0f",  # cloud with rain
    "snow": "\u2744\ufe0f",        # snowflake
    "thunderstorm": "\u26c8\ufe0f", # cloud with lightning and rain
}
```

---

### 4. File-by-File Implementation Plan

#### 4.1 NEW FILE: `dashboard/weather_service.py`

This is the core deliverable. It contains:

**A. Constants and configuration:**

```python
"""
Real-time weather service for the City Cycles dashboard.

Fetches current conditions and 48-hour forecasts from Open-Meteo API
for NYC and London. Designed to run independently of the batch pipeline.
"""

import logging
import requests
from dataclasses import dataclass, field
from datetime import datetime
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
```

**B. Data classes:**

```python
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
```

**C. WMO mapping functions:**

```python
def get_weather_description(code: int) -> str:
    return WMO_CODE_DESCRIPTIONS.get(code, f"Unknown ({code})")

def get_weather_category(code: int) -> str:
    return WMO_CODE_CATEGORIES.get(code, "unknown")

def get_weather_emoji(code: int) -> str:
    category = get_weather_category(code)
    return WMO_CODE_EMOJIS.get(category, "\u2753")  # question mark for unknown
```

**D. API fetch functions (pure, no Streamlit):**

```python
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
        fetched_at=datetime.utcnow(),
    )


class WeatherAPIError(Exception):
    """Raised when the weather API request fails or returns invalid data."""
    pass
```

**E. Parsing functions (pure, testable):**

```python
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
```

**F. Streamlit caching wrapper (thin boundary):**

```python
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
```

Note: The `@st.cache_data` decorator is inside the function to avoid importing `streamlit` at module level, which would break testability. Only Streamlit callers use `get_city_weather_cached`; tests call `fetch_city_weather` directly.

**G. Display component functions (Streamlit UI):**

```python
def render_current_weather(weather: CityWeather, use_fahrenheit: bool = False) -> None:
    """Render current weather conditions as Streamlit metrics.

    Args:
        weather: CityWeather data for a single city.
        use_fahrenheit: If True, display temperature in Fahrenheit (for NYC).
    """
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
    """Render a 48-hour forecast as a Plotly chart.

    Args:
        weather: CityWeather data for a single city.
        use_fahrenheit: If True, display temperature in Fahrenheit.
    """
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
    wind_unit = "mph" if use_fahrenheit else "km/h"

    fig = go.Figure()

    # Temperature line (primary y-axis)
    fig.add_trace(go.Scatter(
        x=df["time"], y=df["temperature"],
        mode="lines+markers",
        name=f"Temperature ({temp_unit})",
        line=dict(color="#FF6B6B", width=2),
        marker=dict(size=4),
        hovertemplate="%{y:.0f}" + temp_unit + "<br>%{text}<extra></extra>",
        text=df["conditions"],
    ))

    # Precipitation probability bars (secondary y-axis)
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
    """Render a compact forecast summary table.

    Shows next 12 hours as a quick-reference table.
    """
    import streamlit as st
    import pandas as pd

    forecast = weather.forecast[:12]  # Next 12 hours
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
```

#### 4.2 MODIFY: `dashboard/app.py`

The changes to `app.py` are minimal for Phase 03. Phase 05 will do the major rewrite. Phase 03 adds a weather fragment that auto-refreshes and appears at the top of the page, above the existing dashboard content.

**Changes needed:**

1. Add import at top of file (after existing imports, around line 7):
```python
from dashboard.weather_service import (
    get_city_weather_cached,
    render_current_weather,
    render_forecast_chart,
    render_forecast_table,
)
```

2. Add a weather fragment function and call it after the title (line 68), before the session state setup:

```python
@st.fragment(run_every="15m")
def weather_panel():
    """Auto-refreshing weather panel. Reruns every 15 minutes independently."""
    col_nyc, col_london = st.columns(2)

    nyc_weather = get_city_weather_cached("nyc")
    london_weather = get_city_weather_cached("london")

    with col_nyc:
        if nyc_weather:
            render_current_weather(nyc_weather, use_fahrenheit=True)
        else:
            st.warning("Weather data unavailable for NYC")

    with col_london:
        if london_weather:
            render_current_weather(london_weather, use_fahrenheit=False)
        else:
            st.warning("Weather data unavailable for London")

    # Expandable forecast section
    with st.expander("48-Hour Forecast"):
        forecast_col_nyc, forecast_col_london = st.columns(2)
        with forecast_col_nyc:
            if nyc_weather:
                render_forecast_chart(nyc_weather, use_fahrenheit=True)
                render_forecast_table(nyc_weather, use_fahrenheit=True)
        with forecast_col_london:
            if london_weather:
                render_forecast_chart(london_weather, use_fahrenheit=False)
                render_forecast_table(london_weather, use_fahrenheit=False)

# Render the weather panel
weather_panel()

st.divider()
```

This insertion point is after line 68 (`st.title(...)`) and before line 70 (`dashboard_min_date = ...`). The `st.divider()` visually separates weather from the historical analytics below.

3. The import at line 13 currently does:
```python
from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files
```
The `dashboard.weather_service` import needs the `dashboard` directory to be importable. Since `dashboard/` has no `__init__.py`, the import should use a relative path approach consistent with the existing codebase. Looking at line 12 of `app.py`:
```python
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
```
This already adds the project root to `sys.path`. So `from dashboard.weather_service import ...` will NOT work because `dashboard/` is not a package. We need to either:
- Option A: Add `dashboard/__init__.py` (empty file)
- Option B: Use direct import: `from weather_service import ...` (since `app.py` runs from the `dashboard/` directory via Streamlit)

Looking at Streamlit's execution model, `streamlit run dashboard/app.py` runs from the project root, and `sys.path` already contains the project root (line 12). The safest approach is **Option A: add an empty `dashboard/__init__.py`**. This makes the import `from dashboard.weather_service import ...` work reliably regardless of working directory.

#### 4.3 NEW FILE: `dashboard/__init__.py`

An empty file to make `dashboard` a proper Python package, enabling `from dashboard.weather_service import ...`.

```python
# dashboard package
```

#### 4.4 MODIFY: `requirements.txt`

**No changes needed.** The original requirement specified adding `streamlit-autorefresh`, but since we are using the built-in `st.fragment(run_every="15m")`, and `requests` is already in requirements.txt, no dependency changes are required.

If the user still wants to document the Open-Meteo dependency (which is an API, not a pip package), that can be noted in CLAUDE.md or README.md instead.

#### 4.5 NEW FILE: `tests/test_weather_service.py`

The test file follows the patterns from `tests/test_dashboard.py` and `tests/test_extraction.py` (mocking `requests.get`).

```python
"""
Tests for dashboard/weather_service.py.

Tests the weather service module that fetches current conditions and
48-hour forecasts from Open-Meteo for NYC and London.

All HTTP calls are mocked. No real API interactions.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from dashboard.weather_service import (
    fetch_city_weather,
    get_weather_description,
    get_weather_category,
    get_weather_emoji,
    _parse_current_weather,
    _parse_hourly_forecast,
    WeatherAPIError,
    WMO_CODE_DESCRIPTIONS,
    WMO_CODE_CATEGORIES,
    CITY_COORDINATES,
    CurrentWeather,
    HourlyForecastEntry,
    CityWeather,
)
```

**Test class structure:**

```python
class TestWMOCodeMapping:
    """Tests for WMO weather code interpretation."""

    def test_clear_sky_description(self):
        assert get_weather_description(0) == "Clear sky"

    def test_overcast_description(self):
        assert get_weather_description(3) == "Overcast"

    def test_heavy_rain_description(self):
        assert get_weather_description(65) == "Heavy rain"

    def test_thunderstorm_description(self):
        assert get_weather_description(95) == "Thunderstorm"

    def test_unknown_code_description(self):
        result = get_weather_description(999)
        assert "Unknown" in result
        assert "999" in result

    def test_clear_category(self):
        assert get_weather_category(0) == "clear"
        assert get_weather_category(1) == "clear"

    def test_rain_category(self):
        assert get_weather_category(61) == "rain"
        assert get_weather_category(63) == "rain"
        assert get_weather_category(80) == "rain"

    def test_snow_category(self):
        assert get_weather_category(71) == "snow"
        assert get_weather_category(77) == "snow"

    def test_unknown_category(self):
        assert get_weather_category(999) == "unknown"

    def test_all_codes_have_descriptions(self):
        """Every code in CATEGORIES should also be in DESCRIPTIONS."""
        for code in WMO_CODE_CATEGORIES:
            assert code in WMO_CODE_DESCRIPTIONS

    def test_emoji_returns_string_for_all_categories(self):
        categories_seen = set()
        for code in WMO_CODE_CATEGORIES:
            emoji = get_weather_emoji(code)
            assert isinstance(emoji, str)
            assert len(emoji) > 0
            categories_seen.add(get_weather_category(code))
        # Verify we tested all known categories
        assert "clear" in categories_seen
        assert "rain" in categories_seen
        assert "snow" in categories_seen
```

**Fixture for mock API response:**

```python
@pytest.fixture
def mock_api_response():
    """A realistic Open-Meteo API response for NYC."""
    return {
        "latitude": 40.71,
        "longitude": -74.01,
        "generationtime_ms": 0.05,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 51.0,
        "current_units": {
            "time": "iso8601",
            "interval": "seconds",
            "temperature_2m": "\u00b0C",
            "relative_humidity_2m": "%",
            "precipitation": "mm",
            "rain": "mm",
            "snowfall": "cm",
            "weather_code": "wmo code",
            "cloud_cover": "%",
            "wind_speed_10m": "km/h",
            "wind_gusts_10m": "km/h",
            "apparent_temperature": "\u00b0C",
        },
        "current": {
            "time": "2026-02-12T15:00",
            "interval": 900,
            "temperature_2m": 5.0,
            "apparent_temperature": 2.0,
            "relative_humidity_2m": 65,
            "precipitation": 0.0,
            "rain": 0.0,
            "snowfall": 0.0,
            "weather_code": 3,
            "cloud_cover": 75,
            "wind_speed_10m": 15.0,
            "wind_gusts_10m": 25.0,
        },
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "\u00b0C",
            "relative_humidity_2m": "%",
            "precipitation_probability": "%",
            "precipitation": "mm",
            "weather_code": "wmo code",
            "wind_speed_10m": "km/h",
            "apparent_temperature": "\u00b0C",
        },
        "hourly": {
            "time": [
                "2026-02-12T00:00",
                "2026-02-12T01:00",
                "2026-02-12T02:00",
            ],
            "temperature_2m": [3.0, 2.5, 2.0],
            "apparent_temperature": [0.5, 0.0, -0.5],
            "relative_humidity_2m": [70, 72, 74],
            "precipitation_probability": [10, 20, 30],
            "precipitation": [0.0, 0.1, 0.2],
            "weather_code": [2, 3, 61],
            "wind_speed_10m": [10.0, 12.0, 14.0],
        },
    }
```

**Parsing tests:**

```python
class TestParseCurrentWeather:
    """Tests for _parse_current_weather()."""

    def test_parses_all_fields(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        assert isinstance(result, CurrentWeather)
        assert result.city == "nyc"
        assert result.temperature_c == 5.0
        assert result.apparent_temperature_c == 2.0
        assert result.relative_humidity == 65
        assert result.precipitation_mm == 0.0
        assert result.weather_code == 3
        assert result.weather_description == "Overcast"
        assert result.weather_category == "cloudy"
        assert result.cloud_cover == 75
        assert result.wind_speed_kmh == 15.0
        assert result.wind_gusts_kmh == 25.0

    def test_temperature_fahrenheit_conversion(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        # 5.0 C = 41.0 F
        assert abs(result.temperature_f - 41.0) < 0.1

    def test_wind_speed_mph_conversion(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        # 15.0 km/h * 0.621371 = 9.32 mph
        assert abs(result.wind_speed_mph - 9.32) < 0.1

    def test_time_parsing(self, mock_api_response):
        result = _parse_current_weather("nyc", mock_api_response)
        assert result.time == datetime(2026, 2, 12, 15, 0)

    def test_raises_on_missing_key(self):
        bad_data = {"current": {"time": "2026-02-12T15:00"}}
        with pytest.raises(KeyError):
            _parse_current_weather("nyc", bad_data)


class TestParseHourlyForecast:
    """Tests for _parse_hourly_forecast()."""

    def test_parses_correct_count(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        assert len(result) == 3  # 3 hours in fixture

    def test_each_entry_is_correct_type(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        for entry in result:
            assert isinstance(entry, HourlyForecastEntry)

    def test_first_entry_values(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        first = result[0]
        assert first.temperature_c == 3.0
        assert first.precipitation_probability == 10
        assert first.weather_code == 2
        assert first.weather_description == "Partly cloudy"

    def test_last_entry_has_rain(self, mock_api_response):
        result = _parse_hourly_forecast(mock_api_response)
        last = result[2]
        assert last.weather_code == 61
        assert last.weather_category == "rain"
        assert last.precipitation_mm == 0.2

    def test_raises_on_missing_hourly_key(self):
        bad_data = {"hourly": {"time": ["2026-02-12T00:00"]}}
        with pytest.raises(KeyError):
            _parse_hourly_forecast(bad_data)
```

**Integration tests (mocked HTTP):**

```python
class TestFetchCityWeather:
    """Tests for fetch_city_weather() with mocked HTTP."""

    def test_successful_fetch(self, mock_api_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("dashboard.weather_service.requests.get",
                   return_value=mock_response):
            result = fetch_city_weather("nyc")

        assert isinstance(result, CityWeather)
        assert result.current.city == "nyc"
        assert len(result.forecast) == 3
        assert isinstance(result.fetched_at, datetime)

    def test_raises_on_invalid_city(self):
        with pytest.raises(ValueError, match="Unknown city"):
            fetch_city_weather("tokyo")

    def test_raises_on_http_error(self):
        from requests.exceptions import RequestException
        with patch("dashboard.weather_service.requests.get",
                   side_effect=RequestException("Connection refused")):
            with pytest.raises(WeatherAPIError, match="Connection refused"):
                fetch_city_weather("nyc")

    def test_raises_on_api_error_response(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "error": True,
            "reason": "Invalid parameter"
        }
        mock_response.raise_for_status = MagicMock()

        with patch("dashboard.weather_service.requests.get",
                   return_value=mock_response):
            with pytest.raises(WeatherAPIError, match="Invalid parameter"):
                fetch_city_weather("london")

    def test_raises_on_malformed_json(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"current": {}, "hourly": {}}
        mock_response.raise_for_status = MagicMock()

        with patch("dashboard.weather_service.requests.get",
                   return_value=mock_response):
            with pytest.raises(WeatherAPIError):
                fetch_city_weather("nyc")

    def test_london_fetch_uses_correct_coordinates(self, mock_api_response):
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("dashboard.weather_service.requests.get",
                   return_value=mock_response) as mock_get:
            fetch_city_weather("london")

        called_url = mock_get.call_args[0][0]
        assert "51.5074" in called_url
        assert "-0.1278" in called_url


class TestCityCoordinates:
    """Tests for the CITY_COORDINATES constant."""

    def test_nyc_coordinates(self):
        assert CITY_COORDINATES["nyc"]["latitude"] == 40.7128
        assert CITY_COORDINATES["nyc"]["longitude"] == -74.0060

    def test_london_coordinates(self):
        assert CITY_COORDINATES["london"]["latitude"] == 51.5074
        assert CITY_COORDINATES["london"]["longitude"] == -0.1278

    def test_both_cities_have_labels(self):
        for city in CITY_COORDINATES:
            assert "label" in CITY_COORDINATES[city]
```

---

### 5. Implementation Sequence

The implementation should proceed in this order:

1. **Create `dashboard/__init__.py`** -- empty file, enables package imports
2. **Create `dashboard/weather_service.py`** -- the core module with all constants, dataclasses, WMO mappings, fetch/parse functions, cached wrapper, and render components
3. **Create `tests/test_weather_service.py`** -- comprehensive test suite
4. **Run tests** -- `/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/test_weather_service.py -v` -- verify all tests pass
5. **Modify `dashboard/app.py`** -- add import and weather_panel fragment
6. **Run full test suite** -- `/Users/chris/Projects/city-cycles/venv/bin/python -m pytest tests/ -v` -- ensure no regressions (baseline: 83 pass, 3 skip)
7. **Manual smoke test** -- `streamlit run dashboard/app.py` -- verify weather panel renders, verify 15-minute refresh

---

### 6. Potential Challenges and Mitigations

**Challenge 1: `st.fragment` interaction with the existing session state model.**
The existing app.py uses session state extensively for date filters and page selection. The weather fragment is independent (no session state, no user inputs), so it should not interfere. However, `st.fragment` reruns draw elements in-place and should not affect elements outside the fragment. If there are issues, the fragment can be wrapped in `st.container()` for isolation.

**Challenge 2: Network failures on Streamlit Cloud.**
Open-Meteo is a free API with no rate limits for reasonable usage, but network issues can happen. The `get_city_weather_cached` function returns `None` on failure, and the `weather_panel` fragment displays `st.warning()` in that case. The 15-minute cache means a temporary outage does not cause repeated failures.

**Challenge 3: `st.cache_data` with frozen dataclasses.**
`@st.cache_data` serializes return values. Frozen dataclasses are serializable by default. If issues arise with `datetime` fields, the alternative is to use `@st.cache_resource` instead, though `cache_data` is preferred for data objects.

**Challenge 4: Import structure for testing.**
The test file imports `from dashboard.weather_service import ...`. This requires the `dashboard/__init__.py` to exist and the project root to be in `sys.path` (which pytest handles via `tests/__init__.py` or conftest). If import issues arise, a `conftest.py` at the project root can add the path.

**Challenge 5: The `streamlit-autorefresh` requirement.**
The user explicitly asked to add `streamlit-autorefresh` to requirements.txt. The plan recommends against this in favor of the built-in `st.fragment(run_every="15m")`, which is objectively superior (no third-party dependency, supports partial rerun, first-party API). This should be communicated to the user during implementation.

---

### 7. Summary of All File Changes

| File | Action | Lines Changed (approx) |
|------|--------|----------------------|
| `dashboard/__init__.py` | CREATE | 1 line |
| `dashboard/weather_service.py` | CREATE | ~280 lines |
| `dashboard/app.py` | MODIFY | ~35 lines inserted after line 68 |
| `tests/test_weather_service.py` | CREATE | ~250 lines |
| `requirements.txt` | NO CHANGE | (streamlit-autorefresh NOT needed) |

---

### Critical Files for Implementation
- `/Users/chris/Projects/city-cycles/dashboard/weather_service.py` - Core new module: all weather fetch, parse, cache, and display logic
- `/Users/chris/Projects/city-cycles/dashboard/app.py` - Must be modified to import weather service and add the `@st.fragment(run_every="15m")` weather panel
- `/Users/chris/Projects/city-cycles/tests/test_weather_service.py` - Comprehensive test suite for the weather service, following the mocking patterns from `test_extraction.py`
- `/Users/chris/Projects/city-cycles/tests/test_extraction.py` - Reference file: pattern to follow for mocking `requests.get` calls
- `/Users/chris/Projects/city-cycles/dashboard/__init__.py` - Must be created (empty) to enable package-style imports of `dashboard.weather_service`