# Phase 05: Atmospheric UI & Dashboard Redesign

**Status:** 🔧 IN PROGRESS
**Started:** 2026-02-12

## PR Title
feat: redesign dashboard with atmospheric UI, weather animations, and time-of-day theming

## Risk Level: Medium
## Estimated Effort: 3-5 days
## Dependencies: Phases 03 (Real-time Weather), 04 (Recommendation Engine)
## Unlocks: Complete product experience

## Files Impact
| Action | File |
|--------|------|
| CREATE | dashboard/static/weather_animations.css |
| CREATE | dashboard/static/atmospheric_theme.css |
| CREATE | dashboard/theme/__init__.py |
| CREATE | dashboard/theme/time_of_day.py |
| ~~CREATE~~ | ~~dashboard/theme/color_palette.py~~ REMOVED — colors live in plotly_template.py |
| CREATE | dashboard/theme/plotly_template.py |
| CREATE | dashboard/utils/__init__.py |
| CREATE | dashboard/utils/query_helpers.py |
| CREATE | dashboard/utils/css_injector.py |
| CREATE | dashboard/components/__init__.py |
| CREATE | dashboard/components/city_toggle.py |
| CREATE | dashboard/components/weather_hero.py |
| CREATE | dashboard/components/recommendation_cards.py |
| CREATE | dashboard/components/forecast_strip.py |
| CREATE | dashboard/components/biking_score_gauge.py |
| CREATE | dashboard/components/chart_factory.py |
| CREATE | dashboard/pages/__init__.py |
| CREATE | dashboard/pages/landing.py |
| CREATE | dashboard/pages/ride_analytics.py |
| CREATE | dashboard/pages/weather_deep_dive.py |
| CREATE | dashboard/pages/comparison.py |
| CREATE | .streamlit/config.toml |
| REWRITE | dashboard/app.py |
| MODIFY | .gitignore |
| CREATE | tests/test_dashboard_theme.py |
| CREATE | tests/test_dashboard_css_injector.py |
| CREATE | tests/test_dashboard_components.py |
| CREATE | tests/test_dashboard_chart_factory.py |

## Context
This is the largest phase — transforming the dashboard from a plain analytics tool into an atmospheric, weather-first experience. Rain/snow CSS animations, time-of-day gradient backgrounds, a weather-first landing page with biking score and recommendations, deep-dive analytics pages, city toggle, and a consistent dark atmospheric Plotly theme. The existing 535-line monolithic app.py is restructured into a multi-page Streamlit app with extracted components.

---

# Phase 05: Atmospheric UI & Dashboard Redesign -- Implementation Plan

## 1. Current State Assessment

**Existing dashboard** (`/Users/chris/Projects/city-cycles/dashboard/app.py`, 535 lines):
- Single-file Streamlit app with radio-button sidebar navigation (NYC / London / Comparison)
- In-memory DuckDB connection querying local Parquet mart files
- Uses `st.set_page_config(layout="wide")` with the bicycle emoji page icon
- No custom CSS, no `.streamlit/config.toml`, no `static/` directory
- Three inline `st.markdown(..., unsafe_allow_html=True)` calls for comparison section headers
- Session state pattern: pending/applied filter values, explicit "Apply Date Filter" button
- Data manager (`streamlit_data_manager/parquet_file_manager.py`) downloads 5 mart Parquet files from S3
- Plotly charts use default styling (no custom templates or color palettes)
- No existing multi-page infrastructure (no `pages/` directory)

**Streamlit version**: 1.54.0 (supports `st.navigation` / `st.Page` for multi-page apps)

**CSS injection status**: `st.html()` CSS injection works in 1.54 but requires high-specificity selectors (`.stHeading h1`, etc.). `st.markdown(..., unsafe_allow_html=True)` remains the most reliable method. Must use `!important` and class-targeted selectors for Streamlit overrides.

**Gitignore concern**: `*.toml` is gitignored except `!railway.toml`. Must add `!.streamlit/config.toml` exception.

**Test pattern**: Tests avoid importing `dashboard/app.py` directly due to import-time side effects (Streamlit calls, S3 downloads). Query logic is tested via standalone DuckDB connections. 83 pass, 3 skip baseline.

---

## 2. Architecture Design

### 2.1 Multi-Page App Structure

Use Streamlit's `st.navigation` / `st.Page` API (available since Streamlit 1.36+, well-supported in 1.54). This replaces the current sidebar radio approach with proper page routing.

**New file structure:**

```
dashboard/
    app.py                          # Entrypoint -- st.navigation, shared state, CSS injection
    pages/
        __init__.py
        landing.py                  # Weather-first landing page
        ride_analytics.py           # Existing NYC/London analysis (refactored from app.py)
        weather_deep_dive.py        # Weather-ride correlations
        comparison.py               # Existing comparison (refactored from app.py)
    components/
        __init__.py
        weather_hero.py             # Hero section with weather display
        recommendation_cards.py     # Insight card renderer
        forecast_strip.py           # Horizontal forecast chart
        city_toggle.py              # NYC <-> London toggle widget
        biking_score_gauge.py       # Circular gauge / large score display
        chart_factory.py            # Plotly chart builder with atmospheric theming
    static/
        weather_animations.css      # Rain, snow, sun, cloud CSS animations
        atmospheric_theme.css       # Time-of-day gradients, base dark theme
    theme/
        __init__.py
        time_of_day.py              # Gradient calculation based on city local time
        color_palette.py            # Weather-aware color palette generator
        plotly_template.py          # Custom Plotly chart template
    utils/
        __init__.py
        css_injector.py             # CSS loading and injection helper
        query_helpers.py            # Extracted DuckDB query functions from current app.py
.streamlit/
    config.toml                     # Streamlit theme configuration
```

### 2.2 Entrypoint Rewrite (`dashboard/app.py`)

The current `app.py` is a 535-line monolith. The new entrypoint becomes a lean orchestrator:

```python
"""
City Cycles Analytics Dashboard -- Atmospheric UI
Entrypoint file. Configures pages, injects CSS, manages shared state.
"""
import streamlit as st
import os
import sys

# Add parent directory to path for module imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files
from dashboard.utils.css_injector import inject_atmospheric_css
from dashboard.theme.time_of_day import get_current_gradient
from dashboard.pages import landing, ride_analytics, weather_deep_dive, comparison

# --- Page config (must be first Streamlit call) ---
st.set_page_config(
    page_title="City Cycles Analytics",
    page_icon="\U0001f6b2",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Data setup ---
ensure_local_parquet_files()

# --- CSS injection (atmospheric theme + weather animations) ---
inject_atmospheric_css()

# --- Session state defaults ---
if 'selected_city' not in st.session_state:
    st.session_state.selected_city = 'nyc'

# --- Page navigation ---
pg = st.navigation({
    "Overview": [
        st.Page(landing.render, title="Dashboard", icon="\U0001f326\ufe0f", default=True),
    ],
    "Analytics": [
        st.Page(ride_analytics.render, title="Ride Analytics", icon="\U0001f6b2"),
        st.Page(weather_deep_dive.render, title="Weather Deep Dive", icon="\U0001f321\ufe0f"),
        st.Page(comparison.render, title="City Comparison", icon="\U0001f30d"),
    ],
})

pg.run()
```

Key design decisions:
- `st.navigation` groups pages into "Overview" and "Analytics" sections in the sidebar
- The landing page is `default=True` -- first thing users see
- City toggle is stored in `st.session_state.selected_city` and shared across all pages
- CSS injection happens once in the entrypoint before page rendering

### 2.3 Data Layer: Extract Query Helpers

All DuckDB query logic currently inline in `app.py` moves to `dashboard/utils/query_helpers.py`. This module:

- Creates a single `@st.cache_resource` DuckDB connection
- Wraps `run_query` and `run_query_params` (existing pattern)
- Provides typed query functions: `get_total_rides(location, start, end)`, `get_monthly_trends(location, start, end, agg_type)`, etc.
- Uses `DATA_DIR` resolved from project root (existing pattern)
- Adds `@st.cache_data(ttl=3600)` for expensive queries

```python
"""
Dashboard query helpers -- extracted from monolithic app.py.
All DuckDB queries live here, cached and parameterized.
"""
import streamlit as st
import duckdb
import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Persistent in-memory DuckDB connection."""
    return duckdb.connect(database=':memory:')

def run_query(query: str) -> pd.DataFrame:
    return get_connection().execute(query).fetchdf()

def run_query_params(query: str, params: list) -> pd.DataFrame:
    return get_connection().execute(query, params).fetchdf()

@st.cache_data(ttl=3600)
def get_total_rides(location: str, start_date: str, end_date: str) -> float:
    """Get total rides for a location within a date range."""
    query = f"""
    SELECT SUM(metric_value) as total_rides
    FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics_long.parquet')}'
    WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
    """
    result = run_query_params(query, [location, start_date, end_date])
    return result['total_rides'][0] if not result.empty else 0

# ... similar functions for avg_daily_rides, avg_duration, monthly_trends, etc.
```

---

## 3. CSS Animation System

### 3.1 Weather Animations (`dashboard/static/weather_animations.css`)

All animations are pure CSS, using `@keyframes` and `position: fixed` overlays so they render on top of (or behind) the Streamlit content. Each animation is wrapped in a class that gets toggled by the Python layer based on the current weather condition.

**Rain animation** -- 40-60 pseudo-particle droplets falling at varied speeds and horizontal positions, using `background: linear-gradient` on thin div elements:

```css
/* === RAIN ANIMATION === */
.weather-rain {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    z-index: 1;
    overflow: hidden;
}

.raindrop {
    position: absolute;
    top: -20px;
    width: 2px;
    height: 15px;
    background: linear-gradient(transparent, rgba(174, 194, 224, 0.6));
    border-radius: 0 0 2px 2px;
    animation: rain-fall linear infinite;
}

@keyframes rain-fall {
    0% {
        transform: translateY(-20px);
        opacity: 0;
    }
    10% {
        opacity: 1;
    }
    100% {
        transform: translateY(100vh);
        opacity: 0.3;
    }
}

/* Generate 40 raindrops with varied positions and timing */
.raindrop:nth-child(1)  { left: 2%;  animation-duration: 0.8s; animation-delay: 0.1s; }
.raindrop:nth-child(2)  { left: 5%;  animation-duration: 1.0s; animation-delay: 0.3s; }
.raindrop:nth-child(3)  { left: 8%;  animation-duration: 0.7s; animation-delay: 0.0s; }
/* ... nth-child rules for 40 drops spread across 2%-98% left positions,
   durations between 0.6s-1.2s, delays between 0.0s-0.9s */
.raindrop:nth-child(40) { left: 97%; animation-duration: 0.9s; animation-delay: 0.5s; }
```

**Snow animation** -- 30 snowflake particles with gentle swaying horizontal motion (`translateX` oscillation) combined with vertical fall:

```css
/* === SNOW ANIMATION === */
.weather-snow {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    z-index: 1;
    overflow: hidden;
}

.snowflake {
    position: absolute;
    top: -10px;
    width: 8px;
    height: 8px;
    background: rgba(255, 255, 255, 0.8);
    border-radius: 50%;
    animation: snow-fall linear infinite;
    filter: blur(1px);
}

@keyframes snow-fall {
    0% {
        transform: translateY(-10px) translateX(0) rotate(0deg);
        opacity: 0;
    }
    10% {
        opacity: 1;
    }
    50% {
        transform: translateY(50vh) translateX(30px) rotate(180deg);
    }
    100% {
        transform: translateY(100vh) translateX(-20px) rotate(360deg);
        opacity: 0.2;
    }
}

/* 30 snowflakes with varied sizing, positions, speeds */
.snowflake:nth-child(1) { left: 3%;  width: 6px; height: 6px; animation-duration: 5s; animation-delay: 0.2s; }
/* ... etc for 30 flakes, durations 3s-8s, sizes 4px-10px */
```

**Sun / Clear animation** -- warm glow radial gradient overlay that slowly pulses:

```css
/* === SUN / CLEAR ANIMATION === */
.weather-clear {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    z-index: 0;
    background: radial-gradient(
        ellipse at 85% 15%,
        rgba(255, 223, 100, 0.12) 0%,
        rgba(255, 180, 50, 0.05) 40%,
        transparent 70%
    );
    animation: sun-pulse 6s ease-in-out infinite;
}

@keyframes sun-pulse {
    0%, 100% { opacity: 0.7; }
    50% { opacity: 1.0; }
}
```

**Cloudy animation** -- soft gray overlay with slow drift:

```css
/* === CLOUDY ANIMATION === */
.weather-cloudy {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    z-index: 0;
    background: linear-gradient(
        135deg,
        rgba(150, 160, 175, 0.08) 0%,
        rgba(130, 140, 155, 0.12) 30%,
        rgba(160, 170, 180, 0.06) 60%,
        transparent 100%
    );
    animation: cloud-drift 20s ease-in-out infinite;
}

@keyframes cloud-drift {
    0%, 100% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
}
```

### 3.2 Time-of-Day Gradients (`dashboard/static/atmospheric_theme.css`)

CSS custom properties define the gradient. Python sets the `data-time-period` attribute on the root element, which CSS uses to select the gradient:

```css
/* === TIME-OF-DAY GRADIENT SYSTEM === */

/* Base dark theme overrides for Streamlit */
[data-testid="stAppViewContainer"] {
    transition: background 2s ease;
}

/* Night: 10pm - 5am */
[data-time-period="night"] [data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #0a0e27 0%, #1a1a3e 50%, #0d1117 100%) !important;
}

/* Dawn: 5am - 7am */
[data-time-period="dawn"] [data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #1a1a3e 0%, #4a2040 25%, #c97035 60%, #e8a765 100%) !important;
}

/* Morning: 7am - 10am */
[data-time-period="morning"] [data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #87CEEB 0%, #B0D4E8 40%, #d4e6f1 100%) !important;
}

/* Day: 10am - 4pm */
[data-time-period="day"] [data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #5DADE2 0%, #85C1E9 30%, #AED6F1 70%, #d6eaf8 100%) !important;
}

/* Golden Hour: 4pm - 7pm */
[data-time-period="golden"] [data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #E8A838 0%, #D4740A 30%, #B55A10 60%, #5D3A1A 100%) !important;
}

/* Dusk: 7pm - 10pm */
[data-time-period="dusk"] [data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #4A235A 0%, #6C3483 30%, #2C3E50 70%, #1B2631 100%) !important;
}

/* === GLOBAL DARK THEME OVERRIDES === */

/* Make sidebar semi-transparent dark */
[data-testid="stSidebar"] {
    background: rgba(13, 17, 23, 0.85) !important;
    backdrop-filter: blur(10px);
}

/* Text readability on all backgrounds */
[data-testid="stAppViewContainer"] .stMarkdown,
[data-testid="stAppViewContainer"] .stMetric,
[data-testid="stAppViewContainer"] h1,
[data-testid="stAppViewContainer"] h2,
[data-testid="stAppViewContainer"] h3,
[data-testid="stAppViewContainer"] p {
    color: #EAEAEA !important;
    text-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
}

/* Metric value emphasis */
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #FFFFFF !important;
    font-weight: 700;
    text-shadow: 0 2px 4px rgba(0, 0, 0, 0.4);
}

/* Card-style containers */
[data-testid="stAppViewContainer"] .stExpander,
[data-testid="stAppViewContainer"] [data-testid="stDataFrame"] {
    background: rgba(30, 35, 45, 0.7) !important;
    border-radius: 12px;
    border: 1px solid rgba(255, 255, 255, 0.08);
    backdrop-filter: blur(8px);
}
```

### 3.3 Time-of-Day Python Module (`dashboard/theme/time_of_day.py`)

```python
"""
Calculate time-of-day period based on city's local timezone.
NYC = America/New_York, London = Europe/London.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

CITY_TIMEZONES = {
    'nyc': ZoneInfo('America/New_York'),
    'london': ZoneInfo('Europe/London'),
}

TIME_PERIODS = [
    # (start_hour, end_hour, period_name)
    (0, 5, 'night'),
    (5, 7, 'dawn'),
    (7, 10, 'morning'),
    (10, 16, 'day'),
    (16, 19, 'golden'),
    (19, 22, 'dusk'),
    (22, 24, 'night'),
]

def get_time_period(city: str) -> str:
    """Return the time-of-day period name for the given city's current local time."""
    tz = CITY_TIMEZONES.get(city, ZoneInfo('UTC'))
    local_hour = datetime.now(tz).hour
    for start, end, period in TIME_PERIODS:
        if start <= local_hour < end:
            return period
    return 'night'
```

### 3.4 CSS Injector (`dashboard/utils/css_injector.py`)

```python
"""
Load and inject CSS files into Streamlit.
Uses st.markdown with unsafe_allow_html=True (most reliable method in Streamlit 1.54).
"""
import streamlit as st
import os

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static')

def _load_css_file(filename: str) -> str:
    """Read a CSS file from the static directory."""
    filepath = os.path.join(STATIC_DIR, filename)
    with open(filepath, 'r') as f:
        return f.read()

def inject_atmospheric_css() -> None:
    """Inject all atmospheric CSS into the Streamlit page."""
    weather_css = _load_css_file('weather_animations.css')
    theme_css = _load_css_file('atmospheric_theme.css')
    combined = f"<style>{weather_css}\n{theme_css}</style>"
    st.markdown(combined, unsafe_allow_html=True)

def inject_weather_animation(weather_code: int) -> None:
    """
    Inject the appropriate weather animation HTML based on WMO weather code.
    
    WMO codes (from Phase 03 weather_service):
      0-1: Clear -> .weather-clear
      2-3: Cloudy -> .weather-cloudy
      45-48: Fog -> .weather-cloudy
      51-67: Drizzle/Rain -> .weather-rain
      71-77: Snow -> .weather-snow
      80-82: Rain showers -> .weather-rain
      85-86: Snow showers -> .weather-snow
      95-99: Thunderstorm -> .weather-rain
    """
    if weather_code <= 1:
        animation_class = 'weather-clear'
        particles = '<div class="weather-clear"></div>'
    elif weather_code <= 48:
        animation_class = 'weather-cloudy'
        particles = '<div class="weather-cloudy"></div>'
    elif weather_code <= 67 or 80 <= weather_code <= 82 or weather_code >= 95:
        # Rain: generate 40 raindrop divs
        drops = ''.join(f'<div class="raindrop"></div>' for _ in range(40))
        particles = f'<div class="weather-rain">{drops}</div>'
    elif 71 <= weather_code <= 77 or 85 <= weather_code <= 86:
        # Snow: generate 30 snowflake divs
        flakes = ''.join(f'<div class="snowflake"></div>' for _ in range(30))
        particles = f'<div class="weather-snow">{flakes}</div>'
    else:
        particles = '<div class="weather-clear"></div>'
    
    st.markdown(particles, unsafe_allow_html=True)

def inject_time_period(period: str) -> None:
    """Set the data-time-period attribute on the page root via JavaScript."""
    js = f"""
    <script>
        const root = document.documentElement;
        root.setAttribute('data-time-period', '{period}');
    </script>
    """
    st.markdown(js, unsafe_allow_html=True)
```

---

## 4. Page Implementations

### 4.1 Landing Page (`dashboard/pages/landing.py`)

The landing page is the atmospheric showcase. It integrates Phase 03 (weather_service) and Phase 04 (recommendation_engine).

```python
"""
Landing page -- Weather-first atmospheric dashboard.
Shows current conditions, biking score, recommendations, forecast.
"""
import streamlit as st
from dashboard.components.city_toggle import render_city_toggle
from dashboard.components.weather_hero import render_weather_hero
from dashboard.components.recommendation_cards import render_recommendations
from dashboard.components.forecast_strip import render_forecast_strip
from dashboard.components.biking_score_gauge import render_biking_score
from dashboard.utils.css_injector import inject_weather_animation, inject_time_period
from dashboard.theme.time_of_day import get_time_period

# Phase 03 and Phase 04 (actual module paths)
from dashboard.weather_service import get_city_weather_cached, fetch_city_weather
from dashboard.recommendation_engine import get_recommendations, WeatherConditions, RecommendationResult

def render():
    """Render the atmospheric landing page."""
    # --- City toggle at top ---
    city = render_city_toggle()
    
    # --- Time-of-day gradient ---
    period = get_time_period(city)
    inject_time_period(period)
    
    # --- Fetch weather data (Phase 03) ---
    weather = get_current_weather(city)
    forecast = get_forecast(city)
    
    # --- Weather animation overlay ---
    inject_weather_animation(weather.wmo_code)
    
    # --- Hero section ---
    render_weather_hero(city, weather)
    
    # --- Key metrics row ---
    col1, col2, col3 = st.columns(3)
    with col1:
        render_biking_score(get_biking_score(city, weather))
    with col2:
        st.metric("Temperature", f"{weather.temperature_c:.0f}\u00b0C / {weather.temperature_f:.0f}\u00b0F")
    with col3:
        st.metric("Condition", weather.condition_text)
    
    st.divider()
    
    # --- Recommendations ---
    st.subheader("Riding Insights")
    recommendations = get_recommendations(city, weather)
    render_recommendations(recommendations)
    
    st.divider()
    
    # --- Forecast strip ---
    st.subheader("Next 24 Hours")
    render_forecast_strip(forecast)
```

### 4.2 Ride Analytics Page (`dashboard/pages/ride_analytics.py`)

This page contains the refactored existing dashboard logic from lines 271-438 of the current `app.py`. Key changes:

- City selection via `st.session_state.selected_city` instead of sidebar radio
- Queries moved to `query_helpers.py`
- Plotly charts use the atmospheric template from `plotly_template.py`
- Weather overlay bars on ride trend charts (precipitation data behind ride lines)
- Date range filter remains in sidebar (Streamlit sidebar is shared across pages)

The existing query logic for `total_rides`, `avg_daily_rides`, `avg_duration`, monthly ride trends, duration trends, hourly patterns, member percentage, and station growth all move here verbatim, but styled through the chart factory.

### 4.3 Weather Deep Dive Page (`dashboard/pages/weather_deep_dive.py`)

New page. Provides:
- **Temperature vs Rides scatter** -- daily ride count (y) vs average temperature (x), colored by season
- **Precipitation impact** -- bar chart showing ride counts on dry vs rainy vs snowy days
- **Seasonal patterns** -- ride counts grouped by meteorological season, year-over-year
- **Weather condition breakdown** -- pie/donut chart showing distribution of WMO weather codes during riding hours

This page depends on historical weather data that would need to be added to the data pipeline (a new mart: `mart_daily_weather_rides.parquet`). The plan should note this as a data dependency that may need its own dbt model.

### 4.4 Comparison Page (`dashboard/pages/comparison.py`)

Refactored from lines 186-532 of the current `app.py`. Same logic, but:
- Uses chart factory for atmospheric styling
- Adds weather comparison between cities (current temp, condition side-by-side)
- Same query patterns, moved to `query_helpers.py`

---

## 5. Component Implementations

### 5.1 City Toggle (`dashboard/components/city_toggle.py`)

```python
"""
Prominent city toggle switch between NYC and London.
Persists selection in session state.
"""
import streamlit as st

CITY_CONFIG = {
    'nyc': {'label': 'New York City', 'emoji': '\U0001f5fd', 'timezone': 'America/New_York'},
    'london': {'label': 'London', 'emoji': '\U0001f1ec\U0001f1e7', 'timezone': 'Europe/London'},
}

def render_city_toggle() -> str:
    """Render a city toggle and return the selected city key ('nyc' or 'london')."""
    col1, col2, col3 = st.columns([2, 3, 2])
    with col2:
        selected = st.toggle(
            "London",
            value=(st.session_state.get('selected_city', 'nyc') == 'london'),
            key='city_toggle_widget'
        )
        city = 'london' if selected else 'nyc'
        st.session_state.selected_city = city
        config = CITY_CONFIG[city]
        st.markdown(
            f"<h2 style='text-align:center;margin:0;'>{config['emoji']} {config['label']}</h2>",
            unsafe_allow_html=True
        )
    return city
```

### 5.2 Weather Hero (`dashboard/components/weather_hero.py`)

Renders the large temperature display and weather condition. Uses styled HTML:

```python
"""Hero section: large temperature, weather condition, city name."""
import streamlit as st

def render_weather_hero(city: str, weather) -> None:
    """Render the atmospheric hero section."""
    city_names = {'nyc': 'New York City', 'london': 'London'}
    hero_html = f"""
    <div style="
        text-align: center;
        padding: 2rem 0;
        margin-bottom: 1rem;
    ">
        <h1 style="
            font-size: 4rem;
            font-weight: 200;
            margin: 0;
            letter-spacing: 0.05em;
            color: #FFFFFF;
            text-shadow: 0 2px 10px rgba(0,0,0,0.3);
        ">{weather.temperature_c:.0f}\u00b0</h1>
        <p style="
            font-size: 1.4rem;
            font-weight: 300;
            margin: 0.5rem 0;
            color: rgba(255,255,255,0.85);
            text-transform: uppercase;
            letter-spacing: 0.15em;
        ">{weather.condition_text}</p>
        <p style="
            font-size: 1rem;
            color: rgba(255,255,255,0.6);
            margin: 0;
        ">{city_names.get(city, city)}</p>
    </div>
    """
    st.markdown(hero_html, unsafe_allow_html=True)
```

### 5.3 Recommendation Cards (`dashboard/components/recommendation_cards.py`)

Renders 3-5 insight cards from the Phase 04 recommendation engine. Each card is a styled container with icon, title, and description.

```python
"""Render recommendation insight cards in a responsive grid."""
import streamlit as st

CARD_STYLE = """
    background: rgba(30, 35, 50, 0.6);
    border-radius: 12px;
    padding: 1.2rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    backdrop-filter: blur(8px);
    height: 100%;
"""

def render_recommendations(recommendations: list) -> None:
    """Render a row of recommendation cards."""
    cols = st.columns(min(len(recommendations), 5))
    for i, rec in enumerate(recommendations):
        with cols[i % len(cols)]:
            st.markdown(f"""
            <div style="{CARD_STYLE}">
                <p style="font-size: 1.5rem; margin: 0 0 0.5rem 0;">{rec.icon}</p>
                <p style="font-weight: 600; margin: 0 0 0.3rem 0; color: #FFFFFF;">{rec.title}</p>
                <p style="font-size: 0.85rem; color: rgba(255,255,255,0.7); margin: 0;">{rec.description}</p>
            </div>
            """, unsafe_allow_html=True)
```

### 5.4 Forecast Strip (`dashboard/components/forecast_strip.py`)

Renders the next 12-24 hours as a compact horizontal Plotly chart -- temperature line with precipitation bars behind it, colored by weather condition:

```python
"""Compact horizontal forecast chart for next 24 hours."""
import streamlit as st
import plotly.graph_objects as go

def render_forecast_strip(forecast) -> None:
    """Render a compact 24-hour forecast strip chart."""
    fig = go.Figure()
    
    # Precipitation bars (background)
    fig.add_trace(go.Bar(
        x=forecast.hours,
        y=forecast.precipitation_mm,
        name='Precip',
        marker_color='rgba(100, 150, 220, 0.3)',
        yaxis='y2',
    ))
    
    # Temperature line (foreground)
    fig.add_trace(go.Scatter(
        x=forecast.hours,
        y=forecast.temperature_c,
        name='Temp',
        mode='lines+markers',
        line=dict(color='#FFFFFF', width=2),
        marker=dict(size=6, color='#FFFFFF'),
    ))
    
    fig.update_layout(
        height=180,
        margin=dict(l=40, r=20, t=10, b=30),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='rgba(255,255,255,0.7)',
        showlegend=False,
        yaxis=dict(title='', gridcolor='rgba(255,255,255,0.05)'),
        yaxis2=dict(overlaying='y', side='right', showgrid=False, showticklabels=False),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
    )
    
    st.plotly_chart(fig, use_container_width=True)
```

### 5.5 Biking Score Gauge (`dashboard/components/biking_score_gauge.py`)

Renders the biking score from Phase 04 as a large number with a color indicator:

```python
"""Biking score display -- large number with color-coded condition ring."""
import streamlit as st

def _score_color(score: int) -> str:
    """Return hex color based on score (0-100)."""
    if score >= 80:
        return '#2ECC71'   # green
    elif score >= 60:
        return '#F39C12'   # amber
    elif score >= 40:
        return '#E67E22'   # orange
    else:
        return '#E74C3C'   # red

def render_biking_score(score: int) -> None:
    """Render the biking score as a styled metric."""
    color = _score_color(score)
    st.markdown(f"""
    <div style="text-align: center;">
        <p style="font-size: 0.85rem; color: rgba(255,255,255,0.6); margin: 0 0 0.3rem 0; 
           text-transform: uppercase; letter-spacing: 0.1em;">Biking Score</p>
        <p style="font-size: 3.5rem; font-weight: 700; margin: 0; color: {color};
           text-shadow: 0 0 20px {color}40;">{score}</p>
        <p style="font-size: 0.75rem; color: rgba(255,255,255,0.5); margin: 0;">out of 100</p>
    </div>
    """, unsafe_allow_html=True)
```

---

## 6. Plotly Chart Theming

### 6.1 Custom Plotly Template (`dashboard/theme/plotly_template.py`)

```python
"""
Atmospheric Plotly chart template.
Dark background, weather-aware colors, consistent typography.
"""
import plotly.graph_objects as go
import plotly.io as pio

ATMOSPHERIC_COLORS = [
    '#5DADE2',  # sky blue
    '#E74C3C',  # warm red
    '#2ECC71',  # green
    '#F39C12',  # amber
    '#9B59B6',  # purple
    '#1ABC9C',  # teal
    '#E67E22',  # orange
    '#3498DB',  # darker blue
]

RAIN_COLORS = ['#2C3E50', '#5DADE2', '#85C1E9', '#AED6F1', '#D6EAF8']
SUN_COLORS = ['#F39C12', '#E74C3C', '#E67E22', '#F5B041', '#FAD7A0']

def create_atmospheric_template() -> go.layout.Template:
    """Create a Plotly template matching the atmospheric dashboard theme."""
    template = go.layout.Template()
    
    template.layout = go.Layout(
        paper_bgcolor='rgba(0, 0, 0, 0)',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        font=dict(
            family='-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
            color='rgba(255, 255, 255, 0.8)',
            size=13,
        ),
        title=dict(
            font=dict(size=18, color='#FFFFFF'),
            x=0.0,
        ),
        xaxis=dict(
            gridcolor='rgba(255, 255, 255, 0.06)',
            linecolor='rgba(255, 255, 255, 0.1)',
            zerolinecolor='rgba(255, 255, 255, 0.1)',
        ),
        yaxis=dict(
            gridcolor='rgba(255, 255, 255, 0.06)',
            linecolor='rgba(255, 255, 255, 0.1)',
            zerolinecolor='rgba(255, 255, 255, 0.1)',
        ),
        colorway=ATMOSPHERIC_COLORS,
        legend=dict(
            bgcolor='rgba(0, 0, 0, 0)',
            font=dict(color='rgba(255, 255, 255, 0.7)'),
        ),
        hoverlabel=dict(
            bgcolor='rgba(20, 25, 35, 0.9)',
            font_color='#FFFFFF',
            bordercolor='rgba(255, 255, 255, 0.1)',
        ),
    )
    
    return template

def register_template() -> None:
    """Register the atmospheric template as 'atmospheric' in Plotly's template registry."""
    pio.templates['atmospheric'] = create_atmospheric_template()
    pio.templates.default = 'atmospheric'
```

### 6.2 Chart Factory (`dashboard/components/chart_factory.py`)

A wrapper that creates Plotly charts with consistent atmospheric styling. The existing `px.line(...)` and `px.bar(...)` calls in `app.py` are replaced with calls to this factory:

```python
"""
Chart factory -- creates consistently-themed Plotly figures.
All charts use the atmospheric template and consistent color palettes.
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dashboard.theme.plotly_template import ATMOSPHERIC_COLORS

MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

def monthly_trend_chart(df: pd.DataFrame, y_col: str, y_label: str, 
                        title: str, color_col: str = 'year') -> go.Figure:
    """Create a monthly trend line chart with year overlay."""
    fig = px.line(df, x='month', y=y_col, color=color_col,
                  title=title, labels={y_col: y_label, 'month': 'Month'},
                  template='atmospheric')
    fig.update_xaxes(
        tickmode='array',
        tickvals=list(range(1, 13)),
        ticktext=MONTH_LABELS,
    )
    return fig

def hourly_bar_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create an hourly distribution bar chart."""
    fig = px.bar(df, x='hour_of_day', y='ride_count', title=title,
                 template='atmospheric',
                 color_discrete_sequence=['#5DADE2'])
    fig.update_layout(bargap=0.15)
    return fig

# ... additional factory methods for station growth, comparison, etc.
```

---

## 7. Streamlit Theme Configuration

### 7.1 `.streamlit/config.toml`

```toml
[theme]
primaryColor = "#5DADE2"
backgroundColor = "#0d1117"
secondaryBackgroundColor = "#161b22"
textColor = "#EAEAEA"
font = "sans serif"

[server]
headless = true
```

### 7.2 Gitignore Update

The `.gitignore` line `*.toml` with only `!railway.toml` excluded must be updated to also exclude `.streamlit/config.toml`:

Add: `!.streamlit/config.toml`

---

## 8. Upstream Dependencies (Phase 03 + Phase 04 Interfaces)

The landing page and weather deep dive require interfaces from Phases 03 and 04. Phase 05 should define the **expected interfaces** so those phases know what to deliver. If Phases 03/04 are not complete when implementation begins, the landing page should gracefully degrade.

### 8.1 Expected Weather Service Interface (Phase 03)

```python
# What Phase 05 expects from weather_service.weather_service

class CurrentWeather:
    temperature_c: float
    temperature_f: float
    condition_text: str      # e.g., "Partly Cloudy", "Light Rain"
    wmo_code: int            # WMO weather interpretation code (0-99)
    wind_speed_kmh: float
    humidity_percent: float
    feels_like_c: float

class HourlyForecast:
    hours: list[str]         # ISO timestamps for next 24 hours
    temperature_c: list[float]
    precipitation_mm: list[float]
    wmo_codes: list[int]

def get_current_weather(city: str) -> CurrentWeather: ...
def get_forecast(city: str) -> HourlyForecast: ...
```

### 8.2 Expected Recommendation Engine Interface (Phase 04)

```python
# What Phase 05 expects from recommendation_engine.recommendation_engine

class Recommendation:
    icon: str          # emoji
    title: str         # short title, e.g. "Great Day to Ride"
    description: str   # 1-2 sentence insight

def get_biking_score(city: str, weather: CurrentWeather) -> int: ...  # 0-100
def get_recommendations(city: str, weather: CurrentWeather) -> list[Recommendation]: ...
```

### 8.3 ~~Graceful Degradation Pattern~~ REMOVED

Phases 03 and 04 are both merged. Direct imports — no try/except wrappers needed.

---

## 9. Weather Deep Dive: Data Dependency

The Weather Deep Dive page needs historical weather data correlated with ride data. This requires:

1. **New dbt model**: `mart_daily_weather_rides` joining daily ride counts with historical weather observations
2. **Historical weather data source**: Open-Meteo Archive API (free, no API key) provides historical weather by date and coordinates
3. **New parquet mart**: `mart_daily_weather_rides.parquet` exported to S3 and added to `MARTS` list in `parquet_file_manager.py`

This is a significant data pipeline addition. **Recommendation**: Phase 05 should implement the Weather Deep Dive page UI with placeholder data, and document the data pipeline requirements for a separate task. The page can show a "Historical weather data coming soon" message until the pipeline work is complete.

---

## 10. Testing Strategy

### 10.1 Test File Structure

```
tests/
    test_dashboard_theme.py              # Time-of-day calculation, color palette
    test_dashboard_css_injector.py       # CSS loading, weather code mapping
    test_dashboard_components.py         # Component rendering (HTML output validation)
    test_dashboard_chart_factory.py      # Plotly template, chart creation
    test_dashboard_query_helpers.py      # Extracted query functions (extends existing test_dashboard.py)
```

### 10.2 Test Design

Tests follow the existing pattern: avoid importing `dashboard/app.py` directly (due to Streamlit side effects). Instead, test individual modules.

**`test_dashboard_theme.py`** -- Test the time-of-day calculation:

```python
"""Tests for dashboard/theme/time_of_day.py."""
import pytest
from unittest.mock import patch
from datetime import datetime
from zoneinfo import ZoneInfo

class TestTimePeriod:
    """Test time-of-day period calculation."""

    def test_night_period(self):
        """Hours 22-23 and 0-4 should return 'night'."""
        from dashboard.theme.time_of_day import get_time_period
        # Mock datetime.now to return 2am NYC time
        mock_dt = datetime(2026, 2, 12, 2, 0, tzinfo=ZoneInfo('America/New_York'))
        with patch('dashboard.theme.time_of_day.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_dt
            assert get_time_period('nyc') == 'night'

    def test_dawn_period(self):
        """Hours 5-6 should return 'dawn'."""
        # ... similar pattern

    def test_day_period(self):
        """Hours 10-15 should return 'day'."""
        # ...

    def test_golden_hour_period(self):
        """Hours 16-18 should return 'golden'."""
        # ...

    def test_london_uses_london_timezone(self):
        """London should use Europe/London timezone."""
        # ...

    def test_unknown_city_uses_utc(self):
        """Unknown city should fall back to UTC."""
        # ...
```

**`test_dashboard_css_injector.py`** -- Test CSS file loading and weather code mapping:

```python
"""Tests for dashboard/utils/css_injector.py."""
import pytest
from unittest.mock import patch, mock_open

class TestWeatherCodeMapping:
    """Test WMO weather code to animation class mapping."""

    def test_clear_codes(self):
        """WMO codes 0-1 should produce clear animation."""
        # Test the mapping logic without actually calling st.markdown

    def test_rain_codes(self):
        """WMO codes 51-67 should produce rain animation with 40 drops."""
        # Verify the HTML contains 40 raindrop divs

    def test_snow_codes(self):
        """WMO codes 71-77 should produce snow animation with 30 flakes."""
        # Verify the HTML contains 30 snowflake divs

    def test_cloudy_codes(self):
        """WMO codes 2-48 should produce cloudy animation."""
        # ...

class TestCSSFileLoading:
    """Test CSS file loading from static directory."""

    def test_loads_weather_animations_css(self):
        """Should load weather_animations.css from static directory."""
        # ...

    def test_loads_atmospheric_theme_css(self):
        """Should load atmospheric_theme.css from static directory."""
        # ...

    def test_raises_on_missing_file(self):
        """Should raise FileNotFoundError for missing CSS files."""
        # ...
```

**`test_dashboard_chart_factory.py`** -- Test Plotly template and chart creation:

```python
"""Tests for dashboard/theme/plotly_template.py and dashboard/components/chart_factory.py."""
import pytest
import plotly.graph_objects as go

class TestAtmosphericTemplate:
    """Test the custom Plotly template."""

    def test_template_has_transparent_background(self):
        """Template should have transparent paper and plot backgrounds."""
        from dashboard.theme.plotly_template import create_atmospheric_template
        template = create_atmospheric_template()
        assert template.layout.paper_bgcolor == 'rgba(0, 0, 0, 0)'
        assert template.layout.plot_bgcolor == 'rgba(0, 0, 0, 0)'

    def test_template_has_color_sequence(self):
        """Template should define a colorway."""
        from dashboard.theme.plotly_template import create_atmospheric_template
        template = create_atmospheric_template()
        assert len(template.layout.colorway) >= 5

    def test_register_template(self):
        """register_template should add 'atmospheric' to Plotly registry."""
        from dashboard.theme.plotly_template import register_template
        import plotly.io as pio
        register_template()
        assert 'atmospheric' in pio.templates
```

**`test_dashboard_components.py`** -- Test component HTML output:

```python
"""Tests for dashboard component rendering functions."""
import pytest

class TestBikingScoreGauge:
    """Test the biking score gauge component."""

    def test_score_color_high(self):
        """Score >= 80 should return green."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(85) == '#2ECC71'

    def test_score_color_medium(self):
        """Score 60-79 should return amber."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(65) == '#F39C12'

    def test_score_color_low(self):
        """Score < 40 should return red."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(30) == '#E74C3C'

class TestCityToggle:
    """Test city toggle configuration."""

    def test_city_config_has_both_cities(self):
        """CITY_CONFIG should contain 'nyc' and 'london'."""
        from dashboard.components.city_toggle import CITY_CONFIG
        assert 'nyc' in CITY_CONFIG
        assert 'london' in CITY_CONFIG

    def test_city_config_has_required_fields(self):
        """Each city config should have label, emoji, and timezone."""
        from dashboard.components.city_toggle import CITY_CONFIG
        for city_key, config in CITY_CONFIG.items():
            assert 'label' in config
            assert 'emoji' in config
            assert 'timezone' in config
```

---

## 11. Implementation Sequence

### Step 1: Infrastructure (no visual changes yet)
1. Create `.streamlit/config.toml` with dark theme
2. Update `.gitignore` to allow `.streamlit/config.toml`
3. Create `dashboard/static/` directory
4. Create `dashboard/theme/` package with `__init__.py`
5. Create `dashboard/utils/` package with `__init__.py`
6. Create `dashboard/components/` package with `__init__.py`
7. Create `dashboard/pages/` package with `__init__.py`

### Step 2: Extract query helpers
1. Create `dashboard/utils/query_helpers.py` -- extract all DuckDB query logic from `app.py`
2. Create `dashboard/utils/css_injector.py` -- CSS loading utilities
3. Write `tests/test_dashboard_query_helpers.py` (extends existing `test_dashboard.py`)

### Step 3: CSS and theming
1. Create `dashboard/static/weather_animations.css` (rain, snow, clear, cloudy)
2. Create `dashboard/static/atmospheric_theme.css` (time-of-day gradients, dark theme overrides)
3. Create `dashboard/theme/time_of_day.py`
4. Create `dashboard/theme/color_palette.py`
5. Create `dashboard/theme/plotly_template.py`
6. Write `tests/test_dashboard_theme.py` and `tests/test_dashboard_css_injector.py`

### Step 4: Components
1. Create `dashboard/components/city_toggle.py`
2. Create `dashboard/components/weather_hero.py`
3. Create `dashboard/components/recommendation_cards.py`
4. Create `dashboard/components/forecast_strip.py`
5. Create `dashboard/components/biking_score_gauge.py`
6. Create `dashboard/components/chart_factory.py`
7. Write `tests/test_dashboard_components.py` and `tests/test_dashboard_chart_factory.py`

### Step 5: Page modules
1. Create `dashboard/pages/landing.py` (with graceful degradation for missing Phase 03/04)
2. Create `dashboard/pages/ride_analytics.py` (refactored from app.py lines 271-438)
3. Create `dashboard/pages/weather_deep_dive.py` (placeholder for historical data)
4. Create `dashboard/pages/comparison.py` (refactored from app.py lines 186-532)

### Step 6: Entrypoint rewrite
1. Rewrite `dashboard/app.py` to use `st.navigation` / `st.Page`
2. Remove all inline page logic (moved to page modules)
3. Test locally with `streamlit run dashboard/app.py`

### Step 7: Update data manager
1. Add any new mart files to `MARTS` list in `streamlit_data_manager/parquet_file_manager.py`

### Step 8: Dependency management
1. Add `requests` (for Phase 03 weather API, if not already present -- it is: `requests==2.32.5`)
2. No new dependencies needed for Phase 05 itself; `zoneinfo` is stdlib in Python 3.9+

---

## 12. Risk Assessment and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| CSS specificity conflicts with Streamlit internals | Medium | Use `!important`, target Streamlit's `data-testid` attributes, test in browser DevTools |
| Phase 03/04 not ready when Phase 05 starts | High | Graceful degradation pattern (try/except ImportError). Landing page works without weather. |
| Weather animations cause performance issues on low-end devices | Low | Animations use `pointer-events: none`, are pure CSS (no JS), limited particle count (30-40) |
| Time-of-day gradient makes text unreadable | Medium | All text uses `text-shadow` for contrast; tested against all 6 gradient periods |
| `st.navigation` API changes in future Streamlit versions | Low | Pin `streamlit==1.54.0` in requirements.txt; API is stable |
| `.streamlit/config.toml` gitignore conflict | Low | Update `.gitignore` early; verify with `git status` |
| Historical weather data mart not yet available | Medium | Weather Deep Dive shows placeholder; document data pipeline requirements separately |

---

## 13. Color Palette Reference

For the junior team's reference, all color codes used across the system:

| Context | Color | Hex | Usage |
|---------|-------|-----|-------|
| Primary blue | Sky blue | `#5DADE2` | Primary accent, chart default |
| Warm red | Coral | `#E74C3C` | Alerts, low scores |
| Green | Emerald | `#2ECC71` | Good scores, positive trends |
| Amber | Gold | `#F39C12` | Warnings, medium scores |
| Purple | Amethyst | `#9B59B6` | Chart series 5 |
| Teal | Turquoise | `#1ABC9C` | Chart series 6 |
| Orange | Carrot | `#E67E22` | Chart series 7, low-medium scores |
| Text primary | Near white | `#EAEAEA` | Body text |
| Text secondary | Light gray | `rgba(255,255,255,0.7)` | Descriptions, captions |
| Text tertiary | Dim gray | `rgba(255,255,255,0.5)` | Subtle labels |
| Background dark | GitHub dark | `#0d1117` | Base background |
| Sidebar bg | Semi-transparent | `rgba(13,17,23,0.85)` | Sidebar with blur |
| Card background | Dark blue | `rgba(30,35,50,0.6)` | Card containers |
| Grid lines | Very subtle | `rgba(255,255,255,0.06)` | Chart grids |
| Raindrop | Blue-gray | `rgba(174,194,224,0.6)` | Rain animation |
| Snowflake | Near white | `rgba(255,255,255,0.8)` | Snow animation |

---

### Critical Files for Implementation
- `/Users/chris/Projects/city-cycles/dashboard/app.py` - Core file to restructure: current 535-line monolith becomes lean entrypoint with st.navigation
- `/Users/chris/Projects/city-cycles/streamlit_data_manager/parquet_file_manager.py` - Must update MARTS list if new weather mart is added
- `/Users/chris/Projects/city-cycles/.gitignore` - Must add exception for `.streamlit/config.toml` (currently blocked by `*.toml` rule)
- `/Users/chris/Projects/city-cycles/tests/test_dashboard.py` - Existing test pattern to follow; new tests must avoid importing app.py directly due to side effects
- `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/marts/mart_daily_metrics_long.sql` - Reference for existing mart schema; needed to understand query patterns that the refactored pages must preserve