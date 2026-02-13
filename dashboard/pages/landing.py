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
from dashboard.weather_service import get_city_weather_cached
from dashboard.recommendation_engine import (
    get_recommendations,
    WeatherConditions,
)


def _weather_to_conditions(weather_data) -> WeatherConditions:
    """Bridge Phase 03 CityWeather to Phase 04 WeatherConditions."""
    current = weather_data.current
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


@st.fragment(run_every="15m")
def _weather_content(city: str) -> None:
    """Auto-refreshing weather content block."""
    weather = get_city_weather_cached(city)
    if not weather:
        st.warning(f"Weather data unavailable for {city}.")
        return

    # Weather animation overlay
    inject_weather_animation(weather.current.weather_code)

    # Hero section
    render_weather_hero(city, weather.current)

    # Key metrics row
    col1, col2, col3 = st.columns(3)
    conditions = _weather_to_conditions(weather)
    result = get_recommendations(conditions)

    with col1:
        render_biking_score(result.biking_score.score, result.biking_score.label)
    with col2:
        current = weather.current
        st.metric("Temperature",
                  f"{current.temperature_c:.0f}\u00b0C / {current.temperature_f:.0f}\u00b0F")
    with col3:
        st.metric("Wind", f"{current.wind_speed_kmh:.0f} km/h")

    st.divider()

    # Recommendations
    st.subheader("Riding Insights")
    render_recommendations(result)

    st.divider()

    # Forecast strip
    st.subheader("Next 24 Hours")
    render_forecast_strip(weather.forecast)


def render():
    """Render the atmospheric landing page."""
    # City toggle at top
    city = render_city_toggle()

    # Time-of-day gradient
    period = get_time_period(city)
    inject_time_period(period)

    # Weather content (auto-refreshing fragment)
    _weather_content(city)
