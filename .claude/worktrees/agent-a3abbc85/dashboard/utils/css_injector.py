"""
Load and inject CSS files into Streamlit.
Uses st.markdown with unsafe_allow_html=True (most reliable in Streamlit 1.54).
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


# ---------------------------------------------------------------------------
# WMO weather code to animation class mapping
# ---------------------------------------------------------------------------

_WMO_RAIN_CODES = set(range(51, 68)) | {80, 81, 82, 95, 96, 99}
_WMO_SNOW_CODES = set(range(71, 78)) | {85, 86}
_WMO_CLOUD_CODES = set(range(2, 49))


def get_animation_class(weather_code: int) -> str:
    """Map a WMO weather code to a CSS animation class name."""
    if weather_code <= 1:
        return 'weather-clear'
    if weather_code in _WMO_SNOW_CODES:
        return 'weather-snow'
    if weather_code in _WMO_RAIN_CODES:
        return 'weather-rain'
    if weather_code in _WMO_CLOUD_CODES:
        return 'weather-cloudy'
    return 'weather-clear'


def _build_particle_html(weather_code: int) -> str:
    """Build the HTML for weather animation particles."""
    if weather_code <= 1:
        return '<div class="weather-clear"></div>'
    if weather_code in _WMO_SNOW_CODES:
        flakes = ''.join('<div class="snowflake"></div>' for _ in range(30))
        return f'<div class="weather-snow">{flakes}</div>'
    if weather_code in _WMO_RAIN_CODES:
        drops = ''.join('<div class="raindrop"></div>' for _ in range(40))
        return f'<div class="weather-rain">{drops}</div>'
    if weather_code in _WMO_CLOUD_CODES:
        return '<div class="weather-cloudy"></div>'
    return '<div class="weather-clear"></div>'


def inject_weather_animation(weather_code: int) -> None:
    """Inject the appropriate weather animation based on WMO code."""
    html = _build_particle_html(weather_code)
    st.markdown(html, unsafe_allow_html=True)


def inject_time_period(period: str) -> None:
    """Set the data-time-period attribute on the page root."""
    js = f"""
    <script>
        document.documentElement.setAttribute('data-time-period', '{period}');
    </script>
    """
    st.markdown(js, unsafe_allow_html=True)
