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
from dashboard.theme.plotly_template import register_template
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

# --- Register atmospheric Plotly template ---
register_template()

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
        st.Page(ride_analytics.render, title="Ride Analytics", icon="\U0001f6b2", url_path="ride-analytics"),
        st.Page(weather_deep_dive.render, title="Weather Deep Dive", icon="\U0001f321\ufe0f", url_path="weather-deep-dive"),
        st.Page(comparison.render, title="City Comparison", icon="\U0001f30d", url_path="city-comparison"),
    ],
})

pg.run()
