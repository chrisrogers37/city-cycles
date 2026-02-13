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
