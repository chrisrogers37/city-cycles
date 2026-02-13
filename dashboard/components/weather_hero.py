"""Hero section: large temperature, weather condition, city name."""

import streamlit as st


def render_weather_hero(city: str, current_weather) -> None:
    """Render the atmospheric hero section.

    Args:
        city: 'nyc' or 'london'
        current_weather: A CurrentWeather dataclass from weather_service.
    """
    city_names = {'nyc': 'New York City', 'london': 'London'}
    temp_c = current_weather.temperature_c
    description = current_weather.weather_description
    emoji = current_weather.weather_emoji

    hero_html = f"""
    <div style="
        text-align: center;
        padding: 2rem 0;
        margin-bottom: 1rem;
    ">
        <p style="
            font-size: 2rem;
            margin: 0 0 0.5rem 0;
        ">{emoji}</p>
        <h1 style="
            font-size: 4rem;
            font-weight: 200;
            margin: 0;
            letter-spacing: 0.05em;
            color: #FFFFFF;
            text-shadow: 0 2px 10px rgba(0,0,0,0.3);
        ">{temp_c:.0f}\u00b0</h1>
        <p style="
            font-size: 1.4rem;
            font-weight: 300;
            margin: 0.5rem 0;
            color: rgba(255,255,255,0.85);
            text-transform: uppercase;
            letter-spacing: 0.15em;
        ">{description}</p>
        <p style="
            font-size: 1rem;
            color: rgba(255,255,255,0.6);
            margin: 0;
        ">{city_names.get(city, city)}</p>
    </div>
    """
    st.markdown(hero_html, unsafe_allow_html=True)
