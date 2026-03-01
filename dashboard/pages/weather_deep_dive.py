"""
Weather Deep Dive page -- weather-ride correlations and impact analysis.
Uses existing mart_weather_ride_correlation and mart_weather_impact_summary.
"""

import streamlit as st
import plotly.express as px

from dashboard.utils.query_helpers import run_query, run_query_params, parquet_path, parquet_exists
from dashboard.theme.plotly_template import register_template

register_template()

# Mart files this page depends on
_CORRELATION_MART = 'mart_weather_ride_correlation.parquet'
_IMPACT_MART = 'mart_weather_impact_summary.parquet'


def _check_data_available() -> bool:
    """Check whether the required weather mart files exist locally.

    Returns True if both mart files are present, False otherwise.
    When False, the caller should show an empty-state message.
    """
    return parquet_exists(_CORRELATION_MART) and parquet_exists(_IMPACT_MART)


def render():
    """Render the weather deep dive page."""
    st.title("\U0001f321\ufe0f Weather & Ride Analysis")

    city_label = st.sidebar.radio("City:", ["NYC", "London"], key='weather_city')
    location = city_label.lower()

    # --- Empty-state check ---
    if not _check_data_available():
        st.info(
            "Weather analytics are not yet available.\n\n"
            "This page will show how temperature, precipitation, and weather conditions "
            "affect bike ridership patterns in each city. Data is updated monthly."
        )
        return

    # --- Temperature vs Rides ---
    st.subheader("Temperature vs Ride Volume")
    st.caption("Average daily rides grouped by temperature range")

    temp_query = f"""
    SELECT
        CASE
            WHEN temperature_celsius < 0 THEN 'Below 0\u00b0C'
            WHEN temperature_celsius < 5 THEN '0-5\u00b0C'
            WHEN temperature_celsius < 10 THEN '5-10\u00b0C'
            WHEN temperature_celsius < 15 THEN '10-15\u00b0C'
            WHEN temperature_celsius < 20 THEN '15-20\u00b0C'
            WHEN temperature_celsius < 25 THEN '20-25\u00b0C'
            WHEN temperature_celsius < 30 THEN '25-30\u00b0C'
            ELSE '30\u00b0C+'
        END as temp_range,
        MIN(temperature_celsius) as temp_sort,
        round(avg(ride_count), 0) as avg_rides,
        count(*) as days_observed
    FROM '{parquet_path(_CORRELATION_MART)}'
    WHERE location = $1
    GROUP BY temp_range
    ORDER BY temp_sort
    """
    try:
        temp_df = run_query_params(temp_query, [location])
        if not temp_df.empty:
            fig = px.bar(temp_df, x='temp_range', y='avg_rides',
                         title=f"{city_label}: Average Daily Rides by Temperature",
                         labels={'avg_rides': 'Avg Daily Rides', 'temp_range': 'Temperature Range'},
                         template='atmospheric', color_discrete_sequence=['#5DADE2'])
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Show data"):
                st.dataframe(temp_df[['temp_range', 'avg_rides', 'days_observed']])
        else:
            st.info(f"No temperature data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading temperature data: {e}")

    # --- Precipitation Impact ---
    st.subheader("Precipitation Impact on Rides")
    precip_query = f"""
    SELECT
        CASE
            WHEN precipitation_mm = 0 THEN 'Dry'
            WHEN precipitation_mm < 2 THEN 'Light (0-2mm)'
            WHEN precipitation_mm < 10 THEN 'Moderate (2-10mm)'
            ELSE 'Heavy (10mm+)'
        END as precip_category,
        MIN(precipitation_mm) as precip_sort,
        round(avg(ride_count), 0) as avg_rides,
        count(*) as days_observed
    FROM '{parquet_path(_CORRELATION_MART)}'
    WHERE location = $1
    GROUP BY precip_category
    ORDER BY precip_sort
    """
    try:
        precip_df = run_query_params(precip_query, [location])
        if not precip_df.empty:
            fig = px.bar(precip_df, x='precip_category', y='avg_rides',
                         title=f"{city_label}: Average Daily Rides by Precipitation",
                         labels={'avg_rides': 'Avg Daily Rides', 'precip_category': 'Precipitation'},
                         template='atmospheric', color_discrete_sequence=['#3498DB'])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No precipitation data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading precipitation data: {e}")

    # --- Weather Condition Breakdown ---
    st.subheader("Impact by Weather Condition")
    st.caption("How each weather condition affects ride volume vs clear weather baseline")

    impact_query = f"""
    SELECT dimension_value as weather_condition,
           round(avg(pct_change_rides_vs_clear), 1) as pct_change,
           round(avg(avg_rides), 0) as avg_rides,
           sum(observation_count) as total_observations
    FROM '{parquet_path(_IMPACT_MART)}'
    WHERE location = $1
      AND dimension_type = 'weather_condition'
      AND dimension_value != 'clear'
    GROUP BY dimension_value
    ORDER BY pct_change
    """
    try:
        impact_df = run_query_params(impact_query, [location])
        if not impact_df.empty:
            fig = px.bar(impact_df, x='weather_condition', y='pct_change',
                         title=f"{city_label}: Ride Volume Change by Weather Condition",
                         labels={'pct_change': '% Change vs Clear', 'weather_condition': 'Condition'},
                         template='atmospheric',
                         color='pct_change',
                         color_continuous_scale='RdYlGn',
                         range_color=[impact_df['pct_change'].min(), 0])
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Show data"):
                st.dataframe(impact_df)
        else:
            st.info(f"No weather impact data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading weather impact data: {e}")

    # --- Hourly Weather Impact ---
    st.subheader("Weather Impact by Hour of Day")

    hour_impact_query = f"""
    SELECT hour_of_day,
           round(avg(CASE WHEN dimension_value = 'rain' THEN pct_change_rides_vs_clear END), 1) as rain_impact,
           round(avg(CASE WHEN dimension_value = 'snow' THEN pct_change_rides_vs_clear END), 1) as snow_impact,
           round(avg(CASE WHEN dimension_value = 'fog' THEN pct_change_rides_vs_clear END), 1) as fog_impact
    FROM '{parquet_path(_IMPACT_MART)}'
    WHERE location = $1 AND dimension_type = 'weather_condition'
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    try:
        hour_impact_df = run_query_params(hour_impact_query, [location])
        if not hour_impact_df.empty:
            fig = px.line(hour_impact_df, x='hour_of_day',
                          y=['rain_impact', 'snow_impact', 'fog_impact'],
                          title=f"{city_label}: Weather Impact by Hour",
                          labels={'value': '% Change vs Clear', 'hour_of_day': 'Hour'},
                          template='atmospheric')
            fig.update_layout(legend_title_text='Condition')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No hourly impact data available for {city_label}.")
    except Exception as e:
        st.error(f"Error loading hourly impact data: {e}")
