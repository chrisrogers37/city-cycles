"""
Ride Analytics page -- per-city ride trends, duration, hourly patterns, station growth.
Refactored from the original monolithic app.py.
"""

import streamlit as st
import plotly.express as px
import pandas as pd
import os

from dashboard.utils.query_helpers import run_query, run_query_params, parquet_path, parquet_exists
from dashboard.components.chart_factory import monthly_trend_chart, hourly_bar_chart, station_growth_chart
from dashboard.theme.plotly_template import register_template

register_template()

DASHBOARD_MIN_DATE = pd.to_datetime('2019-01-01').date()
DASHBOARD_MAX_DATE = pd.to_datetime('2024-12-31').date()


def _get_max_date(location: str) -> object:
    """Get the max available date for a location."""
    query = f"SELECT MAX(date) as max_date FROM '{parquet_path('mart_daily_metrics.parquet')}' WHERE location = $1"
    if location == 'nyc':
        query += " AND date <= '2024-12-31'"
    try:
        result = run_query_params(query, [location])
        if not result.empty and result['max_date'][0] is not None:
            return pd.to_datetime(result['max_date'][0]).date()
    except Exception:
        pass
    return DASHBOARD_MAX_DATE


def _get_min_date(location: str, max_date) -> object:
    """Get the min available date for a location within range."""
    query = f"SELECT MIN(date) as min_date FROM '{parquet_path('mart_daily_metrics.parquet')}' WHERE location = $1 AND date >= $2 AND date <= $3"
    try:
        result = run_query_params(query, [location, str(DASHBOARD_MIN_DATE), str(max_date)])
        if not result.empty and result['min_date'][0] is not None:
            return max(pd.to_datetime(result['min_date'][0]).date(), DASHBOARD_MIN_DATE)
    except Exception:
        pass
    return DASHBOARD_MIN_DATE


def render():
    """Render the ride analytics page."""
    # City selection in sidebar
    city_label = st.sidebar.radio("City:", ["NYC", "London"], key='analytics_city')
    location = city_label.lower()

    # Date range
    max_date = _get_max_date(location)
    min_date = _get_min_date(location, max_date)

    date_range = st.sidebar.date_input(
        "Date range:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key='analytics_date_range'
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = str(date_range[0]), str(date_range[1])
    else:
        start_date, end_date = str(min_date), str(max_date)

    st.title(f"\U0001f6b2 {city_label} Ride Analytics")

    # --- Key Metrics ---
    total_rides_query = f"""
    SELECT SUM(metric_value) as total_rides
    FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
    WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
    """
    avg_daily_query = f"""
    SELECT AVG(daily_rides) as avg_daily_rides FROM (
        SELECT date, SUM(metric_value) as daily_rides
        FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
        WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
        GROUP BY date
    ) t
    """
    avg_duration_query = f"""
    SELECT SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
           NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0)
           AS avg_ride_duration_minutes
    FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
    WHERE location = $1 AND date BETWEEN $2 AND $3
    """

    params = [location, start_date, end_date]

    try:
        total_rides = run_query_params(total_rides_query, params)['total_rides'][0] or 0
        avg_daily = run_query_params(avg_daily_query, params)['avg_daily_rides'][0] or 0
        avg_duration = run_query_params(avg_duration_query, params)['avg_ride_duration_minutes'][0] or 0
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
        total_rides = avg_daily = avg_duration = 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rides", f"{total_rides:,.0f}")
    col2.metric("Average Daily Rides", f"{avg_daily:,.1f}")
    col3.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")

    # --- Monthly Ride Trends ---
    st.subheader("Rides by Month (Overlayed by Year)")
    rides_agg_type = st.radio("Aggregation:", ["Average Daily Rides", "Total Rides"],
                              key=f"rides_agg_{location}", horizontal=True)
    agg_func = "AVG" if rides_agg_type == "Average Daily Rides" else "SUM"
    rides_trend_query = f"""
    SELECT EXTRACT(MONTH FROM date) AS month, year, {agg_func}(metric_value) as metric_value
    FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
    WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
    GROUP BY month, year ORDER BY month, year
    """
    try:
        rides_trend_df = run_query_params(rides_trend_query, params)
        fig = monthly_trend_chart(rides_trend_df, 'metric_value', rides_agg_type,
                                  f"{rides_agg_type} per Month (Overlayed by Year)")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Show data table"):
            st.dataframe(rides_trend_df)
    except Exception as e:
        st.error(f"Error creating rides trend chart: {e}")

    # --- Duration Trends ---
    st.subheader("Average Trip Duration by Month (Overlayed by Year)")
    duration_trend_query = f"""
    SELECT EXTRACT(MONTH FROM date) AS month, year,
      SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
        NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_duration
    FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
    WHERE location = $1 AND date BETWEEN $2 AND $3
      AND metric_name IN ('total_minutes_biked', 'total_rides')
    GROUP BY month, year ORDER BY month, year
    """
    try:
        duration_df = run_query_params(duration_trend_query, params)
        fig = monthly_trend_chart(duration_df, 'avg_duration', 'Avg Trip Duration (min)',
                                  "Average Trip Duration by Month (Overlayed by Year)")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("True average: total minutes biked / total rides per period")
        with st.expander("Show data table"):
            st.dataframe(duration_df)
    except Exception as e:
        st.error(f"Error creating duration trend chart: {e}")

    # --- Hourly Patterns ---
    st.subheader("Time of Day Analysis")
    hour_query = f"SELECT hour_of_day, ride_count FROM '{parquet_path('mart_hourly_patterns_summary.parquet')}' WHERE location = $1 ORDER BY hour_of_day"
    try:
        hour_df = run_query_params(hour_query, [location])
        fig = hourly_bar_chart(hour_df, f"{city_label} Rides by Hour of Day")
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating hourly patterns chart: {e}")

    # --- NYC Member Percentage ---
    if location == 'nyc':
        st.subheader("Member Percentage Trend")
        member_query = f"SELECT month, member_percentage FROM '{parquet_path('mart_nyc_member_analysis.parquet')}' WHERE month BETWEEN $1 AND $2 ORDER BY month"
        try:
            member_df = run_query_params(member_query, [start_date, end_date])
            fig = px.line(member_df, x='month', y='member_percentage',
                          title="NYC Member Percentage Over Time", template='atmospheric')
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating member percentage chart: {e}")

    # --- Station Growth ---
    st.subheader("Station Growth")
    station_query = f"""
    SELECT year, station_count as metric_value
    FROM '{parquet_path('mart_station_growth.parquet')}'
    WHERE location = $1
      AND year BETWEEN EXTRACT(YEAR FROM $2::DATE) AND EXTRACT(YEAR FROM $3::DATE)
    ORDER BY year
    """
    try:
        station_df = run_query_params(station_query, [location, start_date, end_date])
        fig = station_growth_chart(station_df, "Station Count by Year")
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating station growth chart: {e}")

    # --- Station Weather Performance ---
    st.subheader("Station Weather Performance")

    # Pre-flight: check both required parquets exist
    _swp_file = 'mart_station_weather_performance.parquet'
    _sd_file = 'mart_station_directory.parquet'
    if not parquet_exists(_swp_file) or not parquet_exists(_sd_file):
        st.info(
            "Station weather performance data is not yet available. "
            "Run the full pipeline to generate weather mart data."
        )
    else:
        conditions_query = f"""
            SELECT DISTINCT weather_condition
            FROM '{parquet_path(_swp_file)}'
            WHERE location = $1 ORDER BY weather_condition
        """
        try:
            conditions_df = run_query_params(conditions_query, [location])
            available_conditions = conditions_df['weather_condition'].tolist()
        except Exception:
            available_conditions = []

        non_clear = [c for c in available_conditions if c != 'clear']
        if not non_clear:
            st.info(f"No weather condition data available for {city_label}.")
        else:
            selected_condition = st.selectbox("Weather Condition:", non_clear,
                                              key=f"weather_condition_{location}")
            selected_hour = st.slider("Hour Range:", min_value=0, max_value=23,
                                      value=(7, 19), key=f"weather_hour_{location}")

            resilience_query = f"""
                SELECT s.station_id, d.station_name, d.latitude, d.longitude,
                    round(avg(s.pct_change_vs_clear), 1) as avg_pct_change,
                    sum(s.total_rides) as total_rides_in_condition,
                    round(avg(s.avg_duration_minutes), 1) as avg_duration
                FROM '{parquet_path(_swp_file)}' s
                JOIN '{parquet_path(_sd_file)}' d
                    ON s.location = d.location AND s.station_id = d.station_id
                WHERE s.location = $1 AND s.weather_condition = $2
                  AND s.hour_of_day BETWEEN $3 AND $4
                  AND s.pct_change_vs_clear IS NOT NULL
                GROUP BY s.station_id, d.station_name, d.latitude, d.longitude
                ORDER BY avg_pct_change DESC LIMIT 20
            """
            try:
                resilience_df = run_query_params(resilience_query, [
                    location, selected_condition, selected_hour[0], selected_hour[1]
                ])
                if not resilience_df.empty:
                    st.markdown(f"**Top 20 Most Weather-Resilient Stations ({selected_condition.replace('_', ' ').title()})**")
                    st.dataframe(
                        resilience_df[['station_name', 'avg_pct_change',
                                       'total_rides_in_condition', 'avg_duration']],
                        use_container_width=True,
                        column_config={
                            'station_name': 'Station',
                            'avg_pct_change': st.column_config.NumberColumn('% Change vs Clear', format='%.1f%%'),
                            'total_rides_in_condition': st.column_config.NumberColumn('Total Rides', format='%d'),
                            'avg_duration': st.column_config.NumberColumn('Avg Duration (min)', format='%.1f'),
                        }
                    )

                    # NYC Map
                    if location == 'nyc':
                        map_df = resilience_df.dropna(subset=['latitude', 'longitude']).copy()
                        if not map_df.empty:
                            st.markdown(f"**Station Weather Impact Map ({selected_condition.replace('_', ' ').title()})**")
                            fig_map = px.scatter_mapbox(
                                map_df, lat='latitude', lon='longitude',
                                color='avg_pct_change', size='total_rides_in_condition',
                                hover_name='station_name',
                                hover_data={'avg_pct_change': ':.1f', 'total_rides_in_condition': ':,', 'avg_duration': ':.1f'},
                                color_continuous_scale='RdYlGn',
                                range_color=[map_df['avg_pct_change'].min(), 0],
                                mapbox_style='open-street-map', zoom=11,
                                center={'lat': 40.7128, 'lon': -74.0060},
                                title=f'NYC Station Impact During {selected_condition.replace("_", " ").title()}'
                            )
                            fig_map.update_layout(height=600)
                            st.plotly_chart(fig_map, use_container_width=True)
                else:
                    st.info(f"No station weather data available for {city_label} with the selected filters.")
            except Exception as e:
                st.error(f"Error loading station weather data: {e}")
