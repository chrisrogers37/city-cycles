"""
City Comparison page -- NYC vs London side-by-side analytics.
Refactored from the original monolithic app.py.
"""

import streamlit as st
import plotly.express as px
import pandas as pd

from dashboard.utils.query_helpers import run_query, run_query_params, parquet_path
from dashboard.components.chart_factory import comparison_line_chart, grouped_bar_chart
from dashboard.theme.plotly_template import register_template

register_template()

DASHBOARD_MIN_DATE = pd.to_datetime('2019-01-01').date()
DASHBOARD_MAX_DATE = pd.to_datetime('2024-12-31').date()


def render():
    """Render the city comparison page."""
    st.title("\U0001f30d NYC vs London Comparison")

    # Date range in sidebar
    date_range = st.sidebar.date_input(
        "Date range:",
        value=(DASHBOARD_MIN_DATE, DASHBOARD_MAX_DATE),
        min_value=DASHBOARD_MIN_DATE,
        max_value=DASHBOARD_MAX_DATE,
        key='comparison_date_range'
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = str(date_range[0]), str(date_range[1])
    else:
        start_date, end_date = str(DASHBOARD_MIN_DATE), str(DASHBOARD_MAX_DATE)

    # --- Key Metrics Side-by-Side ---
    st.header("Key Metrics")

    year_query = f"""
        SELECT MAX(year) as latest_year FROM '{parquet_path('mart_station_growth.parquet')}'
        WHERE year BETWEEN EXTRACT(YEAR FROM $1::DATE) AND EXTRACT(YEAR FROM $2::DATE)
    """
    try:
        latest_year = run_query_params(year_query, [start_date, end_date])['latest_year'][0] or 2024
    except Exception:
        latest_year = 2024

    pop_query = f"""
        SELECT location, MAX(metric_value) as population
        FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
        WHERE metric_name = 'population' AND year = $1
        GROUP BY location
    """
    rides_query = f"""
        SELECT location, SUM(metric_value) as total_rides
        FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
        WHERE metric_name = 'total_rides' AND date BETWEEN $1 AND $2
        GROUP BY location
    """
    duration_query = f"""
        SELECT location,
          SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
            NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0)
            AS avg_ride_duration_minutes
        FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
        WHERE date BETWEEN $1 AND $2
        GROUP BY location
    """

    try:
        pop_df = run_query_params(pop_query, [latest_year]).set_index('location')
        rides_df = run_query_params(rides_query, [start_date, end_date]).set_index('location')
        duration_df = run_query_params(duration_query, [start_date, end_date]).set_index('location')
    except Exception as e:
        st.error(f"Error loading comparison data: {e}")
        return

    col_nyc, col_london = st.columns(2)
    for col, loc, label in [(col_nyc, 'nyc', 'NYC'), (col_london, 'london', 'London')]:
        with col:
            st.subheader(label)
            rides = rides_df.loc[loc, 'total_rides'] if loc in rides_df.index else None
            pop = int(pop_df.loc[loc, 'population']) if loc in pop_df.index else None
            dur = duration_df.loc[loc, 'avg_ride_duration_minutes'] if loc in duration_df.index else None
            per_capita = (rides / pop * 1000) if rides and pop else None

            st.metric("Total Rides", f"{rides:,.0f}" if rides else "N/A")
            st.metric("Rides Per 1,000 Capita", f"{per_capita:,.1f}" if per_capita else "N/A")
            st.metric(f"Est. Population ({latest_year})", f"{pop:,}" if pop else "N/A")
            st.metric("Avg Ride Duration", f"{dur:.1f} min" if dur else "N/A")

    # --- Comparative Trends ---
    st.header("Comparative Trends")
    trend_agg = st.radio("Aggregation:", ["Monthly", "Yearly"], key="trend_agg", horizontal=True)
    comparison_metric = st.radio("Metric:", ["Overall Rides", "Per Capita Rides"],
                                 key="comparison_metric", horizontal=True)

    date_expr = "date_trunc('month', date)" if trend_agg == "Monthly" else "date_trunc('year', date)"
    x_label = "Month" if trend_agg == "Monthly" else "Year"
    metric_name = 'total_rides' if comparison_metric == "Overall Rides" else 'rides_per_1000'
    y_label = "Total Rides" if comparison_metric == "Overall Rides" else "Rides per 1,000 Residents"

    trend_query = f"""
    SELECT {date_expr} as period, location, SUM(metric_value) as metric_value
    FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
    WHERE date BETWEEN $1 AND $2 AND metric_name = '{metric_name}'
    GROUP BY period, location ORDER BY period, location
    """
    try:
        trend_df = run_query_params(trend_query, [start_date, end_date])
        fig = comparison_line_chart(trend_df, 'period', 'metric_value',
                                    f"Comparative {comparison_metric} Over Time ({trend_agg})",
                                    y_label, x_label)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Show data table"):
            st.dataframe(trend_df)
    except Exception as e:
        st.error(f"Error creating comparative trends chart: {e}")

    # --- Comparative Duration ---
    st.header("Comparative Average Ride Duration")
    comp_duration_query = f"""
    SELECT {date_expr} as period, location,
      SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
        NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_duration
    FROM '{parquet_path('mart_daily_metrics_long.parquet')}'
    WHERE date BETWEEN $1 AND $2 AND metric_name IN ('total_minutes_biked', 'total_rides')
    GROUP BY period, location ORDER BY period, location
    """
    try:
        dur_df = run_query_params(comp_duration_query, [start_date, end_date])
        fig = comparison_line_chart(dur_df, 'period', 'avg_duration',
                                    f"Comparative Average Ride Duration ({trend_agg})",
                                    'Avg Ride Duration (min)', x_label)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Show data table"):
            st.dataframe(dur_df)
    except Exception as e:
        st.error(f"Error creating comparative duration chart: {e}")

    # --- Comparative Station Growth ---
    st.header("Comparative Station Growth")
    station_query = f"""
    SELECT year, location, station_count
    FROM '{parquet_path('mart_station_growth.parquet')}'
    WHERE year BETWEEN EXTRACT(YEAR FROM $1::DATE) AND EXTRACT(YEAR FROM $2::DATE)
    ORDER BY year, location
    """
    try:
        station_df = run_query_params(station_query, [start_date, end_date])
        fig = grouped_bar_chart(station_df, 'year', 'station_count',
                                'Comparative Station Count by Year', 'Station Count')
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Show data table"):
            st.dataframe(station_df)
    except Exception as e:
        st.error(f"Error creating comparative station growth chart: {e}")

    # --- Weather Impact Comparison ---
    st.header("Weather Impact on Ridership")
    weather_query = f"""
    SELECT location, weather_condition,
        round(avg(pct_change_vs_clear), 1) as avg_pct_change,
        sum(total_rides) as total_rides
    FROM '{parquet_path('mart_station_weather_performance.parquet')}'
    WHERE pct_change_vs_clear IS NOT NULL AND weather_condition != 'clear'
    GROUP BY location, weather_condition
    ORDER BY location, weather_condition
    """
    try:
        weather_df = run_query(weather_query)
        if not weather_df.empty:
            fig = grouped_bar_chart(weather_df, 'weather_condition', 'avg_pct_change',
                                    'Average Station Ridership Change by Weather Condition',
                                    '% Change vs Clear Weather')
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading weather comparison: {e}")
