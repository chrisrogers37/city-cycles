import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import duckdb
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the parent directory to the path so we can import our modules
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files

# Ensure all required Parquet files are present locally
ensure_local_parquet_files()

# Persistent DuckDB connection (in-memory or file-based)
DUCKDB_CONN = duckdb.connect(database=':memory:')

# Helper function for running queries
def run_query(query):
    return DUCKDB_CONN.execute(query).fetchdf()

# Page config
st.set_page_config(
    page_title="City Cycles Analytics",
    page_icon="🚲",
    layout="wide"
)

# Title
st.title("🚲 City Cycles Analytics Dashboard")

dashboard_min_date = pd.to_datetime('2019-01-01').date()
dashboard_max_date = pd.to_datetime('2024-12-31').date()

# --- Session State Defaults ---
def set_default_state():
    if 'pending_page' not in st.session_state:
        st.session_state['pending_page'] = 'NYC'
    if 'pending_start_date' not in st.session_state:
        st.session_state['pending_start_date'] = dashboard_min_date
    if 'pending_end_date' not in st.session_state:
        st.session_state['pending_end_date'] = dashboard_max_date
    if 'applied_page' not in st.session_state:
        st.session_state['applied_page'] = 'NYC'
    if 'applied_start_date' not in st.session_state:
        st.session_state['applied_start_date'] = dashboard_min_date
    if 'applied_end_date' not in st.session_state:
        st.session_state['applied_end_date'] = dashboard_max_date
    if 'date_filter_applied' not in st.session_state:
        st.session_state['date_filter_applied'] = False
set_default_state()

# --- Get max available date for each city ---
nyc_max_date_query = "SELECT MAX(date) as max_date FROM 'data/mart_daily_metrics.parquet' WHERE location = 'nyc' AND date <= '2024-12-31'"
london_max_date_query = "SELECT MAX(date) as max_date FROM 'data/mart_daily_metrics.parquet' WHERE location = 'london'"

try:
    nyc_max_date_result = run_query(nyc_max_date_query)
    london_max_date_result = run_query(london_max_date_query)
    nyc_max_date = pd.to_datetime(nyc_max_date_result['max_date'][0]).date() if not nyc_max_date_result.empty and nyc_max_date_result['max_date'][0] else dashboard_max_date
    london_max_date = pd.to_datetime(london_max_date_result['max_date'][0]).date() if not london_max_date_result.empty and london_max_date_result['max_date'][0] else dashboard_max_date
except Exception as e:
    st.warning(f"Could not fetch date ranges from local Parquet, using defaults: {e}")
    nyc_max_date = dashboard_max_date
    london_max_date = dashboard_max_date

# --- Sidebar UI for pending filter values ---
st.sidebar.header("")
st.sidebar.radio(
    "Select a page:",
    ["NYC", "London", "Comparison"],
    index=["NYC", "London", "Comparison"].index(st.session_state['pending_page']),
    key='pending_page'
)

# --- Use only applied_page to determine min/max date for the date input widget ---
if st.session_state['applied_page'] == "Comparison":
    comparison_max_date = min(nyc_max_date, london_max_date)
    date_query = f"SELECT MIN(date) as min_date FROM 'data/mart_daily_metrics.parquet' WHERE date >= '{dashboard_min_date}' AND date <= '{comparison_max_date}'"
    try:
        date_df = run_query(date_query)
        min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date) if not date_df.empty else dashboard_min_date
        max_date = comparison_max_date
    except Exception as e:
        min_date = dashboard_min_date
        max_date = comparison_max_date
elif st.session_state['applied_page'] == "NYC":
    date_query = f"SELECT MIN(date) as min_date FROM 'data/mart_daily_metrics.parquet' WHERE location = 'nyc' AND date >= '{dashboard_min_date}' AND date <= '{nyc_max_date}'"
    try:
        date_df = run_query(date_query)
        min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date) if not date_df.empty else dashboard_min_date
        max_date = nyc_max_date
    except Exception as e:
        min_date = dashboard_min_date
        max_date = nyc_max_date
elif st.session_state['applied_page'] == "London":
    date_query = f"SELECT MIN(date) as min_date FROM 'data/mart_daily_metrics.parquet' WHERE location = 'london' AND date >= '{dashboard_min_date}' AND date <= '{london_max_date}'"
    try:
        date_df = run_query(date_query)
        min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date) if not date_df.empty else dashboard_min_date
        max_date = london_max_date
    except Exception as e:
        min_date = dashboard_min_date
        max_date = london_max_date
else:
    min_date = dashboard_min_date
    max_date = dashboard_max_date

# Ensure we have valid date values
if min_date is None or max_date is None:
    min_date = dashboard_min_date
    max_date = dashboard_max_date

# Clamp pending_start_date and pending_end_date to min_date and max_date
pending_start = max(st.session_state['pending_start_date'], min_date)
pending_end = min(st.session_state['pending_end_date'], max_date)
if pending_start > pending_end:
    pending_start = min_date
    pending_end = max_date

try:
    st.sidebar.date_input(
        "Select date range:",
        value=(pending_start, pending_end),
        min_value=min_date,
        max_value=max_date,
        key='pending_date_input'
    )
    # Update pending_start_date and pending_end_date from the widget value
    if isinstance(st.session_state['pending_date_input'], tuple) and len(st.session_state['pending_date_input']) == 2:
        st.session_state['pending_start_date'], st.session_state['pending_end_date'] = st.session_state['pending_date_input']
except Exception as e:
    st.error(f"Error with date input: {e}")
    st.session_state['pending_start_date'] = min_date
    st.session_state['pending_end_date'] = max_date

apply_filter = st.sidebar.button("Apply Date Filter")
reset_filter = st.sidebar.button("Reset Dashboard")

st.sidebar.markdown("<span style='color:#fff'><b>Note:</b> After switching dashboards or changing the date range, you must click 'Apply Date Filter' to update the dashboard.</span>", unsafe_allow_html=True)

if reset_filter:
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    set_default_state()
    st.rerun()

if apply_filter:
    st.session_state['applied_page'] = st.session_state['pending_page']
    st.session_state['applied_start_date'] = st.session_state['pending_start_date']
    st.session_state['applied_end_date'] = st.session_state['pending_end_date']
    st.session_state['date_filter_applied'] = True
    st.rerun()

applied_page = st.session_state.get('applied_page', 'NYC')
applied_start_date = st.session_state.get('applied_start_date', dashboard_min_date)
applied_end_date = st.session_state.get('applied_end_date', dashboard_max_date)

if st.session_state.get('date_filter_applied', False) and applied_start_date and applied_end_date:
    def date_filter_sql(start_date, end_date):
        return f"date BETWEEN '{max(start_date, dashboard_min_date)}' AND '{min(end_date, dashboard_max_date)}'"
    date_filter = date_filter_sql(applied_start_date, applied_end_date)

    # --- All dashboard queries below should use 'data/mart_*.parquet' instead of S3 URIs ---
    if applied_page == "Comparison":
        st.subheader("NYC vs London")
    else:
        st.subheader(applied_page)

    if applied_page == "Comparison":
        st.header("Comparative Analytics")
        col_nyc, col_london = st.columns(2)

        year_query = f"""
            SELECT MAX(year) as latest_year FROM 'data/mart_station_growth.parquet'
            WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{applied_start_date}') AND EXTRACT(YEAR FROM DATE '{applied_end_date}')
        """
        try:
            latest_year_result = run_query(year_query)
            latest_year = latest_year_result['latest_year'][0] if not latest_year_result.empty and latest_year_result['latest_year'][0] else 2024
        except Exception as e:
            st.error(f"Error getting latest year: {e}")
            latest_year = 2024

        pop_query = f"""
            SELECT location, MAX(metric_value) as population
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE metric_name = 'population' AND year = {latest_year}
            GROUP BY location
        """
        try:
            pop_df = run_query(pop_query).set_index('location')
            nyc_pop = int(pop_df.loc['nyc', 'population']) if 'nyc' in pop_df.index else None
            london_pop = int(pop_df.loc['london', 'population']) if 'london' in pop_df.index else None
        except Exception as e:
            st.error(f"Error getting population data: {e}")
            nyc_pop = None
            london_pop = None

        rides_query = f"""
            SELECT location, SUM(metric_value) as total_rides
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE metric_name = 'total_rides' AND {date_filter}
            GROUP BY location
        """
        try:
            rides_df = run_query(rides_query).set_index('location')
            nyc_rides = rides_df.loc['nyc', 'total_rides'] if 'nyc' in rides_df.index else None
            london_rides = rides_df.loc['london', 'total_rides'] if 'london' in rides_df.index else None
            nyc_rides_per_1000 = (nyc_rides / nyc_pop * 1000) if nyc_rides and nyc_pop else None
            london_rides_per_1000 = (london_rides / london_pop * 1000) if london_rides and london_pop else None
        except Exception as e:
            st.error(f"Error getting rides data: {e}")
            nyc_rides = None
            london_rides = None
            nyc_rides_per_1000 = None
            london_rides_per_1000 = None

        nyc_duration_query = f"""
            SELECT SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) / 
                   NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_ride_duration_minutes
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE location = 'nyc' AND {date_filter}
        """
        try:
            nyc_avg_duration = run_query(nyc_duration_query)['avg_ride_duration_minutes'][0]
        except Exception as e:
            st.error(f"Error getting NYC duration: {e}")
            nyc_avg_duration = None
        
        london_duration_query = f"""
            SELECT SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) / 
                   NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_ride_duration_minutes
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE location = 'london' AND {date_filter}
        """
        try:
            london_avg_duration = run_query(london_duration_query)['avg_ride_duration_minutes'][0]
        except Exception as e:
            st.error(f"Error getting London duration: {e}")
            london_avg_duration = None

        with col_nyc:
            st.subheader("NYC")
            st.metric("Total Rides", f"{nyc_rides:,.0f}" if nyc_rides else "N/A")
            st.metric("Rides Per 1,000 Capita in Period", f"{nyc_rides_per_1000:,.1f}" if nyc_rides_per_1000 else "N/A")
            st.metric(f"Est. Population ({latest_year})", f"{nyc_pop:,}" if nyc_pop else "N/A")
            st.metric("Avg Ride Duration", f"{nyc_avg_duration:.1f} min" if nyc_avg_duration else "N/A")
        with col_london:
            st.subheader("London")
            st.metric("Total Rides", f"{london_rides:,.0f}" if london_rides else "N/A")
            st.metric("Rides Per 1,000 Capita in Period", f"{london_rides_per_1000:,.1f}" if london_rides_per_1000 else "N/A")
            st.metric(f"Est. Population ({latest_year})", f"{london_pop:,}" if london_pop else "N/A")
            st.metric("Avg Ride Duration", f"{london_avg_duration:.1f} min" if london_avg_duration else "N/A")
    elif applied_page in ["NYC", "London"]:
        total_rides_query = f"""
        SELECT SUM(metric_value) as total_rides
        FROM 'data/mart_daily_metrics_long.parquet'
        WHERE location = '{applied_page.lower()}'
          AND {date_filter}
          AND metric_name = 'total_rides'
        """
        try:
            total_rides_result = run_query(total_rides_query)
            total_rides = total_rides_result['total_rides'][0] if not total_rides_result.empty else 0
        except Exception as e:
            st.error(f"Error getting total rides: {e}")
            total_rides = 0

        avg_daily_query = f"""
        SELECT AVG(daily_rides) as avg_daily_rides
        FROM (
            SELECT date, SUM(metric_value) as daily_rides
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE location = '{applied_page.lower()}'
              AND {date_filter}
              AND metric_name = 'total_rides'
            GROUP BY date
        ) t
        """
        try:
            avg_daily_result = run_query(avg_daily_query)
            avg_daily = avg_daily_result['avg_daily_rides'][0] if not avg_daily_result.empty else 0
        except Exception as e:
            st.error(f"Error getting average daily rides: {e}")
            avg_daily = 0

        avg_duration_query = f"""
            SELECT SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) / 
                   NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_ride_duration_minutes
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE location = '{applied_page.lower()}' AND {date_filter}
        """
        try:
            avg_duration_result = run_query(avg_duration_query)
            avg_duration = avg_duration_result['avg_ride_duration_minutes'][0] if not avg_duration_result.empty else 0
        except Exception as e:
            st.error(f"Error getting average duration: {e}")
            avg_duration = 0

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Rides", f"{total_rides:,.0f}")
        with col2:
            st.metric("Average Daily Rides", f"{avg_daily:,.1f}")
        with col3:
            st.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")

    # --- Trends Section ---
    if applied_page in ["NYC", "London"]:
        st.subheader("Rides by Month (Overlayed by Year)")
        rides_agg_type = st.radio("Aggregation:", ["Average Daily Rides", "Total Rides"], key=f"rides_agg_{applied_page}", horizontal=True)
        if rides_agg_type == "Average Daily Rides":
            rides_trend_query = f"""
            SELECT EXTRACT(MONTH FROM date) AS month, year, AVG(metric_value) as metric_value
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE location = '{applied_page.lower()}'
              AND {date_filter}
              AND metric_name = 'total_rides'
            GROUP BY month, year
            ORDER BY month, year
            """
            y_label = "Average Daily Rides"
            chart_title = "Average Daily Rides per Month (Overlayed by Year)"
        else:
            rides_trend_query = f"""
            SELECT EXTRACT(MONTH FROM date) AS month, year, SUM(metric_value) as metric_value
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE location = '{applied_page.lower()}'
              AND {date_filter}
              AND metric_name = 'total_rides'
            GROUP BY month, year
            ORDER BY month, year
            """
            y_label = "Total Rides"
            chart_title = "Total Rides per Month (Overlayed by Year)"
        try:
            rides_trend_df = run_query(rides_trend_query)
            fig_rides = px.line(
                rides_trend_df,
                x='month', y='metric_value', color='year',
                title=chart_title,
                labels={'metric_value': y_label, 'month': 'Month'}
            )
            fig_rides.update_xaxes(
                tickmode='array',
                tickvals=list(range(1, 13)),
                ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            )
            st.plotly_chart(fig_rides, use_container_width=True)
            with st.expander("Show data table for rides trend"):
                st.dataframe(rides_trend_df)
        except Exception as e:
            st.error(f"Error creating rides trend chart: {e}")

        st.subheader("Average Trip Duration by Month (Overlayed by Year)")
        duration_trend_query = f"""
        SELECT
          EXTRACT(MONTH FROM date) AS month,
          year,
          SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
            NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_duration
        FROM 'data/mart_daily_metrics_long.parquet'
        WHERE location = '{applied_page.lower()}'
          AND {date_filter}
          AND metric_name IN ('total_minutes_biked', 'total_rides')
        GROUP BY month, year
        ORDER BY month, year
        """
        try:
            duration_trend_df = run_query(duration_trend_query)
            fig_duration = px.line(
                duration_trend_df,
                x='month', y='avg_duration', color='year',
                title="Average Trip Duration by Month (Overlayed by Year)",
                labels={'avg_duration': 'Avg Trip Duration (min)', 'month': 'Month'}
            )
            fig_duration.update_xaxes(
                tickmode='array',
                tickvals=list(range(1, 13)),
                ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            )
            st.plotly_chart(fig_duration, use_container_width=True)
            st.caption("True average: total minutes biked / total rides per period")
            with st.expander("Show data table for trip duration trend"):
                st.dataframe(duration_trend_df)
        except Exception as e:
            st.error(f"Error creating duration trend chart: {e}")

        st.subheader("Time of Day Analysis")
        hour_query = f"SELECT hour_of_day, ride_count FROM 'data/mart_hourly_patterns.parquet' WHERE location = '{applied_page.lower()}' ORDER BY hour_of_day"
        try:
            hour_df = run_query(hour_query)
            fig_hour = px.bar(hour_df, x='hour_of_day', y='ride_count', title=f"{applied_page} Rides by Hour of Day")
            st.plotly_chart(fig_hour, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating hourly patterns chart: {e}")

        if applied_page == "NYC":
            st.subheader("Member Percentage Trend")
            member_query = f"SELECT month, member_percentage FROM 'data/mart_nyc_member_analysis.parquet' WHERE month BETWEEN '{applied_start_date}' AND '{applied_end_date}' ORDER BY month"
            try:
                member_df = run_query(member_query)
                fig_member = px.line(member_df, x='month', y='member_percentage', title="NYC Member Percentage Over Time")
                st.plotly_chart(fig_member, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating member percentage chart: {e}")

        st.subheader("Station Growth")
        station_query = f"""
        SELECT year, station_count as metric_value
        FROM 'data/mart_station_growth.parquet'
        WHERE location = '{applied_page.lower()}'
        AND year BETWEEN EXTRACT(YEAR FROM DATE '{applied_start_date}') AND EXTRACT(YEAR FROM DATE '{applied_end_date}')
        ORDER BY year
        """
        try:
            station_df = run_query(station_query)
            fig_station = px.bar(station_df, x='year', y='metric_value', title=f"Station Count by Year")
            st.plotly_chart(fig_station, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating station growth chart: {e}")

    elif applied_page == "Comparison":
        st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Comparative Trends</h2>", unsafe_allow_html=True)
        trend_agg = st.radio("Aggregation:", ["Monthly", "Yearly"], key="trend_agg", horizontal=True)
        comparison_metric = st.radio("Select Metric:", ["Overall Rides", "Per Capita Rides"], key="comparison_metric", horizontal=True)
        if trend_agg == "Monthly":
            date_expr = "date_trunc('month', date)"
            x_label = "Month"
        else:
            date_expr = "date_trunc('year', date)"
            x_label = "Year"
        if comparison_metric == "Overall Rides":
            comparison_query = f"""
            SELECT {date_expr} as period, location, SUM(metric_value) as metric_value
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE {date_filter}
              AND metric_name = 'total_rides'
            GROUP BY period, location
            ORDER BY period, location
            """
            y_label = "Total Rides"
            chart_title = f"Comparative Total Rides Over Time ({trend_agg})"
        else:
            comparison_query = f"""
            SELECT {date_expr} as period, location, SUM(metric_value) as metric_value
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE {date_filter}
              AND metric_name = 'rides_per_1000'
            GROUP BY period, location
            ORDER BY period, location
            """
            y_label = "Rides per 1,000 Residents"
            chart_title = f"Comparative Per Capita Rides Over Time ({trend_agg})"
        try:
            comparison_df = run_query(comparison_query)
            fig_comparison = px.line(
                comparison_df,
                x='period', y='metric_value', color='location',
                title=chart_title,
                labels={'metric_value': y_label, 'period': x_label}
            )
            st.plotly_chart(fig_comparison, use_container_width=True)
            with st.expander("Show data table for comparative trends"):
                st.dataframe(comparison_df)
        except Exception as e:
            st.error(f"Error creating comparative trends chart: {e}")

        st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Comparative Average Ride Duration</h2>", unsafe_allow_html=True)
        duration_query = f"""
            SELECT {date_expr} as period, location,
              SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
                NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_duration
            FROM 'data/mart_daily_metrics_long.parquet'
            WHERE {date_filter}
              AND metric_name IN ('total_minutes_biked', 'total_rides')
            GROUP BY period, location
            ORDER BY period, location
        """
        try:
            duration_df = run_query(duration_query)
            fig_duration = px.line(
                duration_df,
                x='period', y='avg_duration', color='location',
                title=f'Comparative Average Ride Duration Over Time ({trend_agg})',
                labels={'avg_duration': 'Avg Ride Duration (min)', 'period': x_label}
            )
            st.plotly_chart(fig_duration, use_container_width=True)
            with st.expander("Show data table for duration trends"):
                st.dataframe(duration_df)
        except Exception as e:
            st.error(f"Error creating comparative duration chart: {e}")

        st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Comparative Station Growth</h2>", unsafe_allow_html=True)
        station_query = f"""
            SELECT year, location, station_count
            FROM 'data/mart_station_growth.parquet'
            WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{applied_start_date}') AND EXTRACT(YEAR FROM DATE '{applied_end_date}')
            ORDER BY year, location
        """
        try:
            station_df = run_query(station_query)
            fig_station = px.bar(
                station_df,
                x='year', y='station_count', color='location',
                barmode='group',
                title='Comparative Station Count by Year',
                labels={'station_count': 'Station Count', 'year': 'Year'}
            )
            st.plotly_chart(fig_station, use_container_width=True)
            with st.expander("Show data table for station trends"):
                st.dataframe(station_df)
        except Exception as e:
            st.error(f"Error creating comparative station growth chart: {e}")
else:
    st.info("Select a page and date range, then click 'Apply Date Filter' to update the dashboard.") 