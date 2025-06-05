import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define mart schema for maintainability
MART_SCHEMA = "dbt_models_marts"

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

# Page config
st.set_page_config(
    page_title="City Cycles Analytics",
    page_icon="🚲",
    layout="wide"
)

# Title
st.title("🚲 City Cycles Analytics Dashboard")

# Restrict date range to 2019-01-01 through 2024-12-31
dashboard_min_date = pd.to_datetime('2019-01-01').date()
dashboard_max_date = pd.to_datetime('2024-12-31').date()

# Sidebar for city selection
page = st.sidebar.radio("Select a page:", ["NYC", "London", "Comparison"])

# Connect to database
conn = get_db_connection()

# --- Date Range Picker ---
if page == "Comparison":
    date_query = f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE date >= '{dashboard_min_date}' AND date <= '{dashboard_max_date}'"
else:
    city = page.lower()
    date_query = f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE location = '{city}' AND date >= '{dashboard_min_date}' AND date <= '{dashboard_max_date}'"
date_df = pd.read_sql(date_query, conn)
min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date)
max_date = min(pd.to_datetime(date_df['max_date'][0]).date(), dashboard_max_date)

# --- Date Range Picker with Apply Button ---
start_date, end_date = st.sidebar.date_input(
    "Select date range:",
    value=(min_date, max_date),
    min_value=dashboard_min_date,
    max_value=dashboard_max_date
)
apply_filter = st.sidebar.button("Apply Date Filter")

if 'date_filter_applied' not in st.session_state:
    st.session_state['date_filter_applied'] = False
if apply_filter:
    st.session_state['date_filter_applied'] = True
    st.session_state['applied_start_date'] = start_date
    st.session_state['applied_end_date'] = end_date

# Use applied dates if filter applied, else None
applied_start_date = st.session_state.get('applied_start_date', None)
applied_end_date = st.session_state.get('applied_end_date', None)

# Only render dashboard if filter applied and both dates are present
if st.session_state.get('date_filter_applied', False) and applied_start_date and applied_end_date:
    def date_filter_sql(start_date, end_date):
        return f"date BETWEEN '{max(start_date, dashboard_min_date)}' AND '{min(end_date, dashboard_max_date)}'"
    date_filter = date_filter_sql(applied_start_date, applied_end_date)

    # City/Comparison Title above KPIs
    if page == "Comparison":
        st.subheader("NYC vs London")
    else:
        st.subheader(page)

    # --- KPI Section ---
    st.header("Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)

    # Helper: get metric name for toggle
    metric_label = lambda m: {
        'total_rides': 'Total Rides',
        'avg_daily_rides': 'Average Daily Rides',
        'avg_duration_minutes': 'Average Ride Duration',
        'rides_per_1000': 'Rides per 1,000 Residents'
    }.get(m, m.replace('_', ' ').title())

    if page in ["NYC", "London"]:
        # Total rides in period
        total_rides_query = f"""
        SELECT SUM(metric_value) as total_rides
        FROM {MART_SCHEMA}.mart_daily_metrics_long
        WHERE location = '{city}'
          AND {date_filter}
          AND metric_name = 'total_rides'
        """
        total_rides = pd.read_sql(total_rides_query, conn)['total_rides'][0]

        # Average daily rides in period
        avg_daily_query = f"""
        SELECT AVG(daily_rides) as avg_daily_rides
        FROM (
            SELECT date, SUM(metric_value) as daily_rides
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE location = '{city}'
              AND {date_filter}
              AND metric_name = 'total_rides'
            GROUP BY date
        ) t
        """
        avg_daily = pd.read_sql(avg_daily_query, conn)['avg_daily_rides'][0]

        # Average ride duration
        avg_duration_query = f"""
        SELECT AVG(metric_value) as avg_duration
        FROM {MART_SCHEMA}.mart_daily_metrics_long
        WHERE location = '{city}'
          AND {date_filter}
          AND metric_name = 'avg_duration_minutes'
        """
        avg_duration = pd.read_sql(avg_duration_query, conn)['avg_duration'][0]

        with col1:
            st.metric("Total Rides", f"{total_rides:,.0f}")
        with col2:
            st.metric("Average Daily Rides", f"{avg_daily:,.1f}")
        with col3:
            st.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")

    elif page == "Comparison":
        # Total rides in period (all cities)
        total_rides = pd.read_sql(f"SELECT SUM(metric_value) as total_rides FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE {date_filter} AND metric_name = 'total_rides'", conn)['total_rides'][0]

        # Average daily rides in period (all cities)
        avg_daily = pd.read_sql(f"SELECT AVG(daily_rides) as avg_daily_rides FROM (SELECT date, SUM(metric_value) as daily_rides FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE {date_filter} AND metric_name = 'total_rides' GROUP BY date) t", conn)['avg_daily_rides'][0]

        # Average ride duration (all cities)
        avg_duration = pd.read_sql(f"SELECT AVG(metric_value) as avg_duration FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE {date_filter} AND metric_name = 'avg_duration_minutes'", conn)['avg_duration'][0]

        with col1:
            st.metric("Total Rides (Combined)", f"{total_rides:,.0f}")
        with col2:
            st.metric("Average Daily Rides (Combined)", f"{avg_daily:,.1f}")
        with col3:
            st.metric("Average Ride Duration (Combined)", f"{avg_duration:.1f} minutes")

    # --- Trends Section ---
    st.header("Trends")

    if page in ["NYC", "London"]:
        # --- Rides Trend ---
        st.subheader("Rides by Month (Overlayed by Year)")
        rides_agg_type = st.radio("Aggregation:", ["Average Daily Rides", "Total Rides"], key=f"rides_agg_{page}", horizontal=True)
        if rides_agg_type == "Average Daily Rides":
            rides_trend_query = f"""
            SELECT EXTRACT(MONTH FROM date) AS month, year, AVG(metric_value) as metric_value
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE location = '{city}'
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
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE location = '{city}'
              AND {date_filter}
              AND metric_name = 'total_rides'
            GROUP BY month, year
            ORDER BY month, year
            """
            y_label = "Total Rides"
            chart_title = "Total Rides per Month (Overlayed by Year)"

        rides_trend_df = pd.read_sql(rides_trend_query, conn)
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

        # --- Trip Duration Trend ---
        st.subheader("Average Trip Duration by Month (Overlayed by Year)")
        duration_trend_query = f"""
        SELECT EXTRACT(MONTH FROM date) AS month, year, AVG(metric_value) as avg_duration
        FROM {MART_SCHEMA}.mart_daily_metrics_long
        WHERE location = '{city}'
          AND {date_filter}
          AND metric_name = 'avg_duration_minutes'
        GROUP BY month, year
        ORDER BY month, year
        """
        duration_trend_df = pd.read_sql(duration_trend_query, conn)
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
        st.caption("Approximation: monthly aggregation of daily averages")
        with st.expander("Show data table for trip duration trend"):
            st.dataframe(duration_trend_df)

        # --- Time of Day Analysis ---
        st.subheader("Time of Day Analysis")
        hour_query = f"SELECT hour_of_day, ride_count FROM {MART_SCHEMA}.mart_{city}_hourly_patterns ORDER BY hour_of_day"
        hour_df = pd.read_sql(hour_query, conn)
        fig_hour = px.bar(hour_df, x='hour_of_day', y='ride_count', title=f"{page} Rides by Hour of Day")
        st.plotly_chart(fig_hour, use_container_width=True)

        # --- Member % (NYC only) ---
        if city == "nyc":
            st.subheader("Member Percentage Trend")
            member_query = f"SELECT month, member_percentage FROM {MART_SCHEMA}.mart_nyc_member_analysis WHERE month BETWEEN '{start_date}' AND '{end_date}' ORDER BY month"
            member_df = pd.read_sql(member_query, conn)
            fig_member = px.line(member_df, x='month', y='member_percentage', title="NYC Member Percentage Over Time")
            st.plotly_chart(fig_member, use_container_width=True)

        # --- Station Growth ---
        st.subheader("Station Growth")
        station_query = f"""
        SELECT year, station_count as metric_value
        FROM {MART_SCHEMA}.mart_{city}_station_growth
        WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{start_date}') AND EXTRACT(YEAR FROM DATE '{end_date}')
        ORDER BY year
        """
        station_df = pd.read_sql(station_query, conn)
        fig_station = px.bar(station_df, x='year', y='metric_value', title=f"Station Count by Year")
        st.plotly_chart(fig_station, use_container_width=True)

    elif page == "Comparison":
        st.subheader("NYC vs London: Comparative Analytics")

        # --- Daily Metrics Comparison: Overlay by Month, Color by Year ---
        st.subheader("Rides by Month: NYC vs London (Overlayed by Year)")
        rides_metric_type = st.radio("Metric:", ["Absolute", "Per Capita"], key="rides_trend_comp", horizontal=True)
        rides_metric = "total_rides" if rides_metric_type == "Absolute" else "rides_per_1000"
        comp_query = f"""
        SELECT EXTRACT(MONTH FROM date) AS month, location, year, AVG(metric_value) as metric_value
        FROM {MART_SCHEMA}.mart_daily_metrics_long
        WHERE {date_filter}
          AND metric_name = '{rides_metric}'
        GROUP BY month, location, year
        ORDER BY month, location, year
        """
        comp_df = pd.read_sql(comp_query, conn)
        fig_comp = px.line(
            comp_df,
            x='month', y='metric_value', color='location', line_dash='year',
            title=f"{'Rides' if rides_metric == 'total_rides' else 'Rides Per 1000'} by Month: NYC vs London (Overlayed by Year)"
        )
        fig_comp.update_xaxes(
            tickmode='array',
            tickvals=list(range(1, 13)),
            ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        )
        st.plotly_chart(fig_comp, use_container_width=True)

        # --- Station Growth Comparison ---
        st.subheader("Station Growth Comparison")
        station_metric_type = st.radio("Metric:", ["Absolute", "Per Capita"], key="station_trend_comp", horizontal=True)
        station_metric = "station_count" if station_metric_type == "Absolute" else "stations_per_1000"
        station_comp_query = f"""
        SELECT year, location, {station_metric} as metric_value
        FROM (
            SELECT year, location, station_count, stations_per_1000 FROM {MART_SCHEMA}.mart_nyc_station_growth
            UNION ALL
            SELECT year, location, station_count, stations_per_1000 FROM {MART_SCHEMA}.mart_london_station_growth
        ) t
        WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{start_date}') AND EXTRACT(YEAR FROM DATE '{end_date}')
        ORDER BY year, location
        """
        station_comp_df = pd.read_sql(station_comp_query, conn)
        fig_station_comp = px.line(
            station_comp_df, x='year', y='metric_value', color='location',
            title=f"{station_metric.replace('_', ' ').title()} by Year: NYC vs London"
        )
        st.plotly_chart(fig_station_comp, use_container_width=True)

else:
    st.info("Select a start and end date, then click 'Apply Date Filter' to update the dashboard.")

# Close database connection
conn.close() 