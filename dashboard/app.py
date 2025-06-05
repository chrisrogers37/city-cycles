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

# Connect to database
conn = get_db_connection()

# KPI Section
st.header("Key Performance Indicators")

# Create two columns for KPIs
col1, col2 = st.columns(2)

# Query for daily rides KPI
with col1:
    daily_rides_query = """
    SELECT 
        AVG(daily_rides) as avg_daily_rides
    FROM (
        SELECT 
            DATE(start_time) as ride_date,
            COUNT(*) as daily_rides
        FROM dbt_models.int_nyc_rides
        GROUP BY DATE(start_time)
        UNION ALL
        SELECT 
            DATE(start_time) as ride_date,
            COUNT(*) as daily_rides
        FROM dbt_models.int_london_rides
        GROUP BY DATE(start_time)
    ) combined_rides
    """
    avg_daily_rides = pd.read_sql(daily_rides_query, conn).iloc[0]['avg_daily_rides']
    st.metric("Average Daily Rides", f"{avg_daily_rides:,.0f}")

# Query for average duration KPI
with col2:
    duration_query = """
    SELECT 
        AVG(duration_seconds)/60 as avg_duration_minutes
    FROM (
        SELECT duration_seconds
        FROM dbt_models.int_nyc_rides
        UNION ALL
        SELECT duration_seconds
        FROM dbt_models.int_london_rides
    ) combined_rides
    """
    avg_duration = pd.read_sql(duration_query, conn).iloc[0]['avg_duration_minutes']
    st.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")

# Trends Section
st.header("Ride Trends")

# Query for daily rides trend
rides_trend_query = """
SELECT 
    DATE(start_time) as ride_date,
    EXTRACT(YEAR FROM start_time) as year,
    COUNT(*) as daily_rides
FROM dbt_models.int_nyc_rides
GROUP BY DATE(start_time), EXTRACT(YEAR FROM start_time)
ORDER BY ride_date
"""
rides_trend_df = pd.read_sql(rides_trend_query, conn)

# Plot daily rides trend
fig_rides = px.line(
    rides_trend_df,
    x='ride_date',
    y='daily_rides',
    color='year',
    title='Daily Rides Trend by Year',
    labels={'ride_date': 'Date', 'daily_rides': 'Number of Rides', 'year': 'Year'}
)
st.plotly_chart(fig_rides, use_container_width=True)

# Query for duration trend
duration_trend_query = """
SELECT 
    DATE(start_time) as ride_date,
    EXTRACT(YEAR FROM start_time) as year,
    AVG(duration_seconds)/60 as avg_duration_minutes
FROM dbt_models.int_nyc_rides
GROUP BY DATE(start_time), EXTRACT(YEAR FROM start_time)
ORDER BY ride_date
"""
duration_trend_df = pd.read_sql(duration_trend_query, conn)

# Plot duration trend
fig_duration = px.line(
    duration_trend_df,
    x='ride_date',
    y='avg_duration_minutes',
    color='year',
    title='Average Ride Duration Trend by Year',
    labels={'ride_date': 'Date', 'avg_duration_minutes': 'Duration (minutes)', 'year': 'Year'}
)
st.plotly_chart(fig_duration, use_container_width=True)

# Time of Day Analysis
st.header("Time of Day Analysis")

# Query for time of day distribution
time_of_day_query = """
SELECT 
    EXTRACT(HOUR FROM start_time) as hour_of_day,
    COUNT(*) as ride_count
FROM dbt_models.int_nyc_rides
GROUP BY EXTRACT(HOUR FROM start_time)
ORDER BY hour_of_day
"""
time_of_day_df = pd.read_sql(time_of_day_query, conn)

# Create heatmap for time of day
fig_time = px.bar(
    time_of_day_df,
    x='hour_of_day',
    y='ride_count',
    title='Rides by Hour of Day',
    labels={'hour_of_day': 'Hour of Day', 'ride_count': 'Number of Rides'}
)
st.plotly_chart(fig_time, use_container_width=True)

# Member Analysis (NYC only)
st.header("NYC Member Analysis")

# Query for member percentage trend
member_trend_query = """
SELECT 
    DATE_TRUNC('month', start_time) as month,
    COUNT(*) FILTER (WHERE user_type = 'member') * 100.0 / COUNT(*) as member_percentage
FROM dbt_models.int_nyc_rides
GROUP BY DATE_TRUNC('month', start_time)
ORDER BY month
"""
member_trend_df = pd.read_sql(member_trend_query, conn)

# Plot member percentage trend
fig_member = px.line(
    member_trend_df,
    x='month',
    y='member_percentage',
    title='Monthly Member Percentage Trend (NYC)',
    labels={'month': 'Month', 'member_percentage': 'Member Percentage (%)'}
)
st.plotly_chart(fig_member, use_container_width=True)

# Station Growth Analysis
st.header("Station Growth Analysis")

# Query for station growth
station_growth_query = """
WITH station_counts AS (
    SELECT 
        EXTRACT(YEAR FROM start_time) as year,
        COUNT(DISTINCT start_station_id) as station_count
    FROM dbt_models.int_nyc_rides
    GROUP BY EXTRACT(YEAR FROM start_time)
)
SELECT 
    year,
    station_count,
    LAG(station_count) OVER (ORDER BY year) as prev_year_count,
    (station_count - LAG(station_count) OVER (ORDER BY year)) * 100.0 / 
    LAG(station_count) OVER (ORDER BY year) as yoy_growth
FROM station_counts
ORDER BY year
"""
station_growth_df = pd.read_sql(station_growth_query, conn)

# Plot station growth
fig_stations = px.bar(
    station_growth_df,
    x='year',
    y='yoy_growth',
    title='Year-over-Year Station Growth',
    labels={'year': 'Year', 'yoy_growth': 'Growth Rate (%)'}
)
st.plotly_chart(fig_stations, use_container_width=True)

# Close database connection
conn.close() 