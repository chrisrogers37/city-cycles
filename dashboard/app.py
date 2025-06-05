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
        AVG(total_rides) as avg_daily_rides
    FROM (
        SELECT total_rides
        FROM dbt_models.mart_london_daily_metrics
        UNION ALL
        SELECT total_rides
        FROM dbt_models.mart_nyc_daily_metrics
    ) combined_rides
    """
    avg_daily_rides = pd.read_sql(daily_rides_query, conn).iloc[0]['avg_daily_rides']
    st.metric("Average Daily Rides", f"{avg_daily_rides:,.0f}")

# Query for average duration KPI
with col2:
    duration_query = """
    SELECT 
        AVG(avg_duration_minutes) as avg_duration_minutes
    FROM (
        SELECT avg_duration_minutes
        FROM dbt_models.mart_london_daily_metrics
        UNION ALL
        SELECT avg_duration_minutes
        FROM dbt_models.mart_nyc_daily_metrics
    ) combined_durations
    """
    avg_duration = pd.read_sql(duration_query, conn).iloc[0]['avg_duration_minutes']
    st.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")

# Trends Section
st.header("Ride Trends")

# Query for daily rides trend
rides_trend_query = """
SELECT 
    date,
    year,
    total_rides as daily_rides
FROM dbt_models.mart_london_daily_metrics
ORDER BY date
"""
rides_trend_df = pd.read_sql(rides_trend_query, conn)

# Plot daily rides trend
fig_rides = px.line(
    rides_trend_df,
    x='date',
    y='daily_rides',
    color='year',
    title='Daily Rides Trend by Year',
    labels={'date': 'Date', 'daily_rides': 'Number of Rides', 'year': 'Year'}
)
st.plotly_chart(fig_rides, use_container_width=True)

# Query for duration trend
duration_trend_query = """
SELECT 
    date,
    year,
    avg_duration_minutes
FROM dbt_models.mart_london_daily_metrics
ORDER BY date
"""
duration_trend_df = pd.read_sql(duration_trend_query, conn)

# Plot duration trend
fig_duration = px.line(
    duration_trend_df,
    x='date',
    y='avg_duration_minutes',
    color='year',
    title='Average Ride Duration Trend by Year',
    labels={'date': 'Date', 'avg_duration_minutes': 'Duration (minutes)', 'year': 'Year'}
)
st.plotly_chart(fig_duration, use_container_width=True)

# Time of Day Analysis
st.header("Time of Day Analysis")

# Query for time of day distribution
time_of_day_query = """
SELECT 
    hour_of_day,
    ride_count
FROM dbt_models.mart_london_hourly_patterns
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
SELECT 
    year,
    station_count,
    prev_year_count,
    yoy_growth
FROM dbt_models.mart_london_station_growth
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