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

# --- Get max available date for each city ---
nyc_max_date = pd.read_sql(
    f"""SELECT MAX(date) as max_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE location = 'nyc' AND date <= '2024-12-31'""", conn
)['max_date'][0]
london_max_date = pd.read_sql(
    f"""SELECT MAX(date) as max_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE location = 'london'""", conn
)['max_date'][0]
nyc_max_date = pd.to_datetime(nyc_max_date).date() if nyc_max_date else dashboard_max_date
london_max_date = pd.to_datetime(london_max_date).date() if london_max_date else dashboard_max_date

# --- Date Range Picker with Apply Button ---
if page == "Comparison":
    comparison_max_date = min(nyc_max_date, london_max_date)
    date_query = f"SELECT MIN(date) as min_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE date >= '{dashboard_min_date}' AND date <= '{comparison_max_date}'"
    date_df = pd.read_sql(date_query, conn)
    min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date)
    max_date = comparison_max_date
elif page == "NYC":
    date_query = f"SELECT MIN(date) as min_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE location = 'nyc' AND date >= '{dashboard_min_date}' AND date <= '{nyc_max_date}'"
    date_df = pd.read_sql(date_query, conn)
    min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date)
    max_date = nyc_max_date
elif page == "London":
    date_query = f"SELECT MIN(date) as min_date FROM {MART_SCHEMA}.mart_daily_metrics_long WHERE location = 'london' AND date >= '{dashboard_min_date}' AND date <= '{london_max_date}'"
    date_df = pd.read_sql(date_query, conn)
    min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date)
    max_date = london_max_date
else:
    min_date = dashboard_min_date
    max_date = dashboard_max_date

start_date, end_date = st.sidebar.date_input(
    "Select date range:",
    value=(min_date, max_date),
    min_value=dashboard_min_date,
    max_value=max_date
)
apply_filter = st.sidebar.button("Apply Date Filter")

# Add note about re-applying date filter (now below the button, in white)
st.sidebar.markdown("<span style='color:#fff'><b>Note:</b> After switching dashboards or changing the date range, you must click 'Apply Date Filter' to update the dashboard.</span>", unsafe_allow_html=True)

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
    if page == "Comparison":
        # Only render the new KPI section (remove the old one)
        st.header("Comparative Analytics")
        col_nyc, col_london = st.columns(2)

        # Get latest year in filter
        year_query = f"""
            SELECT MAX(year) as latest_year FROM {MART_SCHEMA}.mart_nyc_station_growth
            WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{applied_start_date}') AND EXTRACT(YEAR FROM DATE '{applied_end_date}')
        """
        latest_year = pd.read_sql(year_query, conn)['latest_year'][0]

        # Get population for latest year for each city
        pop_query = f"""
            SELECT location, MAX(metric_value) as population
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE metric_name = 'population' AND year = {latest_year}
            GROUP BY location
        """
        pop_df = pd.read_sql(pop_query, conn).set_index('location')
        nyc_pop = int(pop_df.loc['nyc', 'population']) if 'nyc' in pop_df.index else None
        london_pop = int(pop_df.loc['london', 'population']) if 'london' in pop_df.index else None

        # Calculate rides per 1000 in period for each city
        rides_query = f"""
            SELECT location, SUM(metric_value) as total_rides
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE metric_name = 'total_rides' AND {date_filter}
            GROUP BY location
        """
        rides_df = pd.read_sql(rides_query, conn).set_index('location')
        nyc_rides = rides_df.loc['nyc', 'total_rides'] if 'nyc' in rides_df.index else None
        london_rides = rides_df.loc['london', 'total_rides'] if 'london' in rides_df.index else None
        nyc_rides_per_1000 = (nyc_rides / nyc_pop * 1000) if nyc_rides and nyc_pop else None
        london_rides_per_1000 = (london_rides / london_pop * 1000) if london_rides and london_pop else None

        # Average Ride Duration
        nyc_avg_duration = pd.read_sql(f"""
            SELECT AVG(metric_value) as avg_duration
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE location = 'nyc' AND {date_filter} AND metric_name = 'avg_duration_minutes'
        """, conn)['avg_duration'][0]
        london_avg_duration = pd.read_sql(f"""
            SELECT AVG(metric_value) as avg_duration
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE location = 'london' AND {date_filter} AND metric_name = 'avg_duration_minutes'
        """, conn)['avg_duration'][0]

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
    elif page in ["NYC", "London"]:
        # Total rides in period
        total_rides_query = f"""
        SELECT SUM(metric_value) as total_rides
        FROM {MART_SCHEMA}.mart_daily_metrics_long
        WHERE location = '{page.lower()}'
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
            WHERE location = '{page.lower()}'
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
        WHERE location = '{page.lower()}'
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

    # --- Trends Section ---
    # st.header("Trends")
    # if page == "Comparison":
    #     st.subheader("NYC vs London: Comparative Analytics")

    if page in ["NYC", "London"]:
        # --- Rides Trend ---
        st.subheader("Rides by Month (Overlayed by Year)")
        rides_agg_type = st.radio("Aggregation:", ["Average Daily Rides", "Total Rides"], key=f"rides_agg_{page}", horizontal=True)
        if rides_agg_type == "Average Daily Rides":
            rides_trend_query = f"""
            SELECT EXTRACT(MONTH FROM date) AS month, year, AVG(metric_value) as metric_value
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE location = '{page.lower()}'
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
            WHERE location = '{page.lower()}'
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
        WHERE location = '{page.lower()}'
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
        hour_query = f"SELECT hour_of_day, ride_count FROM {MART_SCHEMA}.mart_{page.lower()}_hourly_patterns ORDER BY hour_of_day"
        hour_df = pd.read_sql(hour_query, conn)
        fig_hour = px.bar(hour_df, x='hour_of_day', y='ride_count', title=f"{page} Rides by Hour of Day")
        st.plotly_chart(fig_hour, use_container_width=True)

        # --- Member % (NYC only) ---
        if page == "NYC":
            st.subheader("Member Percentage Trend")
            member_query = f"SELECT month, member_percentage FROM {MART_SCHEMA}.mart_nyc_member_analysis WHERE month BETWEEN '{start_date}' AND '{end_date}' ORDER BY month"
            member_df = pd.read_sql(member_query, conn)
            fig_member = px.line(member_df, x='month', y='member_percentage', title="NYC Member Percentage Over Time")
            st.plotly_chart(fig_member, use_container_width=True)

        # --- Station Growth ---
        st.subheader("Station Growth")
        station_query = f"""
        SELECT year, station_count as metric_value
        FROM {MART_SCHEMA}.mart_{page.lower()}_station_growth
        WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{start_date}') AND EXTRACT(YEAR FROM DATE '{end_date}')
        ORDER BY year
        """
        station_df = pd.read_sql(station_query, conn)
        fig_station = px.bar(station_df, x='year', y='metric_value', title=f"Station Count by Year")
        st.plotly_chart(fig_station, use_container_width=True)

    elif page == "Comparison":
        # Remove the 'NYC vs London: Comparative Analytics' subheader from the comparison page
        # (Delete or comment out the line: st.subheader("NYC vs London: Comparative Analytics"))

        # --- Comparative Trends Section ---
        st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Comparative Trends</h2>", unsafe_allow_html=True)

        # 1. Comparative Rides
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
            FROM {MART_SCHEMA}.mart_daily_metrics_long
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
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE {date_filter}
              AND metric_name = 'rides_per_1000'
            GROUP BY period, location
            ORDER BY period, location
            """
            y_label = "Rides per 1,000 Residents"
            chart_title = f"Comparative Per Capita Rides Over Time ({trend_agg})"
        comparison_df = pd.read_sql(comparison_query, conn)
        fig_comparison = px.line(
            comparison_df,
            x='period', y='metric_value', color='location',
            title=chart_title,
            labels={'metric_value': y_label, 'period': x_label}
        )
        st.plotly_chart(fig_comparison, use_container_width=True)
        with st.expander("Show data table for comparative trends"):
            st.dataframe(comparison_df)

        # 2. Comparative Average Ride Duration
        st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Comparative Average Ride Duration</h2>", unsafe_allow_html=True)
        duration_query = f"""
            SELECT {date_expr} as period, location, AVG(metric_value) as avg_duration
            FROM {MART_SCHEMA}.mart_daily_metrics_long
            WHERE {date_filter}
              AND metric_name = 'avg_duration_minutes'
            GROUP BY period, location
            ORDER BY period, location
        """
        duration_df = pd.read_sql(duration_query, conn)
        fig_duration = px.line(
            duration_df,
            x='period', y='avg_duration', color='location',
            title=f'Comparative Average Ride Duration Over Time ({trend_agg})',
            labels={'avg_duration': 'Avg Ride Duration (min)', 'period': x_label}
        )
        st.plotly_chart(fig_duration, use_container_width=True)
        with st.expander("Show data table for duration trends"):
            st.dataframe(duration_df)

        # 3. Comparative Station Growth
        st.markdown("<h2 style='font-size:2.2rem; margin-top:2em;'>Comparative Station Growth</h2>", unsafe_allow_html=True)
        station_query = f"""
            SELECT year, location, station_count
            FROM (
                SELECT year, location, station_count FROM {MART_SCHEMA}.mart_nyc_station_growth
                UNION ALL
                SELECT year, location, station_count FROM {MART_SCHEMA}.mart_london_station_growth
            ) t
            WHERE year BETWEEN EXTRACT(YEAR FROM DATE '{applied_start_date}') AND EXTRACT(YEAR FROM DATE '{applied_end_date}')
            ORDER BY year, location
        """
        station_df = pd.read_sql(station_query, conn)
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

else:
    st.info("Select a start and end date, then click 'Apply Date Filter' to update the dashboard.")

# Close database connection
conn.close() 