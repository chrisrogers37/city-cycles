"""
Tests for Weather Deep Dive page logic.

Tests query patterns and data availability checks without importing Streamlit.
Follows the same pattern as test_dashboard.py -- uses standalone DuckDB connections
to validate queries against the actual mart schema.
"""

import os
import pytest
import duckdb
import pandas as pd


class TestWeatherDeepDiveQueries:
    """Test the SQL queries used by the weather deep dive page."""

    @pytest.fixture
    def conn_with_correlation_data(self, tmp_path):
        """Create a DuckDB connection with mart_weather_ride_correlation test data."""
        conn = duckdb.connect(":memory:")
        # Schema matches mart_weather_ride_correlation.sql output columns
        conn.execute("""
            CREATE TABLE correlation AS
            SELECT * FROM (VALUES
                ('nyc', '2023-06-15'::DATE, 8, 150, 720.0, 100, 50,
                 22.5, 20.0, 65.0, 0.0, 0.0, 0.0, 0.0, 0, 'clear', 10.0,
                 15.0, 25.0, false, 'none', 'warm', 'light'),
                ('nyc', '2023-06-16'::DATE, 8, 180, 700.0, 120, 60,
                 25.0, 23.0, 60.0, 2.5, 2.5, 0.0, 0.0, 61, 'rain', 20.0,
                 18.0, 30.0, true, 'light', 'warm', 'moderate'),
                ('nyc', '2023-01-10'::DATE, 8, 50, 600.0, 40, 10,
                 -2.0, -5.0, 80.0, 0.0, 0.0, 0.0, 0.0, 0, 'clear', 5.0,
                 8.0, 10.0, false, 'none', 'freezing', 'calm'),
                ('london', '2023-06-15'::DATE, 8, 200, 900.0, 0, 200,
                 18.0, 16.0, 70.0, 0.0, 0.0, 0.0, 0.0, 0, 'clear', 12.0,
                 10.0, 20.0, false, 'none', 'mild', 'light')
            ) AS t(location, date, hour_of_day, ride_count, avg_duration_seconds,
                   member_rides, casual_rides, temperature_celsius,
                   apparent_temperature_celsius, relative_humidity_pct,
                   precipitation_mm, rain_mm, snowfall_cm, snow_depth_m,
                   weather_code, weather_condition, cloud_cover_pct,
                   wind_speed_kmh, wind_gusts_kmh, is_precipitation,
                   precipitation_intensity, temperature_band, wind_category)
        """)
        # Write to parquet for file-based queries
        df = conn.execute("SELECT * FROM correlation").fetchdf()
        parquet_path = str(tmp_path / "mart_weather_ride_correlation.parquet")
        df.to_parquet(parquet_path)
        yield conn, parquet_path
        conn.close()

    def test_temperature_query_uses_ride_count(self, conn_with_correlation_data):
        """Temperature query must use ride_count (not total_rides) column."""
        conn, parquet_path = conn_with_correlation_data
        query = f"""
        SELECT
            CASE
                WHEN temperature_celsius < 0 THEN 'Below 0C'
                WHEN temperature_celsius < 5 THEN '0-5C'
                WHEN temperature_celsius < 10 THEN '5-10C'
                WHEN temperature_celsius < 15 THEN '10-15C'
                WHEN temperature_celsius < 20 THEN '15-20C'
                WHEN temperature_celsius < 25 THEN '20-25C'
                WHEN temperature_celsius < 30 THEN '25-30C'
                ELSE '30C+'
            END as temp_range,
            MIN(temperature_celsius) as temp_sort,
            round(avg(ride_count), 0) as avg_rides,
            count(*) as days_observed
        FROM '{parquet_path}'
        WHERE location = 'nyc'
        GROUP BY temp_range
        ORDER BY temp_sort
        """
        result = conn.execute(query).fetchdf()
        assert len(result) >= 2  # At least freezing and warm ranges
        assert 'avg_rides' in result.columns

    def test_precipitation_query_uses_ride_count(self, conn_with_correlation_data):
        """Precipitation query must use ride_count (not total_rides) column."""
        conn, parquet_path = conn_with_correlation_data
        query = f"""
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
        FROM '{parquet_path}'
        WHERE location = 'nyc'
        GROUP BY precip_category
        ORDER BY precip_sort
        """
        result = conn.execute(query).fetchdf()
        assert len(result) >= 1
        assert 'avg_rides' in result.columns

    def test_city_filter_returns_only_selected_city(self, conn_with_correlation_data):
        """Queries filtered by location should only return that city's data."""
        conn, parquet_path = conn_with_correlation_data
        query = f"""
        SELECT DISTINCT location FROM '{parquet_path}' WHERE location = 'london'
        """
        result = conn.execute(query).fetchdf()
        assert len(result) == 1
        assert result['location'][0] == 'london'

    def test_total_rides_column_does_not_exist(self, conn_with_correlation_data):
        """The mart should NOT have a total_rides column -- it uses ride_count."""
        conn, parquet_path = conn_with_correlation_data
        result = conn.execute(f"SELECT * FROM '{parquet_path}' LIMIT 1").fetchdf()
        assert 'total_rides' not in result.columns
        assert 'ride_count' in result.columns


class TestWeatherDeepDiveImpactQueries:
    """Test the weather impact summary queries."""

    @pytest.fixture
    def conn_with_impact_data(self, tmp_path):
        """Create a DuckDB connection with mart_weather_impact_summary test data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE impact AS
            SELECT * FROM (VALUES
                ('nyc', 8, 'weather_condition', 'rain', NULL::BOOLEAN, NULL::VARCHAR,
                 30, 150.0, 720.0, 100.0, 50.0, 220.0, 680.0, -31.8, 5.9),
                ('nyc', 8, 'weather_condition', 'clear', NULL::BOOLEAN, NULL::VARCHAR,
                 60, 220.0, 680.0, 150.0, 70.0, 220.0, 680.0, 0.0, 0.0),
                ('nyc', 8, 'weather_condition', 'snow', NULL::BOOLEAN, NULL::VARCHAR,
                 10, 80.0, 500.0, 60.0, 20.0, 220.0, 680.0, -63.6, -26.5),
                ('nyc', 8, 'weather_condition', 'fog', NULL::BOOLEAN, NULL::VARCHAR,
                 15, 190.0, 700.0, 130.0, 60.0, 220.0, 680.0, -13.6, 2.9),
                ('london', 8, 'weather_condition', 'rain', NULL::BOOLEAN, NULL::VARCHAR,
                 25, 120.0, 800.0, 0.0, 120.0, 180.0, 750.0, -33.3, 6.7)
            ) AS t(location, hour_of_day, dimension_type, dimension_value,
                   is_precipitation, temperature_band, observation_count,
                   avg_rides, avg_duration_seconds, avg_member_rides,
                   avg_casual_rides, baseline_avg_rides,
                   baseline_avg_duration_seconds, pct_change_rides_vs_clear,
                   pct_change_duration_vs_clear)
        """)
        df = conn.execute("SELECT * FROM impact").fetchdf()
        parquet_path = str(tmp_path / "mart_weather_impact_summary.parquet")
        df.to_parquet(parquet_path)
        yield conn, parquet_path
        conn.close()

    def test_impact_query_excludes_clear(self, conn_with_impact_data):
        """Weather impact query should exclude clear weather from results."""
        conn, parquet_path = conn_with_impact_data
        query = f"""
        SELECT dimension_value as weather_condition,
               round(avg(pct_change_rides_vs_clear), 1) as pct_change
        FROM '{parquet_path}'
        WHERE location = 'nyc'
          AND dimension_type = 'weather_condition'
          AND dimension_value != 'clear'
        GROUP BY dimension_value
        ORDER BY pct_change
        """
        result = conn.execute(query).fetchdf()
        assert 'clear' not in result['weather_condition'].values
        assert len(result) == 3  # rain, snow, fog

    def test_hourly_impact_pivots_conditions(self, conn_with_impact_data):
        """Hourly impact query should pivot rain/snow/fog into separate columns."""
        conn, parquet_path = conn_with_impact_data
        query = f"""
        SELECT hour_of_day,
               round(avg(CASE WHEN dimension_value = 'rain' THEN pct_change_rides_vs_clear END), 1) as rain_impact,
               round(avg(CASE WHEN dimension_value = 'snow' THEN pct_change_rides_vs_clear END), 1) as snow_impact,
               round(avg(CASE WHEN dimension_value = 'fog' THEN pct_change_rides_vs_clear END), 1) as fog_impact
        FROM '{parquet_path}'
        WHERE location = 'nyc' AND dimension_type = 'weather_condition'
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        result = conn.execute(query).fetchdf()
        assert len(result) >= 1
        assert result['rain_impact'][0] == pytest.approx(-31.8, abs=0.1)
        assert result['snow_impact'][0] == pytest.approx(-63.6, abs=0.1)
        assert result['fog_impact'][0] == pytest.approx(-13.6, abs=0.1)


class TestParquetExists:
    """Test the parquet_exists helper function."""

    def test_returns_true_for_existing_file(self, tmp_path):
        """parquet_exists should return True when the file exists."""
        from unittest.mock import patch
        test_file = tmp_path / "test.parquet"
        test_file.touch()
        with patch('dashboard.utils.query_helpers.DATA_DIR', str(tmp_path)):
            from dashboard.utils.query_helpers import parquet_exists
            assert parquet_exists("test.parquet") is True

    def test_returns_false_for_missing_file(self, tmp_path):
        """parquet_exists should return False when the file does not exist."""
        from unittest.mock import patch
        with patch('dashboard.utils.query_helpers.DATA_DIR', str(tmp_path)):
            from dashboard.utils.query_helpers import parquet_exists
            assert parquet_exists("nonexistent.parquet") is False
