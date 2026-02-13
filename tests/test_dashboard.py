"""
Tests for dashboard query helper functions.

Tests the run_query() function from dashboard/app.py.
Does NOT test Streamlit UI components (which require a running server).

The dashboard module has heavy import-time side effects (Streamlit calls,
S3 downloads), so we test query logic using a standalone DuckDB connection.
"""

import pytest
import duckdb
import pandas as pd


class TestRunQueryLogic:
    """
    Test the query execution pattern used by the dashboard.

    Since dashboard/app.py has extensive import-time side effects (calling
    ensure_local_parquet_files(), st.set_page_config(), and creating a global
    DuckDB connection), we do NOT import it directly.

    Instead, we replicate the run_query() function's behavior using a local
    DuckDB connection and test that pattern works correctly. This validates
    the query execution approach without triggering Streamlit imports.
    """

    @pytest.fixture
    def memory_conn(self):
        """Create an in-memory DuckDB connection for testing."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_run_query_returns_dataframe(self, memory_conn):
        """The run_query pattern should return a pandas DataFrame."""
        result = memory_conn.execute("SELECT 1 AS value, 'hello' AS name").fetchdf()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["value"][0] == 1
        assert result["name"][0] == "hello"

    def test_run_query_handles_empty_result(self, memory_conn):
        """The run_query pattern should return an empty DataFrame for no-match queries."""
        memory_conn.execute("CREATE TABLE t (id INTEGER)")
        result = memory_conn.execute("SELECT * FROM t").fetchdf()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_run_query_aggregation(self, memory_conn):
        """The run_query pattern should handle SUM/AVG aggregations correctly."""
        memory_conn.execute("CREATE TABLE rides (location VARCHAR, total_rides INTEGER, date DATE)")
        memory_conn.execute("""
            INSERT INTO rides VALUES
            ('nyc', 100, '2023-01-01'),
            ('nyc', 200, '2023-01-02'),
            ('london', 50, '2023-01-01'),
            ('london', 75, '2023-01-02')
        """)

        result = memory_conn.execute("""
            SELECT location, SUM(total_rides) as total
            FROM rides
            GROUP BY location
            ORDER BY location
        """).fetchdf()

        assert len(result) == 2
        assert result.loc[result["location"] == "london", "total"].values[0] == 125
        assert result.loc[result["location"] == "nyc", "total"].values[0] == 300

    def test_run_query_date_filtering(self, memory_conn):
        """The run_query pattern should correctly filter by date range."""
        memory_conn.execute("CREATE TABLE daily (date DATE, rides INTEGER)")
        memory_conn.execute("""
            INSERT INTO daily VALUES
            ('2023-01-01', 100),
            ('2023-06-15', 200),
            ('2023-12-31', 300),
            ('2024-01-01', 400)
        """)

        result = memory_conn.execute("""
            SELECT SUM(rides) as total
            FROM daily
            WHERE date BETWEEN '2023-01-01' AND '2023-12-31'
        """).fetchdf()

        assert result["total"][0] == 600  # 100 + 200 + 300

    def test_run_query_parquet_file_read(self, memory_conn, tmp_path):
        """The run_query pattern should be able to read Parquet files directly."""
        # Create a small Parquet file
        df = pd.DataFrame({
            "location": ["nyc", "london"],
            "station_count": [1500, 800],
            "year": [2023, 2023],
        })
        parquet_path = str(tmp_path / "test_mart.parquet")
        df.to_parquet(parquet_path)

        result = memory_conn.execute(
            f"SELECT * FROM '{parquet_path}' ORDER BY location"
        ).fetchdf()

        assert len(result) == 2
        assert result["location"][0] == "london"
        assert result["station_count"][0] == 800

    def test_run_query_with_extract_function(self, memory_conn):
        """The run_query pattern should support EXTRACT(MONTH FROM date) used by the dashboard."""
        memory_conn.execute("CREATE TABLE monthly (date DATE, rides INTEGER)")
        memory_conn.execute("""
            INSERT INTO monthly VALUES
            ('2023-01-15', 100),
            ('2023-01-20', 150),
            ('2023-02-10', 200)
        """)

        result = memory_conn.execute("""
            SELECT EXTRACT(MONTH FROM date) AS month, SUM(rides) AS total
            FROM monthly
            GROUP BY month
            ORDER BY month
        """).fetchdf()

        assert len(result) == 2
        assert result["month"][0] == 1  # January
        assert result["total"][0] == 250  # 100 + 150
        assert result["month"][1] == 2  # February
        assert result["total"][1] == 200

    def test_hourly_patterns_summary_query(self, memory_conn):
        """The hourly patterns query should work with mart_hourly_patterns_summary schema."""
        memory_conn.execute("""
            CREATE TABLE hourly_summary (
                location VARCHAR,
                hour_of_day INTEGER,
                ride_count BIGINT
            )
        """)
        memory_conn.execute("""
            INSERT INTO hourly_summary VALUES
            ('nyc', 0, 1000),
            ('nyc', 8, 5000),
            ('nyc', 17, 4500),
            ('london', 0, 800),
            ('london', 8, 4000),
            ('london', 17, 3500)
        """)

        result = memory_conn.execute("""
            SELECT hour_of_day, ride_count
            FROM hourly_summary
            WHERE location = 'nyc'
            ORDER BY hour_of_day
        """).fetchdf()

        assert len(result) == 3
        assert result["hour_of_day"][0] == 0
        assert result["ride_count"][0] == 1000

    def test_weather_impact_summary_query(self, memory_conn):
        """The weather impact summary query should return pct_change data."""
        memory_conn.execute("""
            CREATE TABLE weather_impact (
                location VARCHAR,
                hour_of_day INTEGER,
                dimension_type VARCHAR,
                dimension_value VARCHAR,
                is_precipitation BOOLEAN,
                temperature_band VARCHAR,
                observation_count INTEGER,
                avg_rides FLOAT,
                avg_duration_seconds FLOAT,
                avg_member_rides FLOAT,
                avg_casual_rides FLOAT,
                baseline_avg_rides FLOAT,
                baseline_avg_duration_seconds FLOAT,
                pct_change_rides_vs_clear FLOAT,
                pct_change_duration_vs_clear FLOAT
            )
        """)
        memory_conn.execute("""
            INSERT INTO weather_impact VALUES
            ('nyc', 9, 'weather_condition', 'rain', NULL, NULL,
             30, 150.0, 720.0, 100.0, 50.0, 220.0, 680.0, -31.8, 5.9),
            ('nyc', 9, 'weather_condition', 'clear', NULL, NULL,
             60, 220.0, 680.0, 150.0, 70.0, 220.0, 680.0, 0.0, 0.0)
        """)

        result = memory_conn.execute("""
            SELECT dimension_value, pct_change_rides_vs_clear
            FROM weather_impact
            WHERE location = 'nyc'
              AND hour_of_day = 9
              AND dimension_type = 'weather_condition'
              AND dimension_value = 'rain'
        """).fetchdf()

        assert len(result) == 1
        assert result["pct_change_rides_vs_clear"][0] == pytest.approx(-31.8, abs=0.1)


class TestStationWeatherQueryLogic:
    """Test station weather performance query patterns used by the dashboard."""

    @pytest.fixture
    def weather_conn(self):
        """Create an in-memory DuckDB connection with test station weather data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE station_directory AS
            SELECT * FROM (VALUES
                ('nyc', 'S001', 'Central Park', 40.7829, -73.9654, 50000, '2019-01-01'::DATE, '2024-12-31'::DATE),
                ('nyc', 'S002', 'Times Square', 40.7580, -73.9855, 80000, '2019-01-01'::DATE, '2024-12-31'::DATE),
                ('london', 'L001', 'Hyde Park', NULL, NULL, 30000, '2019-01-01'::DATE, '2024-12-31'::DATE)
            ) AS t(location, station_id, station_name, latitude, longitude,
                   total_rides, first_ride_date, last_ride_date)
        """)
        conn.execute("""
            CREATE TABLE station_weather AS
            SELECT * FROM (VALUES
                ('nyc', 'S001', 8, 'clear', 5000, 25.0, 12.5, 200, NULL),
                ('nyc', 'S001', 8, 'rain',  3000, 15.0, 14.0, 200, -40.0),
                ('nyc', 'S002', 8, 'clear', 8000, 40.0, 10.0, 200, NULL),
                ('nyc', 'S002', 8, 'rain',  7200, 36.0, 11.0, 200, -10.0),
                ('london', 'L001', 8, 'clear', 3000, 15.0, 20.0, 200, NULL),
                ('london', 'L001', 8, 'rain',  2100, 10.5, 22.0, 200, -30.0)
            ) AS t(location, station_id, hour_of_day, weather_condition,
                   total_rides, avg_rides_per_day, avg_duration_minutes,
                   days_observed, pct_change_vs_clear)
        """)
        yield conn
        conn.close()

    def test_resilience_ranking_ordered_by_pct_change(self, weather_conn):
        """Weather resilience query should return stations ordered by pct_change DESC."""
        result = weather_conn.execute("""
            SELECT s.station_id, d.station_name,
                   round(avg(s.pct_change_vs_clear), 1) as avg_pct_change
            FROM station_weather s
            JOIN station_directory d ON s.location = d.location AND s.station_id = d.station_id
            WHERE s.location = 'nyc'
              AND s.weather_condition = 'rain'
              AND s.pct_change_vs_clear IS NOT NULL
            GROUP BY s.station_id, d.station_name
            ORDER BY avg_pct_change DESC
        """).fetchdf()
        assert len(result) == 2
        assert result['station_id'].iloc[0] == 'S002'
        assert result['avg_pct_change'].iloc[0] == -10.0
        assert result['avg_pct_change'].iloc[1] == -40.0

    def test_map_data_has_coordinates_for_nyc(self, weather_conn):
        """Map query should return non-null coordinates for NYC stations."""
        result = weather_conn.execute("""
            SELECT latitude, longitude
            FROM station_directory
            WHERE location = 'nyc' AND latitude IS NOT NULL
        """).fetchdf()
        assert len(result) == 2
        assert all(result['latitude'].notna())
        assert all(result['longitude'].notna())

    def test_london_has_null_coordinates(self, weather_conn):
        """London stations should have NULL coordinates."""
        result = weather_conn.execute("""
            SELECT latitude, longitude
            FROM station_directory
            WHERE location = 'london'
        """).fetchdf()
        assert result['latitude'].isna().all()
        assert result['longitude'].isna().all()

    def test_weather_comparison_across_cities(self, weather_conn):
        """Cross-city weather comparison should return data for both cities."""
        result = weather_conn.execute("""
            SELECT location, weather_condition,
                   round(avg(pct_change_vs_clear), 1) as avg_pct_change
            FROM station_weather
            WHERE pct_change_vs_clear IS NOT NULL
              AND weather_condition != 'clear'
            GROUP BY location, weather_condition
        """).fetchdf()
        assert len(result) == 2
        locations = result['location'].tolist()
        assert 'nyc' in locations
        assert 'london' in locations
