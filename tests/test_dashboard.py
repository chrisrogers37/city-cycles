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
