"""
Shared test fixtures for City Cycles test suite.

Provides reusable fixtures for:
- Temporary DuckDB databases (real, not mocked)
- Mocked S3 clients (no real AWS calls)
- Sample DataFrames matching each bike share schema
"""

import pytest
import os
import tempfile
import duckdb
import pandas as pd
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# DuckDB fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_db_path():
    """
    Create a temporary file path for a DuckDB database.

    Yields the path string. The file is deleted after the test completes.
    Do NOT create any DuckDB connection here -- let individual tests control
    when the connection is opened and closed.
    """
    fd, db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    # Remove the empty file so DuckDB can create its own
    os.unlink(db_path)
    yield db_path
    # Cleanup after test
    if os.path.exists(db_path):
        os.unlink(db_path)
    # DuckDB also creates .wal files
    wal_path = db_path + ".wal"
    if os.path.exists(wal_path):
        os.unlink(wal_path)


@pytest.fixture
def duckdb_connection(temp_db_path):
    """
    Create a real DuckDB connection to a temporary database.

    Yields the connection. Closes it after the test completes.
    This is a plain duckdb.connect() -- NOT a DuckDBManager instance.
    Use this when you need a lightweight connection without S3 extensions.
    """
    conn = duckdb.connect(temp_db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# S3 mock fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_s3_client():
    """
    Create a MagicMock that behaves like a boto3 S3 client.

    This does NOT patch any specific module. Individual tests should use
    unittest.mock.patch() to inject this mock into the module under test.

    Yields the mock client.
    """
    mock_client = MagicMock()
    # Provide a default exceptions attribute that mimics botocore
    mock_client.exceptions = MagicMock()
    yield mock_client


# ---------------------------------------------------------------------------
# Sample DataFrame fixtures (one per schema)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_nyc_legacy_df():
    """
    Sample DataFrame matching the NYC legacy bike share schema.

    Columns use the RAW column names (with spaces) as they appear in source CSVs
    before transformation. 2 rows.
    """
    return pd.DataFrame({
        "tripduration": [300, 600],
        "starttime": ["2019-01-01 00:00:00", "2019-01-01 00:05:00"],
        "stoptime": ["2019-01-01 00:05:00", "2019-01-01 00:15:00"],
        "start station id": ["123", "456"],
        "start station name": ["Station A", "Station B"],
        "start station latitude": [40.7128, 40.7580],
        "start station longitude": [-74.0060, -73.9855],
        "end station id": ["789", "012"],
        "end station name": ["Station C", "Station D"],
        "end station latitude": [40.7282, 40.7484],
        "end station longitude": [-73.7949, -73.9856],
        "bikeid": ["1001", "1002"],
        "usertype": ["Subscriber", "Customer"],
        "birth year": [1990, 1985],
        "gender": [1, 2],
    })


@pytest.fixture
def sample_nyc_modern_df():
    """
    Sample DataFrame matching the NYC modern bike share schema.

    Columns use the exact names from modern CitiBike CSV files. 2 rows.
    """
    return pd.DataFrame({
        "ride_id": ["ABC123", "DEF456"],
        "rideable_type": ["classic_bike", "electric_bike"],
        "started_at": ["2023-12-01 08:00:00", "2023-12-01 08:30:00"],
        "ended_at": ["2023-12-01 08:15:00", "2023-12-01 08:45:00"],
        "start_station_name": ["Station A", "Station B"],
        "start_station_id": ["STA001", "STA002"],
        "end_station_name": ["Station C", "Station D"],
        "end_station_id": ["STA003", "STA004"],
        "start_lat": [40.7128, 40.7580],
        "start_lng": [-74.0060, -73.9855],
        "end_lat": [40.7282, 40.7484],
        "end_lng": [-73.7949, -73.9856],
        "member_casual": ["member", "casual"],
    })


@pytest.fixture
def sample_london_legacy_df():
    """
    Sample DataFrame matching the London legacy bike share schema.

    Columns use the exact names from legacy TfL CSV files. 2 rows.
    """
    return pd.DataFrame({
        "Rental Id": ["rental001", "rental002"],
        "Bike Id": ["bike001", "bike002"],
        "Start Date": ["18/12/2019 08:00", "18/12/2019 08:30"],
        "End Date": ["18/12/2019 08:15", "18/12/2019 08:45"],
        "StartStation Id": ["100", "200"],
        "StartStation Name": ["Hyde Park Corner", "Waterloo Station"],
        "EndStation Id": ["300", "400"],
        "EndStation Name": ["Kings Cross", "Paddington"],
        "Duration": [900, 900],
    })


@pytest.fixture
def sample_london_modern_df():
    """
    Sample DataFrame matching the London modern bike share schema.

    Columns use the exact names from modern TfL CSV files. 2 rows.
    """
    return pd.DataFrame({
        "Number": ["num001", "num002"],
        "Bike number": ["bike001", "bike002"],
        "Bike model": ["CLASSIC", "PBSC_EBIKE"],
        "Start date": ["2023-03-06 08:00", "2023-03-06 08:30"],
        "End date": ["2023-03-06 08:15", "2023-03-06 08:45"],
        "Total duration": ["00:15:00", "00:15:00"],
        "Total duration (ms)": [900000, 900000],
        "Start station number": ["100", "200"],
        "Start station": ["Hyde Park Corner", "Waterloo Station"],
        "End station number": ["300", "400"],
        "End station": ["Kings Cross", "Paddington"],
    })


@pytest.fixture
def sample_weather_df():
    """
    Sample DataFrame matching the hourly weather schema.

    Contains 2 rows of weather data as output by extraction/weather.py.
    """
    return pd.DataFrame({
        "timestamp": ["2023-06-15T12:00", "2023-06-15T13:00"],
        "city": ["nyc", "nyc"],
        "temperature_2m": [25.0, 26.5],
        "relative_humidity_2m": [65.0, 60.0],
        "apparent_temperature": [24.0, 25.5],
        "precipitation": [0.0, 2.5],
        "rain": [0.0, 2.5],
        "snowfall": [0.0, 0.0],
        "snow_depth": [0.0, 0.0],
        "weather_code": [0, 61],
        "cloud_cover": [25.0, 80.0],
        "wind_speed_10m": [10.0, 20.0],
        "wind_gusts_10m": [15.0, 30.0],
        "source_file": ["weather_nyc_2023.parquet", "weather_nyc_2023.parquet"],
    })
