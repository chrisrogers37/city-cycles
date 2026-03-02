"""
Dashboard query helpers -- extracted from monolithic app.py.
All DuckDB queries live here, cached and parameterized.
"""

import streamlit as st
import duckdb
import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Persistent in-memory DuckDB connection."""
    return duckdb.connect(database=':memory:')


def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    return get_connection().execute(query).fetchdf()


def run_query_params(query: str, params: list) -> pd.DataFrame:
    """Execute a parameterized query and return results as a DataFrame."""
    return get_connection().execute(query, params).fetchdf()


def parquet_path(filename: str) -> str:
    """Resolve a mart Parquet filename to its full path in DATA_DIR."""
    return os.path.join(DATA_DIR, filename)


def parquet_exists(filename: str) -> bool:
    """Check whether a mart parquet file exists on disk.

    Use this as a pre-flight check before running queries against a mart
    parquet. Returns False if the file has not been downloaded from S3
    (e.g., because it was missing from the bucket or the download failed).

    Args:
        filename: The mart parquet filename, e.g. 'mart_weather_ride_correlation.parquet'.

    Returns:
        True if the file exists in DATA_DIR, False otherwise.
    """
    return os.path.isfile(os.path.join(DATA_DIR, filename))
