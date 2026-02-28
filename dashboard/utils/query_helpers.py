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
    """Check whether a mart Parquet file exists locally in DATA_DIR."""
    return os.path.isfile(parquet_path(filename))
