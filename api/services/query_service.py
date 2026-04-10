"""
DuckDB query execution service — port of dashboard/utils/query_helpers.py
without Streamlit dependencies.
"""

import duckdb
import pandas as pd

from api.dependencies import get_duckdb_conn, parquet_path


def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    conn = get_duckdb_conn()
    return conn.execute(query).fetchdf()


def run_query_params(query: str, params: list) -> pd.DataFrame:
    """Execute a parameterized query and return results as a DataFrame."""
    conn = get_duckdb_conn()
    return conn.execute(query, params).fetchdf()
