"""
Shared dependencies for the City Cycles API.

DuckDB connection factory, path resolution, and city parameter validation.
"""

import os
from enum import Enum

import duckdb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")


class CityParam(str, Enum):
    nyc = "nyc"
    london = "london"


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Fresh per-request connection (cheap, ~1ms, avoids concurrency issues)."""
    return duckdb.connect(":memory:")


def parquet_path(filename: str) -> str:
    """Resolve a mart parquet filename to its full path in DATA_DIR."""
    return os.path.join(DATA_DIR, filename)


def parquet_exists(filename: str) -> bool:
    """Check whether a mart parquet file exists locally."""
    return os.path.isfile(parquet_path(filename))
