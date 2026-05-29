"""
Shared dependencies for the City Cycles API.

DuckDB connection factory, path resolution, and city parameter validation.
"""

import os
from enum import Enum

import duckdb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

ALLOWED_PARQUETS: frozenset[str] = frozenset(
    {
        "mart_daily_metrics.parquet",
        "mart_daily_metrics_long.parquet",
        "mart_hourly_patterns_summary.parquet",
        "mart_hourly_rides.parquet",
        "mart_nyc_member_analysis.parquet",
        "mart_similar_day_stats.parquet",
        "mart_station_directory.parquet",
        "mart_station_growth.parquet",
        "mart_station_weather_performance.parquet",
        "mart_weather_impact_summary.parquet",
        "mart_weather_ride_correlation.parquet",
    }
)


class CityParam(str, Enum):
    nyc = "nyc"
    london = "london"


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Fresh per-request connection (cheap, ~1ms, avoids concurrency issues)."""
    return duckdb.connect(":memory:")


def _validate_parquet_filename(filename: str) -> None:
    """Raise ValueError if filename is not in the known mart allowlist."""
    if filename not in ALLOWED_PARQUETS:
        raise ValueError(f"Unknown parquet file: {filename}")


def parquet_path(filename: str) -> str:
    """Resolve a mart parquet filename to its full path in DATA_DIR."""
    _validate_parquet_filename(filename)
    return os.path.join(DATA_DIR, filename)


def parquet_exists(filename: str) -> bool:
    """Check whether a mart parquet file exists locally."""
    _validate_parquet_filename(filename)
    return os.path.isfile(os.path.join(DATA_DIR, filename))
