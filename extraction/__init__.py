"""
Extraction Package

Handles data extraction from web sources for bike share and weather data.
"""

from .nyc import download_all_zips, list_nyc_citibike_files, download_and_store_zip
from .london import download_all_csvs, list_london_csv_files, download_and_store_csv
from .weather import (
    backfill_all as weather_backfill_all,
    incremental_update_all as weather_incremental_update_all,
    backfill_city as weather_backfill_city,
    incremental_update as weather_incremental_update,
)

__all__ = [
    # NYC functions
    'download_all_zips',
    'list_nyc_citibike_files',
    'download_and_store_zip',
    # London functions
    'download_all_csvs',
    'list_london_csv_files',
    'download_and_store_csv',
    # Weather functions
    'weather_backfill_all',
    'weather_incremental_update_all',
    'weather_backfill_city',
    'weather_incremental_update',
] 