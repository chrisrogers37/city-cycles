"""
Extraction Package

Handles data extraction from web sources for bike share data.
"""

from .nyc import download_all_zips, list_nyc_citibike_files, download_and_store_zip
from .london import download_all_csvs, list_london_csv_files, download_and_store_csv

__all__ = [
    # NYC functions
    'download_all_zips',
    'list_nyc_citibike_files', 
    'download_and_store_zip',
    # London functions
    'download_all_csvs',
    'list_london_csv_files',
    'download_and_store_csv'
] 