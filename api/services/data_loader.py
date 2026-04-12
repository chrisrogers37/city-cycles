"""
S3 parquet file loader for the City Cycles API.

Downloads mart parquet files from S3 to local data/ directory on startup.
Adapted from streamlit_data_manager/parquet_file_manager.py.
"""

import logging
import os

import boto3
from botocore.exceptions import ClientError

from api.dependencies import DATA_DIR

logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET", "city-cycles-data-ctr37")

MARTS = [
    "mart_daily_metrics.parquet",
    "mart_hourly_patterns_summary.parquet",
    "mart_nyc_member_analysis.parquet",
    "mart_station_growth.parquet",
    "mart_daily_metrics_long.parquet",
    "mart_hourly_rides.parquet",
    "mart_weather_ride_correlation.parquet",
    "mart_weather_impact_summary.parquet",
    "mart_station_directory.parquet",
    "mart_station_weather_performance.parquet",
    "mart_similar_day_stats.parquet",
]


def ensure_local_parquet_files():
    """Download all mart parquet files from S3 if not already present locally."""
    s3 = boto3.client('s3')
    os.makedirs(DATA_DIR, exist_ok=True)
    for mart in MARTS:
        local_path = os.path.join(DATA_DIR, mart)
        if not os.path.exists(local_path):
            try:
                print(f"Downloading {mart} from S3...")
                s3.download_file(S3_BUCKET, f"marts/{mart}", local_path)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ("404", "NoSuchKey"):
                    logger.warning(f"Mart not found in S3, skipping: {mart}")
                else:
                    raise
