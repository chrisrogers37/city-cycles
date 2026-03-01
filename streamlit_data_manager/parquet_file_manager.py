#!/usr/bin/env python3
"""
HTTPFS connector for querying S3 Parquet files directly.

This module provides utilities for connecting to S3 Parquet files using DuckDB's HTTPFS extension,
enabling the dashboard to query mart data without loading the full database.
"""

import logging

import boto3
from botocore.exceptions import ClientError
import os

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

# Always resolve data directory at project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

def ensure_local_parquet_files():
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