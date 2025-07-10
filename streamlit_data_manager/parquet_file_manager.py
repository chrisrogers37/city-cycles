#!/usr/bin/env python3
"""
HTTPFS connector for querying S3 Parquet files directly.

This module provides utilities for connecting to S3 Parquet files using DuckDB's HTTPFS extension,
enabling the dashboard to query mart data without loading the full database.
"""

import boto3
import os

S3_BUCKET = "city-cycles-data-ctr37"
MARTS = [
    "mart_daily_metrics.parquet",
    "mart_hourly_patterns.parquet",
    "mart_nyc_member_analysis.parquet",
    "mart_station_growth.parquet",
    "mart_daily_metrics_long.parquet"
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
            print(f"Downloading {mart} from S3...")
            s3.download_file(S3_BUCKET, f"marts/{mart}", local_path) 