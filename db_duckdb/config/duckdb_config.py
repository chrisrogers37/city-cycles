"""
Configuration for DuckDB migration from S3 Parquet files.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# S3 Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "city-cycles-data-ctr37")

# S3 URI patterns for each schema
S3_URIS = {
    'raw_nyc_legacy': f's3://{S3_BUCKET}/extracted_bike_ride_parquet/nyc/nyclegacybikesharerecord/*.parquet',
    'raw_nyc_modern': f's3://{S3_BUCKET}/extracted_bike_ride_parquet/nyc/nycmodernbikesharerecord/*.parquet',
    'raw_london_legacy': f's3://{S3_BUCKET}/extracted_bike_ride_parquet/london/londonlegacybikesharerecord/*.parquet',
    'raw_london_modern': f's3://{S3_BUCKET}/extracted_bike_ride_parquet/london/londonmodernbikesharerecord/*.parquet'
}

# Table schemas for DuckDB
TABLE_SCHEMAS = {
    'raw_nyc_legacy': """
        CREATE TABLE raw_nyc_legacy (
            tripduration INTEGER,
            bikeid VARCHAR,
            starttime VARCHAR,
            stoptime VARCHAR,
            start_station_id VARCHAR,
            start_station_name VARCHAR,
            start_station_latitude DOUBLE,
            start_station_longitude DOUBLE,
            end_station_id VARCHAR,
            end_station_name VARCHAR,
            end_station_latitude DOUBLE,
            end_station_longitude DOUBLE,
            usertype VARCHAR,
            birth_year INTEGER,
            gender INTEGER,
            source_file VARCHAR
        )
    """,
    
    'raw_nyc_modern': """
        CREATE TABLE raw_nyc_modern (
            ride_id VARCHAR,
            rideable_type VARCHAR,
            started_at VARCHAR,
            ended_at VARCHAR,
            start_station_id VARCHAR,
            start_station_name VARCHAR,
            end_station_id VARCHAR,
            end_station_name VARCHAR,
            start_lat DOUBLE,
            start_lng DOUBLE,
            end_lat DOUBLE,
            end_lng DOUBLE,
            member_casual VARCHAR,
            source_file VARCHAR
        )
    """,
    
    'raw_london_legacy': """
        CREATE TABLE raw_london_legacy (
            rental_id VARCHAR,
            bike_id VARCHAR,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            duration INTEGER,
            start_station_id VARCHAR,
            start_station_name VARCHAR,
            end_station_id VARCHAR,
            end_station_name VARCHAR,
            source_file VARCHAR
        )
    """,
    
    'raw_london_modern': """
        CREATE TABLE raw_london_modern (
            number VARCHAR,
            bike_number VARCHAR,
            bike_model VARCHAR,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            total_duration VARCHAR,
            total_duration_ms BIGINT,
            start_station_number VARCHAR,
            start_station VARCHAR,
            end_station_number VARCHAR,
            end_station VARCHAR,
            source_file VARCHAR
        )
    """
}

# Database configuration
DB_CONFIG = {
    'db_path': os.environ.get('DUCKDB_PATH', './data/city_cycles.duckdb'),
    'memory_limit': os.environ.get('DUCKDB_MEMORY_LIMIT', '8GB'),
    'threads': int(os.environ.get('DUCKDB_THREADS', '4'))
}

# Validation queries to verify data integrity
VALIDATION_QUERIES = {
    'raw_nyc_legacy': """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            MIN(starttime) as earliest_start,
            MAX(stoptime) as latest_stop,
            COUNT(DISTINCT bikeid) as unique_bikes,
            COUNT(DISTINCT (bikeid || '_' || starttime || '_' || stoptime || '_' || start_station_id)) as unique_rides
        FROM raw_nyc_legacy
    """,
    
    'raw_nyc_modern': """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            MIN(started_at) as earliest_start,
            MAX(ended_at) as latest_stop,
            COUNT(DISTINCT ride_id) as unique_rides
        FROM raw_nyc_modern
    """,
    
    'raw_london_legacy': """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            MIN(start_date) as earliest_start,
            MAX(end_date) as latest_stop,
            COUNT(DISTINCT bike_id) as unique_bikes,
            COUNT(DISTINCT rental_id) as unique_rides
        FROM raw_london_legacy
    """,
    
    'raw_london_modern': """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            MIN(start_date) as earliest_start,
            MAX(end_date) as latest_stop,
            COUNT(DISTINCT bike_number) as unique_bikes,
            COUNT(DISTINCT number) as unique_rides
        FROM raw_london_modern
    """
} 