import os
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from io import BytesIO

S3_BUCKET = os.environ.get("S3_BUCKET")
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT", 5432)

s3 = boto3.client("s3")

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )

def download_csv_from_s3(s3_key):
    csv_buffer = BytesIO()
    s3.download_fileobj(S3_BUCKET, s3_key, csv_buffer)
    csv_buffer.seek(0)
    return csv_buffer

def insert_nyc_legacy(df):
    cols = [
        "tripduration", "bikeid", "starttime", "stoptime", "start_station_id", "start_station_name",
        "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name",
        "end_station_latitude", "end_station_longitude", "usertype", "birth_year", "gender",
        "source_file"
    ]
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO raw_nyc_legacy ({', '.join(cols)}) VALUES %s",
                df[cols].values.tolist()
            )
        conn.commit()

def insert_nyc_modern(df):
    cols = [
        "ride_id", "rideable_type", "started_at", "ended_at", "start_station_id", "start_station_name",
        "end_station_id", "end_station_name", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
        "source_file"
    ]
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO raw_nyc_modern ({', '.join(cols)}) VALUES %s",
                df[cols].values.tolist()
            )
        conn.commit()

def insert_london_legacy(df):
    cols = [
        "rental_id", "bike_id", "start_date", "end_date", "duration", "start_station_id", "start_station_name",
        "end_station_id", "end_station_name", "source_file"
    ]
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO raw_london_legacy ({', '.join(cols)}) VALUES %s",
                df[cols].values.tolist()
            )
        conn.commit()

def insert_london_modern(df):
    cols = [
        "number", "bike_number", "bike_model", "start_date", "end_date", "total_duration", "total_duration_ms",
        "start_station_number", "start_station", "end_station_number", "end_station", "source_file"
    ]
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO raw_london_modern ({', '.join(cols)}) VALUES %s",
                df[cols].values.tolist()
            )
        conn.commit()

# Example usage:
# s3_key = 'london_csv/193JourneyDataExtract18Dec2019-24Dec2019.csv'
# csv_buffer = download_csv_from_s3(s3_key)
# df = pd.read_csv(csv_buffer)
# insert_london_legacy(df) 