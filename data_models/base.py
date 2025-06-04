import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import Type, List
from dotenv import load_dotenv
import boto3
from io import BytesIO

class BaseBikeShareRecord:
    staging_table: str = None
    s3_prefix: str = None
    _registry: List[Type['BaseBikeShareRecord']] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls not in BaseBikeShareRecord._registry:
            BaseBikeShareRecord._registry.append(cls)

    @classmethod
    def list_s3_files(cls, prefix=None, year=None):
        load_dotenv()
        S3_BUCKET = os.environ["S3_BUCKET"]
        s3 = boto3.client("s3")
        if prefix is None:
            prefix = cls.s3_prefix
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv"):
                    if year is None or str(year) in key:
                        files.append(key)
        return files

    @classmethod
    def download_csv_from_s3(cls, s3_key):
        load_dotenv()
        S3_BUCKET = os.environ["S3_BUCKET"]
        s3 = boto3.client("s3")
        csv_buffer = BytesIO()
        s3.download_fileobj(S3_BUCKET, s3_key, csv_buffer)
        csv_buffer.seek(0)
        return csv_buffer

    @classmethod
    def assign_model(cls, filename, s3_prefix, df_head=None):
        # Filter models based on s3_prefix
        prefix_to_location = {"london_csv/": "london", "nyc_csv/": "nyc"}
        location = None
        for prefix, loc in prefix_to_location.items():
            if s3_prefix and s3_prefix.startswith(prefix):
                location = loc
                break
        if not location:
            print(f"No location found for s3_prefix: {s3_prefix}")
            return None

        # Filter models by location
        models = [m for m in cls._registry if m.s3_prefix.startswith(s3_prefix[:len(prefix)])]
        print(f"Assigning model for file: {filename} (s3_prefix: {s3_prefix})")
        for model in models:
            result = model.matches_file(filename, df_head)
            print(f"  Checked {model.__name__}: matches_file={result}")
            if result:
                return model
        print(f"  No model matched for file: {filename}")
        return None

    @classmethod
    def load_from_s3(cls, prefix=None, year=None, dry_run=False, filename=None):
        files = cls.list_s3_files(prefix=prefix, year=year)
        if filename:
            files = [f for f in files if os.path.basename(f) == filename]
        print(f"Found {len(files)} files in S3 prefix '{prefix}'")
        for s3_key in files:
            filename = os.path.basename(s3_key)
            print(f"\nProcessing {s3_key}")
            csv_buffer = cls.download_csv_from_s3(s3_key)
            df = pd.read_csv(csv_buffer)
            model = cls.assign_model(filename, prefix, df.head())
            if model is None:
                print(f"ERROR: No model matched file {filename}")
                continue
            print(f"Matched model: {model.__name__} (table: {model.staging_table})")
            df_aligned = model.from_dataframe(df, filename)
            if dry_run:
                print(f"[DRY RUN] Would insert {len(df_aligned)} rows into {model.staging_table}")
            else:
                model.to_database(df_aligned)
                print(f"Inserted {len(df_aligned)} rows into {model.staging_table}")

    @classmethod
    def to_database(cls, df: pd.DataFrame):
        load_dotenv()
        DB_HOST = os.environ.get("DB_HOST")
        DB_USER = os.environ.get("DB_USER")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_NAME = os.environ.get("DB_NAME")
        DB_PORT = os.environ.get("DB_PORT", 5432)
        cols = list(cls.__dataclass_fields__.keys())
        with psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        ) as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"INSERT INTO {cls.staging_table} ({', '.join(cols)}) VALUES %s",
                    df[cols].values.tolist()
                )
            conn.commit()

    @classmethod
    def get_schema_sql(cls) -> str:
        type_map = {
            "str": "TEXT",
            "int": "INTEGER",
            "float": "FLOAT",
            "Optional[int]": "INTEGER",
            "Optional[float]": "FLOAT",
            "datetime": "TIMESTAMP"
        }
        lines = [f"CREATE TABLE IF NOT EXISTS {cls.staging_table} ("]
        for field, fdef in cls.__dataclass_fields__.items():
            t = str(fdef.type)
            t = t.replace("<class '","").replace("'>","").replace("typing.","")
            sql_type = type_map.get(t, "TEXT")
            lines.append(f"    {field} {sql_type},")
        lines.append("    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);")
        return "\n".join(lines)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        # Validate columns
        expected_columns = list(cls.__dataclass_fields__.keys())
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in dataframe: {missing_columns}")

        # ... existing code ... 