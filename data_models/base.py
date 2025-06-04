import os
import sys
import psutil
import gc
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import Type, List
from dotenv import load_dotenv
import boto3
from io import BytesIO
from datetime import datetime
import numpy as np

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
    def _validate_type(cls, value, expected_type) -> bool:
        """Validate if a value matches the expected type, handling special cases."""
        if expected_type == str:
            # For string fields, allow both strings and numbers (which will be converted)
            return isinstance(value, (str, int, float, np.integer))
        elif expected_type in (int, float):
            return pd.api.types.is_numeric_dtype(type(value))
        elif expected_type == datetime:
            # For datetime fields, we expect strings that can be parsed
            if not isinstance(value, str):
                return False
            try:
                pd.to_datetime(value)
                return True
            except:
                return False
        elif isinstance(expected_type, tuple):
            # Handle multiple possible types (e.g., (int, float))
            return any(cls._validate_type(value, t) for t in expected_type)
        return isinstance(value, expected_type)

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns.
        
        This method should be implemented by subclasses to check if the dataframe
        contains the expected columns. Type validation is handled during transformation.
        """
        raise NotImplementedError("Subclasses must implement validate_schema")

    @classmethod
    def assign_model(cls, filename, s3_prefix, df_head=None):
        """Assign a model based on schema validation rather than file name patterns."""
        if df_head is None:
            print(f"ERROR: Cannot assign model without dataframe head for {filename}")
            return None

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
        
        # Try each model's schema validation
        for model in models:
            try:
                if model.validate_schema(df_head):
                    print(f"Matched {filename} to {model.__name__}")
                    return model
            except Exception as e:
                print(f"Schema validation failed for {model.__name__}: {str(e)}")
                continue
        # If no model matched, print missing columns for each model
        for model in models:
            required_columns = getattr(model, 'required_columns', None)
            if required_columns is None:
                # Try to get from validate_schema closure if possible
                try:
                    required_columns = model.validate_schema.__code__.co_consts[1]
                except Exception:
                    required_columns = []
            missing_columns = [col for col in required_columns if col not in df_head.columns]
            if missing_columns:
                print(f"For {model.__name__}, missing columns: {', '.join(missing_columns)}")
        print(f"ERROR: No model matched schema for {filename}")
        return None

    @classmethod
    def load_from_s3(cls, prefix=None, year=None, dry_run=False, filename=None, chunksize=10000):
        files = cls.list_s3_files(prefix=prefix, year=year)
        if filename:
            files = [f for f in files if os.path.basename(f) == filename]
        print(f"Found {len(files)} files in S3 prefix '{prefix}'")
        for s3_key in files:
            filename = os.path.basename(s3_key)
            print(f"\nProcessing {s3_key}")
            csv_buffer = cls.download_csv_from_s3(s3_key)
            chunk_iter = pd.read_csv(csv_buffer, chunksize=chunksize)
            total_rows = 0
            chunk_num = 0
            model = None
            for chunk in chunk_iter:
                chunk_num += 1
                if model is None:
                    model = cls.assign_model(filename, prefix, chunk.head())
                    if model is None:
                        print(f"ERROR: No model matched file {filename} (chunk {chunk_num})")
                        break
                df_aligned = model.to_dataframe(chunk, filename)
                total_rows += len(df_aligned)
                if dry_run:
                    print(f"[DRY RUN] Chunk {chunk_num}: Would insert {len(df_aligned)} rows into {model.staging_table}")
                else:
                    model.to_database(df_aligned)
                    print(f"Inserted chunk {chunk_num}: {len(df_aligned)} rows into {model.staging_table}")
                # Log memory usage
                process = psutil.Process(os.getpid())
                mem_mb = process.memory_info().rss / 1024 / 1024
                print(f"[Memory] After chunk {chunk_num}: {mem_mb:.2f} MB used")
                # Explicitly delete DataFrames and run garbage collection
                del chunk
                del df_aligned
                gc.collect()
            print(f"Finished {filename}: {total_rows} rows processed.")

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
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Transform raw dataframe into standardized model format.
        
        This method should be implemented by subclasses to:
        1. Rename columns to match model's field names
        2. Convert data types as needed
        3. Add source_file column
        4. Return only the columns defined in the model
        """
        raise NotImplementedError("Subclasses must implement to_dataframe")

    @classmethod
    def create_table(cls):
        """Executes the DDL statement for this model's table in the database."""
        load_dotenv()
        DB_HOST = os.environ.get("DB_HOST")
        DB_USER = os.environ.get("DB_USER")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_NAME = os.environ.get("DB_NAME")
        DB_PORT = os.environ.get("DB_PORT", 5432)
        ddl = cls.get_schema_sql()
        with psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        print(f"Created table (if not exists): {cls.staging_table}")

    @classmethod
    def create_all_tables(cls):
        """Executes the DDL for all registered models."""
        for model in cls._registry:
            model.create_table()

    @classmethod
    def truncate_table(cls):
        """Truncates the staging table for this model."""
        load_dotenv()
        DB_HOST = os.environ.get("DB_HOST")
        DB_USER = os.environ.get("DB_USER")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_NAME = os.environ.get("DB_NAME")
        DB_PORT = os.environ.get("DB_PORT", 5432)
        
        with psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {cls.staging_table}")
            conn.commit()
        print(f"Truncated table: {cls.staging_table}")

    @classmethod
    def delete_by_source_file(cls, source_file: str):
        """Deletes records from the staging table for a specific source file."""
        load_dotenv()
        DB_HOST = os.environ.get("DB_HOST")
        DB_USER = os.environ.get("DB_USER")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_NAME = os.environ.get("DB_NAME")
        DB_PORT = os.environ.get("DB_PORT", 5432)
        
        with psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {cls.staging_table} WHERE source_file = %s", (source_file,))
            conn.commit()
        print(f"Deleted records for source file: {source_file} from {cls.staging_table}")

    @classmethod
    def load_from_s3_with_model(cls, model_class, prefix=None, year=None, dry_run=False, filename=None, chunksize=10000):
        """Load data using a specific model class instead of auto-detecting."""
        if not issubclass(model_class, BaseBikeShareRecord):
            raise ValueError(f"{model_class.__name__} is not a valid bike share record model")
            
        files = model_class.list_s3_files(prefix=prefix, year=year)
        if filename:
            files = [f for f in files if os.path.basename(f) == filename]
        print(f"Found {len(files)} files in S3 prefix '{prefix}'")
        
        for s3_key in files:
            filename = os.path.basename(s3_key)
            print(f"\nProcessing {s3_key}")
            csv_buffer = model_class.download_csv_from_s3(s3_key)
            chunk_iter = pd.read_csv(csv_buffer, chunksize=chunksize)
            total_rows = 0
            chunk_num = 0
            
            for chunk in chunk_iter:
                chunk_num += 1
                df_aligned = model_class.to_dataframe(chunk, filename)
                total_rows += len(df_aligned)
                if dry_run:
                    print(f"[DRY RUN] Chunk {chunk_num}: Would insert {len(df_aligned)} rows into {model_class.staging_table}")
                else:
                    model_class.to_database(df_aligned)
                    print(f"Inserted chunk {chunk_num}: {len(df_aligned)} rows into {model_class.staging_table}")
                # Log memory usage
                process = psutil.Process(os.getpid())
                mem_mb = process.memory_info().rss / 1024 / 1024
                print(f"[Memory] After chunk {chunk_num}: {mem_mb:.2f} MB used")
                # Explicitly delete DataFrames and run garbage collection
                del chunk
                del df_aligned
                gc.collect()
            print(f"Finished {filename}: {total_rows} rows processed.")

    @classmethod
    def reload_file(cls, filename: str, prefix=None, dry_run=False, chunksize=10000):
        """Delete and reload a specific file."""
        # First delete existing records
        if not dry_run:
            cls.delete_by_source_file(filename)
        
        # Then load the file
        cls.load_from_s3(prefix=prefix, filename=filename, dry_run=dry_run, chunksize=chunksize) 