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
    def list_s3_files(cls, prefix=None):
        """List files in S3 bucket matching the model's pattern."""
        s3_client = boto3.client('s3')
        bucket = os.getenv('S3_BUCKET')
        
        if prefix:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        else:
            response = s3_client.list_objects_v2(Bucket=bucket)
            
        if 'Contents' not in response:
            return []
            
        return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]

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
    def load_from_s3(cls, prefix=None, dry_run=False, filename=None, chunksize=5000):
        """Load data from S3 into database."""
        if filename:
            files = [filename]
        else:
            files = cls.list_s3_files(prefix)
            
        if not files:
            print(f"No files found in S3 bucket {os.getenv('S3_BUCKET')} with prefix {prefix}")
            return
            
        print(f"Found {len(files)} files in S3")
        
        for file in files:
            try:
                print(f"\nProcessing {file}")
                df = cls.read_from_s3(file)
                if df is None:
                    continue
                    
                if dry_run:
                    print(f"Would insert {len(df)} rows into {cls.__name__}")
                    continue
                    
                cls.insert_dataframe(df, chunksize=chunksize)
                print(f"Successfully loaded {file}")
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
                continue

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
    def load_from_s3_with_model(cls, model_class, prefix=None, dry_run=False, filename=None, chunksize=5000):
        """Load data from S3 into database using a specific model class."""
        if not issubclass(model_class, BaseBikeShareRecord):
            raise ValueError(f"Model class {model_class.__name__} must be a subclass of BaseBikeShareRecord")
            
        if filename:
            files = [filename]
        else:
            files = model_class.list_s3_files(prefix)
            
        if not files:
            print(f"No files found in S3 bucket {os.getenv('S3_BUCKET')} with prefix {prefix}")
            return
            
        print(f"Found {len(files)} files in S3")
        
        # Process files one at a time to manage memory
        for file in files:
            try:
                print(f"\nProcessing {file}")
                
                # Download and validate schema
                s3_client = boto3.client('s3')
                bucket = os.getenv('S3_BUCKET')
                
                # Read first row for schema validation
                response = s3_client.get_object(Bucket=bucket, Key=file)
                df_head = pd.read_csv(response['Body'], nrows=1)
                
                # Validate schema
                if not model_class.validate_schema(df_head):
                    print(f"Skipping {file} - schema mismatch")
                    continue
                    
                print(f"Schema validation passed for {file}")
                
                # Reset buffer for full read
                response = s3_client.get_object(Bucket=bucket, Key=file)
                
                # Process file in chunks
                chunk_num = 0
                for chunk in pd.read_csv(response['Body'], chunksize=chunksize):
                    chunk_num += 1
                    print(f"Processing chunk {chunk_num} of {file}")
                    
                    if dry_run:
                        print(f"Would insert {len(chunk)} rows into {model_class.__name__}")
                        continue
                        
                    model_class.insert_dataframe(chunk, chunksize=chunksize)
                    print(f"Inserted chunk {chunk_num}: {len(chunk)} rows into {model_class.__name__}")
                    
                    # Clear memory
                    del chunk
                    gc.collect()
                    
                print(f"Successfully loaded {file}")
                
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
                continue
            finally:
                # Clear memory after each file
                gc.collect()

    @classmethod
    def reload_file(cls, filename, prefix=None, dry_run=False, chunksize=5000):
        """Delete and reload a specific file."""
        if dry_run:
            print(f"Would delete and reload {filename}")
            return
            
        cls.delete_file(filename)
        cls.load_from_s3(prefix, dry_run, filename, chunksize)

    @classmethod
    def insert_dataframe(cls, df, chunksize=5000):
        """Insert a DataFrame into the database in chunks."""
        if df is None or len(df) == 0:
            return
            
        # Transform data using the model's to_dataframe method
        df_aligned = cls.to_dataframe(df, df['source_file'].iloc[0])
        
        # Insert in chunks
        for i in range(0, len(df_aligned), chunksize):
            chunk = df_aligned.iloc[i:i + chunksize]
            cls.to_database(chunk)
            
        # Clear memory
        del df_aligned
        gc.collect() 