"""
Simplified Extracted File Manager

Replaces the over-engineered metadata tracking system with simple file existence checks.
Maintains backward compatibility while providing a much simpler and more reliable approach.
"""

import os
import boto3
import tempfile
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import psutil
import time
import shutil
from datetime import datetime
from typing import List, Dict, Optional
from functools import wraps
from dotenv import load_dotenv
from botocore.exceptions import ClientError, ReadTimeoutError, ConnectTimeoutError, EndpointConnectionError

from data_models.base import BaseBikeShareRecord

# Load environment variables
load_dotenv()


# Retryable exception types (transient errors that are worth retrying)
RETRYABLE_EXCEPTIONS = (
    ReadTimeoutError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ConnectionError,
)


def retry_on_transient_error(max_attempts=3, initial_delay=1.0, backoff_factor=2.0):
    """
    Decorator that retries a function on transient errors with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)
        backoff_factor: Multiplier for delay between retries (default: 2.0)
    
    Example:
        With max_attempts=3, initial_delay=1, backoff_factor=2:
        - Attempt 1: immediate
        - Attempt 2: wait 1s
        - Attempt 3: wait 2s
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except RETRYABLE_EXCEPTIONS as e:
                    last_exception = e
                    
                    if attempt < max_attempts:
                        # Extract filename from args if available for better logging
                        func_name = func.__name__
                        context = ""
                        if args and len(args) > 1:
                            # For instance methods, args[0] is self, args[1] might be filename
                            if isinstance(args[1], str):
                                context = f" for {args[1]}"
                        
                        print(f"  ⚠ Transient error in {func_name}{context} (attempt {attempt}/{max_attempts}): {type(e).__name__}: {e}")
                        print(f"  ⏳ Retrying in {delay:.1f}s...")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"  ✗ Max retries ({max_attempts}) exceeded for {func.__name__}")
                        raise
                        
                except Exception as e:
                    # Non-retryable exception, raise immediately
                    print(f"  ✗ Non-retryable error in {func.__name__}: {type(e).__name__}: {e}")
                    raise
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


class ExtractedFileManager:
    """
    Simplified file manager that uses file existence checks instead of complex metadata tracking.
    
    This replaces the over-engineered metadata.json approach with simple S3 file existence checks,
    making the system much more reliable and easier to debug.
    """
    
    def __init__(self, s3_bucket: Optional[str] = None, batch_size: int = 5):
        self.s3_bucket = s3_bucket or os.environ.get("S3_BUCKET")
        if not self.s3_bucket:
            raise ValueError("S3_BUCKET environment variable is not set!")
        
        self.s3_client = boto3.client("s3")
        self.batch_size = batch_size  # Process ZIPs in batches to manage memory
        self._parquet_key_cache: Optional[set] = None  # Lazy-loaded cache of existing parquet S3 keys
    
    def _log_memory_usage(self, stage: str):
        """Log current memory usage for debugging."""
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        print(f"Memory usage at {stage}: {memory_mb:.1f} MB")
    
    def _cleanup_memory(self):
        """Force garbage collection and cleanup."""
        gc.collect()
        # Small delay to allow cleanup
        time.sleep(0.1)
    
    def extract_all_zips_simple(self) -> Dict[str, bool]:
        """
        Extract all ZIPs using the existing filetree system.
        Skips extraction if CSVs already exist.
        """
        print("Extracting ZIP files...")
        
        # List all ZIP files
        zip_files = self._list_s3_files("extracted_bike_ride_zips/nyc/")
        zip_files = [f for f in zip_files if f.endswith('.zip')]
        
        results = {}
        total_zips = len(zip_files)
        
        print(f"Found {total_zips} ZIP files to process in batches of {self.batch_size}")
        
        for i in range(0, total_zips, self.batch_size):
            batch = zip_files[i:i + self.batch_size]
            print(f"\nProcessing batch {i//self.batch_size + 1}/{(total_zips + self.batch_size - 1)//self.batch_size}")
            
            for zip_file in batch:
                print(f"Processing {zip_file}...")
                self._log_memory_usage(f"before processing {zip_file}")
                
                try:
                    self._extract_zip_using_filetree(zip_file)
                    results[zip_file] = True
                except Exception as e:
                    print(f"Failed to extract {zip_file}: {e}")
                    results[zip_file] = False
                
                # Cleanup memory after each ZIP
                self._cleanup_memory()
                self._log_memory_usage(f"after processing {zip_file}")
            
            # Extra cleanup between batches
            print(f"Batch {i//self.batch_size + 1} complete. Performing batch cleanup...")
            self._cleanup_memory()
            self._log_memory_usage(f"after batch {i//self.batch_size + 1}")
        
        return results
    
    def convert_all_csvs_simple(self) -> Dict[str, bool]:
        """
        Convert all CSVs to parquet with schema-based organization.
        Skips conversion if parquet already exists.
        """
        print("Converting CSVs to parquet...")
        
        # Get all CSV files
        nyc_csvs = self._list_s3_files("extracted_bike_ride_csvs/nyc/")
        london_csvs = self._list_s3_files("extracted_bike_ride_csvs/london/")
        
        all_csvs = nyc_csvs + london_csvs
        
        results = {}
        for csv_file in all_csvs:
            if not csv_file.endswith('.csv'):
                continue
            
            # Skip MacOSX artifacts (files starting with ._ or in __MACOSX directories)
            csv_filename = csv_file.split('/')[-1]
            if csv_filename.startswith('._') or '__MACOSX' in csv_file:
                print(f"Skipping MacOSX artifact: {csv_filename}")
                continue
            
            # Check if parquet already exists
            if self._parquet_exists_for_csv(csv_file):
                print(f"Skipping {csv_file} - parquet already exists")
                results[csv_file] = True
                continue
            
            # Convert to parquet
            print(f"Converting {csv_file}...")
            self._log_memory_usage(f"before converting {csv_file}")
            
            try:
                self._convert_csv_to_parquet(csv_file)
                results[csv_file] = True
            except Exception as e:
                print(f"Failed to convert {csv_file}: {e}")
                results[csv_file] = False
            
            # Cleanup memory after each CSV
            self._cleanup_memory()
            self._log_memory_usage(f"after converting {csv_file}")
        
        return results
    
    def run_simplified_pipeline(self) -> Dict[str, Dict[str, bool]]:
        """
        Run the complete simplified pipeline.
        Returns results for both extraction and conversion phases.
        """
        print("=== RUNNING SIMPLIFIED PIPELINE ===")
        
        # Phase 1: Extract ZIPs
        extraction_results = self.extract_all_zips_simple()
        
        # Phase 2: Convert CSVs
        conversion_results = self.convert_all_csvs_simple()
        
        return {
            "extraction": extraction_results,
            "conversion": conversion_results
        }
    
    def _extract_zip_using_filetree(self, zip_s3_key: str):
        """Extract ZIP using memory-efficient approach"""
        # Download ZIP to temp file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            # Download ZIP from S3
            self.s3_client.download_file(self.s3_bucket, zip_s3_key, temp_zip_path)
            
            # Process ZIP directly without loading everything into memory
            csv_count = 0
            skipped_count = 0
            
            with zipfile.ZipFile(temp_zip_path, 'r') as zf:
                # List all files in ZIP
                file_list = zf.namelist()
                print(f"DEBUG: Files in ZIP '{os.path.basename(zip_s3_key)}':")
                for filename in file_list:
                    print(f"  - {filename} (dir: {filename.endswith('/')})")
                
                # Process files one by one
                for filename in file_list:
                    # Skip directories and MacOSX artifacts
                    if filename.endswith('/') or filename.startswith('._') or '__MACOSX' in filename:
                        continue
                    
                    # Handle nested ZIPs separately
                    if filename.lower().endswith('.zip'):
                        print(f"DEBUG: Found nested ZIP: {filename}")
                        try:
                            # Extract nested ZIP to temp file using streaming (memory-efficient)
                            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as nested_temp:
                                # Stream the nested ZIP in chunks instead of loading all into memory
                                with zf.open(filename) as nested_source:
                                    shutil.copyfileobj(nested_source, nested_temp, length=8*1024*1024)  # 8MB chunks
                                nested_temp_path = nested_temp.name
                            
                            # Process nested ZIP
                            nested_csv_count, nested_skipped_count = self._process_nested_zip(nested_temp_path, filename)
                            csv_count += nested_csv_count
                            skipped_count += nested_skipped_count
                            
                            # Clean up nested temp file
                            os.unlink(nested_temp_path)
                            
                        except Exception as e:
                            print(f"Failed to extract {filename}: {e}")
                            continue
                    
                    # Handle CSV files directly
                    elif filename.lower().endswith('.csv'):
                        print(f"DEBUG: Found CSV file: {filename}")
                        result = self._upload_csv_from_zip_entry(zf, filename)
                        if result == 'uploaded':
                            csv_count += 1
                        elif result == 'skipped':
                            skipped_count += 1
            
            print(f"  Processed {csv_count + skipped_count} CSV files from {os.path.basename(zip_s3_key)} ({csv_count} new, {skipped_count} skipped)")
            
        finally:
            # Clean up temp file
            os.unlink(temp_zip_path)
            gc.collect()
    
    def _process_nested_zip(self, nested_zip_path: str, nested_zip_name: str):
        """Process a nested ZIP file with memory management"""
        csv_count = 0
        skipped_count = 0
        
        try:
            with zipfile.ZipFile(nested_zip_path, 'r') as nested_zf:
                for filename in nested_zf.namelist():
                    if filename.lower().endswith('.csv'):
                        print(f"DEBUG: Found CSV file: {filename}")
                        result = self._upload_csv_from_zip_entry(nested_zf, filename)
                        if result == 'uploaded':
                            csv_count += 1
                        elif result == 'skipped':
                            skipped_count += 1
            
            print(f"DEBUG: Extracted folder '{nested_zip_name}' contains {csv_count + skipped_count} children")
            
        except Exception as e:
            print(f"Failed to extract {nested_zip_name}: {e}")
        
        return csv_count, skipped_count

    def _upload_csv_from_zip_entry(self, zf: zipfile.ZipFile, filename: str) -> str:
        """Upload a single CSV entry from a ZIP file to S3.

        Args:
            zf: An open ZipFile object containing the CSV
            filename: The filename/path within the ZIP archive

        Returns:
            'uploaded' if the file was uploaded to S3
            'skipped' if the file already exists in S3
            'artifact' if the file is a MacOSX artifact (not uploaded)
        """
        # Skip MacOSX artifacts
        if filename.startswith('._') or '__MACOSX' in filename:
            print(f"  Skipping MacOSX artifact: {filename}")
            return 'artifact'

        csv_filename = os.path.basename(filename)
        csv_s3_key = f"extracted_bike_ride_csvs/nyc/{csv_filename}"

        # Idempotency: check if CSV already exists
        if self._file_exists_in_s3(csv_s3_key):
            print(f"  Skipping {csv_filename} - already exists")
            return 'skipped'

        # Stream CSV content directly to S3
        with zf.open(filename) as csv_stream:
            self.s3_client.upload_fileobj(
                csv_stream,
                self.s3_bucket,
                csv_s3_key
            )
        print(f"  ✓ Uploaded {csv_filename}")
        return 'uploaded'

    @retry_on_transient_error(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def _convert_csv_to_parquet(self, csv_file: str):
        """Convert CSV to parquet with schema detection and proper organization"""
        # Download CSV sample to determine schema
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_csv:
            temp_csv_path = temp_csv.name
        
        try:
            # Download first 1MB to determine schema
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=csv_file,
                Range='bytes=0-1048576'  # 1MB
            )
            
            with open(temp_csv_path, 'wb') as f:
                f.write(response['Body'].read())
            
            # Read sample and determine schema
            sample_df = pd.read_csv(temp_csv_path, nrows=100)
            model = self._find_matching_model(sample_df, csv_file)
            
            if model is None:
                raise ValueError(f"No matching schema for {csv_file}")
            
            # Determine city and schema
            city = "nyc" if "nyc" in csv_file else "london"
            schema = model.__name__.lower()
            
            # Create parquet S3 key
            csv_filename = csv_file.split('/')[-1]
            parquet_filename = csv_filename.replace('.csv', '.parquet')
            parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
            
            # Check if parquet already exists
            if self._file_exists_in_s3(parquet_s3_key):
                print(f"  ✓ Parquet already exists: {parquet_s3_key}")
                return True
            
            # Convert full CSV to parquet
            self._stream_csv_to_parquet(csv_file, parquet_s3_key, model)
            print(f"  ✓ Converted to {parquet_s3_key}")
            return True
            
        finally:
            os.unlink(temp_csv_path)
    
    def _find_matching_model(self, df: pd.DataFrame, csv_file: str):
        """Find matching data model for CSV"""
        # Get all registered models
        models = BaseBikeShareRecord._registry
        
        # Filter by location
        if "nyc" in csv_file:
            models = [m for m in models if m.s3_prefix == "nyc_csv/"]
        elif "london" in csv_file:
            models = [m for m in models if m.s3_prefix == "london_csv/"]
        
        # Test each model
        for model in models:
            if model.validate_schema(df):
                return model
        
        return None
    
    @retry_on_transient_error(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def _stream_csv_to_parquet(self, csv_s3_key: str, parquet_s3_key: str, model):
        """Stream CSV to parquet using chunked reading from a temp file.

        Downloads the CSV from S3 to a local temp file, then reads it in chunks
        using pandas to keep memory usage bounded regardless of file size.
        """
        temp_csv_path = None
        temp_parquet_path = None

        try:
            # Create temp files for CSV download and Parquet output
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_csv:
                temp_csv_path = temp_csv.name
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
                temp_parquet_path = temp_parquet.name

            # Download CSV from S3 to temp file (avoids loading into memory)
            self.s3_client.download_file(self.s3_bucket, csv_s3_key, temp_csv_path)

            # Build dtype hints that cover all known column names across all schemas.
            # Columns not present in a given CSV are silently ignored by pandas.
            dtype_hints = {
                'start station id': str,
                'end station id': str,
                'start_station_id': str,
                'end_station_id': str,
                'start_station_name': str,
                'end_station_name': str,
                'ride_id': str,
                'rideable_type': str,
                'member_casual': str,
                'usertype': str,
                'bikeid': str,
                'Rental Id': str,
                'Bike Id': str,
                'StartStation Id': str,
                'EndStation Id': str,
                'Number': str,
                'Bike number': str,
                'Start station number': str,
                'End station number': str,
            }

            chunk_size = 50_000  # Rows per chunk (increased from 10k for better throughput)
            writer = None

            for df_chunk in pd.read_csv(temp_csv_path, chunksize=chunk_size, dtype=dtype_hints):
                # Apply model transformation (renames columns, casts types, adds source_file)
                df_transformed = model.to_dataframe(df_chunk, csv_s3_key)

                # Convert to PyArrow table -- schema is inferred from the DataFrame
                # which has already been standardized by model.to_dataframe()
                table = pa.Table.from_pandas(df_transformed, preserve_index=False)

                # Initialize writer on first chunk using the inferred schema
                if writer is None:
                    writer = pq.ParquetWriter(temp_parquet_path, table.schema)

                writer.write_table(table)

            if writer:
                writer.close()

            # Upload parquet to S3
            with open(temp_parquet_path, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, parquet_s3_key)

        finally:
            # Clean up both temp files
            if temp_csv_path and os.path.exists(temp_csv_path):
                os.unlink(temp_csv_path)
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                os.unlink(temp_parquet_path)
    

    
    def _build_parquet_cache(self) -> set:
        """Build a set of all existing parquet S3 keys for fast existence checks.

        Uses a single S3 list_objects_v2 call (with pagination) instead of
        individual HEAD requests per file. The cache is stored as a set of
        S3 keys for O(1) lookup.
        """
        parquet_keys = set()
        prefix = "extracted_bike_ride_parquet/"

        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                parquet_keys.add(obj['Key'])

        print(f"  Cached {len(parquet_keys)} existing parquet files")
        return parquet_keys

    def _parquet_exists_for_csv(self, csv_file: str) -> bool:
        """Check if parquet already exists for this CSV using a cached S3 listing.

        On the first call, builds a cache of all parquet S3 keys via a single
        paginated list_objects_v2 call. Subsequent calls use O(1) set lookups.
        """
        # Lazy-initialize the cache on first call
        if self._parquet_key_cache is None:
            self._parquet_key_cache = self._build_parquet_cache()

        csv_filename = csv_file.split('/')[-1]
        parquet_filename = csv_filename.replace('.csv', '.parquet')

        # Check all possible schema locations against the cache
        cities = ["nyc", "london"]
        schemas = ["nyclegacybikesharerecord", "nycmodernbikesharerecord",
                  "londonlegacybikesharerecord", "londonmodernbikesharerecord"]

        for city in cities:
            for schema in schemas:
                parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
                if parquet_s3_key in self._parquet_key_cache:
                    return True

        return False
    
    def _file_exists_in_s3(self, s3_key: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except ClientError:
            return False
    
    def _list_s3_files(self, prefix: str) -> List[str]:
        """List files in S3 with given prefix"""
        response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
        if 'Contents' not in response:
            return []
        return [obj['Key'] for obj in response['Contents']] 