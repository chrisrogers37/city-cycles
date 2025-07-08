"""
Main manager class for extracted files
"""

import os
import json
import boto3
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from io import BytesIO
import zipfile
import logging
from dotenv import load_dotenv
import botocore
import tempfile
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import gc
import psutil

from .models import FileMetadata, FileStatus, FileType, FileLocation, FileSummary
from data_models.base import BaseBikeShareRecord
from .filetree import ZipFile as ZipFileNode, walk_folder

# Load environment variables
load_dotenv()

class ExtractedFileManager:
    """Manages extracted files on S3 with metadata tracking and validation"""
    
    def __init__(self, s3_bucket: Optional[str] = None):
        self.s3_bucket = s3_bucket or os.environ.get("S3_BUCKET")
        if not self.s3_bucket:
            raise ValueError("S3_BUCKET environment variable is not set!")
        
        self.s3_client = boto3.client("s3")
        self.metadata_key = "extracted_file_manager/metadata.json"
        self._metadata_cache: Dict[str, FileMetadata] = {}
        # Check if metadata.json exists; if not, run scan
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=self.metadata_key)
            self._load_metadata()
        except self.s3_client.exceptions.NoSuchKey:
            print("metadata.json not found in S3. Running initial scan...")
            self.scan_s3_files()
        except Exception as e:
            # If any other error, try to load metadata, but print warning
            print(f"Warning: error checking metadata.json: {e}. Attempting to load metadata anyway.")
            self._load_metadata()
    
    def _load_metadata(self):
        """Load metadata from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.metadata_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            self._metadata_cache = {
                filename: FileMetadata.from_dict(meta_data)
                for filename, meta_data in data.items()
            }
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'NoSuchKey' or error_code == '404':
                # No metadata file exists yet
                self._metadata_cache = {}
            else:
                logging.error(f"Failed to load metadata: {e}")
                self._metadata_cache = {}
        except Exception as e:
            logging.error(f"Failed to load metadata: {e}")
            self._metadata_cache = {}
    
    def _save_metadata(self):
        """Save metadata to S3"""
        try:
            data = {
                filename: file_metadata.to_dict()
                for filename, file_metadata in self._metadata_cache.items()
            }
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.metadata_key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            logging.error(f"Failed to save metadata: {e}")
            raise
    
    def scan_s3_files(self) -> List[FileMetadata]:
        """Scan S3 for extracted files and update metadata"""
        print(f"Scanning S3 bucket {self.s3_bucket} for extracted files...")
        
        # Define prefixes to scan with new enum structure
        prefixes = {
            "extracted_bike_ride_zips/nyc/": (FileType.ZIP, FileLocation.NYC),
            "extracted_bike_ride_csvs/nyc/": (FileType.CSV, FileLocation.NYC),
            "extracted_bike_ride_csvs/london/": (FileType.CSV, FileLocation.LONDON),
            "extracted_bike_ride_parquet/nyc/": (FileType.PARQUET, FileLocation.NYC),
            "extracted_bike_ride_parquet/london/": (FileType.PARQUET, FileLocation.LONDON),
        }
        
        new_files = []
        
        for prefix, (file_type, file_location) in prefixes.items():
            print(f"Scanning prefix: {prefix}")
            paginator = self.s3_client.get_paginator("list_objects_v2")
            
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    filename = os.path.basename(key)
                    
                    # Skip the metadata file itself
                    if key == self.metadata_key:
                        continue
                    
                    # Check if we already have metadata for this file
                    if filename in self._metadata_cache:
                        # Update file size if it changed
                        existing_meta = self._metadata_cache[filename]
                        if existing_meta.file_size_bytes != obj["Size"]:
                            existing_meta.file_size_bytes = obj["Size"]
                            print(f"Updated file size for {filename}")
                        continue
                    
                    # Create new metadata
                    file_metadata = FileMetadata(
                        filename=filename,
                        s3_key=key,
                        file_type=file_type,
                        file_location=file_location,
                        file_size_bytes=obj["Size"],
                        extracted_at=obj["LastModified"],
                        status=FileStatus.EXTRACTED
                    )
                    
                    self._metadata_cache[filename] = file_metadata
                    new_files.append(file_metadata)
                    print(f"Found new file: {filename}")
        
        if new_files:
            self._save_metadata()
            print(f"Added {len(new_files)} new files to metadata")
        
        return new_files
    
    def validate_file_schema(self, filename: str) -> bool:
        """Validate a single file's schema and update metadata"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_metadata = self._metadata_cache[filename]
        
        if file_metadata.status != FileStatus.EXTRACTED:
            print(f"File {filename} is not in EXTRACTED status")
            return False
        
        print(f"Validating schema for {filename}...")
        self._log_memory_usage("before validation")
        
        try:
            # Download a sample of the CSV for validation
            csv_sample = self._download_csv_sample(file_metadata.s3_key)
            df_sample = pd.read_csv(csv_sample, nrows=100)  # Read first 100 rows for validation
            
            # Find matching model (use schema override if set)
            model_class = self._find_matching_model(df_sample, file_metadata.file_type, file_metadata.file_location, file_metadata.schema_override)
            
            if model_class:
                # Update metadata
                self.update_file_status(
                    filename=filename,
                    status=FileStatus.VALIDATED,
                    validated_at=datetime.now(),
                    clear_errors=True
                )
                file_metadata.metadata["schema"] = model_class.__name__
                self._save_metadata()
                print(f"✓ Validation successful - matched schema: {model_class.__name__}")
                return True
            else:
                # Update metadata with failure
                self.update_file_status(
                    filename=filename,
                    status=FileStatus.FAILED,
                    clear_errors=True
                )
                file_metadata.validation_errors = ["No matching data model found"]
                self._save_metadata()
                print(f"✗ Validation failed: No matching data model found for {filename}")
                return False
        except Exception as e:
            # Update metadata with error
            self.update_file_status(
                filename=filename,
                status=FileStatus.FAILED,
                clear_errors=True
            )
            file_metadata.validation_errors = [str(e)]
            self._save_metadata()
            print(f"✗ Validation error for {filename}: {e}")
            return False
        finally:
            self._cleanup_memory()
            self._log_memory_usage("after cleanup")
    
    def _download_csv_sample(self, s3_key: str, sample_size: int = 100) -> BytesIO:
        """Download a sample of a CSV file from S3 using range request"""
        # Request first 5MB (much safer than smaller ranges)
        response = self.s3_client.get_object(
            Bucket=self.s3_bucket, 
            Key=s3_key,
            Range='bytes=0-5242880'  # 5MB
        )
        
        csv_buffer = BytesIO()
        csv_buffer.write(response['Body'].read())
        csv_buffer.seek(0)
        return csv_buffer
    
    def _find_matching_model(self, df_sample: pd.DataFrame, file_type: FileType, file_location: FileLocation, schema_override: Optional[str] = None) -> Optional[type]:
        """Find the appropriate data model for a file"""
        import os
        debug_mode = os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG') == '1'
        
        # If schema override is provided, use it directly
        if schema_override:
            if debug_mode:
                print(f"DEBUG: Using schema override: {schema_override}")
            
            # Find the model by name
            models = BaseBikeShareRecord._registry
            for model in models:
                if model.__name__ == schema_override:
                    if debug_mode:
                        print(f"DEBUG: ✓ Found override model: {model.__name__}")
                    return model
            
            if debug_mode:
                print(f"DEBUG: ✗ Override model '{schema_override}' not found in registry")
            return None
        
        # Get all registered models
        models = BaseBikeShareRecord._registry
        
        # Filter models based on file type and location
        if file_type == FileType.CSV:
            # Filter by location using s3_prefix
            if file_location == FileLocation.NYC:
                models = [m for m in models if m.s3_prefix == "nyc_csv/"]
            elif file_location == FileLocation.LONDON:
                models = [m for m in models if m.s3_prefix == "london_csv/"]
            else:
                if debug_mode:
                    print(f"DEBUG: Unknown location: {file_location}")
                return None
        else:
            if debug_mode:
                print(f"DEBUG: No models found for file type: {file_type}")
            return None
        
        if debug_mode:
            print(f"DEBUG: Found {len(models)} models to test for {file_type}")
            print(f"DEBUG: Available columns in file: {list(df_sample.columns)}")
        
        # Try each model's schema validation
        for model in models:
            try:
                if debug_mode:
                    print(f"DEBUG: Testing model: {model.__name__}")
                if model.validate_schema(df_sample):
                    if debug_mode:
                        print(f"DEBUG: ✓ Matched model: {model.__name__}")
                    return model
                else:
                    # Get detailed validation info
                    if hasattr(model, 'validate_schema'):
                        # Try to get missing columns info
                        try:
                            required_columns = getattr(model, '_required_columns', None)
                            if required_columns and debug_mode:
                                missing_columns = [col for col in required_columns if col not in df_sample.columns]
                                print(f"DEBUG: ✗ Model {model.__name__} failed - missing columns: {missing_columns}")
                            elif debug_mode:
                                print(f"DEBUG: ✗ Model {model.__name__} failed validation")
                        except Exception as e:
                            if debug_mode:
                                print(f"DEBUG: ✗ Model {model.__name__} validation error: {e}")
                    elif debug_mode:
                        print(f"DEBUG: ✗ Model {model.__name__} has no validate_schema method")
            except Exception as e:
                if debug_mode:
                    print(f"DEBUG: ✗ Model {model.__name__} exception: {e}")
                continue
        
        if debug_mode:
            print(f"DEBUG: ✗ No matching model found for {file_type}")
            print(f"DEBUG: File columns: {list(df_sample.columns)}")
            print(f"DEBUG: Expected models for {file_type}: {[m.__name__ for m in models]}")
        return None
    
    def validate_all_files(self, file_type: Optional[FileType] = None) -> Dict[str, bool]:
        """Validate all files or files of a specific type"""
        results = {}
        
        files_to_validate = [
            filename for filename, meta in self._metadata_cache.items()
            if meta.status == FileStatus.EXTRACTED and 
               (file_type is None or meta.file_type == file_type)
        ]
        
        print(f"Validating {len(files_to_validate)} files...")
        
        for filename in files_to_validate:
            results[filename] = self.validate_file_schema(filename)
        
        return results
    
    def get_file_summary(self) -> FileSummary:
        """Get summary statistics for all files"""
        summary = FileSummary()
        
        for file_metadata in self._metadata_cache.values():
            summary.update_from_file(file_metadata)
        
        return summary
    
    def list_files(self, 
                   status: Optional[FileStatus] = None,
                   file_type: Optional[FileType] = None,
                   location: Optional[FileLocation] = None) -> List[FileMetadata]:
        """List files with optional filtering"""
        files = self._filter_files(
            file_type=file_type,
            location=location,
            status=status
        )
        
        # Sort by extracted_at (newest first) - handle timezone-aware datetimes
        def get_sort_key(f):
            if f.extracted_at is None:
                return datetime.min
            # Convert to timezone-naive if it's timezone-aware
            dt = f.extracted_at
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        
        files.sort(key=get_sort_key, reverse=True)
        
        return files
    
    def get_file_metadata(self, filename: str) -> Optional[FileMetadata]:
        """Get metadata for a specific file"""
        return self._metadata_cache.get(filename)
    

    
    def reprocess_file(self, filename: str) -> bool:
        """Reset a file's status to EXTRACTED for reprocessing"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_metadata = self._metadata_cache[filename]
        self.update_file_status(
            filename=filename,
            status=FileStatus.EXTRACTED,
            validated_at=None,
            processed_at=None,
            clear_errors=True
        )
        
        print(f"Reset file status for reprocessing: {filename}")
        return True
    

    

    
    def print_summary(self):
        """Print a formatted summary of all files"""
        summary = self.get_file_summary()
        
        print("\n" + "="*60)
        print("EXTRACTED FILE MANAGER SUMMARY")
        print("="*60)
        print(f"Total files: {summary.total_files}")
        print(f"Total size: {summary.total_size_bytes / (1024**3):.2f} GB")
        print()
        print("By Status:")
        print(f"  Extracted: {summary.extracted_files}")
        print(f"  CSV Converted: {summary.csv_converted_files}")
        print(f"  Validated: {summary.validated_files}")
        print(f"  Parquet Converted: {summary.parquet_converted_files}")
        print(f"  Processed: {summary.processed_files}")
        print(f"  Failed: {summary.failed_files}")
        print(f"  Deleted: {summary.deleted_files}")
        print()
        print("By File Type:")
        for file_type, count in summary.by_file_type.items():
            print(f"  {file_type.value}: {count}")
        print("="*60)
    
    def convert_zip_to_csv(self, filename: str) -> bool:
        """Extract all CSVs from a ZIP (including nested ZIPs) using temp files, upload to S3, and track metadata. Always extracts, regardless of current status. Minimizes memory usage. Skips MacOSX artifacts."""
        import shutil
        def extract_zip_to_csvs(zip_path, parent_zip=None):
            extracted_csvs = []
            city = "nyc"  # This method is only for NYC ZIPs
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    for info in zf.infolist():
                        basename = os.path.basename(info.filename)
                        # Skip MacOSX artifacts: __MACOSX dirs and dot-underscore files
                        if ('__MACOSX' in info.filename) or basename.startswith('._'):
                            print(f"  Skipping MacOSX artifact: {info.filename}")
                            continue
                        if info.filename.lower().endswith('.csv'):
                            # Extract CSV to temp file
                            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_csv:
                                with zf.open(info) as source:
                                    shutil.copyfileobj(source, temp_csv)
                                temp_csv_path = temp_csv.name
                            # Upload to S3, then delete temp file
                            new_csv_filename = basename
                            csv_s3_key = f"extracted_bike_ride_csvs/{city}/{new_csv_filename}"
                            print(f"  Uploading: {new_csv_filename}")
                            with open(temp_csv_path, 'rb') as f:
                                self.s3_client.upload_fileobj(f, self.s3_bucket, csv_s3_key)
                            file_size = os.path.getsize(temp_csv_path)
                            os.unlink(temp_csv_path)
                            # Create/update metadata for the new CSV file
                            csv_file_metadata = FileMetadata(
                                filename=new_csv_filename,
                                s3_key=csv_s3_key,
                                file_type=FileType.CSV,
                                file_location=FileLocation.NYC,
                                file_size_bytes=file_size,
                                extracted_at=datetime.now(),
                                status=FileStatus.EXTRACTED,
                                metadata={"source_zip": filename if not parent_zip else parent_zip}
                            )
                            self._metadata_cache[new_csv_filename] = csv_file_metadata
                            extracted_csvs.append(new_csv_filename)
                            print(f"  ✓ Uploaded {new_csv_filename} to S3")
                        elif info.filename.lower().endswith('.zip'):
                            # Extract nested ZIP to temp file
                            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                                with zf.open(info) as source:
                                    shutil.copyfileobj(source, temp_zip)
                                temp_zip_path = temp_zip.name
                            # Recursively process nested ZIP, then delete temp file
                            print(f"  Recursively extracting nested ZIP: {info.filename}")
                            extracted_csvs.extend(extract_zip_to_csvs(temp_zip_path, parent_zip=info.filename))
                            os.unlink(temp_zip_path)
                        else:
                            # Skip directories and other files
                            continue
            except Exception as e:
                print(f"Error extracting {zip_path}: {e}")
            return extracted_csvs

        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        file_metadata = self._metadata_cache[filename]
        if file_metadata.file_type != FileType.ZIP:
            print(f"File {filename} is not a ZIP file")
            return False
        try:
            print(f"Extracting all CSVs from ZIP (temp file approach, recursive): {filename}")
            # Download ZIP file to temp file
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                temp_zip_path = temp_zip.name
            self.s3_client.download_file(self.s3_bucket, file_metadata.s3_key, temp_zip_path)
            extracted_csvs = extract_zip_to_csvs(temp_zip_path)
            os.unlink(temp_zip_path)
            # Always update ZIP file status and metadata
            self.update_file_status(
                filename=filename,
                status=FileStatus.CSV_CONVERTED,
                csv_converted_at=datetime.now()
            )
            file_metadata.metadata["extracted_csvs"] = extracted_csvs
            file_metadata.metadata["csv_count"] = len(extracted_csvs)
            self._save_metadata()
            print(f"Successfully extracted and uploaded {len(extracted_csvs)} CSV files from {filename}")
            return True
        except Exception as e:
            error_msg = f"Failed to extract ZIP with tempfiles: {str(e)}"
            file_metadata.processing_errors.append(error_msg)
            self.update_file_status(filename=filename, status=FileStatus.FAILED)
            self._save_metadata()
            print(f"Conversion failed: {error_msg}")
            return False
    
    def _get_column_types_for_model(self, model_class) -> Dict[str, str]:
        """Get explicit column types for pyarrow CSV reading based on the data model."""
        import pyarrow as pa
        
        # Define column type mappings for each model
        type_mappings = {
            'LondonModernBikeShareRecord': {
                'Number': pa.string(),
                'Bike number': pa.string(),  # Force string to handle alphanumeric values
                'Bike model': pa.string(),
                'Start date': pa.string(),  # Handle as string, convert in to_dataframe
                'End date': pa.string(),    # Handle as string, convert in to_dataframe
                'Total duration': pa.string(),
                'Total duration (ms)': pa.int64(),
                'Start station number': pa.string(),  # Force string
                'Start station': pa.string(),
                'End station number': pa.string(),    # Force string
                'End station': pa.string(),
            },
            'NYCModernBikeShareRecord': {
                'ride_id': pa.string(),
                'rideable_type': pa.string(),
                'started_at': pa.string(),
                'ended_at': pa.string(),
                'start_station_id': pa.string(),  # Force string to handle alphanumeric values
                'start_station_name': pa.string(),
                'end_station_id': pa.string(),    # Force string to handle alphanumeric values
                'end_station_name': pa.string(),
                'start_lat': pa.float64(),
                'start_lng': pa.float64(),
                'end_lat': pa.float64(),
                'end_lng': pa.float64(),
                'member_casual': pa.string(),
            },
            'NYCLegacyBikeShareRecord': {
                'tripduration': pa.int64(),
                'bikeid': pa.string(),
                'starttime': pa.string(),
                'stoptime': pa.string(),
                'start station id': pa.string(),
                'start station name': pa.string(),
                'start station latitude': pa.float64(),
                'start station longitude': pa.float64(),
                'end station id': pa.string(),
                'end station name': pa.string(),
                'end station latitude': pa.float64(),
                'end station longitude': pa.float64(),
                'usertype': pa.string(),
                'birth year': pa.int64(),
                'gender': pa.int64(),
            },
            'LondonLegacyBikeShareRecord': {
                'Rental Id': pa.string(),
                'Bike Id': pa.string(),
                'Start Date': pa.string(),  # Handle as string, convert in to_dataframe
                'End Date': pa.string(),    # Handle as string, convert in to_dataframe
                'StartStation Id': pa.string(),
                'StartStation Name': pa.string(),
                'EndStation Id': pa.string(),
                'EndStation Name': pa.string(),
                'Duration': pa.int64(),
            }
        }
        
        model_name = model_class.__name__
        return type_mappings.get(model_name, {})
    
    def convert_csv_to_parquet(self, filename: str) -> bool:
        """Convert a CSV file to Parquet with schema-based organization using pyarrow streaming."""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_metadata = self._metadata_cache[filename]
        
        if file_metadata.file_type != FileType.CSV:
            print(f"File {filename} is not a CSV file")
            return False
        
        if file_metadata.status != FileStatus.VALIDATED:
            print(f"File {filename} is not validated")
            return False
        
        temp_csv_path = None
        temp_parquet_path = None
        writer = None
        
        try:
            print(f"Converting CSV to Parquet (streaming): {filename}")
            self._log_memory_usage("before conversion")
            
            # Download CSV to a temp file
            temp_csv = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
            temp_csv_path = temp_csv.name
            temp_csv.close()
            self.s3_client.download_file(self.s3_bucket, file_metadata.s3_key, temp_csv_path)
            
            # Read a sample to determine schema/model
            sample_df = pd.read_csv(temp_csv_path, nrows=100)
            model = self._find_matching_model(sample_df, file_metadata.file_type, file_metadata.file_location, file_metadata.schema_override)
            
            # Clean up sample DataFrame immediately
            del sample_df
            self._cleanup_memory()
            
            if model is None:
                raise ValueError(f"No matching data model found for {filename}")
            
            # Determine city and schema
            city = "nyc" if file_metadata.file_location == FileLocation.NYC else "london"
            schema = model.__name__.lower()
            
            # Create Parquet filename and S3 key
            base_name = os.path.splitext(filename)[0]
            parquet_filename = f"{base_name}.parquet"
            parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
            
            # Prepare temp Parquet file
            temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
            temp_parquet_path = temp_parquet.name
            temp_parquet.close()
            
            # Stream CSV to Parquet using pyarrow with explicit column types
            read_options = pv.ReadOptions(block_size=5_000_000)  # 5MB blocks (reduced from 10MB)
            
            # Get explicit column types for this model to prevent type inference issues
            column_types = self._get_column_types_for_model(model)
            convert_options = pv.ConvertOptions(
                column_types=column_types if column_types else None
            )
            
            with open(temp_csv_path, 'rb') as f:
                reader = pv.open_csv(f, read_options=read_options, convert_options=convert_options)
                
                for batch_num, batch in enumerate(reader):
                    # Convert to pandas DataFrame for model transformation
                    df_chunk = batch.to_pandas()
                    
                    # If using schema override, add missing columns with NULL values
                    if file_metadata.schema_override:
                        df_chunk = self._add_missing_columns_for_model(df_chunk, model, chunk_num=batch_num+1)
                    
                    df_transformed = model.to_dataframe(df_chunk, filename)
                    
                    # Convert back to pyarrow Table
                    table_transformed = pa.Table.from_pandas(df_transformed)
                    
                    if writer is None:
                        writer = pq.ParquetWriter(temp_parquet_path, table_transformed.schema)
                    
                    writer.write_table(table_transformed)
                    
                    # Clean up chunk data immediately
                    del df_chunk
                    del df_transformed
                    del table_transformed
                    
                    # Force cleanup every 10 batches
                    if batch_num % 10 == 0:
                        self._cleanup_memory()
                
                if writer:
                    writer.close()
                    writer = None
            
            # Upload Parquet to S3
            with open(temp_parquet_path, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, parquet_s3_key)
            
            file_size = os.path.getsize(temp_parquet_path)
            
            # Create metadata for the new Parquet file
            parquet_file_metadata = FileMetadata(
                filename=parquet_filename,
                s3_key=parquet_s3_key,
                file_type=FileType.PARQUET,
                file_location=FileLocation.NYC if city == "nyc" else FileLocation.LONDON,
                file_size_bytes=file_size,
                extracted_at=datetime.now(),
                status=FileStatus.EXTRACTED,
                metadata={
                    "source_csv": filename,
                    "schema": schema,
                    "model": model.__name__
                }
            )
            self._metadata_cache[parquet_filename] = parquet_file_metadata
            
            # Update CSV file status
            self.update_file_status(
                filename=filename,
                status=FileStatus.PARQUET_CONVERTED,
                parquet_converted_at=datetime.now()
            )
            file_metadata.metadata["converted_parquet"] = parquet_filename
            self._save_metadata()
            
            print(f"Successfully converted {filename} to {parquet_filename} (schema: {schema})")
            self._log_memory_usage("after conversion")
            return True
            
        except Exception as e:
            error_msg = f"Failed to convert CSV to Parquet: {str(e)}"
            file_metadata.processing_errors.append(error_msg)
            self.update_file_status(filename=filename, status=FileStatus.FAILED)
            self._save_metadata()
            print(f"Conversion failed: {error_msg}")
            return False
            
        finally:
            # Clean up temp files and resources
            if temp_csv_path and os.path.exists(temp_csv_path):
                try:
                    os.unlink(temp_csv_path)
                except Exception as e:
                    print(f"Warning: Failed to delete temp CSV file: {e}")
            
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.unlink(temp_parquet_path)
                except Exception as e:
                    print(f"Warning: Failed to delete temp Parquet file: {e}")
            
            if writer:
                try:
                    writer.close()
                except Exception as e:
                    print(f"Warning: Failed to close Parquet writer: {e}")
            
            # Final memory cleanup
            self._cleanup_memory()
    
    
    def wipe_files(self, 
                   file_type: Optional[FileType] = None,
                   location: Optional[FileLocation] = None,
                   filenames: Optional[List[str]] = None,
                   delete_from_s3: bool = True) -> int:
        """
        Unified wipe method to delete files from S3 and/or metadata.
        
        Args:
            file_type: Filter by file type
            location: Filter by location
            filenames: Specific files to wipe (overrides other filters)
            delete_from_s3: Whether to delete from S3 (default: True)
            
        Returns:
            Number of files wiped
        """
        # Get files to wipe
        files_to_wipe = self._filter_files(
            file_type=file_type,
            location=location,
            filenames=filenames
        )
        
        if not files_to_wipe:
            print("No files found matching wipe criteria")
            return 0
        
        print(f"Found {len(files_to_wipe)} files to wipe")
        
        wiped_count = 0
        
        for file_meta in files_to_wipe:
            try:
                if delete_from_s3:
                    # Delete from S3
                    self.s3_client.delete_object(Bucket=self.s3_bucket, Key=file_meta.s3_key)
                    print(f"Deleted from S3: {file_meta.s3_key}")
                
                # Remove from metadata
                del self._metadata_cache[file_meta.filename]
                wiped_count += 1
                
            except Exception as e:
                print(f"Failed to wipe file {file_meta.filename}: {e}")
        
        # Special handling for Parquet files: reset status of CSV files that generated them
        if file_type == FileType.PARQUET:
            reset_count = 0
            for filename, meta in self._metadata_cache.items():
                # Find CSV files that have parquet_converted status
                if (meta.file_type == FileType.CSV and 
                    meta.status == FileStatus.PARQUET_CONVERTED):
                    
                    # Apply location filter if specified
                    if location and meta.file_location != location:
                        continue
                    
                    # Reset to extracted status so they can be reconverted
                    meta.status = FileStatus.EXTRACTED
                    meta.parquet_converted_at = None
                    # Remove Parquet-related metadata
                    if "converted_parquet" in meta.metadata:
                        del meta.metadata["converted_parquet"]
                    reset_count += 1
                    print(f"Reset CSV file status to EXTRACTED: {meta.filename}")
            
            if reset_count > 0:
                print(f"Reset {reset_count} CSV files to EXTRACTED status for reconversion")
        
        # Save updated metadata
        if wiped_count > 0:
            self._save_metadata()
        
        print(f"Wiped {wiped_count} files")
        return wiped_count
    
    def _log_memory_usage(self, operation: str = ""):
        """Log current memory usage for debugging"""
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        print(f"Memory usage {operation}: {memory_mb:.1f} MB")
    
    def _cleanup_memory(self):
        """Force garbage collection and cleanup"""
        gc.collect()
        self._log_memory_usage("after cleanup")
    
    def list_failed_files(self, city: str = None, file_type: FileType = None) -> List[Dict[str, Any]]:
        """List failed files with optional filtering. Returns list of dicts for CLI display."""
        failed_files = []
        
        for filename, meta in self._metadata_cache.items():
            if meta.status != FileStatus.FAILED:
                continue
            
            # Apply city filter
            if city and city not in meta.s3_key:
                continue
            
            # Apply file type filter
            if file_type and meta.file_type != file_type:
                continue
            
            # Convert to dict format for CLI
            file_info = {
                'key': meta.s3_key,
                'filename': meta.filename,
                'city': 'nyc' if 'nyc' in meta.s3_key else 'london',
                'file_type': meta.file_type.value,
                'status': meta.status.value,
                'size_mb': meta.file_size_bytes / (1024 * 1024) if meta.file_size_bytes else 0,
                'last_modified': meta.extracted_at.isoformat() if meta.extracted_at else None,
                'error': meta.processing_errors[-1] if meta.processing_errors else 'Unknown error'
            }
            failed_files.append(file_info)
        
        # Sort by last modified (newest first)
        failed_files.sort(key=lambda x: x['last_modified'] or '', reverse=True)
        
        return failed_files
    
    def reset_failed_files(self, city: str = None, file_type: FileType = None) -> int:
        """Reset failed files to 'extracted' status. Returns number of files reset."""
        reset_count = 0
        
        for filename, meta in self._metadata_cache.items():
            if meta.status != FileStatus.FAILED:
                continue
            
            # Apply city filter
            if city and city not in meta.s3_key:
                continue
            
            # Apply file type filter
            if file_type and meta.file_type != file_type:
                continue
            
            # Reset to extracted status
            meta.status = FileStatus.EXTRACTED
            meta.processing_errors = []  # Clear errors
            reset_count += 1
        
        # Save updated metadata
        if reset_count > 0:
            self._save_metadata()
        
        return reset_count
    

    
    def set_schema_override(self, filename: str, schema_name: str) -> bool:
        """Set a manual schema override for a file"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        # Validate that the schema exists
        models = BaseBikeShareRecord._registry
        schema_exists = any(model.__name__ == schema_name for model in models)
        
        if not schema_exists:
            print(f"Schema '{schema_name}' not found. Available schemas: {[m.__name__ for m in models]}")
            return False
        
        file_metadata = self._metadata_cache[filename]
        file_metadata.schema_override = schema_name
        
        # Reset status to allow reprocessing
        self.update_file_status(
            filename=filename,
            status=FileStatus.EXTRACTED,
            validated_at=None,
            clear_errors=True
        )
        print(f"Set schema override for {filename}: {schema_name}")
        print(f"File status reset to EXTRACTED for reprocessing")
        return True
    
    def clear_schema_override(self, filename: str) -> bool:
        """Clear a manual schema override for a file"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_metadata = self._metadata_cache[filename]
        if file_metadata.schema_override is None:
            print(f"No schema override set for {filename}")
            return False
        
        old_override = file_metadata.schema_override
        file_metadata.schema_override = None
        
        # Reset status to allow reprocessing
        self.update_file_status(
            filename=filename,
            status=FileStatus.EXTRACTED,
            validated_at=None,
            clear_errors=True
        )
        print(f"Cleared schema override for {filename}: {old_override}")
        print(f"File status reset to EXTRACTED for reprocessing")
        return True
    
    def _add_missing_columns_for_model(self, df_chunk: pd.DataFrame, model_class, chunk_num: int = None) -> pd.DataFrame:
        """Add missing columns to a DataFrame for a model with schema override, with chunk info"""
        required_columns = getattr(model_class, '_required_columns', [])
        if not required_columns:
            print(f"WARNING: No _required_columns found for model {model_class.__name__}")
            return df_chunk
        df_chunk = df_chunk.copy()
        for col in required_columns:
            if col not in df_chunk.columns:
                if any(keyword in col.lower() for keyword in ['id', 'name', 'date', 'time', 'duration', 'model', 'type']):
                    default_val = ""
                elif any(keyword in col.lower() for keyword in ['lat', 'lng', 'longitude', 'latitude']):
                    default_val = 0.0
                elif any(keyword in col.lower() for keyword in ['year', 'gender']):
                    default_val = 0
                else:
                    default_val = ""
                chunk_info = f" [chunk {chunk_num}]" if chunk_num is not None else ""
                print(f"WARNING{chunk_info}: Adding missing column '{col}' with default value: {default_val}")
                df_chunk[col] = default_val
        return df_chunk

    def _filter_files(self, 
                     file_type: Optional[FileType] = None,
                     location: Optional[FileLocation] = None,
                     status: Optional[FileStatus] = None,
                     filenames: Optional[List[str]] = None) -> List[FileMetadata]:
        """Centralized file filtering logic"""
        filtered_files = []
        
        for filename, meta in self._metadata_cache.items():
            # Apply filename filter (highest priority)
            if filenames and filename not in filenames:
                continue
                
            # Apply file type filter
            if file_type and meta.file_type != file_type:
                continue
                
            # Apply location filter
            if location and meta.file_location != location:
                continue
                
            # Apply status filter
            if status and meta.status != status:
                continue
                
            filtered_files.append(meta)
        
        return filtered_files

    def process_files(self, 
                     file_type: Optional[FileType] = None,
                     location: Optional[FileLocation] = None, 
                     status: Optional[FileStatus] = None,
                     filenames: Optional[List[str]] = None,
                     operation: str = "convert") -> Dict[str, bool]:
        """
        Unified file processing method.
        
        Args:
            file_type: Filter by file type (ZIP, CSV, PARQUET)
            location: Filter by location (NYC, LONDON) 
            status: Filter by status (EXTRACTED, VALIDATED, etc.)
            filenames: Specific files to process (overrides other filters)
            operation: "extract", "validate", "convert", "wipe"
            
        Returns:
            Dict mapping filename to success status
        """
        # Get files to process
        files_to_process = self._filter_files(
            file_type=file_type,
            location=location, 
            status=status,
            filenames=filenames
        )
        
        if not files_to_process:
            print(f"No files found matching criteria for {operation} operation")
            return {}
        
        print(f"Processing {len(files_to_process)} files with {operation} operation...")
        
        results = {}
        
        for i, file_meta in enumerate(files_to_process, 1):
            filename = file_meta.filename
            print(f"Processing {i}/{len(files_to_process)}: {filename}")
            
            try:
                if operation == "extract":
                    if file_meta.file_type == FileType.ZIP:
                        results[filename] = self.convert_zip_to_csv(filename)
                    else:
                        print(f"Skipping {filename}: not a ZIP file")
                        results[filename] = False
                        
                elif operation == "validate":
                    if file_meta.file_type in [FileType.CSV]:
                        results[filename] = self.validate_file_schema(filename)
                    else:
                        print(f"Skipping {filename}: not a CSV file")
                        results[filename] = False
                        
                elif operation == "convert":
                    if file_meta.file_type == FileType.CSV:
                        # Auto-validate if not already validated
                        if file_meta.status != FileStatus.VALIDATED:
                            print(f"Auto-validating {filename} before conversion...")
                            validation_success = self.validate_file_schema(filename)
                            if not validation_success:
                                print(f"Skipping {filename}: validation failed")
                                results[filename] = False
                                continue
                        results[filename] = self.convert_csv_to_parquet(filename)
                    else:
                        print(f"Skipping {filename}: not a CSV file")
                        results[filename] = False
                        
                elif operation == "wipe":
                    count = self.wipe_files(filenames=[filename])
                    results[filename] = count == 1
                    
                else:
                    print(f"Unknown operation: {operation}")
                    results[filename] = False
                    
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                results[filename] = False
                
            # Memory cleanup between files
            self._cleanup_memory()
        
        # Print summary
        success_count = sum(1 for success in results.values() if success)
        print(f"Completed {operation} operation: {success_count}/{len(results)} files successful")
        
        return results
    
    def extract_zips(self, location: Optional[FileLocation] = None, filenames: Optional[List[str]] = None) -> Dict[str, bool]:
        """Extract ZIP files to CSV"""
        return self.process_files(
            file_type=FileType.ZIP, 
            location=location, 
            filenames=filenames, 
            operation="extract"
        )

    def convert_csvs(self, location: Optional[FileLocation] = None, filenames: Optional[List[str]] = None) -> Dict[str, bool]:
        """Convert CSV files to Parquet"""
        return self.process_files(
            file_type=FileType.CSV, 
            location=location, 
            filenames=filenames, 
            operation="convert"
        )

    def validate_csvs(self, location: Optional[FileLocation] = None, filenames: Optional[List[str]] = None) -> Dict[str, bool]:
        """Validate CSV files"""
        return self.process_files(
            file_type=FileType.CSV, 
            location=location, 
            filenames=filenames, 
            operation="validate"
        )

    def update_file_status(self, 
                          filename: str, 
                          status: FileStatus, 
                          clear_errors: bool = False,
                          **kwargs) -> bool:
        """
        Update file status and related metadata.
        
        Args:
            filename: Name of the file to update
            status: New status to set
            clear_errors: Whether to clear validation/processing errors
            **kwargs: Additional metadata to update (e.g., validated_at=datetime.now())
        """
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        
        # Update status
        file_meta.status = status
        
        # Clear errors if requested
        if clear_errors:
            file_meta.validation_errors = []
            file_meta.processing_errors = []
        
        # Update additional metadata
        for key, value in kwargs.items():
            if hasattr(file_meta, key):
                setattr(file_meta, key, value)
            else:
                print(f"Warning: Unknown attribute '{key}' for FileMetadata")
        
        self._save_metadata()
        print(f"Updated {filename} status to {status.value}")
        return True 