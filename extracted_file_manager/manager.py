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
import time

from .models import FileMetadata, FileStatus, FileType, FileSummary
from data_models.base import BaseBikeShareRecord

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
                filename: file_meta.to_dict()
                for filename, file_meta in self._metadata_cache.items()
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
        
        # Define prefixes to scan
        prefixes = {
            "extracted_bike_ride_zips/nyc/": FileType.NYC_ZIP,
            "extracted_bike_ride_csvs/nyc/": FileType.NYC_CSV,
            "extracted_bike_ride_csvs/london/": FileType.LONDON_CSV,
            "extracted_bike_ride_parquet/nyc/": FileType.NYC_PARQUET,
            "extracted_bike_ride_parquet/london/": FileType.LONDON_PARQUET,
        }
        
        new_files = []
        
        for prefix, file_type in prefixes.items():
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
                    file_meta = FileMetadata(
                        filename=filename,
                        s3_key=key,
                        file_type=file_type,
                        file_size_bytes=obj["Size"],
                        extracted_at=obj["LastModified"],
                        status=FileStatus.EXTRACTED
                    )
                    
                    self._metadata_cache[filename] = file_meta
                    new_files.append(file_meta)
                    print(f"Found new file: {filename}")
        
        if new_files:
            self._save_metadata()
            print(f"Added {len(new_files)} new files to metadata")
        
        return new_files
    
    def validate_file_schema(self, filename: str) -> bool:
        """Validate a file's schema using the appropriate data model"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        
        # Skip validation for ZIP files (they need to be extracted first)
        if file_meta.file_type == FileType.NYC_ZIP:
            print(f"Skipping schema validation for ZIP file: {filename}")
            return True
        
        try:
            print(f"Validating schema for {filename}...")
            
            # Download a sample of the file for validation
            csv_buffer = self._download_csv_sample(file_meta.s3_key)
            df_sample = pd.read_csv(csv_buffer, nrows=100)
            
            # Determine the appropriate data model based on file type and schema
            model = self._find_matching_model(df_sample, file_meta.file_type)
            
            if model is None:
                error_msg = f"No matching data model found for {filename}"
                file_meta.validation_errors.append(error_msg)
                file_meta.status = FileStatus.FAILED
                self._save_metadata()
                print(f"Validation failed: {error_msg}")
                return False
            
            # Validate schema
            if not model.validate_schema(df_sample):
                error_msg = f"Schema validation failed for {filename}"
                file_meta.validation_errors.append(error_msg)
                file_meta.status = FileStatus.FAILED
                self._save_metadata()
                print(f"Validation failed: {error_msg}")
                return False
            
            # Update metadata
            file_meta.status = FileStatus.VALIDATED
            file_meta.validated_at = datetime.now()
            file_meta.validation_errors = []  # Clear any previous errors
            file_meta.metadata["matched_model"] = model.__name__
            self._save_metadata()
            
            print(f"Validation successful for {filename} (matched {model.__name__})")
            return True
            
        except Exception as e:
            error_msg = f"Validation error for {filename}: {str(e)}"
            file_meta.validation_errors.append(error_msg)
            file_meta.status = FileStatus.FAILED
            self._save_metadata()
            print(f"Validation failed: {error_msg}")
            return False
    
    def _download_csv_sample(self, s3_key: str, sample_size: int = 100) -> BytesIO:
        """Download a sample of a CSV file from S3"""
        csv_buffer = BytesIO()
        self.s3_client.download_fileobj(self.s3_bucket, s3_key, csv_buffer)
        csv_buffer.seek(0)
        return csv_buffer
    
    def _find_matching_model(self, df_sample: pd.DataFrame, file_type: FileType) -> Optional[type]:
        """Find the appropriate data model for a file"""
        # Get all registered models
        models = BaseBikeShareRecord._registry
        
        # Filter models based on file type
        if file_type == FileType.NYC_CSV:
            models = [m for m in models if m.s3_prefix == "nyc_csv/"]
        elif file_type == FileType.LONDON_CSV:
            models = [m for m in models if m.s3_prefix == "london_csv/"]
        else:
            return None
        
        # Try each model's schema validation
        for model in models:
            try:
                if model.validate_schema(df_sample):
                    return model
            except Exception:
                continue
        
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
        
        for file_meta in self._metadata_cache.values():
            summary.update_from_file(file_meta)
        
        return summary
    
    def list_files(self, 
                   status: Optional[FileStatus] = None,
                   file_type: Optional[FileType] = None,
                   limit: Optional[int] = None) -> List[FileMetadata]:
        """List files with optional filtering"""
        files = list(self._metadata_cache.values())
        
        if status:
            files = [f for f in files if f.status == status]
        
        if file_type:
            files = [f for f in files if f.file_type == file_type]
        
        # Sort by extracted_at (newest first)
        files.sort(key=lambda f: f.extracted_at or datetime.min, reverse=True)
        
        if limit:
            files = files[:limit]
        
        return files
    
    def get_file_metadata(self, filename: str) -> Optional[FileMetadata]:
        """Get metadata for a specific file"""
        return self._metadata_cache.get(filename)
    
    def delete_file(self, filename: str, delete_from_s3: bool = False) -> bool:
        """Delete file metadata and optionally from S3"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        
        if delete_from_s3:
            try:
                self.s3_client.delete_object(Bucket=self.s3_bucket, Key=file_meta.s3_key)
                print(f"Deleted file from S3: {file_meta.s3_key}")
            except Exception as e:
                print(f"Failed to delete file from S3: {e}")
                return False
        
        # Update metadata
        file_meta.status = FileStatus.DELETED
        self._save_metadata()
        
        print(f"Marked file as deleted: {filename}")
        return True
    
    def reprocess_file(self, filename: str) -> bool:
        """Reset a file's status to EXTRACTED for reprocessing"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        file_meta.status = FileStatus.EXTRACTED
        file_meta.validated_at = None
        file_meta.processed_at = None
        file_meta.validation_errors = []
        file_meta.processing_errors = []
        self._save_metadata()
        
        print(f"Reset file status for reprocessing: {filename}")
        return True
    
    def mark_as_processed(self, filename: str) -> bool:
        """Mark a file as processed (loaded into database)"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        file_meta.status = FileStatus.PROCESSED
        file_meta.processed_at = datetime.now()
        file_meta.processing_errors = []  # Clear any previous errors
        self._save_metadata()
        
        print(f"Marked file as processed: {filename}")
        return True
    
    def get_files_for_processing(self, file_type: Optional[FileType] = None) -> List[FileMetadata]:
        """Get files that are ready for processing (validated but not processed)"""
        files = [
            meta for meta in self._metadata_cache.values()
            if meta.status == FileStatus.VALIDATED and
               (file_type is None or meta.file_type == file_type)
        ]
        
        # Sort by validated_at (oldest first for processing order)
        files.sort(key=lambda f: f.validated_at or datetime.min)
        
        return files
    
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
        """Convert a ZIP file to CSV and store in extracted_bike_ride_csvs/{city}/"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        
        if file_meta.file_type != FileType.NYC_ZIP:
            print(f"File {filename} is not a ZIP file")
            return False
        
        if file_meta.status != FileStatus.EXTRACTED:
            print(f"File {filename} is not in EXTRACTED status")
            return False
        
        try:
            print(f"Converting ZIP to CSV: {filename}")
            
            # Download ZIP file to temporary file to avoid memory issues
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                temp_zip_path = temp_zip.name
                
            # Download ZIP file to temp file
            self.s3_client.download_file(self.s3_bucket, file_meta.s3_key, temp_zip_path)
            
            # Extract CSV from ZIP using file operations
            csv_filename = None
            csv_content = None
            
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise ValueError(f"No CSV files found in ZIP: {filename}")
                
                # Use the first CSV file (NYC zips typically contain one CSV)
                csv_filename = csv_files[0]
                
                # Read CSV content in chunks to avoid memory issues
                with zip_file.open(csv_filename) as csv_file:
                    csv_content = csv_file.read()
            
            # Clean up temp ZIP file
            os.unlink(temp_zip_path)
            
            # Determine city from file type
            city = "nyc" if file_meta.file_type == FileType.NYC_ZIP else "london"
            
            # Create new CSV filename
            base_name = os.path.splitext(filename)[0]
            new_csv_filename = f"{base_name}.csv"
            csv_s3_key = f"extracted_bike_ride_csvs/{city}/{new_csv_filename}"
            
            # Upload CSV to S3 using streaming
            csv_buffer = BytesIO(csv_content)
            self.s3_client.upload_fileobj(csv_buffer, self.s3_bucket, csv_s3_key)
            
            # Create metadata for the new CSV file
            csv_file_meta = FileMetadata(
                filename=new_csv_filename,
                s3_key=csv_s3_key,
                file_type=FileType.NYC_CSV if city == "nyc" else FileType.LONDON_CSV,
                file_size_bytes=len(csv_content),
                extracted_at=datetime.now(),
                status=FileStatus.EXTRACTED,
                metadata={"source_zip": filename}
            )
            
            self._metadata_cache[new_csv_filename] = csv_file_meta
            
            # Update ZIP file status
            file_meta.status = FileStatus.CSV_CONVERTED
            file_meta.csv_converted_at = datetime.now()
            file_meta.metadata["converted_csv"] = new_csv_filename
            
            self._save_metadata()
            
            print(f"Successfully converted {filename} to {new_csv_filename}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to convert ZIP to CSV: {str(e)}"
            file_meta.processing_errors.append(error_msg)
            file_meta.status = FileStatus.FAILED
            self._save_metadata()
            print(f"Conversion failed: {error_msg}")
            return False
    
    def convert_csv_to_parquet(self, filename: str) -> bool:
        """Convert a CSV file to Parquet with schema-based organization"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        
        if file_meta.file_type not in [FileType.NYC_CSV, FileType.LONDON_CSV]:
            print(f"File {filename} is not a CSV file")
            return False
        
        if file_meta.status != FileStatus.VALIDATED:
            print(f"File {filename} is not validated")
            return False
        
        try:
            print(f"Converting CSV to Parquet: {filename}")
            
            # Download CSV file
            csv_buffer = self._download_csv_sample(file_meta.s3_key, sample_size=None)  # Download full file
            df = pd.read_csv(csv_buffer)
            
            # Determine schema using data model validation
            model = self._find_matching_model(df.head(100), file_meta.file_type)
            if model is None:
                raise ValueError(f"No matching data model found for {filename}")
            
            # Transform data using the model
            df_transformed = model.to_dataframe(df, filename)
            
            # Determine city and schema
            city = "nyc" if file_meta.file_type == FileType.NYC_CSV else "london"
            schema = model.__name__.lower()
            
            # Create Parquet filename and S3 key
            base_name = os.path.splitext(filename)[0]
            parquet_filename = f"{base_name}.parquet"
            parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
            
            # Convert to Parquet and upload
            parquet_buffer = BytesIO()
            df_transformed.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            self.s3_client.upload_fileobj(parquet_buffer, self.s3_bucket, parquet_s3_key)
            
            # Create metadata for the new Parquet file
            parquet_file_meta = FileMetadata(
                filename=parquet_filename,
                s3_key=parquet_s3_key,
                file_type=FileType.NYC_PARQUET if city == "nyc" else FileType.LONDON_PARQUET,
                file_size_bytes=parquet_buffer.tell(),
                extracted_at=datetime.now(),
                status=FileStatus.EXTRACTED,
                metadata={
                    "source_csv": filename,
                    "schema": schema,
                    "model": model.__name__
                }
            )
            
            self._metadata_cache[parquet_filename] = parquet_file_meta
            
            # Update CSV file status
            file_meta.status = FileStatus.PARQUET_CONVERTED
            file_meta.parquet_converted_at = datetime.now()
            file_meta.metadata["converted_parquet"] = parquet_filename
            
            self._save_metadata()
            
            print(f"Successfully converted {filename} to {parquet_filename} (schema: {schema})")
            return True
            
        except Exception as e:
            error_msg = f"Failed to convert CSV to Parquet: {str(e)}"
            file_meta.processing_errors.append(error_msg)
            file_meta.status = FileStatus.FAILED
            self._save_metadata()
            print(f"Conversion failed: {error_msg}")
            return False
    
    def process_pipeline(self, filename: str) -> bool:
        """Process a file through the entire pipeline: ZIP → CSV → Validate → Parquet"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        
        # Step 1: Convert ZIP to CSV (if needed)
        if file_meta.file_type == FileType.NYC_ZIP and file_meta.status == FileStatus.EXTRACTED:
            if not self.convert_zip_to_csv(filename):
                return False
            # Get the converted CSV filename
            csv_filename = file_meta.metadata.get("converted_csv")
            if not csv_filename:
                print(f"No CSV filename found in metadata for {filename}")
                return False
            filename = csv_filename
            file_meta = self._metadata_cache[filename]
        
        # Step 2: Validate CSV
        if file_meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and file_meta.status == FileStatus.EXTRACTED:
            if not self.validate_file_schema(filename):
                return False
        
        # Step 3: Convert CSV to Parquet
        if file_meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and file_meta.status == FileStatus.VALIDATED:
            if not self.convert_csv_to_parquet(filename):
                return False
        
        print(f"Pipeline completed successfully for {filename}")
        return True
    
    def process_all_pipelines(self, file_type: Optional[FileType] = None) -> Dict[str, bool]:
        """Process all files through the pipeline"""
        results = {}
        
        # Get files that need processing
        files_to_process = []
        
        if file_type == FileType.NYC_ZIP:
            files_to_process = [
                filename for filename, meta in self._metadata_cache.items()
                if meta.file_type == FileType.NYC_ZIP and meta.status == FileStatus.EXTRACTED
            ]
        elif file_type in [FileType.NYC_CSV, FileType.LONDON_CSV]:
            files_to_process = [
                filename for filename, meta in self._metadata_cache.items()
                if meta.file_type == file_type and meta.status == FileStatus.VALIDATED
            ]
        else:
            # Process all files that need pipeline processing
            files_to_process = [
                filename for filename, meta in self._metadata_cache.items()
                if ((meta.file_type == FileType.NYC_ZIP and meta.status == FileStatus.EXTRACTED) or
                    (meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and meta.status == FileStatus.VALIDATED))
            ]
        
        print(f"Processing {len(files_to_process)} files through pipeline...")
        
        for filename in files_to_process:
            results[filename] = self.process_pipeline(filename)
        
        return results
    
    def process_single_file(self, filename: str) -> bool:
        """Process a single file through the pipeline with memory management"""
        if filename not in self._metadata_cache:
            print(f"File {filename} not found in metadata")
            return False
        
        file_meta = self._metadata_cache[filename]
        print(f"\nProcessing {filename} ({file_meta.file_type.value})...")
        
        try:
            # Step 1: Convert ZIP to CSV (if needed)
            if file_meta.file_type == FileType.NYC_ZIP and file_meta.status == FileStatus.EXTRACTED:
                print(f"  Step 1: Converting ZIP to CSV...")
                if not self.convert_zip_to_csv(filename):
                    return False
                # Get the converted CSV filename
                csv_filename = file_meta.metadata.get("converted_csv")
                if not csv_filename:
                    print(f"  ERROR: No CSV filename found in metadata for {filename}")
                    return False
                filename = csv_filename
                file_meta = self._metadata_cache[filename]
                print(f"  ✓ ZIP converted to {csv_filename}")
            
            # Step 2: Validate CSV
            if file_meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and file_meta.status == FileStatus.EXTRACTED:
                print(f"  Step 2: Validating schema...")
                if not self.validate_file_schema(filename):
                    return False
                print(f"  ✓ Schema validated")
            
            # Step 3: Convert CSV to Parquet
            if file_meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and file_meta.status == FileStatus.VALIDATED:
                print(f"  Step 3: Converting to Parquet...")
                if not self.convert_csv_to_parquet(filename):
                    return False
                print(f"  ✓ Converted to Parquet")
            
            print(f"  ✓ Pipeline completed successfully for {filename}")
            return True
            
        except Exception as e:
            print(f"  ✗ Pipeline failed for {filename}: {str(e)}")
            return False
    
    def process_files_batch(self, file_type: Optional[FileType] = None, limit: int = 5) -> Dict[str, bool]:
        """Process files in small batches to avoid memory issues"""
        results = {}
        
        # Get files that need processing
        files_to_process = []
        
        if file_type == FileType.NYC_ZIP:
            files_to_process = [
                filename for filename, meta in self._metadata_cache.items()
                if meta.file_type == FileType.NYC_ZIP and meta.status == FileStatus.EXTRACTED
            ]
        elif file_type in [FileType.NYC_CSV, FileType.LONDON_CSV]:
            files_to_process = [
                filename for filename, meta in self._metadata_cache.items()
                if meta.file_type == file_type and meta.status == FileStatus.VALIDATED
            ]
        else:
            # Process all files that need pipeline processing
            files_to_process = [
                filename for filename, meta in self._metadata_cache.items()
                if ((meta.file_type == FileType.NYC_ZIP and meta.status == FileStatus.EXTRACTED) or
                    (meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and meta.status == FileStatus.VALIDATED))
            ]
        
        # Limit the number of files to process
        files_to_process = files_to_process[:limit]
        
        print(f"Processing {len(files_to_process)} files in batch (limit: {limit})...")
        
        for i, filename in enumerate(files_to_process, 1):
            print(f"\n[{i}/{len(files_to_process)}] Processing {filename}")
            results[filename] = self.process_single_file(filename)
            
            # Add a small delay between files to allow memory cleanup
            time.sleep(1)
        
        return results 