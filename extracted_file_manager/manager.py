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
        except self.s3_client.exceptions.NoSuchKey:
            # No metadata file exists yet
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
            "extracted_bike_ride_csvs/london/": FileType.LONDON_CSV,
            "nyc_csv/": FileType.NYC_CSV,
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
        print(f"  Validated: {summary.validated_files}")
        print(f"  Processed: {summary.processed_files}")
        print(f"  Failed: {summary.failed_files}")
        print(f"  Deleted: {summary.deleted_files}")
        print()
        print("By File Type:")
        for file_type, count in summary.by_file_type.items():
            print(f"  {file_type.value}: {count}")
        print("="*60) 