"""
Simplified Extracted File Manager

Replaces the over-engineered metadata tracking system with simple file existence checks.
Maintains backward compatibility while providing a much simpler and more reliable approach.
"""

import os
import boto3
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import gc
import psutil
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

from .filetree import ZipFile as ZipFileNode, walk_folder
from data_models.base import BaseBikeShareRecord

# Load environment variables
load_dotenv()

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
        import time
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
        """Extract ZIP using the existing filetree system"""
        # Download ZIP to temp file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            # Download ZIP from S3
            self.s3_client.download_file(self.s3_bucket, zip_s3_key, temp_zip_path)
            
            # Read ZIP content
            with open(temp_zip_path, 'rb') as f:
                zip_content = f.read()
            
            # Use filetree system to extract
            zip_node = ZipFileNode(os.path.basename(zip_s3_key), zip_content)
            extracted_folder = zip_node.extract()
            
            # Walk through all extracted files and upload CSVs
            csv_count = 0
            skipped_count = 0
            for file_node in walk_folder(extracted_folder):
                if file_node.name.lower().endswith('.csv'):
                    # Skip MacOSX artifacts (files starting with ._ or in __MACOSX directories)
                    if file_node.name.startswith('._') or '__MACOSX' in file_node.name:
                        print(f"  Skipping MacOSX artifact: {file_node.name}")
                        continue
                    
                    # Upload CSV to flat structure
                    csv_s3_key = f"extracted_bike_ride_csvs/nyc/{file_node.name}"
                    
                    # Check if CSV already exists
                    if self._file_exists_in_s3(csv_s3_key):
                        print(f"  Skipping {file_node.name} - already exists")
                        skipped_count += 1
                        continue
                    
                    # Upload CSV content
                    self.s3_client.put_object(
                        Bucket=self.s3_bucket,
                        Key=csv_s3_key,
                        Body=file_node.content
                    )
                    print(f"  ✓ Uploaded {file_node.name}")
                    csv_count += 1
            
            print(f"  Processed {csv_count + skipped_count} CSV files from {os.path.basename(zip_s3_key)} ({csv_count} new, {skipped_count} skipped)")
            
        finally:
            # Clean up temp file
            os.unlink(temp_zip_path)
            # Force cleanup of extracted folder
            if 'extracted_folder' in locals():
                del extracted_folder
            gc.collect()
    
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
    
    def _stream_csv_to_parquet(self, csv_s3_key: str, parquet_s3_key: str, model):
        """Stream CSV to parquet using pyarrow"""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
            temp_parquet_path = temp_parquet.name
        
        try:
            # Stream CSV from S3 to parquet
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=csv_s3_key)
            
            reader = pv.open_csv(response['Body'])
            writer = None
            
            for batch in reader:
                # Convert to pandas and apply model transformation
                df_chunk = batch.to_pandas()
                df_transformed = model.to_dataframe(df_chunk, csv_s3_key)
                
                # Convert back to pyarrow
                table = pa.Table.from_pandas(df_transformed)
                
                if writer is None:
                    writer = pq.ParquetWriter(temp_parquet_path, table.schema)
                
                writer.write_table(table)
            
            if writer:
                writer.close()
            
            # Upload parquet to S3
            with open(temp_parquet_path, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, parquet_s3_key)
                
        finally:
            os.unlink(temp_parquet_path)
    

    
    def _parquet_exists_for_csv(self, csv_file: str) -> bool:
        """Check if parquet already exists for this CSV"""
        csv_filename = csv_file.split('/')[-1]
        parquet_filename = csv_filename.replace('.csv', '.parquet')
        
        # Check all possible schema locations
        cities = ["nyc", "london"]
        schemas = ["nyclegacybikesharerecord", "nycmodernbikesharerecord", 
                  "londonlegacybikesharerecord", "londonmodernbikesharerecord"]
        
        for city in cities:
            for schema in schemas:
                parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
                if self._file_exists_in_s3(parquet_s3_key):
                    return True
        
        return False
    
    def _file_exists_in_s3(self, s3_key: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except:
            return False
    
    def _list_s3_files(self, prefix: str) -> List[str]:
        """List files in S3 with given prefix"""
        response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
        if 'Contents' not in response:
            return []
        return [obj['Key'] for obj in response['Contents']] 