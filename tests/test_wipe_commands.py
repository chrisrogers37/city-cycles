import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from extracted_file_manager.manager import ExtractedFileManager
from extracted_file_manager.models import FileMetadata, FileStatus, FileType


class TestWipeCommands:
    """Test wipe command state management"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.manager = ExtractedFileManager()
        self.manager.s3_client = Mock()
        self.manager.s3_bucket = "test-bucket"
        self.manager._metadata_cache = {}
    
    def test_wipe_all_removes_all_metadata(self):
        """Test that wipe-all removes ALL files from metadata"""
        # Create test files of different types
        zip_file = FileMetadata(
            filename="test.zip",
            s3_key="extracted_bike_ride_zips/nyc/test.zip",
            file_type=FileType.NYC_ZIP,
            status=FileStatus.EXTRACTED,
            file_size_bytes=1000
        )
        
        csv_file = FileMetadata(
            filename="test.csv",
            s3_key="extracted_bike_ride_csvs/nyc/test.csv",
            file_type=FileType.NYC_CSV,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000
        )
        
        parquet_file = FileMetadata(
            filename="test.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/modern/test.parquet",
            file_type=FileType.NYC_PARQUET,
            status=FileStatus.PROCESSED,
            file_size_bytes=3000
        )
        
        # Add files to metadata
        self.manager._metadata_cache = {
            "test.zip": zip_file,
            "test.csv": csv_file,
            "test.parquet": parquet_file
        }
        
        # Run wipe-all
        count = self.manager.wipe_all()
        
        # Verify all files were deleted from S3
        assert self.manager.s3_client.delete_object.call_count == 3
        
        # Verify all files were removed from metadata
        assert len(self.manager._metadata_cache) == 0
        assert count == 3
    
    def test_wipe_parquet_type_resets_csv_status(self):
        """Test that wiping Parquet files resets CSV files that generated them"""
        # Create a CSV file that was converted to Parquet
        csv_file = FileMetadata(
            filename="test.csv",
            s3_key="extracted_bike_ride_csvs/nyc/test.csv",
            file_type=FileType.NYC_CSV,
            status=FileStatus.PARQUET_CONVERTED,
            parquet_converted_at=datetime.now(),
            file_size_bytes=2000,
            metadata={"converted_parquet": "test.parquet"}
        )
        
        # Create the Parquet file it generated
        parquet_file = FileMetadata(
            filename="test.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/modern/test.parquet",
            file_type=FileType.NYC_PARQUET,
            status=FileStatus.PROCESSED,
            file_size_bytes=3000
        )
        
        # Add files to metadata
        self.manager._metadata_cache = {
            "test.csv": csv_file,
            "test.parquet": parquet_file
        }
        
        # Run wipe-type for Parquet files
        count = self.manager.wipe_file_type("nyc_parquet")
        
        # Verify Parquet file was deleted from S3 and metadata
        assert self.manager.s3_client.delete_object.call_count == 1
        assert "test.parquet" not in self.manager._metadata_cache
        assert "test.csv" in self.manager._metadata_cache
        
        # Verify CSV file was reset to EXTRACTED status
        csv_meta = self.manager._metadata_cache["test.csv"]
        assert csv_meta.status == FileStatus.EXTRACTED
        assert csv_meta.parquet_s3_key is None
        assert csv_meta.parquet_schema is None
        assert csv_meta.parquet_converted_at is None
        
        assert count == 1
    
    def test_wipe_parquet_type_with_location_filter(self):
        """Test that location filter works correctly for Parquet wipe"""
        # Create CSV files for different cities
        nyc_csv = FileMetadata(
            filename="nyc.csv",
            s3_key="extracted_bike_ride_csvs/nyc/nyc.csv",
            file_type=FileType.NYC_CSV,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000
        )
        
        london_csv = FileMetadata(
            filename="london.csv",
            s3_key="extracted_bike_ride_csvs/london/london.csv",
            file_type=FileType.LONDON_CSV,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000
        )
        
        # Add files to metadata
        self.manager._metadata_cache = {
            "nyc.csv": nyc_csv,
            "london.csv": london_csv
        }
        
        # Run wipe-type for NYC Parquet files only
        count = self.manager.wipe_file_type("nyc_parquet", location="nyc")
        
        # Verify only NYC CSV was reset
        assert self.manager._metadata_cache["nyc.csv"].status == FileStatus.EXTRACTED
        assert self.manager._metadata_cache["london.csv"].status == FileStatus.PARQUET_CONVERTED
        
        assert count == 0  # No Parquet files to delete, just CSV status resets 