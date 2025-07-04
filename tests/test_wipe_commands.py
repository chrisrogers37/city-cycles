import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from extracted_file_manager.manager import ExtractedFileManager
from extracted_file_manager.models import FileMetadata, FileStatus, FileType, FileLocation


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
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED,
            file_size_bytes=1000
        )
        
        csv_file = FileMetadata(
            filename="test.csv",
            s3_key="extracted_bike_ride_csvs/nyc/test.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000
        )
        
        parquet_file = FileMetadata(
            filename="test.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/modern/test.parquet",
            file_type=FileType.PARQUET,
            file_location=FileLocation.NYC,
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
        count = self.manager.wipe_files()
        
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
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.PARQUET_CONVERTED,
            parquet_converted_at=datetime.now(),
            file_size_bytes=2000,
            metadata={"converted_parquet": "test.parquet"}
        )
        
        # Create the Parquet file it generated
        parquet_file = FileMetadata(
            filename="test.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/modern/test.parquet",
            file_type=FileType.PARQUET,
            file_location=FileLocation.NYC,
            status=FileStatus.PROCESSED,
            file_size_bytes=3000
        )
        
        # Add files to metadata
        self.manager._metadata_cache = {
            "test.csv": csv_file,
            "test.parquet": parquet_file
        }
        
        # Run wipe-type for Parquet files
        count = self.manager.wipe_files(file_type=FileType.PARQUET, location=FileLocation.NYC)
        
        # Verify Parquet file was deleted from S3 and metadata
        assert self.manager.s3_client.delete_object.call_count == 1
        assert "test.parquet" not in self.manager._metadata_cache
        assert "test.csv" in self.manager._metadata_cache
        
        # Verify CSV file was reset to EXTRACTED status
        csv_meta = self.manager._metadata_cache["test.csv"]
        assert csv_meta.status == FileStatus.EXTRACTED
        assert "converted_parquet" not in csv_meta.metadata
        assert csv_meta.parquet_converted_at is None
        
        assert count == 1
    
    def test_wipe_parquet_type_with_location_filter(self):
        """Test that location filter works correctly for Parquet wipe"""
        # Create CSV files for different cities
        nyc_csv = FileMetadata(
            filename="nyc.csv",
            s3_key="extracted_bike_ride_csvs/nyc/nyc.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000
        )
        
        london_csv = FileMetadata(
            filename="london.csv",
            s3_key="extracted_bike_ride_csvs/london/london.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.LONDON,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000
        )
        
        # Create Parquet files for different cities
        nyc_parquet = FileMetadata(
            filename="nyc.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/modern/nyc.parquet",
            file_type=FileType.PARQUET,
            file_location=FileLocation.NYC,
            status=FileStatus.PROCESSED,
            file_size_bytes=3000
        )
        
        london_parquet = FileMetadata(
            filename="london.parquet",
            s3_key="extracted_bike_ride_parquet/london/modern/london.parquet",
            file_type=FileType.PARQUET,
            file_location=FileLocation.LONDON,
            status=FileStatus.PROCESSED,
            file_size_bytes=3000
        )
        
        # Add files to metadata
        self.manager._metadata_cache = {
            "nyc.csv": nyc_csv,
            "london.csv": london_csv,
            "nyc.parquet": nyc_parquet,
            "london.parquet": london_parquet
        }
        
        # Run wipe-type for NYC Parquet files only
        count = self.manager.wipe_files(file_type=FileType.PARQUET, location=FileLocation.NYC)
        
        # Verify NYC Parquet was deleted and NYC CSV was reset
        assert "nyc.parquet" not in self.manager._metadata_cache
        assert "london.parquet" in self.manager._metadata_cache  # London Parquet should remain
        assert self.manager._metadata_cache["nyc.csv"].status == FileStatus.EXTRACTED
        assert self.manager._metadata_cache["london.csv"].status == FileStatus.PARQUET_CONVERTED
        
        assert count == 1  # One Parquet file deleted 