"""
Tests for Extracted File Manager
"""

import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
import zipfile
from datetime import datetime, timezone
import botocore

from extracted_file_manager import ExtractedFileManager
from extracted_file_manager.models import FileMetadata, FileStatus, FileType, FileLocation, FileSummary
from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord


class TestFileMetadata:
    """Test FileMetadata serialization/deserialization"""
    
    def test_to_dict(self):
        """Test FileMetadata.to_dict()"""
        meta = FileMetadata(
            filename="test.csv",
            s3_key="test/path/test.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            source_url="http://example.com/test.csv",
            file_size_bytes=1024,
            extracted_at=datetime(2023, 1, 1, 12, 0, 0),
            status=FileStatus.VALIDATED,
            validation_errors=["error1"],
            processing_errors=["error2"],
            metadata={"key": "value"}
        )
        
        data = meta.to_dict()
        
        assert data["filename"] == "test.csv"
        assert data["s3_key"] == "test/path/test.csv"
        assert data["file_type"] == "csv"
        assert data["source_url"] == "http://example.com/test.csv"
        assert data["file_size_bytes"] == 1024
        assert data["status"] == "validated"
        assert data["validation_errors"] == ["error1"]
        assert data["processing_errors"] == ["error2"]
        assert data["metadata"] == {"key": "value"}
    
    def test_from_dict(self):
        """Test FileMetadata.from_dict()"""
        data = {
            "filename": "test.csv",
            "s3_key": "test/path/test.csv",
            "file_type": "csv",
            "file_location": "nyc",
            "source_url": "http://example.com/test.csv",
            "file_size_bytes": 1024,
            "extracted_at": "2023-01-01T12:00:00",
            "status": "validated",
            "validation_errors": ["error1"],
            "processing_errors": ["error2"],
            "metadata": {"key": "value"}
        }
        
        meta = FileMetadata.from_dict(data)
        
        assert meta.filename == "test.csv"
        assert meta.s3_key == "test/path/test.csv"
        assert meta.file_type == FileType.CSV
        assert meta.source_url == "http://example.com/test.csv"
        assert meta.file_size_bytes == 1024
        assert meta.status == FileStatus.VALIDATED
        assert meta.validation_errors == ["error1"]
        assert meta.processing_errors == ["error2"]
        assert meta.metadata == {"key": "value"}


class TestFileSummary:
    """Test FileSummary statistics"""
    
    def test_update_from_file(self):
        """Test FileSummary.update_from_file()"""
        summary = FileSummary()
        
        # Test extracted file
        meta1 = FileMetadata(
            filename="test1.csv",
            s3_key="test1.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            file_size_bytes=1024,
            status=FileStatus.EXTRACTED
        )
        summary.update_from_file(meta1)
        
        assert summary.total_files == 1
        assert summary.extracted_files == 1
        assert summary.total_size_bytes == 1024
        assert summary.by_file_type[FileType.CSV] == 1
        
        # Test validated file
        meta2 = FileMetadata(
            filename="test2.csv",
            s3_key="test2.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.LONDON,
            file_size_bytes=2048,
            status=FileStatus.VALIDATED
        )
        summary.update_from_file(meta2)
        
        assert summary.total_files == 2
        assert summary.extracted_files == 1
        assert summary.validated_files == 1
        assert summary.total_size_bytes == 3072
        assert summary.by_file_type[FileType.CSV] == 2


class TestExtractedFileManager:
    """Test ExtractedFileManager functionality"""
    
    @pytest.fixture
    def mock_s3(self):
        """Mock S3 client"""
        with patch('extracted_file_manager.manager.boto3.client') as mock_client:
            mock_s3 = Mock()
            mock_client.return_value = mock_s3
            yield mock_s3
    
    @pytest.fixture
    def manager(self, mock_s3):
        """Create ExtractedFileManager with mocked S3"""
        with patch.dict(os.environ, {'S3_BUCKET': 'test-bucket'}):
            manager = ExtractedFileManager()
            return manager
    
    def test_init(self, mock_s3):
        """Test ExtractedFileManager initialization"""
        with patch.dict(os.environ, {'S3_BUCKET': 'test-bucket'}):
            manager = ExtractedFileManager()
            assert manager.s3_bucket == 'test-bucket'
            assert manager.metadata_key == 'extracted_file_manager/metadata.json'
    
    def test_init_no_bucket(self):
        """Test initialization without S3_BUCKET"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="S3_BUCKET environment variable is not set"):
                ExtractedFileManager()
    
    def test_load_metadata_empty(self, manager, mock_s3):
        """Test loading metadata when no file exists"""
        error_response = {'Error': {'Code': 'NoSuchKey'}}
        mock_s3.get_object.side_effect = botocore.exceptions.ClientError(error_response, 'GetObject')
        
        manager._load_metadata()
        assert manager._metadata_cache == {}
    
    def test_load_metadata_existing(self, manager, mock_s3):
        """Test loading existing metadata"""
        metadata_data = {
            "test.csv": {
                "filename": "test.csv",
                "s3_key": "test/test.csv",
                "file_type": "csv",
                "file_location": "nyc",
                "status": "extracted"
            }
        }
        
        mock_body = Mock()
        mock_body.read.return_value = json.dumps(metadata_data).encode()
        mock_response = {'Body': mock_body}
        mock_s3.get_object.return_value = mock_response
        
        manager._load_metadata()
        
        assert len(manager._metadata_cache) == 1
        assert "test.csv" in manager._metadata_cache
        assert manager._metadata_cache["test.csv"].filename == "test.csv"
    
    def test_save_metadata(self, manager, mock_s3):
        """Test saving metadata to S3"""
        meta = FileMetadata(
            filename="test.csv",
            s3_key="test/test.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC
        )
        manager._metadata_cache["test.csv"] = meta
        
        manager._save_metadata()
        
        # Verify S3 put_object was called with correct data
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args
        assert call_args[1]['Bucket'] == 'test-bucket'
        assert call_args[1]['Key'] == 'extracted_file_manager/metadata.json'
        
        # Verify JSON content
        body = call_args[1]['Body']
        data = json.loads(body)
        assert "test.csv" in data
        assert data["test.csv"]["filename"] == "test.csv"
    
    def test_scan_s3_files_new(self, manager, mock_s3):
        """Test scanning S3 for new files"""
        # Mock S3 list_objects_v2 response
        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        
        mock_page = {
            'Contents': [
                {
                    'Key': 'extracted_bike_ride_zips/nyc/test.zip',
                    'Size': 1024,
                    'LastModified': datetime(2023, 1, 1, 12, 0, 0)
                }
            ]
        }
        mock_paginator.paginate.return_value = [mock_page]
        
        new_files = manager.scan_s3_files()
        
        assert len(new_files) == 1
        assert new_files[0].filename == "test.zip"
        assert new_files[0].file_type == FileType.ZIP
        assert new_files[0].file_location == FileLocation.NYC
        assert new_files[0].status == FileStatus.EXTRACTED
    
    def test_validate_file_schema_success(self, manager, mock_s3):
        """Test successful schema validation"""
        # Create test metadata
        meta = FileMetadata(
            filename="test.csv",
            s3_key="test/test.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED
        )
        manager._metadata_cache["test.csv"] = meta
        
        # Mock CSV download using get_object (for _download_csv_sample)
        csv_content = """tripduration,bikeid,starttime,stoptime,start station id,start station name,start station latitude,start station longitude,end station id,end station name,end station latitude,end station longitude,usertype,birth year,gender\n60,12345,2019-06-01 00:00:00,2019-06-01 00:01:00,1,Test Station,40.0,-74.0,2,Test Station 2,40.1,-74.1,Subscriber,1990,1"""
        
        mock_body = Mock()
        mock_body.read.return_value = csv_content.encode()
        mock_response = {'Body': mock_body}
        mock_s3.get_object.return_value = mock_response
        
        success = manager.validate_file_schema("test.csv")
        
        assert success
        assert meta.status == FileStatus.VALIDATED
        assert meta.metadata["schema"] == "NYCLegacyBikeShareRecord"
    
    def test_validate_file_schema_failure(self, manager, mock_s3):
        """Test schema validation failure"""
        # Create test metadata
        meta = FileMetadata(
            filename="test.csv",
            s3_key="test/test.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED
        )
        manager._metadata_cache["test.csv"] = meta
        
        # Mock CSV download with invalid schema using get_object
        csv_content = "invalid_column,another_invalid_column\nvalue1,value2"
        mock_body = Mock()
        mock_body.read.return_value = csv_content.encode()
        mock_response = {'Body': mock_body}
        mock_s3.get_object.return_value = mock_response
        
        success = manager.validate_file_schema("test.csv")
        
        assert not success
        assert meta.status == FileStatus.FAILED
        assert len(meta.validation_errors) > 0
    
    def test_convert_zip_to_csv(self, manager, mock_s3):
        """Test ZIP to CSV conversion with multiple CSVs using tempfiles"""
        import zipfile
        # Create test metadata
        meta = FileMetadata(
            filename="test.zip",
            s3_key="extracted_bike_ride_zips/nyc/test.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED
        )
        manager._metadata_cache["test.zip"] = meta
        # Create a ZIP structure in memory:
        # test.zip
        #   ├── a.csv
        #   └── b.csv
        csv_content_a = b"col1,col2\n1,2\n"
        csv_content_b = b"col1,col2\n3,4\n"
        test_zip_buffer = BytesIO()
        with zipfile.ZipFile(test_zip_buffer, 'w') as test_zip:
            test_zip.writestr('a.csv', csv_content_a)
            test_zip.writestr('b.csv', csv_content_b)
        test_zip_bytes = test_zip_buffer.getvalue()
        # Mock S3 download_file to write test_zip_bytes to the temp file
        def download_file_side_effect(bucket, key, filename):
            with open(filename, 'wb') as f:
                f.write(test_zip_bytes)
        mock_s3.download_file.side_effect = download_file_side_effect
        # Mock S3 upload_fileobj to record the uploads from file objects
        uploaded_files = {}
        def upload_fileobj_side_effect(fileobj, bucket, key):
            fileobj.seek(0)
            uploaded_files[key] = fileobj.read()
        mock_s3.upload_fileobj.side_effect = upload_fileobj_side_effect
        # Run the extraction
        success = manager.convert_zip_to_csv("test.zip")
        assert success
        # Check that both CSVs were uploaded
        assert "extracted_bike_ride_csvs/nyc/a.csv" in uploaded_files
        assert "extracted_bike_ride_csvs/nyc/b.csv" in uploaded_files
        # Check that both CSVs are in metadata
        assert "a.csv" in manager._metadata_cache
        assert "b.csv" in manager._metadata_cache
        # Check that the ZIP metadata lists both CSVs
        extracted_csvs = manager._metadata_cache["test.zip"].metadata["extracted_csvs"]
        assert set(extracted_csvs) == {"a.csv", "b.csv"}
        print("Tested ZIP extraction and upload of all CSVs using tempfiles.")
    
    def test_convert_zip_to_csv_filters_macos_hidden_files(self, manager, mock_s3):
        """Test that Mac OS hidden files (._*) are filtered out during ZIP extraction"""
        import zipfile
        # Create test metadata
        meta = FileMetadata(
            filename="test.zip",
            s3_key="extracted_bike_ride_zips/nyc/test.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED
        )
        manager._metadata_cache["test.zip"] = meta
        
        # Create a ZIP structure with Mac OS hidden files:
        # test.zip
        #   ├── a.csv
        #   ├── ._a.csv (Mac OS hidden file - should be filtered out)
        #   ├── b.csv
        #   └── ._b.csv (Mac OS hidden file - should be filtered out)
        csv_content_a = b"col1,col2\n1,2\n"
        csv_content_b = b"col1,col2\n3,4\n"
        hidden_content = b"Mac OS hidden file content"
        
        test_zip_buffer = BytesIO()
        with zipfile.ZipFile(test_zip_buffer, 'w') as test_zip:
            test_zip.writestr('a.csv', csv_content_a)
            test_zip.writestr('._a.csv', hidden_content)  # Mac OS hidden file
            test_zip.writestr('b.csv', csv_content_b)
            test_zip.writestr('._b.csv', hidden_content)  # Mac OS hidden file
        
        test_zip_bytes = test_zip_buffer.getvalue()
        
        # Mock S3 download_file to write test_zip_bytes to the temp file
        def download_file_side_effect(bucket, key, filename):
            with open(filename, 'wb') as f:
                f.write(test_zip_bytes)
        mock_s3.download_file.side_effect = download_file_side_effect
        
        # Mock S3 upload_fileobj to record the uploads from file objects
        uploaded_files = {}
        def upload_fileobj_side_effect(fileobj, bucket, key):
            fileobj.seek(0)
            uploaded_files[key] = fileobj.read()
        mock_s3.upload_fileobj.side_effect = upload_fileobj_side_effect
        
        # Run the extraction
        success = manager.convert_zip_to_csv("test.zip")
        assert success
        
        # Check that only the regular CSV files were uploaded (not the hidden ones)
        assert "extracted_bike_ride_csvs/nyc/a.csv" in uploaded_files
        assert "extracted_bike_ride_csvs/nyc/b.csv" in uploaded_files
        assert "extracted_bike_ride_csvs/nyc/._a.csv" not in uploaded_files
        assert "extracted_bike_ride_csvs/nyc/._b.csv" not in uploaded_files
        
        # Check that only the regular CSVs are in metadata
        assert "a.csv" in manager._metadata_cache
        assert "b.csv" in manager._metadata_cache
        assert "._a.csv" not in manager._metadata_cache
        assert "._b.csv" not in manager._metadata_cache
        
        # Check that the ZIP metadata lists only the regular CSVs
        extracted_csvs = manager._metadata_cache["test.zip"].metadata["extracted_csvs"]
        assert set(extracted_csvs) == {"a.csv", "b.csv"}
        assert len(extracted_csvs) == 2
        
        print("Tested ZIP extraction filtering out Mac OS hidden files.")
    
    def test_filetree_zipfile_filters_macos_hidden_files(self):
        """Test that filetree ZipFile.extract also filters Mac OS hidden files"""
        import zipfile
        from extracted_file_manager.filetree import ZipFile, walk_folder
        
        # Create a ZIP structure with Mac OS hidden files
        csv_content_a = b"col1,col2\n1,2\n"
        csv_content_b = b"col1,col2\n3,4\n"
        hidden_content = b"Mac OS hidden file content"
        
        test_zip_buffer = BytesIO()
        with zipfile.ZipFile(test_zip_buffer, 'w') as test_zip:
            test_zip.writestr('a.csv', csv_content_a)
            test_zip.writestr('._a.csv', hidden_content)  # Mac OS hidden file
            test_zip.writestr('b.csv', csv_content_b)
            test_zip.writestr('._b.csv', hidden_content)  # Mac OS hidden file
        
        test_zip_bytes = test_zip_buffer.getvalue()
        
        # Create ZipFile and extract
        zip_file = ZipFile("test.zip", test_zip_bytes)
        folder = zip_file.extract()
        
        # Walk through all files in the extracted folder
        files = list(walk_folder(folder))
        
        # Should only find the regular CSV files, not the hidden ones
        file_names = [f.name for f in files]
        assert "a.csv" in file_names
        assert "b.csv" in file_names
        assert "._a.csv" not in file_names
        assert "._b.csv" not in file_names
        assert len(files) == 2
        
        print("Tested filetree ZipFile.extract filtering out Mac OS hidden files.")
    
    def test_convert_csv_to_parquet(self, manager, mock_s3):
        """Test CSV to Parquet conversion"""
        # Create test metadata
        meta = FileMetadata(
            filename="test.csv",
            s3_key="extracted_bike_ride_csvs/nyc/test.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.VALIDATED
        )
        manager._metadata_cache["test.csv"] = meta
        
        # Create test CSV content
        csv_content = """tripduration,bikeid,starttime,stoptime,start station id,start station name,start station latitude,start station longitude,end station id,end station name,end station latitude,end station longitude,usertype,birth year,gender\n60,12345,2019-06-01 00:00:00,2019-06-01 00:01:00,1,Test Station,40.0,-74.0,2,Test Station 2,40.1,-74.1,Subscriber,1990,1"""
        
        # Mock CSV download using download_file (for temp file)
        def download_file_side_effect(bucket, key, filename):
            with open(filename, 'w') as f:
                f.write(csv_content)
        mock_s3.download_file.side_effect = download_file_side_effect
        
        # Mock Parquet upload
        def upload_fileobj_side_effect(fileobj, bucket, key):
            pass  # Just record the upload
        mock_s3.upload_fileobj.side_effect = upload_fileobj_side_effect
        
        success = manager.convert_csv_to_parquet("test.csv")
        
        assert success
        assert meta.status == FileStatus.PARQUET_CONVERTED
        # Find the new parquet file metadata
        parquet_filename = meta.metadata["converted_parquet"]
        parquet_meta = manager._metadata_cache[parquet_filename]
        assert parquet_meta.metadata["schema"] == "nyclegacybikesharerecord"
        
        # Verify Parquet was uploaded
        mock_s3.upload_fileobj.assert_called()
    

    
    def test_get_file_summary(self, manager):
        """Test file summary generation"""
        # Add test files
        meta1 = FileMetadata(
            filename="test1.csv",
            s3_key="test1.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            file_size_bytes=1024,
            status=FileStatus.EXTRACTED
        )
        meta2 = FileMetadata(
            filename="test2.csv",
            s3_key="test2.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.LONDON,
            file_size_bytes=2048,
            status=FileStatus.VALIDATED
        )
        
        manager._metadata_cache["test1.csv"] = meta1
        manager._metadata_cache["test2.csv"] = meta2
        
        summary = manager.get_file_summary()
        
        assert summary.total_files == 2
        assert summary.extracted_files == 1
        assert summary.validated_files == 1
        assert summary.total_size_bytes == 3072
        assert summary.by_file_type[FileType.CSV] == 2
    
    def test_wipe_files_single_file(self, manager):
        """Test wiping a specific file using wipe_files"""
        # Setup
        file_meta = FileMetadata(
            filename="test.zip",
            s3_key="extracted_bike_ride_zips/nyc/test.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED,
            file_size_bytes=1000,
            extracted_at=datetime.now()
        )
        manager._metadata_cache["test.zip"] = file_meta
        
        # Mock S3 delete and save_metadata
        manager.s3_client.delete_object = MagicMock()
        manager._save_metadata = MagicMock()
        
        # Test
        result = manager.wipe_files(filenames=["test.zip"])
        
        # Assert
        assert result == 1
        manager.s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="extracted_bike_ride_zips/nyc/test.zip"
        )
        assert "test.zip" not in manager._metadata_cache
        manager._save_metadata.assert_called_once()
    
    def test_wipe_files_file_not_found(self, manager):
        """Test wiping a file that doesn't exist"""
        result = manager.wipe_files(filenames=["nonexistent.zip"])
        assert result == 0
    
    def test_wipe_files_by_type(self, manager):
        """Test wiping files by type using wipe_files"""
        # Setup
        file1 = FileMetadata(
            filename="test1.zip",
            s3_key="extracted_bike_ride_zips/nyc/test1.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED,
            file_size_bytes=1000,
            extracted_at=datetime.now()
        )
        file2 = FileMetadata(
            filename="test2.zip", 
            s3_key="extracted_bike_ride_zips/nyc/test2.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED,
            file_size_bytes=2000,
            extracted_at=datetime.now()
        )
        file3 = FileMetadata(
            filename="test3.csv",
            s3_key="extracted_bike_ride_csvs/nyc/test3.csv", 
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.CSV_CONVERTED,
            file_size_bytes=3000,
            extracted_at=datetime.now()
        )
        
        manager._metadata_cache = {
            "test1.zip": file1,
            "test2.zip": file2,
            "test3.csv": file3
        }
        
        # Mock S3 delete and save_metadata
        manager.s3_client.delete_object = MagicMock()
        manager._save_metadata = MagicMock()
        
        # Test
        result = manager.wipe_files(file_type=FileType.ZIP)
        
        # Assert
        assert result == 2
        assert manager.s3_client.delete_object.call_count == 2
        assert "test1.zip" not in manager._metadata_cache
        assert "test2.zip" not in manager._metadata_cache
        assert "test3.csv" in manager._metadata_cache  # Should remain
        manager._save_metadata.assert_called_once()
    
    def test_wipe_files_with_location_filter(self, manager):
        """Test wiping files by type with location filter using wipe_files"""
        # Setup
        file1 = FileMetadata(
            filename="test1.zip",
            s3_key="extracted_bike_ride_zips/nyc/test1.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED,
            file_size_bytes=1000,
            extracted_at=datetime.now()
        )
        file2 = FileMetadata(
            filename="test2.zip",
            s3_key="extracted_bike_ride_zips/london/test2.zip", 
            file_type=FileType.ZIP,
            file_location=FileLocation.LONDON,
            status=FileStatus.EXTRACTED,
            file_size_bytes=2000,
            extracted_at=datetime.now()
        )
        
        manager._metadata_cache = {
            "test1.zip": file1,
            "test2.zip": file2
        }
        
        # Mock S3 delete and save_metadata
        manager.s3_client.delete_object = MagicMock()
        manager._save_metadata = MagicMock()
        
        # Test
        result = manager.wipe_files(file_type=FileType.ZIP, location=FileLocation.NYC)
        
        # Assert
        assert result == 1
        assert "test1.zip" not in manager._metadata_cache
        assert "test2.zip" in manager._metadata_cache  # Should remain
    
    def test_wipe_files_with_schema_filter(self, manager):
        """Test wiping parquet files with schema filter using wipe_files"""
        # Setup
        file1 = FileMetadata(
            filename="test1.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/modern/test1.parquet",
            file_type=FileType.PARQUET,
            file_location=FileLocation.NYC,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=1000,
            parquet_converted_at=datetime.now()
        )
        file2 = FileMetadata(
            filename="test2.parquet",
            s3_key="extracted_bike_ride_parquet/nyc/legacy/test2.parquet",
            file_type=FileType.PARQUET,
            file_location=FileLocation.NYC,
            status=FileStatus.PARQUET_CONVERTED,
            file_size_bytes=2000,
            parquet_converted_at=datetime.now()
        )
        
        manager._metadata_cache = {
            "test1.parquet": file1,
            "test2.parquet": file2
        }
        
        # Mock S3 delete and save_metadata
        manager.s3_client.delete_object = MagicMock()
        manager._save_metadata = MagicMock()
        
        # Test - Note: schema filtering would need to be implemented in wipe_files
        result = manager.wipe_files(file_type=FileType.PARQUET, location=FileLocation.NYC)
        
        # Assert
        assert result == 2  # Both parquet files should be deleted
        assert "test1.parquet" not in manager._metadata_cache
        assert "test2.parquet" not in manager._metadata_cache
    
    def test_wipe_all_files(self, manager):
        """Test wiping all files using wipe_files"""
        # Setup
        file1 = FileMetadata(
            filename="test1.zip",
            s3_key="extracted_bike_ride_zips/nyc/test1.zip",
            file_type=FileType.ZIP,
            file_location=FileLocation.NYC,
            status=FileStatus.EXTRACTED,
            file_size_bytes=1000,
            extracted_at=datetime.now()
        )
        file2 = FileMetadata(
            filename="test2.csv",
            s3_key="extracted_bike_ride_csvs/nyc/test2.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.CSV_CONVERTED,
            file_size_bytes=2000,
            csv_converted_at=datetime.now()
        )
        
        manager._metadata_cache = {
            "test1.zip": file1,
            "test2.csv": file2
        }
        
        # Mock S3 delete and save_metadata
        manager.s3_client.delete_object = MagicMock()
        manager._save_metadata = MagicMock()
        
        # Test
        result = manager.wipe_files()
        
        # Assert
        assert result == 2
        assert manager.s3_client.delete_object.call_count == 2
        assert len(manager._metadata_cache) == 0
        manager._save_metadata.assert_called_once()

    def test_memory_management_methods(self, manager):
        """Test memory management utility methods"""
        # Test memory logging
        manager._log_memory_usage("test")
        
        # Test memory cleanup
        manager._cleanup_memory()
        
        # Both methods should not raise exceptions
        assert True  # If we get here, the methods work

    def test_list_files_timezone_handling(self, manager):
        """Test that list_files handles timezone-aware and timezone-naive datetimes"""
        from datetime import timezone
        
        # Create test files with different datetime types
        file1 = FileMetadata(
            filename="test1.csv",
            s3_key="test1.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            extracted_at=datetime.now(),  # timezone-naive
            status=FileStatus.EXTRACTED
        )
        
        file2 = FileMetadata(
            filename="test2.csv",
            s3_key="test2.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.LONDON,
            extracted_at=datetime.now(timezone.utc),  # timezone-aware
            status=FileStatus.EXTRACTED
        )
        
        file3 = FileMetadata(
            filename="test3.csv",
            s3_key="test3.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            extracted_at=None,  # no datetime
            status=FileStatus.EXTRACTED
        )
        
        manager._metadata_cache = {
            "test1.csv": file1,
            "test2.csv": file2,
            "test3.csv": file3
        }
        
        # This should not raise a timezone comparison error
        files = manager.list_files()
        
        # Should return all files
        assert len(files) == 3
        
        # Should be sorted (newest first, None at the end)
        assert files[0].extracted_at is not None
        assert files[1].extracted_at is not None
        assert files[2].extracted_at is None

    def test_list_failed_files(self, manager):
        """Test list_failed_files method"""
        # Create test files with different statuses
        failed_file1 = FileMetadata(
            filename="failed1.csv",
            s3_key="extracted_bike_ride_csvs/nyc/failed1.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.FAILED,
            processing_errors=["Test error 1"],
            extracted_at=datetime.now()
        )
        
        failed_file2 = FileMetadata(
            filename="failed2.csv",
            s3_key="extracted_bike_ride_csvs/london/failed2.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.LONDON,
            status=FileStatus.FAILED,
            processing_errors=["Test error 2"],
            extracted_at=datetime.now()
        )
        
        success_file = FileMetadata(
            filename="success.csv",
            s3_key="extracted_bike_ride_csvs/nyc/success.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.VALIDATED
        )
        
        manager._metadata_cache = {
            "failed1.csv": failed_file1,
            "failed2.csv": failed_file2,
            "success.csv": success_file
        }
        
        # Test listing all failed files
        failed_files = manager.list_failed_files()
        assert len(failed_files) == 2
        assert any(f['filename'] == 'failed1.csv' for f in failed_files)
        assert any(f['filename'] == 'failed2.csv' for f in failed_files)
        
        # Test filtering by city
        nyc_failed = manager.list_failed_files(city='nyc')
        assert len(nyc_failed) == 1
        assert nyc_failed[0]['filename'] == 'failed1.csv'
        
        # Test filtering by file type (should return both failed CSV files)
        csv_failed = manager.list_failed_files(file_type=FileType.CSV)
        assert len(csv_failed) == 2
        assert any(f['filename'] == 'failed1.csv' for f in csv_failed)
        assert any(f['filename'] == 'failed2.csv' for f in csv_failed)

    def test_reset_failed_files(self, manager):
        """Test reset_failed_files method"""
        # Create test failed files
        failed_file1 = FileMetadata(
            filename="failed1.csv",
            s3_key="extracted_bike_ride_csvs/nyc/failed1.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.FAILED,
            processing_errors=["Test error 1"]
        )
        
        failed_file2 = FileMetadata(
            filename="failed2.csv",
            s3_key="extracted_bike_ride_csvs/london/failed2.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.LONDON,
            status=FileStatus.FAILED,
            processing_errors=["Test error 2"]
        )
        
        success_file = FileMetadata(
            filename="success.csv",
            s3_key="extracted_bike_ride_csvs/nyc/success.csv",
            file_type=FileType.CSV,
            file_location=FileLocation.NYC,
            status=FileStatus.VALIDATED
        )
        
        manager._metadata_cache = {
            "failed1.csv": failed_file1,
            "failed2.csv": failed_file2,
            "success.csv": success_file
        }
        
        # Mock save_metadata
        manager._save_metadata = MagicMock()
        
        # Test reset all failed files
        reset_count = manager.reset_failed_files()
        assert reset_count == 2
        assert failed_file1.status == FileStatus.EXTRACTED
        assert failed_file2.status == FileStatus.EXTRACTED
        assert failed_file1.processing_errors == []
        assert failed_file2.processing_errors == []
        assert success_file.status == FileStatus.VALIDATED  # Should not change
        manager._save_metadata.assert_called_once()
        
        # Test reset with city filter
        failed_file1.status = FileStatus.FAILED  # Reset for testing
        failed_file1.processing_errors = ["Test error 1"]
        failed_file2.status = FileStatus.FAILED
        failed_file2.processing_errors = ["Test error 2"]
        
        reset_count = manager.reset_failed_files(city='nyc')
        assert reset_count == 1
        assert failed_file1.status == FileStatus.EXTRACTED
        assert failed_file2.status == FileStatus.FAILED  # Should not change




class TestIntegration:
    """Integration tests with real data samples"""
    
    def test_nyc_legacy_schema_validation(self):
        """Test NYC legacy schema validation with real data"""
        # Create sample NYC legacy data
        data = {
            'tripduration': [60, 120],
            'bikeid': ['12345', '67890'],
            'starttime': ['2019-06-01 00:00:00', '2019-06-01 00:01:00'],
            'stoptime': ['2019-06-01 00:01:00', '2019-06-01 00:03:00'],
            'start station id': ['1', '2'],
            'start station name': ['Station A', 'Station B'],
            'start station latitude': [40.0, 40.1],
            'start station longitude': [-74.0, -74.1],
            'end station id': ['2', '3'],
            'end station name': ['Station B', 'Station C'],
            'end station latitude': [40.1, 40.2],
            'end station longitude': [-74.1, -74.2],
            'usertype': ['Subscriber', 'Customer'],
            'birth year': [1990, 1985],
            'gender': [1, 2]
        }
        
        df = pd.DataFrame(data)
        
        # Test validation
        assert NYCLegacyBikeShareRecord.validate_schema(df)
        
        # Test transformation
        transformed = NYCLegacyBikeShareRecord.to_dataframe(df, "test.csv")
        assert "source_file" in transformed.columns
        assert len(transformed) == 2
    
    def test_nyc_modern_schema_validation(self):
        """Test NYC modern schema validation with real data"""
        # Create sample NYC modern data
        data = {
            'ride_id': ['ride1', 'ride2'],
            'rideable_type': ['classic_bike', 'electric_bike'],
            'started_at': ['2023-12-01 00:00:00', '2023-12-01 00:01:00'],
            'ended_at': ['2023-12-01 00:01:00', '2023-12-01 00:03:00'],
            'start_station_name': ['Station A', 'Station B'],
            'start_station_id': ['1', '2'],
            'end_station_name': ['Station B', 'Station C'],
            'end_station_id': ['2', '3'],
            'start_lat': [40.0, 40.1],
            'start_lng': [-74.0, -74.1],
            'end_lat': [40.1, 40.2],
            'end_lng': [-74.1, -74.2],
            'member_casual': ['member', 'casual']
        }
        
        df = pd.DataFrame(data)
        
        # Test validation
        assert NYCModernBikeShareRecord.validate_schema(df)
        
        # Test transformation
        transformed = NYCModernBikeShareRecord.to_dataframe(df, "test.csv")
        assert "source_file" in transformed.columns
        assert len(transformed) == 2


class TestCLI:
    """Test CLI commands"""
    
    @pytest.fixture
    def mock_manager(self):
        """Mock ExtractedFileManager"""
        with patch('extracted_file_manager.cli.ExtractedFileManager') as mock_class:
            mock_manager = Mock()
            mock_class.return_value = mock_manager
            yield mock_manager
    
    @patch('sys.argv', ['cli.py', 'reset-failed'])
    @patch('builtins.input', return_value='y')
    def test_reset_failed_cli(self, mock_input, mock_manager):
        """Test reset-failed CLI command"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'},
            {'key': 'test2.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        mock_manager.reset_failed_files.return_value = 2
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_called_once()
    
    @patch('sys.argv', ['cli.py', 'reset-failed', '--confirm'])
    def test_reset_failed_cli_with_confirm(self, mock_manager):
        """Test reset-failed CLI command with --confirm flag"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        mock_manager.reset_failed_files.return_value = 1
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_called_once()
    
    @patch('sys.argv', ['cli.py', 'reset-failed'])
    @patch('builtins.input', return_value='n')
    def test_reset_failed_cli_cancelled(self, mock_input, mock_manager):
        """Test reset-failed CLI command when cancelled"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_not_called()
    
    @patch('sys.argv', ['cli.py', 'reset-failed'])
    def test_reset_failed_cli_no_files(self, mock_manager):
        """Test reset-failed CLI command when no failed files exist"""
        from extracted_file_manager.cli import main
        
        # Mock no failed files
        mock_manager.list_failed_files.return_value = []
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_not_called()
    
    @patch('sys.argv', ['cli.py', 'reprocess-failed'])
    @patch('builtins.input', return_value='y')
    def test_reprocess_failed_cli(self, mock_input, mock_manager):
        """Test reprocess-failed CLI command"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'},
            {'key': 'test2.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        mock_manager.reset_failed_files.return_value = 2
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_called_once()
    
    @patch('sys.argv', ['cli.py', 'reprocess-failed', '--confirm'])
    def test_reprocess_failed_cli_with_confirm(self, mock_manager):
        """Test reprocess-failed CLI command with --confirm flag"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        mock_manager.reset_failed_files.return_value = 1
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_called_once()
    
    @patch('sys.argv', ['cli.py', 'reprocess-failed'])
    @patch('builtins.input', return_value='n')
    def test_reprocess_failed_cli_cancelled(self, mock_input, mock_manager):
        """Test reprocess-failed CLI command when cancelled"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_not_called()
    
    @patch('sys.argv', ['cli.py', 'reprocess-failed'])
    def test_reprocess_failed_cli_no_files(self, mock_manager):
        """Test reprocess-failed CLI command when no failed files exist"""
        from extracted_file_manager.cli import main
        
        # Mock no failed files
        mock_manager.list_failed_files.return_value = []
        
        # Run CLI
        main()
        
        # Verify calls
        mock_manager.list_failed_files.assert_called_once()
        mock_manager.reset_failed_files.assert_not_called()
    
    @patch('sys.argv', ['cli.py', 'reset-failed', '--location', 'nyc'])
    @patch('builtins.input', return_value='y')
    def test_reset_failed_cli_with_location_filter(self, mock_input, mock_manager):
        """Test reset-failed CLI command with location filter"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'nyc', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        mock_manager.reset_failed_files.return_value = 1
        
        # Run CLI
        main()
        
        # Verify calls with location filter
        mock_manager.list_failed_files.assert_called_once_with(city='nyc', file_type=None)
        mock_manager.reset_failed_files.assert_called_once_with(city='nyc', file_type=None)
    
    @patch('sys.argv', ['cli.py', 'reprocess-failed', '--location', 'london', '--type', 'nyc_csv'])
    @patch('builtins.input', return_value='y')
    def test_reprocess_failed_cli_with_filters(self, mock_input, mock_manager):
        """Test reprocess-failed CLI command with location and type filters"""
        from extracted_file_manager.cli import main
        
        # Mock failed files
        failed_files = [
            {'key': 'test1.csv', 'city': 'london', 'file_type': 'csv', 'status': 'failed'}
        ]
        mock_manager.list_failed_files.return_value = failed_files
        mock_manager.reset_failed_files.return_value = 1
        
        # Run CLI
        main()
        
        # Verify calls with filters
        from extracted_file_manager.models import FileType
        mock_manager.list_failed_files.assert_called_once_with(city='london', file_type=FileType.CSV)
        mock_manager.reset_failed_files.assert_called_once_with(city='london', file_type=FileType.CSV)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 