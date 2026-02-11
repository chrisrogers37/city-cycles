"""
Tests for Current Extracted File Manager

Tests the simplified file management system that uses file existence checks
instead of complex metadata tracking.
"""

import pytest
import tempfile
import os
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
import zipfile
from datetime import datetime

from botocore.exceptions import ClientError

from extracted_file_manager import ExtractedFileManager
from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord
from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord


class TestExtractedFileManager:
    """Test the simplified ExtractedFileManager"""
    
    @pytest.fixture
    def manager(self):
        """Create a manager instance for testing"""
        with patch.dict(os.environ, {'S3_BUCKET': 'test-bucket'}):
            return ExtractedFileManager()
    
    @pytest.fixture
    def mock_s3_client(self, manager):
        """Mock S3 client"""
        with patch.object(manager, 's3_client') as mock_client:
            yield mock_client
    
    def test_init_with_bucket(self):
        """Test initialization with explicit bucket"""
        manager = ExtractedFileManager(s3_bucket="explicit-bucket")
        assert manager.s3_bucket == "explicit-bucket"
    
    def test_init_with_env_var(self):
        """Test initialization with environment variable"""
        with patch.dict(os.environ, {'S3_BUCKET': 'env-bucket'}):
            manager = ExtractedFileManager()
            assert manager.s3_bucket == "env-bucket"
    
    def test_init_without_bucket_raises_error(self):
        """Test initialization without bucket raises error"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="S3_BUCKET environment variable is not set"):
                ExtractedFileManager()
    
    def test_init_with_batch_size(self):
        """Test initialization with custom batch size"""
        with patch.dict(os.environ, {'S3_BUCKET': 'test-bucket'}):
            manager = ExtractedFileManager(batch_size=10)
            assert manager.batch_size == 10
    
    def test_find_matching_model_nyc_modern(self, manager):
        """Test finding NYC modern model"""
        # Create sample data that matches NYC modern schema
        df = pd.DataFrame({
            'ride_id': ['test1'],
            'rideable_type': ['electric_bike'],
            'started_at': ['2023-01-01 12:00:00'],
            'ended_at': ['2023-01-01 12:30:00'],
            'start_station_name': ['Test Station'],
            'start_station_id': ['123'],
            'end_station_name': ['End Station'],
            'end_station_id': ['456'],
            'start_lat': [40.7],
            'start_lng': [-74.0],
            'end_lat': [40.7],
            'end_lng': [-74.0],
            'member_casual': ['member']
        })
        
        model = manager._find_matching_model(df, "nyc_test.csv")
        assert model == NYCModernBikeShareRecord
    
    def test_find_matching_model_nyc_legacy(self, manager):
        """Test finding NYC legacy model"""
        # Create sample data that matches NYC legacy schema
        df = pd.DataFrame({
            'tripduration': [300],
            'starttime': ['2023-01-01 12:00:00'],
            'stoptime': ['2023-01-01 12:05:00'],
            'start station id': ['123'],
            'start station name': ['Test Station'],
            'start station latitude': [40.7],
            'start station longitude': [-74.0],
            'end station id': ['456'],
            'end station name': ['End Station'],
            'end station latitude': [40.7],
            'end station longitude': [-74.0],
            'bikeid': ['12345'],
            'usertype': ['Subscriber'],
            'birth year': [1990],
            'gender': [1]
        })
        
        model = manager._find_matching_model(df, "nyc_test.csv")
        assert model == NYCLegacyBikeShareRecord
    
    def test_find_matching_model_london_modern(self, manager):
        """Test finding London modern model"""
        # Create sample data that matches London modern schema
        df = pd.DataFrame({
            'Number': ['test1'],
            'Bike number': ['12345'],
            'Bike model': ['Standard'],
            'Start date': ['2023-01-01 12:00:00'],
            'End date': ['2023-01-01 12:05:00'],
            'Total duration': ['5 minutes'],
            'Total duration (ms)': [300000],
            'Start station number': ['123'],
            'Start station': ['Test Station'],
            'End station number': ['456'],
            'End station': ['End Station']
        })
        
        model = manager._find_matching_model(df, "london_test.csv")
        assert model == LondonModernBikeShareRecord
    
    def test_find_matching_model_london_legacy(self, manager):
        """Test finding London legacy model"""
        # Create sample data that matches London legacy schema
        df = pd.DataFrame({
            'Rental Id': ['test1'],
            'Duration': [300],
            'Bike Id': ['12345'],
            'End Date': ['01/01/2023 12:05:00'],
            'EndStation Id': ['456'],
            'EndStation Name': ['End Station'],
            'Start Date': ['01/01/2023 12:00:00'],
            'StartStation Id': ['123'],
            'StartStation Name': ['Test Station']
        })
        
        model = manager._find_matching_model(df, "london_test.csv")
        assert model == LondonLegacyBikeShareRecord
    
    def test_find_matching_model_no_match(self, manager):
        """Test finding no matching model"""
        # Create sample data that doesn't match any schema
        df = pd.DataFrame({
            'unknown_column': ['test']
        })
        
        model = manager._find_matching_model(df, "unknown.csv")
        assert model is None
    
    def test_file_exists_in_s3_exists(self, manager, mock_s3_client):
        """Test checking if file exists in S3 (exists)"""
        mock_s3_client.head_object.return_value = {}
        
        result = manager._file_exists_in_s3("test/path/file.csv")
        assert result is True
        mock_s3_client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test/path/file.csv"
        )
    
    def test_file_exists_in_s3_not_exists(self, manager, mock_s3_client):
        """Test checking if file exists in S3 (not exists)"""
        mock_s3_client.head_object.side_effect = ClientError(
            error_response={'Error': {'Code': '404', 'Message': 'Not Found'}},
            operation_name='HeadObject'
        )

        result = manager._file_exists_in_s3("test/path/file.csv")
        assert result is False
    
    def test_list_s3_files(self, manager, mock_s3_client):
        """Test listing S3 files"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test/file1.csv'},
                {'Key': 'test/file2.csv'}
            ]
        }
        
        files = manager._list_s3_files("test/")
        assert files == ['test/file1.csv', 'test/file2.csv']
    
    def test_list_s3_files_empty(self, manager, mock_s3_client):
        """Test listing S3 files (empty)"""
        mock_s3_client.list_objects_v2.return_value = {}
        
        files = manager._list_s3_files("test/")
        assert files == []
    
    def test_parquet_exists_for_csv(self, manager, mock_s3_client):
        """Test checking if parquet exists for CSV (cache-based)"""
        # Mock the paginator for list_objects_v2
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'extracted_bike_ride_parquet/nyc/nycmodernbikesharerecord/test.parquet'}
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator

        result = manager._parquet_exists_for_csv("extracted_bike_ride_csvs/nyc/test.csv")
        assert result is True
        mock_s3_client.get_paginator.assert_called_once_with('list_objects_v2')

    def test_parquet_exists_for_csv_not_found(self, manager, mock_s3_client):
        """Test checking if parquet exists for CSV (not found, cache-based)"""
        # Mock an empty paginator result (no parquet files exist)
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': []}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator

        result = manager._parquet_exists_for_csv("extracted_bike_ride_csvs/nyc/test.csv")
        assert result is False


class TestDataModelsIntegration:
    """Test integration with data models"""
    
    def test_nyc_modern_schema_validation(self):
        """Test NYC modern schema validation"""
        # Valid NYC modern data
        df = pd.DataFrame({
            'ride_id': ['test1'],
            'rideable_type': ['electric_bike'],
            'started_at': ['2023-01-01 12:00:00'],
            'ended_at': ['2023-01-01 12:30:00'],
            'start_station_name': ['Test Station'],
            'start_station_id': ['123'],
            'end_station_name': ['End Station'],
            'end_station_id': ['456'],
            'start_lat': [40.7],
            'start_lng': [-74.0],
            'end_lat': [40.7],
            'end_lng': [-74.0],
            'member_casual': ['member']
        })
        
        assert NYCModernBikeShareRecord.validate_schema(df) is True
    
    def test_nyc_legacy_schema_validation(self):
        """Test NYC legacy schema validation"""
        # Valid NYC legacy data
        df = pd.DataFrame({
            'tripduration': [300],
            'starttime': ['2023-01-01 12:00:00'],
            'stoptime': ['2023-01-01 12:05:00'],
            'start station id': ['123'],
            'start station name': ['Test Station'],
            'start station latitude': [40.7],
            'start station longitude': [-74.0],
            'end station id': ['456'],
            'end station name': ['End Station'],
            'end station latitude': [40.7],
            'end station longitude': [-74.0],
            'bikeid': ['12345'],
            'usertype': ['Subscriber'],
            'birth year': [1990],
            'gender': [1]
        })
        
        assert NYCLegacyBikeShareRecord.validate_schema(df) is True
    
    def test_london_modern_schema_validation(self):
        """Test London modern schema validation"""
        # Valid London modern data
        df = pd.DataFrame({
            'Number': ['test1'],
            'Bike number': ['12345'],
            'Bike model': ['Standard'],
            'Start date': ['2023-01-01 12:00:00'],
            'End date': ['2023-01-01 12:05:00'],
            'Total duration': ['5 minutes'],
            'Total duration (ms)': [300000],
            'Start station number': ['123'],
            'Start station': ['Test Station'],
            'End station number': ['456'],
            'End station': ['End Station']
        })
        
        assert LondonModernBikeShareRecord.validate_schema(df) is True
    
    def test_london_legacy_schema_validation(self):
        """Test London legacy schema validation"""
        # Valid London legacy data
        df = pd.DataFrame({
            'Rental Id': ['test1'],
            'Duration': [300],
            'Bike Id': ['12345'],
            'End Date': ['01/01/2023 12:05:00'],
            'EndStation Id': ['456'],
            'EndStation Name': ['End Station'],
            'Start Date': ['01/01/2023 12:00:00'],
            'StartStation Id': ['123'],
            'StartStation Name': ['Test Station']
        })
        
        assert LondonLegacyBikeShareRecord.validate_schema(df) is True
