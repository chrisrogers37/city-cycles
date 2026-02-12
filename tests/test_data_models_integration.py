"""
Test integration between extracted_file_manager and data_models to ensure
nothing was broken after removing unused methods from data_models.
"""

import pytest
import pandas as pd
import os
import sys

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDataModelsIntegration:
    """Test that data_models integration with extracted_file_manager still works"""
    
    def test_data_models_import(self):
        """Test that all data models can be imported correctly"""
        from data_models import (
            NYCModernBikeShareRecord,
            NYCLegacyBikeShareRecord,
            LondonModernBikeShareRecord,
            LondonLegacyBikeShareRecord
        )
        
        # Verify all models are imported
        assert NYCModernBikeShareRecord is not None
        assert NYCLegacyBikeShareRecord is not None
        assert LondonModernBikeShareRecord is not None
        assert LondonLegacyBikeShareRecord is not None
    
    def test_model_registry(self):
        """Test that the model registry works correctly"""
        from data_models.base import BaseDataRecord
        
        # Verify registry contains all expected models
        registry = BaseDataRecord._registry
        assert len(registry) == 4, f"Expected 4 models, got {len(registry)}"
        
        # Verify each model has required attributes
        for model in registry:
            assert hasattr(model, 'staging_table')
            assert hasattr(model, 's3_prefix')
            assert hasattr(model, '_required_columns')
            assert hasattr(model, 'validate_schema')
            assert hasattr(model, 'to_dataframe')
    
    def test_nyc_legacy_schema_validation(self):
        """Test NYC legacy schema validation with real data"""
        from data_models import NYCLegacyBikeShareRecord
        
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
        from data_models import NYCModernBikeShareRecord
        
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
    
    def test_london_legacy_schema_validation(self):
        """Test London legacy schema validation with real data"""
        from data_models import LondonLegacyBikeShareRecord
        
        # Create sample London legacy data
        data = {
            'Rental Id': ['rental1', 'rental2'],
            'Bike Id': ['bike1', 'bike2'],
            'Start Date': ['18/12/2019 00:00', '18/12/2019 00:01'],
            'End Date': ['18/12/2019 00:01', '18/12/2019 00:03'],
            'StartStation Id': ['1', '2'],
            'StartStation Name': ['Station A', 'Station B'],
            'EndStation Id': ['2', '3'],
            'EndStation Name': ['Station B', 'Station C'],
            'Duration': [60, 120]
        }
        
        df = pd.DataFrame(data)
        
        # Test validation
        assert LondonLegacyBikeShareRecord.validate_schema(df)
        
        # Test transformation
        transformed = LondonLegacyBikeShareRecord.to_dataframe(df, "test.csv")
        assert "source_file" in transformed.columns
        assert len(transformed) == 2
    
    def test_london_modern_schema_validation(self):
        """Test London modern schema validation with real data"""
        from data_models import LondonModernBikeShareRecord
        
        # Create sample London modern data
        data = {
            'Number': ['number1', 'number2'],
            'Bike number': ['bike1', 'bike2'],
            'Bike model': ['model1', 'model2'],
            'Start date': ['2023-03-06 00:00', '2023-03-06 00:01'],
            'End date': ['2023-03-06 00:01', '2023-03-06 00:03'],
            'Total duration': ['60', '120'],
            'Total duration (ms)': [60000, 120000],
            'Start station number': ['1', '2'],
            'Start station': ['Station A', 'Station B'],
            'End station number': ['2', '3'],
            'End station': ['Station B', 'Station C']
        }
        
        df = pd.DataFrame(data)
        
        # Test validation
        assert LondonModernBikeShareRecord.validate_schema(df)
        
        # Test transformation
        transformed = LondonModernBikeShareRecord.to_dataframe(df, "test.csv")
        assert "source_file" in transformed.columns
        assert len(transformed) == 2
    
    def test_extracted_file_manager_can_import_data_models(self):
        """Test that extracted_file_manager can import and use data_models"""
        from extracted_file_manager.manager import ExtractedFileManager
        from data_models.base import BaseDataRecord
        
        # Verify that the manager can access the model registry
        models = BaseDataRecord._registry
        assert len(models) == 4, f"Expected 4 models, got {len(models)}"
        
        # Verify ExtractedFileManager can be instantiated (this would fail if imports are broken)
        # Note: We don't actually instantiate it here since it requires S3 credentials
        assert ExtractedFileManager is not None
    
    def test_required_columns_consistency(self):
        """Test that all models have consistent _required_columns structure"""
        from data_models import (
            NYCModernBikeShareRecord,
            NYCLegacyBikeShareRecord,
            LondonModernBikeShareRecord,
            LondonLegacyBikeShareRecord
        )
        
        models = [
            NYCModernBikeShareRecord,
            NYCLegacyBikeShareRecord,
            LondonModernBikeShareRecord,
            LondonLegacyBikeShareRecord
        ]
        
        for model in models:
            # Verify _required_columns exists and is a list
            assert hasattr(model, '_required_columns')
            assert isinstance(model._required_columns, list)
            assert len(model._required_columns) > 0
            
            # Verify each required column is a string
            for col in model._required_columns:
                assert isinstance(col, str)
    
    def test_staging_table_names(self):
        """Test that all models have valid staging table names"""
        from data_models.base import BaseDataRecord
        
        for model in BaseDataRecord._registry:
            # Verify staging_table exists and is a string
            assert hasattr(model, 'staging_table')
            assert isinstance(model.staging_table, str)
            assert len(model.staging_table) > 0
            
            # Verify staging_table follows naming convention (raw_ prefix)
            assert model.staging_table.startswith('raw_')
    
    def test_s3_prefix_consistency(self):
        """Test that all models have valid S3 prefixes"""
        from data_models.base import BaseDataRecord
        
        for model in BaseDataRecord._registry:
            # Verify s3_prefix exists and is a string
            assert hasattr(model, 's3_prefix')
            assert isinstance(model.s3_prefix, str)
            assert len(model.s3_prefix) > 0
            
            # Verify s3_prefix ends with /
            assert model.s3_prefix.endswith('/')
