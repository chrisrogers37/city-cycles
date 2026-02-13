"""
Tests for streamlit_data_manager/parquet_file_manager.py.

Tests the ensure_local_parquet_files() function that downloads mart Parquet
files from S3 to the local data/ directory for dashboard consumption.

All S3 calls are mocked. No real S3 interactions.
"""

import pytest
import os
from unittest.mock import patch, MagicMock, call


class TestParquetFileManager:
    """Tests for streamlit_data_manager/parquet_file_manager.py."""

    def test_ensure_creates_data_directory(self, tmp_path):
        """ensure_local_parquet_files() should create the data directory if it does not exist."""
        data_dir = str(tmp_path / "data")
        assert not os.path.exists(data_dir)

        mock_s3 = MagicMock()

        with patch("streamlit_data_manager.parquet_file_manager.boto3.client", return_value=mock_s3), \
             patch("streamlit_data_manager.parquet_file_manager.DATA_DIR", data_dir):
            from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files
            ensure_local_parquet_files()

        assert os.path.isdir(data_dir)

    def test_ensure_downloads_missing_files(self, tmp_path):
        """ensure_local_parquet_files() should download files that do not exist locally."""
        data_dir = str(tmp_path / "data")
        mock_s3 = MagicMock()

        with patch("streamlit_data_manager.parquet_file_manager.boto3.client", return_value=mock_s3), \
             patch("streamlit_data_manager.parquet_file_manager.DATA_DIR", data_dir):
            from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files, MARTS
            ensure_local_parquet_files()

        # Should have called download_file once for each mart
        assert mock_s3.download_file.call_count == len(MARTS)

        # Verify each download call used the correct S3 key pattern
        for c in mock_s3.download_file.call_args_list:
            args = c[0]
            assert args[0] == "city-cycles-data-ctr37"  # S3_BUCKET
            assert args[1].startswith("marts/")
            assert args[1].endswith(".parquet")

    def test_ensure_skips_existing_files(self, tmp_path):
        """ensure_local_parquet_files() should NOT re-download files that already exist locally."""
        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir)

        # Pre-create all expected Parquet files as empty files
        from streamlit_data_manager.parquet_file_manager import MARTS
        for mart in MARTS:
            with open(os.path.join(data_dir, mart), "w") as f:
                f.write("placeholder")

        mock_s3 = MagicMock()

        with patch("streamlit_data_manager.parquet_file_manager.boto3.client", return_value=mock_s3), \
             patch("streamlit_data_manager.parquet_file_manager.DATA_DIR", data_dir):
            from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files
            ensure_local_parquet_files()

        # Should NOT have downloaded anything since all files exist
        mock_s3.download_file.assert_not_called()

    def test_ensure_downloads_only_missing_files(self, tmp_path):
        """ensure_local_parquet_files() should only download files that are missing, skipping existing ones."""
        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir)

        from streamlit_data_manager.parquet_file_manager import MARTS

        # Pre-create only the first 2 mart files
        for mart in MARTS[:2]:
            with open(os.path.join(data_dir, mart), "w") as f:
                f.write("placeholder")

        mock_s3 = MagicMock()
        expected_downloads = len(MARTS) - 2  # Only the missing ones

        with patch("streamlit_data_manager.parquet_file_manager.boto3.client", return_value=mock_s3), \
             patch("streamlit_data_manager.parquet_file_manager.DATA_DIR", data_dir):
            from streamlit_data_manager.parquet_file_manager import ensure_local_parquet_files
            ensure_local_parquet_files()

        assert mock_s3.download_file.call_count == expected_downloads

    def test_marts_list_is_complete(self):
        """The MARTS list should contain all 10 expected mart Parquet files."""
        from streamlit_data_manager.parquet_file_manager import MARTS

        expected = [
            "mart_daily_metrics.parquet",
            "mart_hourly_patterns_summary.parquet",
            "mart_nyc_member_analysis.parquet",
            "mart_station_growth.parquet",
            "mart_daily_metrics_long.parquet",
            "mart_hourly_rides.parquet",
            "mart_weather_ride_correlation.parquet",
            "mart_weather_impact_summary.parquet",
            "mart_station_directory.parquet",
            "mart_station_weather_performance.parquet",
        ]

        assert len(MARTS) == 10
        for mart in expected:
            assert mart in MARTS, f"Missing expected mart: {mart}"

    def test_old_mart_hourly_patterns_removed(self):
        """The old mart_hourly_patterns.parquet should NOT be in the MARTS list."""
        from streamlit_data_manager.parquet_file_manager import MARTS
        assert "mart_hourly_patterns.parquet" not in MARTS

    def test_s3_bucket_constant(self):
        """The S3_BUCKET constant should be set to the expected value."""
        from streamlit_data_manager.parquet_file_manager import S3_BUCKET
        assert S3_BUCKET == "city-cycles-data-ctr37"
