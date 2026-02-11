"""
Tests for the extraction module.

Tests extraction/utils.py, extraction/nyc.py, and extraction/london.py.

IMPORTANT: All extraction modules have module-level side effects that require
S3_BUCKET to be set and boto3.client to be mocked BEFORE import. All imports
are done inside test functions using importlib to avoid import-time failures.
"""

import pytest
import os
import sys
import tempfile
import zipfile
from unittest.mock import patch, MagicMock, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _import_utils(mock_boto_client):
    """
    Import extraction.utils with mocked environment and boto3.

    Args:
        mock_boto_client: The MagicMock to return from boto3.client()

    Returns:
        The extraction.utils module (freshly imported)
    """
    # Remove cached module so we get a fresh import with our mocks
    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("extraction"):
            del sys.modules[mod_name]

    with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
         patch("boto3.client", return_value=mock_boto_client):
        import extraction.utils as utils_mod
        return utils_mod


def _import_nyc(mock_boto_client, mock_public_s3=None):
    """
    Import extraction.nyc with mocked environment and boto3.

    Args:
        mock_boto_client: The MagicMock to return for the private S3 client (utils.py)
        mock_public_s3: Optional MagicMock for the public unsigned S3 client.
                        If None, a new MagicMock is created.

    Returns:
        Tuple of (nyc module, public_s3 mock)
    """
    if mock_public_s3 is None:
        mock_public_s3 = MagicMock()

    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("extraction"):
            del sys.modules[mod_name]

    original_mock_boto_client = mock_boto_client
    original_mock_public_s3 = mock_public_s3

    def side_effect_client(*args, **kwargs):
        """
        boto3.client("s3") is called twice during import:
        1. In utils.py (private_s3) -- no config kwarg
        2. In nyc.py (public_s3) -- with config=Config(signature_version=UNSIGNED)
        """
        if "config" in kwargs:
            return original_mock_public_s3
        return original_mock_boto_client

    with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
         patch("boto3.client", side_effect=side_effect_client):
        import extraction.nyc as nyc_mod
        return nyc_mod, original_mock_public_s3


# ---------------------------------------------------------------------------
# Tests for extraction/utils.py
# ---------------------------------------------------------------------------

class TestExtractionUtils:
    """Tests for extraction/utils.py functions."""

    def test_check_s3_bucket_returns_bucket_name(self):
        """check_s3_bucket() should return the bucket name when S3_BUCKET is set."""
        mock_s3 = MagicMock()
        utils = _import_utils(mock_s3)
        result = utils.check_s3_bucket()
        assert result == "test-bucket"

    def test_file_exists_in_s3_returns_true_when_exists(self):
        """file_exists_in_s3() should return True when head_object succeeds."""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 1234}
        utils = _import_utils(mock_s3)

        result = utils.file_exists_in_s3("some/path/file.zip")
        assert result is True
        mock_s3.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="some/path/file.zip"
        )

    def test_file_exists_in_s3_returns_false_on_404(self):
        """file_exists_in_s3() should return False when S3 returns 404."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        utils = _import_utils(mock_s3)
        result = utils.file_exists_in_s3("nonexistent.zip")
        assert result is False

    def test_file_exists_in_s3_raises_on_non_404_error(self):
        """file_exists_in_s3() should re-raise when S3 returns a non-404 error."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "403", "Message": "Forbidden"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        utils = _import_utils(mock_s3)
        with pytest.raises(ClientError):
            utils.file_exists_in_s3("forbidden.zip")

    def test_upload_to_s3_calls_upload_file(self):
        """upload_to_s3() should call s3.upload_file with correct arguments."""
        mock_s3 = MagicMock()
        utils = _import_utils(mock_s3)

        utils.upload_to_s3("/tmp/local_file.csv", "remote/path/file.csv")
        mock_s3.upload_file.assert_called_once_with(
            "/tmp/local_file.csv", "test-bucket", "remote/path/file.csv"
        )


# ---------------------------------------------------------------------------
# Tests for extraction/nyc.py
# ---------------------------------------------------------------------------

class TestExtractionNYC:
    """Tests for extraction/nyc.py functions."""

    def test_is_valid_zip_returns_true_for_valid_zip(self):
        """is_valid_zip() should return True for a properly formed ZIP file."""
        mock_s3 = MagicMock()
        nyc, _ = _import_nyc(mock_s3)

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with zipfile.ZipFile(tmp_path, "w") as zf:
                zf.writestr("test.csv", "col1,col2\n1,2\n")
            assert nyc.is_valid_zip(tmp_path) is True
        finally:
            os.unlink(tmp_path)

    def test_is_valid_zip_returns_false_for_corrupt_file(self):
        """is_valid_zip() should return False for a corrupt/non-ZIP file."""
        mock_s3 = MagicMock()
        nyc, _ = _import_nyc(mock_s3)

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, mode="w") as tmp:
            tmp.write("this is not a zip file")
            tmp_path = tmp.name
        try:
            assert nyc.is_valid_zip(tmp_path) is False
        finally:
            os.unlink(tmp_path)

    def test_list_nyc_citibike_files_filters_by_year_and_extension(self):
        """list_nyc_citibike_files() should return only .zip files in the requested year range."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        mock_paginator = MagicMock()
        mock_public_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "201901-citibike-tripdata.csv.zip"},
                    {"Key": "202001-citibike-tripdata.csv.zip"},
                    {"Key": "202312-citibike-tripdata.csv.zip"},
                    {"Key": "201801-citibike-tripdata.csv.zip"},  # Before start_year
                    {"Key": "some-readme.txt"},  # Not a .zip
                ]
            }
        ]

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        files = nyc.list_nyc_citibike_files(start_year=2019, end_year=2023)

        assert "201901-citibike-tripdata.csv.zip" in files
        assert "202001-citibike-tripdata.csv.zip" in files
        assert "202312-citibike-tripdata.csv.zip" in files
        assert "201801-citibike-tripdata.csv.zip" not in files
        assert "some-readme.txt" not in files
        assert len(files) == 3

    def test_download_and_store_zip_skips_when_already_exists(self):
        """download_and_store_zip() should return False if the file already exists in S3."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        mock_s3.head_object.return_value = {"ContentLength": 999}
        mock_s3.exceptions.ClientError = Exception

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        result = nyc.download_and_store_zip("202301-citibike-tripdata.csv.zip")

        assert result is False
        mock_public_s3.download_file.assert_not_called()

    def test_download_and_store_zip_downloads_valid_zip(self):
        """download_and_store_zip() should download, validate, and upload a valid ZIP."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        def fake_download(bucket, key, dest_path):
            with zipfile.ZipFile(dest_path, "w") as zf:
                zf.writestr("data.csv", "col1,col2\n1,2\n")

        mock_public_s3.download_file.side_effect = fake_download

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        result = nyc.download_and_store_zip("202301-citibike-tripdata.csv.zip")

        assert result is True
        mock_s3.upload_file.assert_called_once()
        upload_call_args = mock_s3.upload_file.call_args
        assert upload_call_args[0][2] == "extracted_bike_ride_zips/nyc/202301-citibike-tripdata.csv.zip"

    def test_download_and_store_zip_rejects_invalid_zip(self):
        """download_and_store_zip() should return False if the downloaded file is not a valid ZIP."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        def fake_download(bucket, key, dest_path):
            with open(dest_path, "w") as f:
                f.write("not a zip")

        mock_public_s3.download_file.side_effect = fake_download

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        result = nyc.download_and_store_zip("202301-citibike-tripdata.csv.zip")

        assert result is False
        mock_s3.upload_file.assert_not_called()

    def test_download_and_store_zip_handles_download_failure(self):
        """download_and_store_zip() should return False if the download raises an exception."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        mock_public_s3.download_file.side_effect = ConnectionError("Network error")

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        result = nyc.download_and_store_zip("202301-citibike-tripdata.csv.zip")

        assert result is False
        mock_s3.upload_file.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for extraction/london.py
# ---------------------------------------------------------------------------

class TestExtractionLondon:
    """Tests for extraction/london.py functions."""

    def test_download_and_store_csv_skips_existing_file(self):
        """download_and_store_csv() should return False if the file already exists in S3."""
        mock_s3 = MagicMock()

        mock_s3.head_object.return_value = {"ContentLength": 999}
        mock_s3.exceptions.ClientError = Exception

        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("extraction"):
                del sys.modules[mod_name]

        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
             patch("boto3.client", return_value=mock_s3):
            from extraction.london import download_and_store_csv

        result = download_and_store_csv(
            "https://cycling.data.tfl.gov.uk/usage-stats/123JourneyDataExtract.csv",
            "123JourneyDataExtract.csv",
        )

        assert result is False

    def test_download_and_store_csv_downloads_new_file(self):
        """download_and_store_csv() should download and upload a new CSV file."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("extraction"):
                del sys.modules[mod_name]

        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
             patch("boto3.client", return_value=mock_s3):
            from extraction.london import download_and_store_csv

        mock_response = MagicMock()
        mock_response.content = b"col1,col2\nval1,val2\n"
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.london.requests.get", return_value=mock_response):
            result = download_and_store_csv(
                "https://cycling.data.tfl.gov.uk/usage-stats/360JourneyDataExtract06Mar2023-12Mar2023.csv",
                "360JourneyDataExtract06Mar2023-12Mar2023.csv",
            )

        assert result is True
        mock_s3.upload_file.assert_called_once()

    def test_download_and_store_csv_handles_xls_extension(self):
        """download_and_store_csv() should rename .xls files to .csv before uploading."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("extraction"):
                del sys.modules[mod_name]

        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
             patch("boto3.client", return_value=mock_s3):
            from extraction.london import download_and_store_csv

        mock_response = MagicMock()
        mock_response.content = b"col1,col2\nval1,val2\n"
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.london.requests.get", return_value=mock_response):
            result = download_and_store_csv(
                "https://example.com/data.xls",
                "data.xls",
            )

        assert result is True
        upload_call = mock_s3.upload_file.call_args
        s3_key = upload_call[0][2]
        assert s3_key.endswith(".csv")
        assert not s3_key.endswith(".xls")

    def test_download_and_store_csv_returns_false_on_http_error(self):
        """download_and_store_csv() should return False if the HTTP request fails."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("extraction"):
                del sys.modules[mod_name]

        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
             patch("boto3.client", return_value=mock_s3):
            from extraction.london import download_and_store_csv

        from requests.exceptions import RequestException
        with patch("extraction.london.requests.get", side_effect=RequestException("Connection refused")):
            result = download_and_store_csv(
                "https://cycling.data.tfl.gov.uk/bad-url.csv",
                "bad-url.csv",
            )

        assert result is False
        mock_s3.upload_file.assert_not_called()
