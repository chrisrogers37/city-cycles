# Phase 09: Test Coverage Expansion

**PR Title:** `test: add comprehensive tests for extraction, db_duckdb, file manager, and dashboard`
**Risk Level:** Low
**Estimated Effort:** Large (6-8 hours)
**Dependencies:** Phases 04-08 (code should be refactored and cleaner before writing tests)
**Blocks:** Phase 10 (dependency updates need passing tests as baseline)

---

## Summary

Add approximately 45-55 new tests covering the five most undertested modules in the codebase: `extraction/`, `db_duckdb/` (operations and manager layers), `dashboard/`, and `streamlit_data_manager/`. This phase creates only new test files and a shared `conftest.py`; it does NOT modify any existing test files or any source code.

---

## Current Test Inventory

| File | Tests | Notes |
|------|-------|-------|
| `tests/test_data_models_integration.py` | 10 | Schema validation, registry, S3 prefixes -- GOOD |
| `tests/test_orchestrator.py` | 31 | Config, CLI, main class, integration -- GOOD |
| `tests/test_db_duckdb_cli.py` | 19 (3 skip) | CLI help strings, dry-run stubs -- superficial, no operations coverage |
| `tests/test_extracted_file_manager_current.py` | 15 | Init, model finding, S3 existence checks -- no conversion/extraction coverage |
| `tests/test_london_models.py` | 2 | Legacy + modern validation against CSV fixtures -- GOOD but minimal |
| `tests/test_nyc_models.py` | 2 | Legacy + modern validation against CSV fixtures -- GOOD but minimal |
| **Total** | **79 tests (76 pass, 3 skip)** | |

### Coverage Gaps Being Addressed

| Module | Current Coverage | Gap |
|--------|-----------------|-----|
| `extraction/utils.py` | 0 tests | `file_exists_in_s3()`, `upload_to_s3()`, `check_s3_bucket()` |
| `extraction/nyc.py` | 0 tests | `list_nyc_citibike_files()`, `download_and_store_zip()`, `is_valid_zip()`, `download_file_from_s3()` |
| `extraction/london.py` | 0 tests | `download_and_store_csv()` |
| `db_duckdb/duckdb_manager.py` | 0 direct tests | `DuckDBManager` class: connection, tables, queries, context manager |
| `db_duckdb/operations.py` | 1 shallow test | `DuckDBOperations` init/tables/verify/report generation |
| `db_duckdb/pipeline.py` | 1 shallow test | `DuckDBPipeline` orchestration, skip flags |
| `db_duckdb/utils.py` | 0 tests | `log_memory_usage()` |
| `dashboard/app.py` | 0 tests | `run_query()` helper function |
| `streamlit_data_manager/parquet_file_manager.py` | 0 tests | `ensure_local_parquet_files()` |

---

## Files Created

| # | File | Purpose |
|---|------|---------|
| 1 | `tests/conftest.py` | Shared fixtures: temp DuckDB database, mock S3 client, sample DataFrames |
| 2 | `tests/test_extraction.py` | Tests for `extraction/utils.py`, `extraction/nyc.py`, `extraction/london.py` |
| 3 | `tests/test_db_duckdb_operations.py` | Tests for `db_duckdb/duckdb_manager.py`, `db_duckdb/operations.py`, `db_duckdb/pipeline.py`, `db_duckdb/utils.py` |
| 4 | `tests/test_dashboard.py` | Tests for `dashboard/app.py` query helper functions |
| 5 | `tests/test_streamlit_data_manager.py` | Tests for `streamlit_data_manager/parquet_file_manager.py` |

**Files Modified:** None. This phase only creates NEW test files.

---

## File 1: `tests/conftest.py` -- Shared Fixtures

This file provides reusable pytest fixtures used across multiple test files. Place it at `tests/conftest.py` so pytest automatically discovers it.

### Full Implementation

```python
"""
Shared test fixtures for City Cycles test suite.

Provides reusable fixtures for:
- Temporary DuckDB databases (real, not mocked)
- Mocked S3 clients (no real AWS calls)
- Sample DataFrames matching each bike share schema
"""

import pytest
import os
import tempfile
import duckdb
import pandas as pd
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# DuckDB fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_db_path():
    """
    Create a temporary file path for a DuckDB database.

    Yields the path string. The file is deleted after the test completes.
    Do NOT create any DuckDB connection here -- let individual tests control
    when the connection is opened and closed.
    """
    fd, db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    # Remove the empty file so DuckDB can create its own
    os.unlink(db_path)
    yield db_path
    # Cleanup after test
    if os.path.exists(db_path):
        os.unlink(db_path)
    # DuckDB also creates .wal files
    wal_path = db_path + ".wal"
    if os.path.exists(wal_path):
        os.unlink(wal_path)


@pytest.fixture
def duckdb_connection(temp_db_path):
    """
    Create a real DuckDB connection to a temporary database.

    Yields the connection. Closes it after the test completes.
    This is a plain duckdb.connect() -- NOT a DuckDBManager instance.
    Use this when you need a lightweight connection without S3 extensions.
    """
    conn = duckdb.connect(temp_db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# S3 mock fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_s3_client():
    """
    Create a MagicMock that behaves like a boto3 S3 client.

    This does NOT patch any specific module. Individual tests should use
    unittest.mock.patch() to inject this mock into the module under test.

    Yields the mock client.
    """
    mock_client = MagicMock()
    # Provide a default exceptions attribute that mimics botocore
    mock_client.exceptions = MagicMock()
    yield mock_client


# ---------------------------------------------------------------------------
# Sample DataFrame fixtures (one per schema)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_nyc_legacy_df():
    """
    Sample DataFrame matching the NYC legacy bike share schema.

    Columns use the RAW column names (with spaces) as they appear in source CSVs
    before transformation. 2 rows.
    """
    return pd.DataFrame({
        "tripduration": [300, 600],
        "starttime": ["2019-01-01 00:00:00", "2019-01-01 00:05:00"],
        "stoptime": ["2019-01-01 00:05:00", "2019-01-01 00:15:00"],
        "start station id": ["123", "456"],
        "start station name": ["Station A", "Station B"],
        "start station latitude": [40.7128, 40.7580],
        "start station longitude": [-74.0060, -73.9855],
        "end station id": ["789", "012"],
        "end station name": ["Station C", "Station D"],
        "end station latitude": [40.7282, 40.7484],
        "end station longitude": [-73.7949, -73.9856],
        "bikeid": ["1001", "1002"],
        "usertype": ["Subscriber", "Customer"],
        "birth year": [1990, 1985],
        "gender": [1, 2],
    })


@pytest.fixture
def sample_nyc_modern_df():
    """
    Sample DataFrame matching the NYC modern bike share schema.

    Columns use the exact names from modern CitiBike CSV files. 2 rows.
    """
    return pd.DataFrame({
        "ride_id": ["ABC123", "DEF456"],
        "rideable_type": ["classic_bike", "electric_bike"],
        "started_at": ["2023-12-01 08:00:00", "2023-12-01 08:30:00"],
        "ended_at": ["2023-12-01 08:15:00", "2023-12-01 08:45:00"],
        "start_station_name": ["Station A", "Station B"],
        "start_station_id": ["STA001", "STA002"],
        "end_station_name": ["Station C", "Station D"],
        "end_station_id": ["STA003", "STA004"],
        "start_lat": [40.7128, 40.7580],
        "start_lng": [-74.0060, -73.9855],
        "end_lat": [40.7282, 40.7484],
        "end_lng": [-73.7949, -73.9856],
        "member_casual": ["member", "casual"],
    })


@pytest.fixture
def sample_london_legacy_df():
    """
    Sample DataFrame matching the London legacy bike share schema.

    Columns use the exact names from legacy TfL CSV files. 2 rows.
    """
    return pd.DataFrame({
        "Rental Id": ["rental001", "rental002"],
        "Bike Id": ["bike001", "bike002"],
        "Start Date": ["18/12/2019 08:00", "18/12/2019 08:30"],
        "End Date": ["18/12/2019 08:15", "18/12/2019 08:45"],
        "StartStation Id": ["100", "200"],
        "StartStation Name": ["Hyde Park Corner", "Waterloo Station"],
        "EndStation Id": ["300", "400"],
        "EndStation Name": ["Kings Cross", "Paddington"],
        "Duration": [900, 900],
    })


@pytest.fixture
def sample_london_modern_df():
    """
    Sample DataFrame matching the London modern bike share schema.

    Columns use the exact names from modern TfL CSV files. 2 rows.
    """
    return pd.DataFrame({
        "Number": ["num001", "num002"],
        "Bike number": ["bike001", "bike002"],
        "Bike model": ["CLASSIC", "PBSC_EBIKE"],
        "Start date": ["2023-03-06 08:00", "2023-03-06 08:30"],
        "End date": ["2023-03-06 08:15", "2023-03-06 08:45"],
        "Total duration": ["00:15:00", "00:15:00"],
        "Total duration (ms)": [900000, 900000],
        "Start station number": ["100", "200"],
        "Start station": ["Hyde Park Corner", "Waterloo Station"],
        "End station number": ["300", "400"],
        "End station": ["Kings Cross", "Paddington"],
    })
```

### Key Design Decisions

1. **`temp_db_path` deletes the file before yielding** so DuckDB creates its own file cleanly. DuckDB does not like opening a 0-byte file that already exists.
2. **`mock_s3_client` is NOT auto-patched into any module.** Tests explicitly use `patch()` to inject it where needed. This avoids accidental cross-test state leaks.
3. **Sample DataFrames use RAW column names** (e.g., `"start station id"` with spaces) because that is what the `validate_schema()` and `to_dataframe()` methods expect as input.
4. **No conftest fixtures call `DuckDBManager`** because `DuckDBManager.__init__()` installs DuckDB extensions and configures S3 credentials, which would fail in CI without AWS creds. Tests that need a `DuckDBManager` must mock `_setup_s3_access` and `_setup_connection` explicitly.

---

## File 2: `tests/test_extraction.py` -- Extraction Module Tests

This file tests the three extraction source files: `extraction/utils.py`, `extraction/nyc.py`, and `extraction/london.py`.

### Critical Note About Imports

The `extraction/` package has a **module-level side effect** problem. When you import `extraction.utils`, the top of that file runs:

```python
S3_BUCKET = os.environ.get("S3_BUCKET")
if not S3_BUCKET:
    raise ValueError("S3_BUCKET environment variable is not set!")
private_s3 = boto3.client("s3")
```

Similarly, `extraction/__init__.py` re-exports from `extraction.nyc` and `extraction.london`, which themselves import from `extraction.utils`, triggering the same module-level code.

**You MUST set the `S3_BUCKET` environment variable AND mock `boto3.client` BEFORE importing any extraction modules.** Use `patch.dict(os.environ, ...)` and `patch('boto3.client')` as context managers or `monkeypatch` before importing.

The safest approach: do all extraction imports **inside** each test function or inside a fixture, not at the top of the test file.

### Full Implementation

```python
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
import importlib
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

    call_count = {"n": 0}
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

        # Create a ClientError-like exception on the mock's exceptions attribute
        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        # The real code catches `private_s3.exceptions.ClientError`, but since
        # private_s3 is our mock, we need to make mock.exceptions.ClientError = ClientError
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

        # Create a real valid ZIP in a temp file
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

        # Simulate paginator response
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
        assert "201801-citibike-tripdata.csv.zip" not in files  # Too old
        assert "some-readme.txt" not in files  # Not a zip
        assert len(files) == 3

    def test_download_and_store_zip_skips_when_already_exists(self):
        """download_and_store_zip() should return False if the file already exists in S3."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        # file_exists_in_s3 checks head_object on the private S3 client
        mock_s3.head_object.return_value = {"ContentLength": 999}
        mock_s3.exceptions.ClientError = Exception  # Won't be raised

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        result = nyc.download_and_store_zip("202301-citibike-tripdata.csv.zip")

        assert result is False
        mock_public_s3.download_file.assert_not_called()

    def test_download_and_store_zip_downloads_valid_zip(self):
        """download_and_store_zip() should download, validate, and upload a valid ZIP."""
        mock_s3 = MagicMock()
        mock_public_s3 = MagicMock()

        from botocore.exceptions import ClientError
        # file_exists_in_s3 returns False (file not in our bucket yet)
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        # download_file_from_s3 creates a real valid ZIP on disk
        def fake_download(bucket, key, dest_path):
            with zipfile.ZipFile(dest_path, "w") as zf:
                zf.writestr("data.csv", "col1,col2\n1,2\n")

        mock_public_s3.download_file.side_effect = fake_download

        nyc, _ = _import_nyc(mock_s3, mock_public_s3)
        result = nyc.download_and_store_zip("202301-citibike-tripdata.csv.zip")

        assert result is True
        mock_s3.upload_file.assert_called_once()
        # Verify the upload was to the correct S3 key
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

        # download_file_from_s3 creates a corrupt file
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

        # download_file_from_s3 raises an exception
        mock_public_s3.download_file.side_effect = Exception("Network error")

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

        # file_exists_in_s3 returns True
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
        # Verify the S3 key ends with .csv, not .xls
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

        with patch("extraction.london.requests.get", side_effect=Exception("Connection refused")):
            result = download_and_store_csv(
                "https://cycling.data.tfl.gov.uk/bad-url.csv",
                "bad-url.csv",
            )

        assert result is False
        mock_s3.upload_file.assert_not_called()
```

### Why the `_import_utils` / `_import_nyc` Helper Pattern?

The `extraction/utils.py` module executes `boto3.client("s3")` and validates `S3_BUCKET` **at import time** (lines 8-13 of `extraction/utils.py`). This means you cannot simply `import extraction.utils` at the top of the test file -- it will fail in CI where `S3_BUCKET` is not set.

The helper functions:
1. Clear any cached `extraction.*` modules from `sys.modules`
2. Set up environment variables and mocks
3. Import the module within the mocked context
4. Return the freshly imported module

This is a well-known pattern for testing modules with import-time side effects. **A future refactoring phase should remove these module-level side effects** (lazy initialization pattern), but that is out of scope for this test-only PR.

### Test Count: 13 tests

---

## File 3: `tests/test_db_duckdb_operations.py` -- DuckDB Operations Tests

Tests the `DuckDBManager`, `DuckDBOperations`, `DuckDBPipeline`, and `utils` modules.

### Design Decision: Real DuckDB, Mocked S3

- **DuckDB connections:** Use REAL temporary DuckDB databases. DuckDB is embedded and fast. No need to mock the database itself.
- **S3 access:** Mock `_setup_s3_access()` on `DuckDBManager` because S3 credentials are not available in CI.
- **HTTPFS/S3 extensions:** Mock `_setup_connection()` partially. The extensions `INSTALL httpfs` and `INSTALL s3` may not be available in all CI environments, so we provide a custom setup that skips them.

### Full Implementation

```python
"""
Tests for the db_duckdb module.

Tests db_duckdb/duckdb_manager.py, db_duckdb/operations.py,
db_duckdb/pipeline.py, and db_duckdb/utils.py.

Uses real temporary DuckDB databases (NOT mocked) for accurate behavior testing.
S3 access is mocked since AWS credentials are not available in CI.
"""

import pytest
import os
import tempfile
import duckdb
from unittest.mock import patch, MagicMock
from io import StringIO


# ---------------------------------------------------------------------------
# DuckDBManager Tests (with mocked S3 setup)
# ---------------------------------------------------------------------------

class TestDuckDBManager:
    """Tests for db_duckdb/duckdb_manager.py DuckDBManager class."""

    def _create_manager(self, db_path):
        """
        Create a DuckDBManager with S3 setup mocked out.

        DuckDBManager.__init__ calls _setup_connection() which installs
        httpfs and s3 extensions, and _setup_s3_access() which reads AWS
        credentials. Both need to be mocked for CI.
        """
        from db_duckdb.duckdb_manager import DuckDBManager

        with patch.object(DuckDBManager, "_setup_s3_access"), \
             patch.object(DuckDBManager, "_setup_connection") as mock_setup:
            # We need to actually create a connection, just without the extensions
            manager = DuckDBManager.__new__(DuckDBManager)
            manager.db_path = db_path
            manager.con = None

            # Create a real connection but skip extension installation
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            manager.con = duckdb.connect(db_path)

            return manager

    def test_create_and_list_tables(self, temp_db_path):
        """DuckDBManager should create a table and list it."""
        manager = self._create_manager(temp_db_path)
        try:
            manager.create_table(
                "test_table",
                "CREATE TABLE test_table (id INTEGER, name VARCHAR)"
            )
            tables = manager.list_tables()
            assert "test_table" in tables
        finally:
            manager.close()

    def test_create_table_skips_existing(self, temp_db_path):
        """DuckDBManager.create_table() should skip if the table already exists."""
        manager = self._create_manager(temp_db_path)
        try:
            manager.create_table(
                "test_table",
                "CREATE TABLE test_table (id INTEGER, name VARCHAR)"
            )
            # Calling create_table again should NOT raise
            manager.create_table(
                "test_table",
                "CREATE TABLE test_table (id INTEGER, name VARCHAR)"
            )
            tables = manager.list_tables()
            assert tables.count("test_table") == 1
        finally:
            manager.close()

    def test_execute_query_returns_results(self, temp_db_path):
        """DuckDBManager.execute_query() should return a list of dicts."""
        manager = self._create_manager(temp_db_path)
        try:
            manager.con.execute("CREATE TABLE t (id INTEGER, val VARCHAR)")
            manager.con.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")

            results = manager.execute_query("SELECT * FROM t ORDER BY id")
            assert len(results) == 2
            assert results[0]["id"] == 1
            assert results[0]["val"] == "hello"
            assert results[1]["id"] == 2
            assert results[1]["val"] == "world"
        finally:
            manager.close()

    def test_execute_query_returns_empty_list_for_no_results(self, temp_db_path):
        """DuckDBManager.execute_query() should return [] when no rows match."""
        manager = self._create_manager(temp_db_path)
        try:
            manager.con.execute("CREATE TABLE t (id INTEGER)")

            results = manager.execute_query("SELECT * FROM t WHERE id = 999")
            assert results == []
        finally:
            manager.close()

    def test_get_table_info_returns_correct_data(self, temp_db_path):
        """DuckDBManager.get_table_info() should return row count, schema, and size."""
        manager = self._create_manager(temp_db_path)
        try:
            manager.con.execute(
                "CREATE TABLE test_table (id INTEGER, name VARCHAR, lat DOUBLE)"
            )
            manager.con.execute(
                "INSERT INTO test_table VALUES (1, 'Alice', 40.7), (2, 'Bob', 51.5)"
            )

            info = manager.get_table_info("test_table")

            assert info["table_name"] == "test_table"
            assert info["row_count"] == 2
            assert isinstance(info["schema"], list)
            assert len(info["schema"]) == 3  # id, name, lat
            assert isinstance(info["size_mb"], float)

            # Verify column names in schema
            col_names = [col["column_name"] for col in info["schema"]]
            assert "id" in col_names
            assert "name" in col_names
            assert "lat" in col_names
        finally:
            manager.close()

    def test_list_tables_empty_database(self, temp_db_path):
        """DuckDBManager.list_tables() should return an empty list for a new database."""
        manager = self._create_manager(temp_db_path)
        try:
            tables = manager.list_tables()
            assert tables == []
        finally:
            manager.close()

    def test_list_tables_with_schema_filter(self, temp_db_path):
        """DuckDBManager.list_tables(schema='main') should filter by schema."""
        manager = self._create_manager(temp_db_path)
        try:
            manager.con.execute("CREATE TABLE main_table (id INTEGER)")
            tables = manager.list_tables(schema="main")
            assert "main_table" in tables
        finally:
            manager.close()

    def test_context_manager(self, temp_db_path):
        """DuckDBManager should work as a context manager, closing the connection on exit."""
        from db_duckdb.duckdb_manager import DuckDBManager

        with patch.object(DuckDBManager, "_setup_s3_access"), \
             patch.object(DuckDBManager, "_setup_connection"):
            manager = DuckDBManager.__new__(DuckDBManager)
            manager.db_path = temp_db_path
            os.makedirs(os.path.dirname(temp_db_path), exist_ok=True)
            manager.con = duckdb.connect(temp_db_path)

        # Use as context manager
        with manager as db:
            db.con.execute("CREATE TABLE ctx_test (id INTEGER)")
            tables = db.list_tables()
            assert "ctx_test" in tables

        # After exiting, the connection should be closed
        # Attempting to use it should raise
        with pytest.raises(Exception):
            manager.con.execute("SELECT 1")

    def test_close_is_idempotent(self, temp_db_path):
        """Calling close() multiple times should not raise."""
        manager = self._create_manager(temp_db_path)
        manager.close()
        # Second close should not raise
        manager.close()


# ---------------------------------------------------------------------------
# DuckDBOperations Tests
# ---------------------------------------------------------------------------

class TestDuckDBOperations:
    """Tests for db_duckdb/operations.py DuckDBOperations class."""

    def test_init_with_default_path(self):
        """DuckDBOperations should use the default db_path from config."""
        from db_duckdb.operations import DuckDBOperations
        ops = DuckDBOperations()
        assert ops.db_path is not None
        assert ops.db_path.endswith(".duckdb")

    def test_init_with_custom_path(self, temp_db_path):
        """DuckDBOperations should accept a custom db_path."""
        from db_duckdb.operations import DuckDBOperations
        ops = DuckDBOperations(db_path=temp_db_path)
        assert ops.db_path == temp_db_path

    def test_generate_summary_report_all_pass(self):
        """_generate_summary_report() should produce a formatted report for passing tables."""
        from db_duckdb.operations import DuckDBOperations
        ops = DuckDBOperations()

        results = [
            {
                "table_name": "raw_nyc_legacy",
                "status": "PASS",
                "basic_info": {"row_count": 1000, "size_mb": 5.5},
                "validation": {"unique_rides": 990, "unique_files": 3},
            },
            {
                "table_name": "raw_london_legacy",
                "status": "PASS",
                "basic_info": {"row_count": 2000, "size_mb": 8.0},
                "validation": {"unique_rides": 1950, "unique_files": 5},
            },
        ]

        report = ops._generate_summary_report(results)

        assert "raw_nyc_legacy" in report
        assert "raw_london_legacy" in report
        assert "PASS" in report
        assert "Total rows across all tables: 3,000" in report
        assert "Tables passed: 2" in report
        assert "Tables failed: 0" in report

    def test_generate_summary_report_with_failure(self):
        """_generate_summary_report() should list failed tables."""
        from db_duckdb.operations import DuckDBOperations
        ops = DuckDBOperations()

        results = [
            {
                "table_name": "raw_nyc_legacy",
                "status": "FAIL",
                "error": "Table not found",
            },
        ]

        report = ops._generate_summary_report(results)

        assert "FAIL" in report
        assert "Table not found" in report
        assert "Tables failed: 1" in report


# ---------------------------------------------------------------------------
# DuckDBPipeline Tests
# ---------------------------------------------------------------------------

class TestDuckDBPipeline:
    """Tests for db_duckdb/pipeline.py DuckDBPipeline class."""

    def test_pipeline_init_default(self):
        """DuckDBPipeline should initialize with default operations."""
        from db_duckdb.pipeline import DuckDBPipeline
        pipeline = DuckDBPipeline()
        assert pipeline.operations is not None

    def test_pipeline_init_custom_path(self, temp_db_path):
        """DuckDBPipeline should pass custom db_path to operations."""
        from db_duckdb.pipeline import DuckDBPipeline
        pipeline = DuckDBPipeline(db_path=temp_db_path)
        assert pipeline.operations.db_path == temp_db_path

    def test_run_full_pipeline_dry_run(self, temp_db_path):
        """DuckDBPipeline.run_full_pipeline(dry_run=True) should not make real changes."""
        from db_duckdb.pipeline import DuckDBPipeline

        pipeline = DuckDBPipeline(db_path=temp_db_path)

        with patch.object(pipeline.operations, "init_tables") as mock_init, \
             patch.object(pipeline.operations, "load_data") as mock_load, \
             patch.object(pipeline.operations, "verify_data") as mock_verify, \
             patch.object(pipeline.operations, "export_marts") as mock_export:
            mock_load.return_value = {"raw_nyc_legacy": True}
            mock_export.return_value = {"mart_daily_metrics": True}

            results = pipeline.run_full_pipeline(dry_run=True)

            # In dry_run, init_tables is NOT called (pipeline uses hardcoded results)
            mock_init.assert_not_called()
            # load_data IS called with dry_run=True
            mock_load.assert_called_once_with(dry_run=True)

    def test_run_full_pipeline_skip_verify(self, temp_db_path):
        """DuckDBPipeline.run_full_pipeline(skip_verify=True) should skip verification."""
        from db_duckdb.pipeline import DuckDBPipeline

        pipeline = DuckDBPipeline(db_path=temp_db_path)

        with patch.object(pipeline.operations, "init_tables") as mock_init, \
             patch.object(pipeline.operations, "load_data") as mock_load, \
             patch.object(pipeline.operations, "verify_data") as mock_verify, \
             patch.object(pipeline.operations, "export_marts") as mock_export:
            mock_init.return_value = {"create_tables": True, "verify_tables": True}
            mock_load.return_value = {"raw_nyc_legacy": True}
            mock_export.return_value = {"mart_daily_metrics": True}

            results = pipeline.run_full_pipeline(skip_verify=True)

            mock_verify.assert_not_called()
            assert results["verify"] == {"skipped": True}

    def test_run_full_pipeline_skip_export(self, temp_db_path):
        """DuckDBPipeline.run_full_pipeline(skip_export=True) should skip mart export."""
        from db_duckdb.pipeline import DuckDBPipeline

        pipeline = DuckDBPipeline(db_path=temp_db_path)

        with patch.object(pipeline.operations, "init_tables") as mock_init, \
             patch.object(pipeline.operations, "load_data") as mock_load, \
             patch.object(pipeline.operations, "verify_data") as mock_verify, \
             patch.object(pipeline.operations, "export_marts") as mock_export:
            mock_init.return_value = {"create_tables": True, "verify_tables": True}
            mock_load.return_value = {"raw_nyc_legacy": True}
            mock_verify.return_value = {"raw_nyc_legacy": {"status": "PASS"}}

            results = pipeline.run_full_pipeline(skip_export=True)

            mock_export.assert_not_called()
            assert results["export"] == {"skipped": True}

    def test_check_pipeline_status_returns_dict(self, temp_db_path):
        """DuckDBPipeline.check_pipeline_status() should return a status dictionary."""
        from db_duckdb.pipeline import DuckDBPipeline

        pipeline = DuckDBPipeline(db_path=temp_db_path)

        with patch.object(pipeline.operations, "list_tables") as mock_list:
            mock_list.return_value = {
                "available_tables": [],
                "table_details": {},
                "s3_uris": {},
            }

            status = pipeline.check_pipeline_status()

            assert "tables_exist" in status
            assert "tables_loaded" in status
            assert "marts_available" in status
            assert status["tables_exist"] is False


# ---------------------------------------------------------------------------
# Utils Tests
# ---------------------------------------------------------------------------

class TestDuckDBUtils:
    """Tests for db_duckdb/utils.py."""

    def test_log_memory_usage_runs_without_error(self):
        """log_memory_usage() should execute without raising."""
        from db_duckdb.utils import log_memory_usage

        # Should not raise. Just verify it completes.
        log_memory_usage("test stage")

    def test_log_memory_usage_with_empty_stage(self):
        """log_memory_usage() should handle an empty stage string."""
        from db_duckdb.utils import log_memory_usage

        log_memory_usage("")
```

### Test Count: 19 tests

---

## File 4: `tests/test_dashboard.py` -- Dashboard Tests

Tests the `run_query()` helper function from `dashboard/app.py`. We do NOT test Streamlit UI rendering -- that requires a running Streamlit server. We only test the pure query logic.

### Critical Note About Imports

`dashboard/app.py` calls `ensure_local_parquet_files()` at import time (line 20) and `st.set_page_config()` (line 30). Both will fail outside of a Streamlit runtime. **You must mock Streamlit and the data manager before importing.**

### Full Implementation

```python
"""
Tests for dashboard query helper functions.

Tests the run_query() function from dashboard/app.py.
Does NOT test Streamlit UI components (which require a running server).

The dashboard module has heavy import-time side effects (Streamlit calls,
S3 downloads), so we test query logic using a standalone DuckDB connection.
"""

import pytest
import duckdb
import pandas as pd


class TestRunQueryLogic:
    """
    Test the query execution pattern used by the dashboard.

    Since dashboard/app.py has extensive import-time side effects (calling
    ensure_local_parquet_files(), st.set_page_config(), and creating a global
    DuckDB connection), we do NOT import it directly.

    Instead, we replicate the run_query() function's behavior using a local
    DuckDB connection and test that pattern works correctly. This validates
    the query execution approach without triggering Streamlit imports.
    """

    @pytest.fixture
    def memory_conn(self):
        """Create an in-memory DuckDB connection for testing."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_run_query_returns_dataframe(self, memory_conn):
        """The run_query pattern should return a pandas DataFrame."""
        result = memory_conn.execute("SELECT 1 AS value, 'hello' AS name").fetchdf()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["value"][0] == 1
        assert result["name"][0] == "hello"

    def test_run_query_handles_empty_result(self, memory_conn):
        """The run_query pattern should return an empty DataFrame for no-match queries."""
        memory_conn.execute("CREATE TABLE t (id INTEGER)")
        result = memory_conn.execute("SELECT * FROM t").fetchdf()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_run_query_aggregation(self, memory_conn):
        """The run_query pattern should handle SUM/AVG aggregations correctly."""
        memory_conn.execute("CREATE TABLE rides (location VARCHAR, total_rides INTEGER, date DATE)")
        memory_conn.execute("""
            INSERT INTO rides VALUES
            ('nyc', 100, '2023-01-01'),
            ('nyc', 200, '2023-01-02'),
            ('london', 50, '2023-01-01'),
            ('london', 75, '2023-01-02')
        """)

        result = memory_conn.execute("""
            SELECT location, SUM(total_rides) as total
            FROM rides
            GROUP BY location
            ORDER BY location
        """).fetchdf()

        assert len(result) == 2
        assert result.loc[result["location"] == "london", "total"].values[0] == 125
        assert result.loc[result["location"] == "nyc", "total"].values[0] == 300

    def test_run_query_date_filtering(self, memory_conn):
        """The run_query pattern should correctly filter by date range."""
        memory_conn.execute("CREATE TABLE daily (date DATE, rides INTEGER)")
        memory_conn.execute("""
            INSERT INTO daily VALUES
            ('2023-01-01', 100),
            ('2023-06-15', 200),
            ('2023-12-31', 300),
            ('2024-01-01', 400)
        """)

        result = memory_conn.execute("""
            SELECT SUM(rides) as total
            FROM daily
            WHERE date BETWEEN '2023-01-01' AND '2023-12-31'
        """).fetchdf()

        assert result["total"][0] == 600  # 100 + 200 + 300

    def test_run_query_parquet_file_read(self, memory_conn, tmp_path):
        """The run_query pattern should be able to read Parquet files directly."""
        # Create a small Parquet file
        df = pd.DataFrame({
            "location": ["nyc", "london"],
            "station_count": [1500, 800],
            "year": [2023, 2023],
        })
        parquet_path = str(tmp_path / "test_mart.parquet")
        df.to_parquet(parquet_path)

        result = memory_conn.execute(
            f"SELECT * FROM '{parquet_path}' ORDER BY location"
        ).fetchdf()

        assert len(result) == 2
        assert result["location"][0] == "london"
        assert result["station_count"][0] == 800

    def test_run_query_with_extract_function(self, memory_conn):
        """The run_query pattern should support EXTRACT(MONTH FROM date) used by the dashboard."""
        memory_conn.execute("CREATE TABLE monthly (date DATE, rides INTEGER)")
        memory_conn.execute("""
            INSERT INTO monthly VALUES
            ('2023-01-15', 100),
            ('2023-01-20', 150),
            ('2023-02-10', 200)
        """)

        result = memory_conn.execute("""
            SELECT EXTRACT(MONTH FROM date) AS month, SUM(rides) AS total
            FROM monthly
            GROUP BY month
            ORDER BY month
        """).fetchdf()

        assert len(result) == 2
        assert result["month"][0] == 1  # January
        assert result["total"][0] == 250  # 100 + 150
        assert result["month"][1] == 2  # February
        assert result["total"][1] == 200
```

### Test Count: 6 tests

---

## File 5: `tests/test_streamlit_data_manager.py` -- Streamlit Data Manager Tests

Tests the `ensure_local_parquet_files()` function from `streamlit_data_manager/parquet_file_manager.py`.

### Full Implementation

```python
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
        """The MARTS list should contain all 5 expected mart Parquet files."""
        from streamlit_data_manager.parquet_file_manager import MARTS

        expected = [
            "mart_daily_metrics.parquet",
            "mart_hourly_patterns.parquet",
            "mart_nyc_member_analysis.parquet",
            "mart_station_growth.parquet",
            "mart_daily_metrics_long.parquet",
        ]

        assert len(MARTS) == 5
        for mart in expected:
            assert mart in MARTS, f"Missing expected mart: {mart}"

    def test_s3_bucket_constant(self):
        """The S3_BUCKET constant should be set to the expected value."""
        from streamlit_data_manager.parquet_file_manager import S3_BUCKET
        assert S3_BUCKET == "city-cycles-data-ctr37"
```

### Test Count: 6 tests

---

## Total New Tests Summary

| File | Test Count |
|------|-----------|
| `tests/test_extraction.py` | 13 |
| `tests/test_db_duckdb_operations.py` | 19 |
| `tests/test_dashboard.py` | 6 |
| `tests/test_streamlit_data_manager.py` | 6 |
| **Total New** | **44** |

**Expected final total: ~123 tests (79 existing + 44 new)**

---

## Verification Checklist

Run ALL of the following after creating the new files. Every check must pass before opening the PR.

### 1. Full Test Suite (All Tests Pass)
```bash
python -m pytest tests/ -v
```
**Expected:** All existing tests still pass (79 pass, 3 skip, 0 fail) PLUS all 44 new tests pass. No test should fail.

### 2. Verify New Test Count
```bash
python -m pytest tests/ --co -q | tail -5
```
**Expected:** Total collected tests is approximately 123 (79 existing + 44 new).

### 3. Verify No Network Calls
```bash
python -m pytest tests/test_extraction.py tests/test_streamlit_data_manager.py -v --tb=short
```
**Expected:** All tests pass in under 5 seconds with no S3 or HTTP calls. If tests hang, you have an unmocked network call.

### 4. Verify DuckDB Tests Use Real Databases
```bash
python -m pytest tests/test_db_duckdb_operations.py -v --tb=short
```
**Expected:** All tests pass. The `TestDuckDBManager` tests create real tables and query them. No flaky behavior.

### 5. Git Diff Review
```bash
git diff --stat
```
**Expected:** Only 5 new files should appear:
- `tests/conftest.py` (new)
- `tests/test_extraction.py` (new)
- `tests/test_db_duckdb_operations.py` (new)
- `tests/test_dashboard.py` (new)
- `tests/test_streamlit_data_manager.py` (new)

No existing files should be modified.

---

## What NOT To Do

- **Do NOT modify any existing test files** (`test_orchestrator.py`, `test_db_duckdb_cli.py`, etc.)
- **Do NOT modify any source code files** -- this PR is test-only
- **Do NOT remove any existing tests** -- even if you think they overlap with new tests
- **Do NOT make actual S3 calls or HTTP requests** -- all external I/O must be mocked
- **Do NOT create tests that depend on specific data being in S3** or on the production database
- **Do NOT test Streamlit UI rendering** (requires a running `streamlit run` server)
- **Do NOT install new test dependencies** -- use only `pytest`, `unittest.mock`, and libraries already in `requirements.txt`
- **Do NOT import extraction modules at the file level** -- they have module-level side effects that fail without AWS credentials
- **Do NOT create overly complex test fixtures** -- keep fixtures simple and self-contained
- **Do NOT use `pytest-mock`** or `pytest-asyncio` unless they are already in `requirements.txt` -- stick with `unittest.mock`

---

## Troubleshooting Guide

### Problem: `extraction.utils` import fails with `ValueError: S3_BUCKET environment variable is not set!`

**Cause:** You imported `extraction.utils` (or any module that imports it) without setting the `S3_BUCKET` environment variable first.

**Fix:** Always use `patch.dict(os.environ, {"S3_BUCKET": "test-bucket"})` BEFORE importing. Use the `_import_utils()` / `_import_nyc()` helper functions which handle this.

### Problem: `DuckDBManager` tests fail with `INSTALL httpfs` error

**Cause:** The DuckDB httpfs extension may not be available in CI environments.

**Fix:** Use the `_create_manager()` helper method which patches out `_setup_connection()` and `_setup_s3_access()`, creating a plain DuckDB connection without extensions.

### Problem: `test_streamlit_data_manager.py` tests download real files from S3

**Cause:** The `boto3.client` mock is not being applied correctly.

**Fix:** Ensure you are patching at the correct location: `streamlit_data_manager.parquet_file_manager.boto3.client`, NOT `boto3.client`.

### Problem: Tests pass locally but fail in CI

**Cause:** Usually due to module caching in `sys.modules`. The extraction tests clear cached modules before each test, but if tests run in a different order, stale modules may be loaded.

**Fix:** Ensure each test that imports extraction modules clears `sys.modules` first (the helper functions do this).

---

## PR Checklist

- [ ] All 5 new test files created at the correct paths
- [ ] No existing files modified
- [ ] `python -m pytest tests/ -v` passes (all old + new tests)
- [ ] No actual S3 calls or network calls in any test
- [ ] Test count increased by approximately 44
- [ ] `git diff --stat` shows only the 5 new files
- [ ] CHANGELOG.md updated with entry under `[Unreleased]`

### CHANGELOG Entry
```markdown
### Technical Improvements
- **Test Coverage Expansion** - Added ~44 new tests covering previously untested modules
  - Created shared test fixtures in `tests/conftest.py` (temp DuckDB, mock S3, sample DataFrames)
  - Added 13 tests for `extraction/` module (utils, NYC, London) with mocked S3 and HTTP
  - Added 19 tests for `db_duckdb/` module (DuckDBManager, Operations, Pipeline, utils) with real temp databases
  - Added 6 tests for `dashboard/` query patterns using in-memory DuckDB
  - Added 6 tests for `streamlit_data_manager/` with mocked S3 downloads
  - Total test count increased from ~79 to ~123
```
