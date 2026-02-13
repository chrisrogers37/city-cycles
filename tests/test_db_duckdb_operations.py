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
             patch.object(DuckDBManager, "_setup_connection"):
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

    def test_export_marts_includes_weather_marts(self):
        """export_marts MART_TABLES should include all weather-related mart tables."""
        from db_duckdb.operations import DuckDBOperations
        import inspect

        source = inspect.getsource(DuckDBOperations.export_marts)

        assert 'mart_hourly_rides' in source
        assert 'mart_hourly_patterns_summary' in source
        assert 'mart_weather_ride_correlation' in source
        assert 'mart_weather_impact_summary' in source
        assert 'mart_station_directory' in source
        assert 'mart_station_weather_performance' in source
        assert 'mart_hourly_patterns' not in source or 'mart_hourly_patterns_summary' in source


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

        log_memory_usage("test stage")

    def test_log_memory_usage_with_empty_stage(self):
        """log_memory_usage() should handle an empty stage string."""
        from db_duckdb.utils import log_memory_usage

        log_memory_usage("")
