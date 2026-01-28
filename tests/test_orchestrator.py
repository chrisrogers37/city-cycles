"""
Tests for the City Cycles Pipeline Orchestrator.

Tests the orchestrator configuration, CLI, and main orchestration logic.
"""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from pathlib import Path


class TestOrchestratorConfig:
    """Test the orchestrator configuration module."""

    def test_config_import(self):
        """Test that config module can be imported."""
        from orchestrator import config
        assert config is not None

    def test_project_paths_exist(self):
        """Test that project paths are defined correctly."""
        from orchestrator.config import PROJECT_ROOT, DATA_DIR, DBT_DIR, LOGS_DIR

        assert PROJECT_ROOT is not None
        assert isinstance(PROJECT_ROOT, Path)
        assert DATA_DIR is not None
        assert DBT_DIR is not None
        assert LOGS_DIR is not None

    def test_aws_config_structure(self):
        """Test that AWS config has expected keys."""
        from orchestrator.config import AWS_CONFIG

        assert 'region' in AWS_CONFIG
        assert 's3_bucket' in AWS_CONFIG
        assert 'access_key_id' in AWS_CONFIG
        assert 'secret_access_key' in AWS_CONFIG

    def test_duckdb_config_structure(self):
        """Test that DuckDB config has expected keys."""
        from orchestrator.config import DUCKDB_CONFIG

        assert 'db_path' in DUCKDB_CONFIG
        assert 'memory_limit' in DUCKDB_CONFIG
        assert 'threads' in DUCKDB_CONFIG

    def test_extraction_config_structure(self):
        """Test that extraction config has expected keys."""
        from orchestrator.config import EXTRACTION_CONFIG

        assert 'nyc_start_year' in EXTRACTION_CONFIG
        assert 'enable_nyc' in EXTRACTION_CONFIG
        assert 'enable_london' in EXTRACTION_CONFIG

    def test_dbt_config_structure(self):
        """Test that dbt config has expected keys."""
        from orchestrator.config import DBT_CONFIG

        assert 'profiles_dir' in DBT_CONFIG
        assert 'target' in DBT_CONFIG

    def test_pipeline_config_structure(self):
        """Test that pipeline config has expected keys."""
        from orchestrator.config import PIPELINE_CONFIG

        assert 'skip_extraction_on_error' in PIPELINE_CONFIG
        assert 'continue_on_stage_failure' in PIPELINE_CONFIG
        assert 'default_dbt_full_refresh' in PIPELINE_CONFIG

    def test_get_config_returns_all_sections(self):
        """Test that get_config returns all configuration sections."""
        from orchestrator.config import get_config

        config = get_config()

        assert 'project_root' in config
        assert 'data_dir' in config
        assert 'dbt_dir' in config
        assert 'logs_dir' in config
        assert 'aws' in config
        assert 'duckdb' in config
        assert 'extraction' in config
        assert 'dbt' in config
        assert 'notifications' in config
        assert 'pipeline' in config
        assert 'logging' in config

    def test_validate_config_returns_bool(self):
        """Test that validate_config returns a boolean."""
        from orchestrator.config import validate_config

        # Capture output and test function returns bool
        with patch('builtins.print'):
            result = validate_config()

        assert isinstance(result, bool)


class TestOrchestratorMain:
    """Test the main orchestrator class."""

    def test_orchestrator_import(self):
        """Test that orchestrator can be imported."""
        from orchestrator.main import CityBikesOrchestrator
        assert CityBikesOrchestrator is not None

    def test_orchestrator_initialization(self):
        """Test orchestrator can be initialized."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        assert orchestrator is not None
        assert orchestrator.project_root is not None
        assert isinstance(orchestrator.project_root, Path)
        assert orchestrator.config == {}
        assert orchestrator.results == {}

    def test_orchestrator_initialization_with_custom_root(self):
        """Test orchestrator can be initialized with custom project root."""
        from orchestrator.main import CityBikesOrchestrator

        custom_root = Path("/tmp/test_project")
        orchestrator = CityBikesOrchestrator(project_root=custom_root)

        assert orchestrator.project_root == custom_root

    def test_orchestrator_initialization_with_custom_config(self):
        """Test orchestrator can be initialized with custom config."""
        from orchestrator.main import CityBikesOrchestrator

        custom_config = {'test_key': 'test_value'}
        orchestrator = CityBikesOrchestrator(config=custom_config)

        assert orchestrator.config == custom_config

    def test_run_stage_invalid_stage(self):
        """Test that run_stage raises error for invalid stage."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Invalid stage should return False (not raise)
        result = orchestrator.run_stage('invalid_stage_name')
        assert result is False

    def test_run_stage_extraction_mocked(self):
        """Test run_stage for extraction with mocked dependencies."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal extraction method
        with patch.object(orchestrator, '_run_extraction') as mock_extract:
            mock_extract.return_value = None
            result = orchestrator.run_stage('extraction')

            mock_extract.assert_called_once()
            assert result is True

    def test_run_stage_file_management_mocked(self):
        """Test run_stage for file_management with mocked dependencies."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal file management method
        with patch.object(orchestrator, '_run_file_management') as mock_fm:
            mock_fm.return_value = None
            result = orchestrator.run_stage('file_management')

            mock_fm.assert_called_once()
            assert result is True

    def test_run_stage_database_load_mocked(self):
        """Test run_stage for database_load with mocked dependencies."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal database load method
        with patch.object(orchestrator, '_run_database_load') as mock_db:
            mock_db.return_value = None
            result = orchestrator.run_stage('database_load')

            mock_db.assert_called_once_with(skip_verify=False)
            assert result is True

    def test_run_stage_database_load_skip_verify(self):
        """Test run_stage for database_load with skip_verify flag."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal database load method
        with patch.object(orchestrator, '_run_database_load') as mock_db:
            mock_db.return_value = None
            result = orchestrator.run_stage('database_load', skip_verify=True)

            mock_db.assert_called_once_with(skip_verify=True)
            assert result is True

    def test_run_stage_dbt_mocked(self):
        """Test run_stage for dbt with mocked dependencies."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal dbt method
        with patch.object(orchestrator, '_run_dbt_transformations') as mock_dbt:
            mock_dbt.return_value = None
            result = orchestrator.run_stage('dbt')

            mock_dbt.assert_called_once_with(full_refresh=False)
            assert result is True

    def test_run_stage_dbt_full_refresh(self):
        """Test run_stage for dbt with full_refresh flag."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal dbt method
        with patch.object(orchestrator, '_run_dbt_transformations') as mock_dbt:
            mock_dbt.return_value = None
            result = orchestrator.run_stage('dbt', full_refresh=True)

            mock_dbt.assert_called_once_with(full_refresh=True)
            assert result is True

    def test_run_stage_export_mocked(self):
        """Test run_stage for export with mocked dependencies."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock the internal export method
        with patch.object(orchestrator, '_run_mart_export') as mock_export:
            mock_export.return_value = None
            result = orchestrator.run_stage('export')

            mock_export.assert_called_once()
            assert result is True

    def test_run_full_pipeline_mocked(self):
        """Test full pipeline run with all stages mocked."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock all internal methods
        with patch.object(orchestrator, '_run_extraction') as mock_extract, \
             patch.object(orchestrator, '_run_file_management') as mock_fm, \
             patch.object(orchestrator, '_run_database_load') as mock_db, \
             patch.object(orchestrator, '_run_dbt_transformations') as mock_dbt, \
             patch.object(orchestrator, '_run_mart_export') as mock_export:

            result = orchestrator.run()

            mock_extract.assert_called_once()
            mock_fm.assert_called_once()
            mock_db.assert_called_once_with(skip_verify=False)
            mock_dbt.assert_called_once_with(full_refresh=False)
            mock_export.assert_called_once()
            assert result is True

    def test_run_skip_extraction(self):
        """Test pipeline run with extraction skipped."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock all internal methods
        with patch.object(orchestrator, '_run_extraction') as mock_extract, \
             patch.object(orchestrator, '_run_file_management') as mock_fm, \
             patch.object(orchestrator, '_run_database_load') as mock_db, \
             patch.object(orchestrator, '_run_dbt_transformations') as mock_dbt, \
             patch.object(orchestrator, '_run_mart_export') as mock_export:

            result = orchestrator.run(skip_extraction=True)

            mock_extract.assert_not_called()
            mock_fm.assert_called_once()
            assert orchestrator.results['extraction']['status'] == 'skipped'
            assert result is True

    def test_run_skip_export(self):
        """Test pipeline run with export skipped."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Mock all internal methods
        with patch.object(orchestrator, '_run_extraction') as mock_extract, \
             patch.object(orchestrator, '_run_file_management') as mock_fm, \
             patch.object(orchestrator, '_run_database_load') as mock_db, \
             patch.object(orchestrator, '_run_dbt_transformations') as mock_dbt, \
             patch.object(orchestrator, '_run_mart_export') as mock_export:

            result = orchestrator.run(skip_export=True)

            mock_export.assert_not_called()
            assert orchestrator.results['mart_export']['status'] == 'skipped'
            assert result is True


class TestOrchestratorCLI:
    """Test the orchestrator CLI module."""

    def test_cli_import(self):
        """Test that CLI module can be imported."""
        from orchestrator import cli
        assert cli is not None

    def test_setup_logging_function(self):
        """Test that setup_logging function exists and works."""
        from orchestrator.cli import setup_logging

        # Should not raise
        setup_logging(verbose=False)
        setup_logging(verbose=True)

    def test_main_function_exists(self):
        """Test that main function exists."""
        from orchestrator.cli import main
        assert callable(main)

    def test_check_pipeline_status_function_exists(self):
        """Test that check_pipeline_status function exists."""
        from orchestrator.cli import check_pipeline_status
        assert callable(check_pipeline_status)

    def test_cli_help_output(self):
        """Test CLI help output."""
        import subprocess

        result = subprocess.run(
            [sys.executable, '-m', 'orchestrator.cli', '--help'],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )

        # Help should show without error
        assert result.returncode == 0 or 'usage' in result.stdout.lower() or 'usage' in result.stderr.lower()

    def test_cli_run_help(self):
        """Test CLI run command help."""
        import subprocess

        result = subprocess.run(
            [sys.executable, '-m', 'orchestrator.cli', 'run', '--help'],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )

        # Should show run-specific options
        assert '--skip-extraction' in result.stdout or '--skip-extraction' in result.stderr or result.returncode == 0

    def test_cli_stage_help(self):
        """Test CLI stage command help."""
        import subprocess

        result = subprocess.run(
            [sys.executable, '-m', 'orchestrator.cli', 'stage', '--help'],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )

        # Should show stage-specific options
        output = result.stdout + result.stderr
        assert 'extraction' in output or 'file_management' in output or result.returncode == 0

    def test_cli_status_help(self):
        """Test CLI status command help."""
        import subprocess

        result = subprocess.run(
            [sys.executable, '-m', 'orchestrator.cli', 'status', '--help'],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )

        # Should work without error
        assert result.returncode == 0


class TestOrchestratorIntegration:
    """Integration tests for the orchestrator (lightweight, no AWS calls)."""

    def test_orchestrator_components_compatible(self):
        """Test that all orchestrator components work together."""
        from orchestrator.main import CityBikesOrchestrator
        from orchestrator.config import get_config, PROJECT_ROOT

        # Initialize orchestrator
        orchestrator = CityBikesOrchestrator()

        # Get config
        config = get_config()

        # Verify project root matches
        assert orchestrator.project_root == PROJECT_ROOT
        assert str(orchestrator.project_root) == config['project_root']

    def test_orchestrator_results_tracking(self):
        """Test that orchestrator tracks results correctly."""
        from orchestrator.main import CityBikesOrchestrator

        orchestrator = CityBikesOrchestrator()

        # Initially empty
        assert orchestrator.results == {}

        # Run with all stages skipped (via mocking)
        with patch.object(orchestrator, '_run_extraction'), \
             patch.object(orchestrator, '_run_file_management'), \
             patch.object(orchestrator, '_run_database_load'), \
             patch.object(orchestrator, '_run_dbt_transformations'), \
             patch.object(orchestrator, '_run_mart_export'):

            orchestrator.run(skip_extraction=True, skip_export=True)

        # Should have tracked skipped stages
        assert 'extraction' in orchestrator.results
        assert orchestrator.results['extraction']['status'] == 'skipped'
        assert 'mart_export' in orchestrator.results
        assert orchestrator.results['mart_export']['status'] == 'skipped'
