"""
Main Orchestrator Module

Coordinates the end-to-end City Cycles ETL pipeline across all subsystems.
"""

import logging
import sys
import subprocess
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def _log_section(title: str, level: str = "info", width: int = 80):
    """Log a section header with separator lines above and below."""
    log_fn = getattr(logger, level)
    log_fn("=" * width)
    log_fn(title)
    log_fn("=" * width)


def _log_step(step: int, total: int, description: str):
    """Log a pipeline step header with separator lines."""
    logger.info("")
    _log_section(f"[STEP {step}/{total}] {description}")


class CityBikesOrchestrator:
    """
    Global orchestrator for the City Cycles ETL pipeline.
    
    Coordinates all stages of the pipeline:
    1. Extract data from web sources to S3
    2. S3 file management (unzip, schema validation, Parquet conversion)
    3. Load data into DuckDB
    4. Run dbt transformations
    5. Export marts to S3
    """
    
    def __init__(self, project_root: Optional[Path] = None, config: Optional[Dict] = None):
        """
        Initialize the orchestrator.
        
        Args:
            project_root: Path to project root directory
            config: Optional configuration dictionary
        """
        self.project_root = project_root or Path(__file__).parent.parent
        self.config = config or {}
        self.pipeline_start = None
        self.results = {}
        
        logger.info(f"Orchestrator initialized at project root: {self.project_root}")
    
    def run(self, skip_extraction: bool = False, 
            skip_verify: bool = False,
            skip_export: bool = False,
            dbt_full_refresh: bool = False) -> bool:
        """
        Run the complete end-to-end pipeline.
        
        Args:
            skip_extraction: Skip data extraction phase
            skip_verify: Skip data verification after DuckDB load
            skip_export: Skip mart export to S3
            dbt_full_refresh: Run dbt with --full-refresh flag
            
        Returns:
            True if pipeline succeeded, False otherwise
        """
        self.pipeline_start = datetime.now()
        
        _log_section(f"CITY CYCLES PIPELINE - Starting at {self.pipeline_start}")
        
        try:
            # Step 1: Extract data from web to S3
            if not skip_extraction:
                self._run_extraction()
            else:
                logger.info("\n[STEP 1/5] Extraction SKIPPED")
                self.results['extraction'] = {'status': 'skipped'}
            
            # Step 2: S3 file management
            self._run_file_management()
            
            # Step 3: Load into DuckDB
            self._run_database_load(skip_verify=skip_verify)
            
            # Step 4: Run dbt transformations
            self._run_dbt_transformations(full_refresh=dbt_full_refresh)
            
            # Step 5: Export marts to S3
            if not skip_export:
                self._run_mart_export()
            else:
                logger.info("\n[STEP 5/5] Mart export SKIPPED")
                self.results['mart_export'] = {'status': 'skipped'}
            
            # Success!
            self._report_success()
            return True
            
        except Exception as e:
            self._report_failure(e)
            return False
    
    def _run_extraction(self):
        """
        Step 1: Extract bike data from web to S3.
        
        Extracts:
        - NYC CitiBike data from public S3 bucket
        - London Santander Cycles data from TfL website
        """
        _log_step(1, 5, "EXTRACTING BIKE DATA FROM WEB TO S3")
        
        try:
            # Import extraction modules
            from extraction import nyc, london
            
            # NYC extraction
            logger.info("\n→ Extracting NYC CitiBike data...")
            try:
                nyc.download_all_zips()
                logger.info("✓ NYC extraction completed")
            except Exception as e:
                logger.error(f"✗ NYC extraction failed: {e}")
                # Continue with London even if NYC fails
            
            # London extraction
            logger.info("\n→ Extracting London Santander Cycles data...")
            try:
                london.process_and_upload_london_files()
                logger.info("✓ London extraction completed")
            except Exception as e:
                logger.error(f"✗ London extraction failed: {e}")
                # Log but continue - we may have partial data
            
            # Weather extraction
            logger.info("\n→ Extracting weather data from Open-Meteo...")
            try:
                from extraction import weather
                weather.incremental_update_all(days_back=35)
                logger.info("✓ Weather extraction completed")
            except Exception as e:
                logger.error(f"✗ Weather extraction failed: {e}")
                # Continue -- weather is supplementary data

            self.results['extraction'] = {'status': 'success'}
            logger.info("\n✓ Data extraction phase completed")

        except Exception as e:
            logger.error(f"\n✗ Extraction phase failed: {e}")
            raise RuntimeError(f"Extraction phase failed: {e}")
    
    def _run_file_management(self):
        """
        Step 2: S3 file management.
        
        Processes:
        - Extract ZIP files to CSVs
        - Validate schemas
        - Convert CSVs to Parquet
        - Organize by schema type
        """
        _log_step(2, 5, "PROCESSING FILES (UNZIP, SCHEMA VALIDATION, PARQUET)")
        
        try:
            from extracted_file_manager.simplified_pipeline import run_full_pipeline
            
            logger.info("\n→ Running file management pipeline...")
            results = run_full_pipeline()
            
            self.results['file_management'] = results
            logger.info("\n✓ File management phase completed")
            
        except Exception as e:
            logger.error(f"\n✗ File management phase failed: {e}")
            raise RuntimeError(f"File management phase failed: {e}")
    
    def _run_database_load(self, skip_verify: bool = False):
        """
        Step 3: Load data into DuckDB.
        
        Loads:
        - Parquet files from S3 into DuckDB raw tables
        - Validates data integrity (optional)
        """
        _log_step(3, 5, "LOADING DATA INTO DUCKDB")
        
        try:
            from db_duckdb.pipeline import run_full_pipeline
            
            logger.info("\n→ Running DuckDB load pipeline...")
            results = run_full_pipeline(
                skip_export=True,  # We'll export after dbt
                skip_verify=skip_verify
            )
            
            self.results['database_load'] = results
            logger.info("\n✓ Database load phase completed")
            
        except Exception as e:
            logger.error(f"\n✗ Database load phase failed: {e}")
            raise RuntimeError(f"Database load phase failed: {e}")
    
    def _run_dbt_transformations(self, full_refresh: bool = False):
        """
        Step 4: Run dbt transformations.
        
        Executes:
        - Staging models (incremental)
        - Intermediate models (incremental)
        - Unified models (incremental)
        - Mart models (full rebuild)
        """
        _log_step(4, 5, "RUNNING DBT TRANSFORMATIONS")
        
        try:
            dbt_dir = self.project_root / "dbt_city_cycles"
            
            # First, load seed data (population reference data)
            logger.info("\n→ Loading dbt seeds...")
            seed_process = subprocess.Popen(
                ['dbt', 'seed'],
                cwd=str(dbt_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Stream seed output
            for line in seed_process.stdout:
                logger.info(line.rstrip())
            
            seed_return_code = seed_process.wait()
            if seed_return_code != 0:
                raise RuntimeError(f"dbt seed failed with return code {seed_return_code}")
            
            logger.info("\n✓ Seeds loaded successfully")
            
            # Build dbt command
            cmd = ['dbt', 'run']
            if full_refresh:
                cmd.append('--full-refresh')
                logger.info("\n→ Running dbt with FULL REFRESH...")
            else:
                logger.info("\n→ Running dbt (incremental)...")
            
            # Run dbt (stream output instead of capturing to avoid memory issues)
            process = subprocess.Popen(
                cmd,
                cwd=str(dbt_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Stream output line by line
            logger.info("\n--- dbt Output (streaming) ---")
            output_lines = []
            for line in process.stdout:
                logger.info(line.rstrip())
                output_lines.append(line)
            
            # Wait for process to complete
            return_code = process.wait()
            
            if return_code != 0:
                raise subprocess.CalledProcessError(
                    return_code, cmd, output=''.join(output_lines)
                )
            
            self.results['dbt'] = {
                'status': 'success',
                'full_refresh': full_refresh,
                'output': ''.join(output_lines)
            }
            logger.info("\n✓ dbt transformations completed")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"\n✗ dbt transformations failed")
            logger.error(f"--- dbt Error Output ---")
            logger.error(e.output if hasattr(e, 'output') else str(e))
            raise RuntimeError(f"dbt phase failed: {e.output if hasattr(e, 'output') else str(e)}")
    
    def _run_mart_export(self):
        """
        Step 5: Export data marts to S3.
        
        Exports:
        - Mart tables from DuckDB to S3 as Parquet files
        - Used by Streamlit dashboard
        """
        _log_step(5, 5, "EXPORTING DATA MARTS TO S3")
        
        try:
            from db_duckdb.operations import DuckDBOperations
            
            logger.info("\n→ Exporting marts...")
            operations = DuckDBOperations()
            results = operations.export_marts(include_intermediate=False)
            
            self.results['mart_export'] = results
            logger.info("\n✓ Mart export phase completed")
            
        except Exception as e:
            logger.error(f"\n✗ Mart export phase failed: {e}")
            raise RuntimeError(f"Mart export phase failed: {e}")
    
    def _report_success(self):
        """Report successful pipeline completion."""
        duration = (datetime.now() - self.pipeline_start).total_seconds()
        
        logger.info("")
        _log_section("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.1f} minutes)")
        logger.info("\n--- Pipeline Summary ---")
        
        for stage, result in self.results.items():
            status = result.get('status', 'completed')
            logger.info(f"  {stage}: {status}")
        
        logger.info("\n--- Next Steps ---")
        logger.info("1. Verify data in DuckDB")
        logger.info("2. Check Streamlit dashboard for updated data")
        logger.info("3. Review logs for any warnings")
    
    def _report_failure(self, error: Exception):
        """Report pipeline failure."""
        duration = (datetime.now() - self.pipeline_start).total_seconds()
        
        logger.error("")
        _log_section("PIPELINE FAILED", level="error")
        logger.error(f"Error: {error}")
        logger.error(f"Duration before failure: {duration:.2f} seconds ({duration/60:.1f} minutes)")
        logger.error("\n--- Pipeline Summary ---")
        
        for stage, result in self.results.items():
            status = result.get('status', 'completed')
            logger.error(f"  {stage}: {status}")
        
        logger.error("\n--- Troubleshooting ---")
        logger.error("1. Check logs above for error details")
        logger.error("2. Verify AWS credentials and S3 access")
        logger.error("3. Check DuckDB database file permissions")
        logger.error("4. Try running individual stages manually")
    
    def _run_weather_extraction(self):
        """Run weather data extraction independently."""
        _log_step(1, 1, "EXTRACTING WEATHER DATA FROM OPEN-METEO")

        try:
            from extraction import weather

            logger.info("\n→ Running weather incremental update...")
            results = weather.incremental_update_all(days_back=35)

            self.results['weather_extraction'] = {
                'status': 'success',
                'results': results
            }
            logger.info("\n✓ Weather extraction phase completed")

        except Exception as e:
            logger.error(f"\n✗ Weather extraction phase failed: {e}")
            raise RuntimeError(f"Weather extraction phase failed: {e}")

    def run_stage(self, stage: str, **kwargs) -> bool:
        """
        Run a specific pipeline stage.
        
        Args:
            stage: Stage name ('extraction', 'file_management', 'database_load', 'dbt', 'export')
            **kwargs: Stage-specific arguments
            
        Returns:
            True if stage succeeded, False otherwise
        """
        self.pipeline_start = datetime.now()
        logger.info(f"Running stage: {stage}")
        
        try:
            if stage == 'extraction':
                self._run_extraction()
            elif stage == 'file_management':
                self._run_file_management()
            elif stage == 'database_load':
                self._run_database_load(skip_verify=kwargs.get('skip_verify', False))
            elif stage == 'dbt':
                self._run_dbt_transformations(full_refresh=kwargs.get('full_refresh', False))
            elif stage == 'export':
                self._run_mart_export()
            elif stage == 'weather_extraction':
                self._run_weather_extraction()
            else:
                raise ValueError(f"Unknown stage: {stage}")
            
            logger.info(f"✓ Stage '{stage}' completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Stage '{stage}' failed: {e}")
            return False


def main():
    """Entry point for running the orchestrator from command line."""
    orchestrator = CityBikesOrchestrator()
    success = orchestrator.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

