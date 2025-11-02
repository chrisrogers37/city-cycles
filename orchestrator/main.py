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
        
        logger.info("=" * 80)
        logger.info(f"CITY CYCLES PIPELINE - Starting at {self.pipeline_start}")
        logger.info("=" * 80)
        
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
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 1/5] EXTRACTING BIKE DATA FROM WEB TO S3")
        logger.info("=" * 80)
        
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
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 2/5] PROCESSING FILES (UNZIP, SCHEMA VALIDATION, PARQUET)")
        logger.info("=" * 80)
        
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
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 3/5] LOADING DATA INTO DUCKDB")
        logger.info("=" * 80)
        
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
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 4/5] RUNNING DBT TRANSFORMATIONS")
        logger.info("=" * 80)
        
        try:
            dbt_dir = self.project_root / "dbt_city_cycles"
            
            # Build dbt command
            cmd = ['dbt', 'run']
            if full_refresh:
                cmd.append('--full-refresh')
                logger.info("\n→ Running dbt with FULL REFRESH...")
            else:
                logger.info("\n→ Running dbt (incremental)...")
            
            # Run dbt
            result = subprocess.run(
                cmd,
                cwd=str(dbt_dir),
                check=True,
                capture_output=True,
                text=True
            )
            
            # Log output
            logger.info("\n--- dbt Output ---")
            logger.info(result.stdout)
            
            self.results['dbt'] = {
                'status': 'success',
                'full_refresh': full_refresh,
                'output': result.stdout
            }
            logger.info("\n✓ dbt transformations completed")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"\n✗ dbt transformations failed")
            logger.error(f"--- dbt Error Output ---")
            logger.error(e.stderr)
            raise RuntimeError(f"dbt phase failed: {e.stderr}")
    
    def _run_mart_export(self):
        """
        Step 5: Export data marts to S3.
        
        Exports:
        - Mart tables from DuckDB to S3 as Parquet files
        - Used by Streamlit dashboard
        """
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 5/5] EXPORTING DATA MARTS TO S3")
        logger.info("=" * 80)
        
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
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.1f} minutes)")
        logger.info("\n--- Pipeline Summary ---")
        
        for stage, result in self.results.items():
            status = result.get('status', 'completed')
            logger.info(f"  {stage}: {status}")
        
        logger.info("\n--- Next Steps ---")
        logger.info("1. Verify data in DuckDB")
        logger.info("2. Check Streamlit dashboard for updated data")
        logger.info("3. Review logs for any warnings")
        logger.info("=" * 80)
    
    def _report_failure(self, error: Exception):
        """Report pipeline failure."""
        duration = (datetime.now() - self.pipeline_start).total_seconds()
        
        logger.error("\n" + "=" * 80)
        logger.error("✗ PIPELINE FAILED")
        logger.error("=" * 80)
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
        logger.error("=" * 80)
    
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

