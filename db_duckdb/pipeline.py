"""
DuckDB Pipeline Module

Provides orchestration for running the complete ETL pipeline workflow.
"""

import logging
from typing import Dict, List, Optional
from .operations import DuckDBOperations

logger = logging.getLogger(__name__)


class DuckDBPipeline:
    """Orchestrates the complete DuckDB ETL pipeline."""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the pipeline.
        
        Args:
            db_path: Optional custom database path
        """
        self.operations = DuckDBOperations(db_path)
    
    def run_full_pipeline(self, skip_verify: bool = False, 
                         skip_export: bool = False,
                         dry_run: bool = False) -> Dict[str, Dict]:
        """
        Run the complete ETL pipeline.
        
        Args:
            skip_verify: Skip data verification step
            skip_export: Skip mart export step
            dry_run: Run in dry-run mode (no actual changes)
            
        Returns:
            Dictionary with results from each pipeline stage
        """
        results = {}
        
        logger.info("=" * 80)
        logger.info("CITY CYCLES - DUCKDB ETL PIPELINE")
        logger.info("=" * 80)
        
        if dry_run:
            logger.info("DRY RUN MODE - No actual changes will be made")
        
        try:
            # Stage 1: Initialize Tables
            logger.info("\n" + "="*50)
            logger.info("STAGE 1: INITIALIZE RAW TABLES")
            logger.info("="*50)
            
            if dry_run:
                logger.info("[DRY RUN] Would initialize raw tables")
                results['init'] = {'create_tables': True, 'verify_tables': True}
            else:
                results['init'] = self.operations.init_tables(verify=True)
            
            # Stage 2: Load Data
            logger.info("\n" + "="*50)
            logger.info("STAGE 2: LOAD DATA FROM S3")
            logger.info("="*50)
            
            results['load'] = self.operations.load_data(dry_run=dry_run)
            
            # Stage 3: Verify Data (optional)
            if not skip_verify:
                logger.info("\n" + "="*50)
                logger.info("STAGE 3: VERIFY DATA INTEGRITY")
                logger.info("="*50)
                
                results['verify'] = self.operations.verify_data(detailed=True)
            else:
                logger.info("\n" + "="*50)
                logger.info("STAGE 3: VERIFY DATA INTEGRITY (SKIPPED)")
                logger.info("="*50)
                results['verify'] = {'skipped': True}
            
            # Stage 4: Export Marts (optional)
            if not skip_export:
                logger.info("\n" + "="*50)
                logger.info("STAGE 4: EXPORT MARTS TO S3")
                logger.info("="*50)
                
                results['export'] = self.operations.export_marts(dry_run=dry_run)
            else:
                logger.info("\n" + "="*50)
                logger.info("STAGE 4: EXPORT MARTS TO S3 (SKIPPED)")
                logger.info("="*50)
                results['export'] = {'skipped': True}
            
            # Pipeline Summary
            self._print_pipeline_summary(results, dry_run)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"✗ PIPELINE FAILED: {e}")
            logger.error("=" * 80)
            raise
        
        return results
    
    def run_init_stage(self, dry_run: bool = False) -> Dict[str, bool]:
        """Run only the initialization stage."""
        logger.info("Running initialization stage...")
        
        if dry_run:
            logger.info("[DRY RUN] Would initialize raw tables")
            return {'create_tables': True, 'verify_tables': True}
        
        return self.operations.init_tables(verify=True)
    
    def run_load_stage(self, table_name: Optional[str] = None, 
                      dry_run: bool = False, 
                      replace: bool = True) -> Dict[str, bool]:
        """Run only the data loading stage."""
        logger.info("Running data loading stage...")
        
        return self.operations.load_data(
            table_name=table_name,
            dry_run=dry_run,
            replace=replace
        )
    
    def run_verify_stage(self, table_name: Optional[str] = None, 
                        detailed: bool = True) -> Dict[str, Dict]:
        """Run only the data verification stage."""
        logger.info("Running data verification stage...")
        
        return self.operations.verify_data(
            table_name=table_name,
            detailed=detailed
        )
    
    def run_export_stage(self, include_intermediate: bool = False,
                        table_name: Optional[str] = None,
                        dry_run: bool = False) -> Dict[str, bool]:
        """Run only the mart export stage."""
        logger.info("Running mart export stage...")
        
        return self.operations.export_marts(
            include_intermediate=include_intermediate,
            table_name=table_name,
            dry_run=dry_run
        )
    
    def check_pipeline_status(self) -> Dict[str, any]:
        """
        Check the current status of the pipeline.
        
        Returns:
            Dictionary with pipeline status information
        """
        logger.info("Checking pipeline status...")
        
        status = {
            'tables_exist': False,
            'tables_loaded': False,
            'data_verified': False,
            'marts_available': False,
            'details': {}
        }
        
        try:
            # Check if tables exist
            table_info = self.operations.list_tables()
            expected_tables = ['raw_nyc_legacy', 'raw_nyc_modern', 
                             'raw_london_legacy', 'raw_london_modern']
            
            existing_tables = table_info['available_tables']
            status['tables_exist'] = all(table in existing_tables for table in expected_tables)
            status['details']['existing_tables'] = existing_tables
            status['details']['missing_tables'] = [t for t in expected_tables if t not in existing_tables]
            
            # Check if tables have data
            if status['tables_exist']:
                total_rows = 0
                for table_name in expected_tables:
                    if table_name in table_info['table_details']:
                        row_count = table_info['table_details'][table_name].get('row_count', 0)
                        total_rows += row_count
                
                status['tables_loaded'] = total_rows > 0
                status['details']['total_rows'] = total_rows
                status['details']['row_counts'] = {
                    table: table_info['table_details'].get(table, {}).get('row_count', 0)
                    for table in expected_tables
                }
            
            # Check for mart tables (if they exist)
            mart_tables = ['mart_daily_metrics', 'mart_hourly_patterns', 
                          'mart_nyc_member_analysis', 'mart_station_growth']
            
            status['marts_available'] = any(table in existing_tables for table in mart_tables)
            status['details']['mart_tables'] = [t for t in mart_tables if t in existing_tables]
            
        except Exception as e:
            logger.error(f"Failed to check pipeline status: {e}")
            status['error'] = str(e)
        
        return status
    
    def _print_pipeline_summary(self, results: Dict[str, Dict], dry_run: bool = False):
        """Print a summary of the pipeline execution."""
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        
        if dry_run:
            logger.info("DRY RUN MODE - No actual changes were made")
        
        # Stage 1: Init
        init_results = results.get('init', {})
        if 'create_tables' in init_results:
            logger.info("✓ Stage 1 (Init): Raw tables initialized")
        else:
            logger.info("✗ Stage 1 (Init): Failed")
        
        # Stage 2: Load
        load_results = results.get('load', {})
        if load_results:
            successful_loads = sum(1 for success in load_results.values() if success)
            total_loads = len(load_results)
            logger.info(f"✓ Stage 2 (Load): {successful_loads}/{total_loads} tables loaded")
        else:
            logger.info("✗ Stage 2 (Load): Failed")
        
        # Stage 3: Verify
        verify_results = results.get('verify', {})
        if 'skipped' in verify_results:
            logger.info("⚠ Stage 3 (Verify): Skipped")
        elif verify_results:
            failed_verifications = sum(1 for result in verify_results.values() 
                                     if result.get('status') == 'FAIL')
            total_verifications = len(verify_results)
            if failed_verifications == 0:
                logger.info(f"✓ Stage 3 (Verify): {total_verifications} tables verified")
            else:
                logger.info(f"⚠ Stage 3 (Verify): {failed_verifications}/{total_verifications} tables failed verification")
        else:
            logger.info("✗ Stage 3 (Verify): Failed")
        
        # Stage 4: Export
        export_results = results.get('export', {})
        if 'skipped' in export_results:
            logger.info("⚠ Stage 4 (Export): Skipped")
        elif export_results:
            successful_exports = sum(1 for success in export_results.values() if success)
            total_exports = len(export_results)
            logger.info(f"✓ Stage 4 (Export): {successful_exports}/{total_exports} marts exported")
        else:
            logger.info("✗ Stage 4 (Export): Failed")
        
        logger.info("=" * 80)
        
        if not dry_run:
            logger.info("Next steps:")
            logger.info("1. Run dbt transformations: dbt run")
            logger.info("2. Generate marts: dbt run --select marts")
            logger.info("3. Export marts: python -m db_duckdb export")
        
        logger.info("=" * 80)


# Convenience functions for direct usage
def run_full_pipeline(skip_verify: bool = False, 
                     skip_export: bool = False,
                     dry_run: bool = False,
                     db_path: Optional[str] = None) -> Dict[str, Dict]:
    """
    Convenience function to run the full pipeline.
    
    Args:
        skip_verify: Skip data verification step
        skip_export: Skip mart export step
        dry_run: Run in dry-run mode
        db_path: Optional custom database path
        
    Returns:
        Dictionary with results from each pipeline stage
    """
    pipeline = DuckDBPipeline(db_path)
    return pipeline.run_full_pipeline(
        skip_verify=skip_verify,
        skip_export=skip_export,
        dry_run=dry_run
    )


def check_pipeline_status(db_path: Optional[str] = None) -> Dict[str, any]:
    """
    Convenience function to check pipeline status.
    
    Args:
        db_path: Optional custom database path
        
    Returns:
        Dictionary with pipeline status information
    """
    pipeline = DuckDBPipeline(db_path)
    return pipeline.check_pipeline_status()
