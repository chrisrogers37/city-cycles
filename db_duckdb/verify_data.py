#!/usr/bin/env python3
"""
Verify data integrity and provide summary statistics for loaded raw tables.

This script validates the four raw tables and provides comprehensive statistics:
- raw_nyc_legacy: NYC bike share data (legacy format)
- raw_nyc_modern: NYC bike share data (modern format)  
- raw_london_legacy: London bike share data (legacy format)
- raw_london_modern: London bike share data (modern format)
"""

import sys
import os
from pathlib import Path
from typing import Dict, List

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.duckdb_manager import DuckDBManager
from db_duckdb.config.duckdb_config import S3_URIS, DB_CONFIG, VALIDATION_QUERIES
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_table_data(table_name: str, db: DuckDBManager) -> Dict:
    """
    Verify data for a specific table.
    
    Args:
        table_name: Name of the table to verify
        db: DuckDB manager instance
        
    Returns:
        Dictionary with verification results
    """
    
    logger.info(f"Verifying table: {table_name}")
    
    try:
        # Get basic table info
        table_info = db.get_table_info(table_name)
        
        # Run validation query
        validation_result = db.execute_query(VALIDATION_QUERIES[table_name])
        
        # Additional data quality checks
        quality_checks = run_data_quality_checks(table_name, db)
        
        return {
            'table_name': table_name,
            'basic_info': table_info,
            'validation': validation_result[0] if validation_result else {},
            'quality_checks': quality_checks,
            'status': 'PASS'
        }
        
    except Exception as e:
        logger.error(f"Failed to verify table {table_name}: {e}")
        return {
            'table_name': table_name,
            'status': 'FAIL',
            'error': str(e)
        }

def run_data_quality_checks(table_name: str, db: DuckDBManager) -> Dict:
    """
    Run data quality checks for a table.
    
    Args:
        table_name: Name of the table to check
        db: DuckDB manager instance
        
    Returns:
        Dictionary with quality check results
    """
    
    quality_checks = {}
    
    try:
        # Check for null values in key columns
        if table_name == 'raw_nyc_legacy':
            null_check_query = """
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN tripduration IS NULL THEN 1 ELSE 0 END) as null_tripduration,
                    SUM(CASE WHEN bikeid IS NULL THEN 1 ELSE 0 END) as null_bikeid,
                    SUM(CASE WHEN starttime IS NULL THEN 1 ELSE 0 END) as null_starttime,
                    SUM(CASE WHEN stoptime IS NULL THEN 1 ELSE 0 END) as null_stoptime
                FROM raw_nyc_legacy
            """
        elif table_name == 'raw_nyc_modern':
            null_check_query = """
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN ride_id IS NULL THEN 1 ELSE 0 END) as null_ride_id,
                    SUM(CASE WHEN started_at IS NULL THEN 1 ELSE 0 END) as null_started_at,
                    SUM(CASE WHEN ended_at IS NULL THEN 1 ELSE 0 END) as null_ended_at
                FROM raw_nyc_modern
            """
        elif table_name == 'raw_london_legacy':
            null_check_query = """
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN rental_id IS NULL THEN 1 ELSE 0 END) as null_rental_id,
                    SUM(CASE WHEN bike_id IS NULL THEN 1 ELSE 0 END) as null_bike_id,
                    SUM(CASE WHEN start_date IS NULL THEN 1 ELSE 0 END) as null_start_date,
                    SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) as null_end_date
                FROM raw_london_legacy
            """
        elif table_name == 'raw_london_modern':
            null_check_query = """
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN number IS NULL THEN 1 ELSE 0 END) as null_number,
                    SUM(CASE WHEN bike_number IS NULL THEN 1 ELSE 0 END) as null_bike_number,
                    SUM(CASE WHEN start_date IS NULL THEN 1 ELSE 0 END) as null_start_date,
                    SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) as null_end_date
                FROM raw_london_modern
            """
        else:
            return quality_checks
        
        null_check_result = db.execute_query(null_check_query)
        if null_check_result:
            quality_checks['null_checks'] = null_check_result[0]
        
        # Check for duplicate records
        if table_name == 'raw_nyc_modern':
            duplicate_check_query = """
                SELECT COUNT(*) as duplicate_ride_ids
                FROM (
                    SELECT ride_id, COUNT(*) as cnt
                    FROM raw_nyc_modern
                    GROUP BY ride_id
                    HAVING COUNT(*) > 1
                )
            """
        elif table_name == 'raw_london_legacy':
            duplicate_check_query = """
                SELECT COUNT(*) as duplicate_rental_ids
                FROM (
                    SELECT rental_id, COUNT(*) as cnt
                    FROM raw_london_legacy
                    GROUP BY rental_id
                    HAVING COUNT(*) > 1
                )
            """
        elif table_name == 'raw_london_modern':
            duplicate_check_query = """
                SELECT COUNT(*) as duplicate_numbers
                FROM (
                    SELECT number, COUNT(*) as cnt
                    FROM raw_london_modern
                    GROUP BY number
                    HAVING COUNT(*) > 1
                )
            """
        else:
            duplicate_check_query = "SELECT 0 as duplicate_count"
        
        duplicate_check_result = db.execute_query(duplicate_check_query)
        if duplicate_check_result:
            quality_checks['duplicate_checks'] = duplicate_check_result[0]
        
        # Check date ranges
        if table_name == 'raw_nyc_legacy':
            date_range_query = """
                SELECT 
                    MIN(starttime) as earliest_date,
                    MAX(stoptime) as latest_date
                FROM raw_nyc_legacy
            """
        elif table_name == 'raw_nyc_modern':
            date_range_query = """
                SELECT 
                    MIN(started_at) as earliest_date,
                    MAX(ended_at) as latest_date
                FROM raw_nyc_modern
            """
        else:
            date_range_query = f"""
                SELECT 
                    MIN(start_date) as earliest_date,
                    MAX(end_date) as latest_date
                FROM {table_name}
            """
        
        date_range_result = db.execute_query(date_range_query)
        if date_range_result:
            quality_checks['date_ranges'] = date_range_result[0]
        
    except Exception as e:
        logger.warning(f"Data quality checks failed for {table_name}: {e}")
        quality_checks['error'] = str(e)
    
    return quality_checks

def generate_summary_report(verification_results: List[Dict]) -> str:
    """
    Generate a summary report from verification results.
    
    Args:
        verification_results: List of verification results
        
    Returns:
        Formatted summary report
    """
    
    report = []
    report.append("=" * 80)
    report.append("CITY CYCLES - DATA VERIFICATION SUMMARY REPORT")
    report.append("=" * 80)
    report.append("")
    
    total_rows = 0
    failed_tables = []
    
    for result in verification_results:
        table_name = result['table_name']
        status = result['status']
        
        report.append(f"Table: {table_name}")
        report.append(f"Status: {status}")
        
        if status == 'PASS':
            basic_info = result['basic_info']
            validation = result['validation']
            
            total_rows += basic_info['row_count']
            
            report.append(f"  Rows: {basic_info['row_count']:,}")
            report.append(f"  Size: {basic_info['size_mb']} MB")
            
            if validation:
                report.append("  Validation Results:")
                for key, value in validation.items():
                    if key != 'total_rows':  # Avoid duplicate row count
                        report.append(f"    {key}: {value}")
            
            # Add quality check summary
            quality_checks = result.get('quality_checks', {})
            if quality_checks and 'null_checks' in quality_checks:
                null_checks = quality_checks['null_checks']
                report.append("  Data Quality:")
                for key, value in null_checks.items():
                    if key != 'total_rows' and value > 0:
                        report.append(f"    {key}: {value:,}")
            
        else:
            failed_tables.append(table_name)
            report.append(f"  Error: {result.get('error', 'Unknown error')}")
        
        report.append("")
    
    report.append("=" * 80)
    report.append("SUMMARY")
    report.append("=" * 80)
    report.append(f"Total rows across all tables: {total_rows:,}")
    report.append(f"Tables processed: {len(verification_results)}")
    report.append(f"Tables passed: {len(verification_results) - len(failed_tables)}")
    report.append(f"Tables failed: {len(failed_tables)}")
    
    if failed_tables:
        report.append(f"Failed tables: {', '.join(failed_tables)}")
    
    report.append("=" * 80)
    
    return "\n".join(report)

def main():
    """Main function to verify all raw tables."""
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - DATA VERIFICATION")
    logger.info("=" * 60)
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
            # Verify each table
            verification_results = []
            
            for table_name in S3_URIS.keys():
                result = verify_table_data(table_name, db)
                verification_results.append(result)
            
            # Generate and print summary report
            summary_report = generate_summary_report(verification_results)
            print(summary_report)
            
            # Check if any tables failed
            failed_tables = [r for r in verification_results if r['status'] == 'FAIL']
            
            if failed_tables:
                logger.error("=" * 60)
                logger.error("✗ DATA VERIFICATION FAILED")
                logger.error("=" * 60)
                sys.exit(1)
            else:
                logger.info("=" * 60)
                logger.info("✓ DATA VERIFICATION COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
                logger.info("Next steps:")
                logger.info("1. Test dbt pipeline with: dbt run --select staging")
                logger.info("2. Run full dbt pipeline: dbt run")
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ DATA VERIFICATION FAILED: {e}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main() 