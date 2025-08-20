"""
DuckDB Operations Module

Contains all business logic for DuckDB ETL operations, extracted from individual scripts
for use in both CLI and programmatic contexts.
"""

import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from .duckdb_manager import DuckDBManager
from .config.duckdb_config import TABLE_SCHEMAS, DB_CONFIG, S3_URIS, VALIDATION_QUERIES
from .utils import log_memory_usage

logger = logging.getLogger(__name__)


class DuckDBOperations:
    """Main operations class for DuckDB ETL pipeline."""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize DuckDB operations.
        
        Args:
            db_path: Optional custom database path. If None, uses default from config.
        """
        self.db_path = db_path or DB_CONFIG['db_path']
    
    def init_tables(self, verify: bool = True) -> Dict[str, bool]:
        """
        Initialize raw tables in DuckDB.
        
        Args:
            verify: Whether to verify tables after creation
            
        Returns:
            Dictionary mapping operation names to success status
        """
        results = {'create_tables': False, 'verify_tables': False}
        
        logger.info("Starting raw table initialization...")
        
        try:
            with DuckDBManager(db_path=self.db_path) as db:
                # Create each raw table
                for table_name, schema_sql in TABLE_SCHEMAS.items():
                    logger.info(f"Creating table: {table_name}")
                    
                    try:
                        db.create_table(table_name, schema_sql)
                        logger.info(f"✓ Successfully created table: {table_name}")
                        
                    except Exception as e:
                        logger.error(f"✗ Failed to create table {table_name}: {e}")
                        raise
                
                # List all tables to verify creation
                tables = db.list_tables()
                logger.info(f"All tables in database: {tables}")
                
                results['create_tables'] = True
                logger.info("✓ Raw table initialization completed successfully!")
                
                # Verify tables if requested
                if verify:
                    results['verify_tables'] = self._verify_tables(db)
                
        except Exception as e:
            logger.error(f"✗ Raw table initialization failed: {e}")
            raise
        
        return results
    
    def _verify_tables(self, db: DuckDBManager) -> bool:
        """Verify that all tables were created correctly."""
        logger.info("Verifying table creation...")
        
        try:
            expected_tables = list(TABLE_SCHEMAS.keys())
            actual_tables = db.list_tables()
            
            missing_tables = set(expected_tables) - set(actual_tables)
            extra_tables = set(actual_tables) - set(expected_tables)
            
            if missing_tables:
                logger.error(f"✗ Missing tables: {missing_tables}")
                return False
            
            if extra_tables:
                logger.warning(f"⚠ Extra tables found: {extra_tables}")
            
            # Verify table schemas
            for table_name in expected_tables:
                try:
                    table_info = db.get_table_info(table_name)
                    logger.info(f"✓ Table {table_name}: {table_info['row_count']} rows, {table_info['size_mb']} MB")
                except Exception as e:
                    logger.error(f"✗ Failed to verify table {table_name}: {e}")
                    return False
            
            logger.info("✓ All tables verified successfully!")
            return True
            
        except Exception as e:
            logger.error(f"✗ Table verification failed: {e}")
            return False
    
    def load_data(self, table_name: Optional[str] = None, 
                  dry_run: bool = False, 
                  replace: bool = True) -> Dict[str, bool]:
        """
        Load data from S3 Parquet files into DuckDB tables.
        
        Args:
            table_name: Specific table to load. If None, loads all tables.
            dry_run: If True, only show what would be loaded without actually loading
            replace: If True, replace existing tables; if False, append to existing tables
            
        Returns:
            Dictionary mapping table names to success status
        """
        results = {}
        
        logger.info("Starting data loading from S3 Parquet files...")
        
        if dry_run:
            logger.info("DRY RUN MODE - No data will be loaded")
        
        try:
            with DuckDBManager(db_path=self.db_path) as db:
                
                # Determine which tables to load
                tables_to_load = {table_name: S3_URIS[table_name]} if table_name else S3_URIS
                
                for table_name, s3_uri in tables_to_load.items():
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Processing: {table_name}")
                    logger.info(f"S3 URI: {s3_uri}")
                    logger.info(f"{'='*50}")
                    
                    log_memory_usage(f"BEFORE loading {table_name}")
                    try:
                        if dry_run:
                            logger.info(f"[DRY RUN] Would load {table_name} from {s3_uri}")
                            log_memory_usage(f"AFTER dry run for {table_name}")
                            results[table_name] = True
                        else:
                            db.load_parquet_from_s3(s3_uri, table_name, replace=replace)
                            log_memory_usage(f"AFTER loading {table_name}")
                            logger.info(f"✓ Successfully loaded data into {table_name}")
                            results[table_name] = True
                            log_memory_usage(f"AFTER load log for {table_name}")
                    
                    except Exception as e:
                        logger.error(f"✗ Failed to load {table_name}: {e}")
                        log_memory_usage(f"EXCEPTION during {table_name}")
                        results[table_name] = False
                        if not dry_run:
                            raise
                
                if not dry_run:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"✓ DATA LOADING COMPLETED SUCCESSFULLY")
                    logger.info(f"{'='*50}")
                else:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"✓ DRY RUN COMPLETED")
                    logger.info(f"{'='*50}")
                    
        except Exception as e:
            logger.error(f"✗ Data loading failed: {e}")
            log_memory_usage("EXCEPTION during data loading")
            raise
        
        return results
    
    def verify_data(self, table_name: Optional[str] = None, 
                   detailed: bool = False) -> Dict[str, Dict]:
        """
        Verify data integrity and provide summary statistics.
        
        Args:
            table_name: Specific table to verify. If None, verifies all tables.
            detailed: Whether to include detailed quality checks
            
        Returns:
            Dictionary mapping table names to verification results
        """
        results = {}
        
        logger.info("Starting data verification...")
        
        try:
            with DuckDBManager(db_path=self.db_path) as db:
                
                # Determine which tables to verify
                tables_to_verify = [table_name] if table_name else list(S3_URIS.keys())
                
                for table_name in tables_to_verify:
                    if table_name not in S3_URIS:
                        logger.error(f"Invalid table name: {table_name}")
                        continue
                    
                    result = self._verify_table_data(table_name, db, detailed)
                    results[table_name] = result
                
                # Generate summary report
                if not table_name:  # Only for full verification
                    summary_report = self._generate_summary_report(list(results.values()))
                    logger.info("\n" + summary_report)
                
        except Exception as e:
            logger.error(f"✗ Data verification failed: {e}")
            raise
        
        return results
    
    def _verify_table_data(self, table_name: str, db: DuckDBManager, 
                          detailed: bool = False) -> Dict:
        """Verify data for a specific table."""
        logger.info(f"Verifying table: {table_name}")
        
        try:
            # Get basic table info
            table_info = db.get_table_info(table_name)
            
            # Run validation query
            validation_result = db.execute_query(VALIDATION_QUERIES[table_name])
            
            result = {
                'table_name': table_name,
                'basic_info': table_info,
                'validation': validation_result[0] if validation_result else {},
                'status': 'PASS'
            }
            
            # Add detailed quality checks if requested
            if detailed:
                quality_checks = self._run_data_quality_checks(table_name, db)
                result['quality_checks'] = quality_checks
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to verify table {table_name}: {e}")
            return {
                'table_name': table_name,
                'status': 'FAIL',
                'error': str(e)
            }
    
    def _run_data_quality_checks(self, table_name: str, db: DuckDBManager) -> Dict:
        """Run data quality checks for a table."""
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
    
    def _generate_summary_report(self, verification_results: List[Dict]) -> str:
        """Generate a summary report from verification results."""
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
    
    def list_tables(self) -> Dict[str, List[str]]:
        """
        List available tables and their information.
        
        Returns:
            Dictionary with table information
        """
        try:
            with DuckDBManager(db_path=self.db_path) as db:
                tables = db.list_tables()
                
                # Get table info for each table
                table_info = {}
                for table_name in tables:
                    try:
                        info = db.get_table_info(table_name)
                        table_info[table_name] = {
                            'row_count': info['row_count'],
                            'size_mb': info['size_mb']
                        }
                    except Exception as e:
                        table_info[table_name] = {'error': str(e)}
                
                return {
                    'available_tables': tables,
                    'table_details': table_info,
                    's3_uris': S3_URIS
                }
                
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise
    
    def export_marts(self, include_intermediate: bool = False, 
                    table_name: Optional[str] = None,
                    dry_run: bool = False) -> Dict[str, bool]:
        """
        Export mart tables to S3 as Parquet files.
        
        Args:
            include_intermediate: Whether to also export intermediate tables
            table_name: Specific table to export. If None, exports all marts.
            dry_run: If True, only show what would be exported
            
        Returns:
            Dictionary mapping table names to success status
        """
        from .config.duckdb_config import S3_BUCKET
        
        # Mart tables to export
        MART_TABLES = [
            'mart_daily_metrics',
            'mart_hourly_patterns', 
            'mart_nyc_member_analysis',
            'mart_station_growth',
            'mart_daily_metrics_long'
        ]
        
        # Intermediate and unified tables (optional exports)
        INTERMEDIATE_TABLES = [
            'int_london_rides',
            'int_nyc_rides',
            'unified_rides'
        ]
        
        results = {}
        
        logger.info("Starting mart table export to S3...")
        
        if dry_run:
            logger.info("DRY RUN MODE - No files will be exported")
        
        try:
            with DuckDBManager(db_path=self.db_path) as db:
                
                # Determine which tables to export
                if table_name:
                    if table_name in MART_TABLES:
                        tables_to_export = [table_name]
                    elif table_name in INTERMEDIATE_TABLES and include_intermediate:
                        tables_to_export = [table_name]
                    else:
                        logger.error(f"Invalid table name: {table_name}")
                        return {}
                else:
                    tables_to_export = MART_TABLES + (INTERMEDIATE_TABLES if include_intermediate else [])
                
                for table_name in tables_to_export:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Processing: {table_name}")
                    logger.info(f"{'='*50}")
                    
                    # Determine S3 key based on table type
                    if table_name in MART_TABLES:
                        s3_key = f"marts/{table_name}.parquet"
                        full_table_name = f"main_marts.{table_name}"
                    else:
                        s3_key = f"intermediate/{table_name}.parquet"
                        full_table_name = table_name
                    
                    success = self._export_table_to_s3(db, full_table_name, s3_key, dry_run)
                    results[table_name] = success
                    
                    if not success and not dry_run:
                        logger.error(f"Failed to export {table_name}, stopping export process")
                        break
                
                if not dry_run:
                    successful_exports = sum(results.values())
                    total_exports = len(tables_to_export)
                    logger.info(f"\n{'='*50}")
                    logger.info(f"✓ EXPORT COMPLETED: {successful_exports}/{total_exports} tables exported")
                    logger.info(f"{'='*50}")
                else:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"✓ DRY RUN COMPLETED: {len(tables_to_export)} tables would be exported")
                    logger.info(f"{'='*50}")
                    
        except Exception as e:
            logger.error(f"✗ Export process failed: {e}")
            raise
        
        return results
    
    def _export_table_to_s3(self, db: DuckDBManager, table_name: str, 
                           s3_key: str, dry_run: bool = False) -> bool:
        """Export a single table from DuckDB to S3 as Parquet."""
        from .config.duckdb_config import S3_BUCKET
        
        try:
            # Check if table exists by trying to get its info
            try:
                table_info = db.get_table_info(table_name)
                row_count = table_info['row_count']
            except Exception:
                logger.error(f"Table {table_name} does not exist in DuckDB")
                return False
            
            if dry_run:
                logger.info(f"[DRY RUN] Would export {table_name} ({row_count:,} rows) to s3://{S3_BUCKET}/{s3_key}")
                return True
            
            # Export to S3 using DuckDB's S3 write capability
            export_sql = f"""
                COPY {table_name} TO 's3://{S3_BUCKET}/{s3_key}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """
            
            logger.info(f"Exporting {table_name} ({row_count:,} rows) to s3://{S3_BUCKET}/{s3_key}")
            db.con.execute(export_sql)
            
            logger.info(f"✓ Successfully exported {table_name} to s3://{S3_BUCKET}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to export {table_name}: {e}")
            return False
