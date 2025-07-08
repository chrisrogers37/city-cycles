#!/usr/bin/env python3
"""
Load data from S3 Parquet files into DuckDB raw tables.

This script loads the four raw tables from S3 Parquet files:
- raw_nyc_legacy: NYC bike share data (legacy format)
- raw_nyc_modern: NYC bike share data (modern format)  
- raw_london_legacy: London bike share data (legacy format)
- raw_london_modern: London bike share data (modern format)
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.duckdb_manager import DuckDBManager
from db_duckdb.config.duckdb_config import S3_URIS, DB_CONFIG, VALIDATION_QUERIES
import logging
from db_duckdb.utils import log_memory_usage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_all_schemas(dry_run: bool = False, replace: bool = True):
    """
    Load all four raw tables from S3 Parquet files.
    
    Args:
        dry_run: If True, only show what would be loaded without actually loading
        replace: If True, replace existing tables; if False, append to existing tables
    """
    
    logger.info("Starting data loading from S3 Parquet files...")
    
    if dry_run:
        logger.info("DRY RUN MODE - No data will be loaded")
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
            for table_name, s3_uri in S3_URIS.items():
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing: {table_name}")
                logger.info(f"S3 URI: {s3_uri}")
                logger.info(f"{'='*50}")
                
                log_memory_usage(f"BEFORE loading {table_name}")
                try:
                    if dry_run:
                        logger.info(f"[DRY RUN] Would load {table_name} from {s3_uri}")
                        log_memory_usage(f"AFTER dry run for {table_name}")
                    else:
                        db.load_parquet_from_s3(s3_uri, table_name, replace=replace)
                        log_memory_usage(f"AFTER loading {table_name}")
                        logger.info(f"✓ Successfully loaded data into {table_name}")
                        logger.info(f"  Run 'python db_duckdb/verify_data.py' for detailed validation")
                        log_memory_usage(f"AFTER load log for {table_name}")
                
                except Exception as e:
                    logger.error(f"✗ Failed to load {table_name}: {e}")
                    log_memory_usage(f"EXCEPTION during {table_name}")
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

def load_specific_schema(table_name: str, dry_run: bool = False, replace: bool = True):
    """
    Load a specific table from S3 Parquet files.
    
    Args:
        table_name: Name of the table to load
        dry_run: If True, only show what would be loaded without actually loading
        replace: If True, replace existing table; if False, append to existing table
    """
    
    if table_name not in S3_URIS:
        logger.error(f"Invalid table name: {table_name}")
        logger.error(f"Available tables: {list(S3_URIS.keys())}")
        return
    
    s3_uri = S3_URIS[table_name]
    
    logger.info(f"Loading specific table: {table_name}")
    logger.info(f"S3 URI: {s3_uri}")
    
    if dry_run:
        logger.info("DRY RUN MODE - No data will be loaded")
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
            if dry_run:
                logger.info(f"[DRY RUN] Would load {table_name} from {s3_uri}")
            else:
                # Load data from S3
                db.load_parquet_from_s3(s3_uri, table_name, replace=replace)
                
                # Get row count
                table_info = db.get_table_info(table_name)
                rows_loaded = table_info['row_count']
                
                logger.info(f"✓ Successfully loaded {rows_loaded:,} rows into {table_name}")
                
                # Run validation query
                validation_result = db.execute_query(VALIDATION_QUERIES[table_name])
                if validation_result:
                    logger.info(f"Validation results for {table_name}:")
                    for key, value in validation_result[0].items():
                        logger.info(f"  {key}: {value}")
                        
    except Exception as e:
        logger.error(f"✗ Failed to load {table_name}: {e}")
        raise

def list_available_tables():
    """List all available tables and their S3 URIs."""
    
    logger.info("Available tables and their S3 URIs:")
    logger.info("=" * 60)
    
    for table_name, s3_uri in S3_URIS.items():
        logger.info(f"Table: {table_name}")
        logger.info(f"S3 URI: {s3_uri}")
        logger.info("-" * 40)

def main():
    """Main function to handle command line arguments and execute loading."""
    
    parser = argparse.ArgumentParser(description='Load data from S3 Parquet files into DuckDB')
    parser.add_argument('--table', '-t', type=str, help='Load specific table (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be loaded without actually loading')
    parser.add_argument('--append', action='store_true', help='Append to existing tables instead of replacing')
    parser.add_argument('--list', action='store_true', help='List available tables and their S3 URIs')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - S3 TO DUCKDB DATA LOADING")
    logger.info("=" * 60)
    
    if args.list:
        list_available_tables()
        return
    
    try:
        if args.table:
            # Load specific table
            load_specific_schema(args.table, dry_run=args.dry_run, replace=not args.append)
        else:
            # Load all tables
            load_all_schemas(dry_run=args.dry_run, replace=not args.append)
        
        logger.info("=" * 60)
        logger.info("✓ DATA LOADING COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info("Next steps:")
        logger.info("1. Run verify_data.py to validate data integrity")
        logger.info("2. Test dbt pipeline with: dbt run --select staging")
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ DATA LOADING FAILED: {e}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main() 