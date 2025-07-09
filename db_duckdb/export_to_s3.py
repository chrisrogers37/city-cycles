#!/usr/bin/env python3
"""
Export dbt mart tables to S3 as Parquet files for dashboard consumption.

This module exports the mart tables from DuckDB to S3 as Parquet files,
enabling the dashboard to query them directly using DuckDB's HTTPFS extension.
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional, Dict
import logging

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.duckdb_manager import DuckDBManager
from db_duckdb.config.duckdb_config import DB_CONFIG, S3_BUCKET
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mart tables to export
MART_TABLES = [
    'mart_daily_metrics',
    'mart_hourly_patterns', 
    'mart_nyc_member_analysis',
    'mart_station_growth',
    'mart_daily_metrics_long'
]

def export_table_to_s3(db: DuckDBManager, table_name: str, s3_key: str, dry_run: bool = False) -> bool:
    """
    Export a single table from DuckDB to S3 as Parquet.
    
    Args:
        db: DuckDB manager instance
        table_name: Name of the table in DuckDB
        s3_key: S3 key (path) for the Parquet file
        dry_run: If True, only show what would be exported
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if table exists
        tables = db.list_tables()
        if table_name not in tables:
            logger.error(f"Table {table_name} does not exist in DuckDB")
            return False
        
        # Get table info
        table_info = db.get_table_info(table_name)
        row_count = table_info['row_count']
        
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

def export_all_marts(dry_run: bool = False, replace: bool = True) -> Dict[str, bool]:
    """
    Export all mart tables to S3.
    
    Args:
        dry_run: If True, only show what would be exported
        replace: If True, overwrite existing files in S3
        
    Returns:
        Dict mapping table names to success status
    """
    logger.info("Starting mart table export to S3...")
    
    if dry_run:
        logger.info("DRY RUN MODE - No files will be exported")
    
    results = {}
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
            for table_name in MART_TABLES:
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing: {table_name}")
                logger.info(f"{'='*50}")
                
                # Create S3 key for the table
                s3_key = f"marts/{table_name}.parquet"
                
                success = export_table_to_s3(db, table_name, s3_key, dry_run)
                results[table_name] = success
                
                if not success and not dry_run:
                    logger.error(f"Failed to export {table_name}, stopping export process")
                    break
            
            if not dry_run:
                successful_exports = sum(results.values())
                total_exports = len(MART_TABLES)
                logger.info(f"\n{'='*50}")
                logger.info(f"✓ EXPORT COMPLETED: {successful_exports}/{total_exports} tables exported")
                logger.info(f"{'='*50}")
            else:
                logger.info(f"\n{'='*50}")
                logger.info(f"✓ DRY RUN COMPLETED: {len(MART_TABLES)} tables would be exported")
                logger.info(f"{'='*50}")
                
    except Exception as e:
        logger.error(f"✗ Export process failed: {e}")
        raise
    
    return results

def list_s3_exports():
    """List existing mart exports in S3."""
    try:
        s3_client = boto3.client('s3')
        
        logger.info(f"Existing mart exports in s3://{S3_BUCKET}/marts/:")
        logger.info("=" * 60)
        
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='marts/'
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                size_mb = obj['Size'] / (1024 * 1024)
                last_modified = obj['LastModified']
                logger.info(f"  {key} ({size_mb:.1f} MB, {last_modified})")
        else:
            logger.info("  No mart exports found")
            
    except ClientError as e:
        logger.error(f"Failed to list S3 exports: {e}")

def main():
    """Main function to handle command line arguments and execute export."""
    
    parser = argparse.ArgumentParser(description='Export dbt mart tables to S3 as Parquet files')
    parser.add_argument('--table', '-t', type=str, help='Export specific table (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be exported without actually exporting')
    parser.add_argument('--list', action='store_true', help='List existing exports in S3')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - MART EXPORT TO S3")
    logger.info("=" * 60)
    
    if args.list:
        list_s3_exports()
        return
    
    try:
        if args.table:
            # Export specific table
            if args.table not in MART_TABLES:
                logger.error(f"Invalid table name: {args.table}")
                logger.error(f"Available tables: {MART_TABLES}")
                return
            
            with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
                s3_key = f"marts/{args.table}.parquet"
                success = export_table_to_s3(db, args.table, s3_key, args.dry_run)
                if success:
                    logger.info(f"✓ Successfully exported {args.table}")
                else:
                    logger.error(f"✗ Failed to export {args.table}")
        else:
            # Export all tables
            results = export_all_marts(dry_run=args.dry_run)
            
            if not args.dry_run:
                successful = sum(results.values())
                total = len(MART_TABLES)
                logger.info(f"Export summary: {successful}/{total} tables exported successfully")
        
        logger.info("=" * 60)
        logger.info("✓ EXPORT PROCESS COMPLETED")
        logger.info("=" * 60)
        logger.info("Next steps:")
        logger.info("1. Use httpfs_connector.py to query the exported Parquet files")
        logger.info("2. Update dashboard to use S3 Parquet files instead of local database")
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ EXPORT PROCESS FAILED: {e}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main() 