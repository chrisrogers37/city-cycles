#!/usr/bin/env python3
"""
Initialize raw tables in DuckDB for the City Cycles analytics pipeline.

This script creates the four raw tables that serve as the foundation for the dbt transformation pipeline:
- raw_nyc_legacy: NYC bike share data (legacy format)
- raw_nyc_modern: NYC bike share data (modern format)  
- raw_london_legacy: London bike share data (legacy format)
- raw_london_modern: London bike share data (modern format)
"""

import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.duckdb_manager import DuckDBManager
from db_duckdb.config.duckdb_config import TABLE_SCHEMAS, DB_CONFIG
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_raw_tables():
    """Create all raw tables in DuckDB."""
    
    logger.info("Starting raw table initialization...")
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
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
            
            logger.info("✓ Raw table initialization completed successfully!")
            
    except Exception as e:
        logger.error(f"✗ Raw table initialization failed: {e}")
        sys.exit(1)

def verify_tables():
    """Verify that all tables were created correctly."""
    
    logger.info("Verifying table creation...")
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
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

def main():
    """Main function to initialize and verify raw tables."""
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - DUCKDB RAW TABLE INITIALIZATION")
    logger.info("=" * 60)
    
    # Create tables
    create_raw_tables()
    
    # Verify tables
    if verify_tables():
        logger.info("=" * 60)
        logger.info("✓ RAW TABLE INITIALIZATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info("Next steps:")
        logger.info("1. Run load_from_s3.py to load data from S3 Parquet files")
        logger.info("2. Run verify_data.py to validate data integrity")
        logger.info("3. Test dbt pipeline with: dbt run --select staging")
    else:
        logger.error("=" * 60)
        logger.error("✗ RAW TABLE INITIALIZATION FAILED")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main() 