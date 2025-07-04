#!/usr/bin/env python3
"""
Debug script to investigate table creation issues.
"""

import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.duckdb_manager import DuckDBManager
from db_duckdb.config.duckdb_config import DB_CONFIG, TABLE_SCHEMAS

def main():
    """Debug table creation issues."""
    
    print("=" * 60)
    print("CITY CYCLES - TABLE CREATION DEBUG")
    print("=" * 60)
    
    # Check if database file exists
    db_path = DB_CONFIG['db_path']
    print(f"Database path: {db_path}")
    print(f"Database file exists: {os.path.exists(db_path)}")
    if os.path.exists(db_path):
        print(f"Database file size: {os.path.getsize(db_path)} bytes")
    
    try:
        with DuckDBManager(db_path=db_path) as db:
            
            # List all tables
            tables = db.list_tables()
            print(f"\nTables found: {tables}")
            
            # Check expected tables
            expected_tables = list(TABLE_SCHEMAS.keys())
            print(f"Expected tables: {expected_tables}")
            
            missing_tables = set(expected_tables) - set(tables)
            extra_tables = set(tables) - set(expected_tables)
            
            if missing_tables:
                print(f"\n❌ MISSING TABLES: {missing_tables}")
            if extra_tables:
                print(f"\n⚠️  EXTRA TABLES: {extra_tables}")
            
            # Check each table's schema
            for table_name in tables:
                print(f"\n--- {table_name} ---")
                try:
                    # Get schema
                    schema_result = db.con.execute(f"DESCRIBE {table_name}").fetchall()
                    print(f"Columns: {len(schema_result)}")
                    for col in schema_result:
                        print(f"  {col[0]}: {col[1]}")
                    
                    # Get row count
                    row_count = db.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"Row count: {row_count}")
                    
                except Exception as e:
                    print(f"Error checking {table_name}: {e}")
            
            # Try to create missing tables
            if missing_tables:
                print(f"\n" + "=" * 60)
                print("ATTEMPTING TO CREATE MISSING TABLES")
                print("=" * 60)
                
                for table_name in missing_tables:
                    print(f"\nCreating {table_name}...")
                    try:
                        schema_sql = TABLE_SCHEMAS[table_name]
                        db.create_table(table_name, schema_sql)
                        print(f"✅ Successfully created {table_name}")
                    except Exception as e:
                        print(f"❌ Failed to create {table_name}: {e}")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 