#!/usr/bin/env python3
"""
Simple script to query DuckDB database and see what was created.
"""

import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.duckdb_manager import DuckDBManager
from db_duckdb.config.duckdb_config import DB_CONFIG

def main():
    """Query the database and show what was created."""
    
    print("=" * 60)
    print("CITY CYCLES - DUCKDB DATABASE QUERY")
    print("=" * 60)
    
    try:
        with DuckDBManager(db_path=DB_CONFIG['db_path']) as db:
            
            # List all tables
            tables = db.list_tables()
            print(f"\nTables in database: {tables}")
            
            # Get info for each table
            for table_name in tables:
                print(f"\n--- {table_name} ---")
                try:
                    table_info = db.get_table_info(table_name)
                    print(f"Rows: {table_info['row_count']:,}")
                    print(f"Size: {table_info['size_mb']} MB")
                    print("Schema:")
                    for col in table_info['schema']:
                        print(f"  {col['column_name']}: {col['column_type']}")
                except Exception as e:
                    print(f"Error getting info for {table_name}: {e}")
            
            # Sample data from each table
            print(f"\n" + "=" * 60)
            print("SAMPLE DATA")
            print("=" * 60)
            
            for table_name in tables:
                print(f"\n--- Sample from {table_name} ---")
                try:
                    sample = db.execute_query(f"SELECT * FROM {table_name} LIMIT 3")
                    if sample:
                        for i, row in enumerate(sample):
                            print(f"Row {i+1}: {row}")
                    else:
                        print("No data in table")
                except Exception as e:
                    print(f"Error querying {table_name}: {e}")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 