#!/usr/bin/env python3
"""
Test script to verify database path resolution.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.config.duckdb_config import DB_PATH, PROJECT_ROOT

def test_path_resolution():
    """Test and display the path resolution."""
    
    print("=" * 60)
    print("DATABASE PATH RESOLUTION TEST")
    print("=" * 60)
    
    print(f"Current working directory: {os.getcwd()}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Database path: {DB_PATH}")
    
    # Check if the path is absolute
    if os.path.isabs(DB_PATH):
        print(f"✓ Database path is absolute: {DB_PATH}")
    else:
        print(f"⚠ Database path is relative: {DB_PATH}")
        abs_path = os.path.abspath(DB_PATH)
        print(f"  Absolute path would be: {abs_path}")
    
    # Check if the directory exists
    db_dir = os.path.dirname(DB_PATH)
    if os.path.exists(db_dir):
        print(f"✓ Database directory exists: {db_dir}")
    else:
        print(f"✗ Database directory does not exist: {db_dir}")
    
    # Check if the database file exists
    if os.path.exists(DB_PATH):
        size = os.path.getsize(DB_PATH)
        print(f"✓ Database file exists: {DB_PATH} ({size:,} bytes)")
    else:
        print(f"✗ Database file does not exist: {DB_PATH}")
    
    print("=" * 60)

if __name__ == "__main__":
    test_path_resolution() 