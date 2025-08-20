"""
DuckDB ETL Package

Manages the DuckDB-based ETL pipeline for City Cycles analytics project.
Provides data loading from S3 Parquet files, data validation, and mart export capabilities.
"""

from .duckdb_manager import DuckDBManager
from .operations import DuckDBOperations
from .pipeline import DuckDBPipeline, run_full_pipeline, check_pipeline_status

__all__ = [
    'DuckDBManager',
    'DuckDBOperations', 
    'DuckDBPipeline',
    'run_full_pipeline',
    'check_pipeline_status'
] 