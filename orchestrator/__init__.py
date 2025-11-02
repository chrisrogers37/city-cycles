"""
City Cycles Pipeline Orchestrator

A global orchestrator for the City Cycles ETL pipeline that coordinates:
- Data extraction from web sources to S3
- S3 file management (unzip, schema validation, Parquet conversion)
- DuckDB data loading
- dbt transformations
- Mart export to S3
"""

from .main import CityBikesOrchestrator

__version__ = "1.0.0"
__all__ = ["CityBikesOrchestrator"]

