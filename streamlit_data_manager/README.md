# streamlit_data_manager

This module ensures that all required Parquet files for the City Cycles dashboard are present in the local `data/` directory. If any required file is missing, it will be automatically downloaded from S3 when the dashboard starts.

- **parquet_file_manager.py**: Checks for required Parquet files and downloads them from S3 if needed.

This ensures the dashboard always has access to the latest data files without manual intervention. 