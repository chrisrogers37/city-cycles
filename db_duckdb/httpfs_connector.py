#!/usr/bin/env python3
"""
HTTPFS connector for querying S3 Parquet files directly.

This module provides utilities for connecting to S3 Parquet files using DuckDB's HTTPFS extension,
enabling the dashboard to query mart data without loading the full database.
"""

import duckdb
import os
from typing import List, Dict, Optional, Any
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HTTPFSConnector:
    """Connector for querying S3 Parquet files using DuckDB's HTTPFS extension."""
    
    def __init__(self, s3_bucket: str = None):
        """
        Initialize HTTPFS connector.
        
        Args:
            s3_bucket: S3 bucket name (defaults to environment variable)
        """
        load_dotenv()
        
        self.s3_bucket = s3_bucket or os.environ.get("S3_BUCKET", "city-cycles-data-ctr37")
        self.con = None
        self._setup_connection()
        self._setup_s3_access()
    
    def _setup_connection(self):
        """Set up DuckDB connection with HTTPFS extension."""
        try:
            # Create in-memory DuckDB connection
            self.con = duckdb.connect(':memory:')
            
            # Install and load HTTPFS extension
            self.con.execute("INSTALL httpfs")
            self.con.execute("LOAD httpfs")
            
            # Set memory limits for dashboard environment
            self.con.execute("SET memory_limit='256MB'")
            self.con.execute("SET threads=2")
            
            logger.info("HTTPFS connector initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize HTTPFS connector: {e}")
            raise
    
    def _setup_s3_access(self):
        """Configure S3 access for HTTPFS."""
        # Get AWS credentials from environment
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        
        if aws_access_key_id and aws_secret_access_key:
            # Set S3 credentials
            self.con.execute(f"SET s3_region='{aws_region}'")
            self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
            self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
            logger.info("S3 access configured for HTTPFS")
        else:
            logger.warning("AWS credentials not found. S3 access may be limited.")
    
    def query_mart(self, mart_name: str, query: str) -> List[Dict[str, Any]]:
        """
        Query a specific mart table from S3.
        
        Args:
            mart_name: Name of the mart table (e.g., 'mart_daily_metrics')
            query: SQL query to execute
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            # Create S3 URI for the mart table
            s3_uri = f"s3://{self.s3_bucket}/marts/{mart_name}.parquet"
            
            # Execute query against S3 Parquet file
            result = self.con.execute(query, {"s3_uri": s3_uri}).fetchall()
            
            # Convert to list of dictionaries
            if result:
                columns = [desc[0] for desc in self.con.description]
                return [dict(zip(columns, row)) for row in result]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Failed to query {mart_name}: {e}")
            raise
    
    def get_mart_info(self, mart_name: str) -> Dict[str, Any]:
        """
        Get information about a mart table in S3.
        
        Args:
            mart_name: Name of the mart table
            
        Returns:
            Dictionary with table information
        """
        try:
            s3_uri = f"s3://{self.s3_bucket}/marts/{mart_name}.parquet"
            
            # Get schema
            schema_query = f"DESCRIBE SELECT * FROM '{s3_uri}' LIMIT 0"
            schema_result = self.con.execute(schema_query).fetchall()
            schema = [{"column_name": row[0], "column_type": row[1]} for row in schema_result]
            
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM '{s3_uri}'"
            count_result = self.con.execute(count_query).fetchone()
            row_count = count_result[0] if count_result else 0
            
            return {
                "mart_name": mart_name,
                "s3_uri": s3_uri,
                "row_count": row_count,
                "schema": schema
            }
            
        except Exception as e:
            logger.error(f"Failed to get info for {mart_name}: {e}")
            raise
    
    def list_available_marts(self) -> List[str]:
        """List all available mart tables in S3."""
        try:
            # Query S3 to list available mart files
            list_query = f"SELECT name FROM s3_list_directory('s3://{self.s3_bucket}/marts/') WHERE name LIKE '%.parquet'"
            result = self.con.execute(list_query).fetchall()
            
            # Extract mart names from file names
            marts = []
            for row in result:
                filename = row[0]
                if filename.endswith('.parquet'):
                    mart_name = filename.replace('.parquet', '')
                    marts.append(mart_name)
            
            return marts
            
        except Exception as e:
            logger.error(f"Failed to list available marts: {e}")
            return []
    
    def execute_custom_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a custom query against S3 Parquet files.
        
        Args:
            query: SQL query with placeholders for S3 URIs
            params: Dictionary of parameters for the query
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            if params:
                result = self.con.execute(query, params).fetchall()
            else:
                result = self.con.execute(query).fetchall()
            
            # Convert to list of dictionaries
            if result:
                columns = [desc[0] for desc in self.con.description]
                return [dict(zip(columns, row)) for row in result]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Failed to execute custom query: {e}")
            raise
    
    def close(self):
        """Close the DuckDB connection."""
        if self.con:
            self.con.close()
            logger.info("HTTPFS connector closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Convenience functions for common dashboard queries
def get_daily_metrics(location: str = None, year: int = None) -> List[Dict[str, Any]]:
    """
    Get daily metrics data.
    
    Args:
        location: Filter by location ('nyc' or 'london')
        year: Filter by year
        
    Returns:
        List of daily metrics records
    """
    with HTTPFSConnector() as connector:
        query = "SELECT * FROM '{s3_uri}'"
        params = {"s3_uri": f"s3://{connector.s3_bucket}/marts/mart_daily_metrics.parquet"}
        
        if location:
            query += f" WHERE location = '{location}'"
        if year:
            query += f" AND year = {year}"
        
        query += " ORDER BY date, location"
        
        return connector.execute_custom_query(query, params)

def get_hourly_patterns(location: str = None, day_type: str = None) -> List[Dict[str, Any]]:
    """
    Get hourly patterns data.
    
    Args:
        location: Filter by location ('nyc' or 'london')
        day_type: Filter by day type ('weekday' or 'weekend')
        
    Returns:
        List of hourly patterns records
    """
    with HTTPFSConnector() as connector:
        query = "SELECT * FROM '{s3_uri}'"
        params = {"s3_uri": f"s3://{connector.s3_bucket}/marts/mart_hourly_patterns.parquet"}
        
        if location:
            query += f" WHERE location = '{location}'"
        if day_type:
            query += f" AND day_type = '{day_type}'"
        
        query += " ORDER BY location, day_type, hour_of_day"
        
        return connector.execute_custom_query(query, params)

def get_station_growth(location: str = None) -> List[Dict[str, Any]]:
    """
    Get station growth data.
    
    Args:
        location: Filter by location ('nyc' or 'london')
        
    Returns:
        List of station growth records
    """
    with HTTPFSConnector() as connector:
        query = "SELECT * FROM '{s3_uri}'"
        params = {"s3_uri": f"s3://{connector.s3_bucket}/marts/mart_station_growth.parquet"}
        
        if location:
            query += f" WHERE location = '{location}'"
        
        query += " ORDER BY location, year"
        
        return connector.execute_custom_query(query, params)

# Example usage and testing
if __name__ == "__main__":
    # Test the connector
    with HTTPFSConnector() as connector:
        print("Available marts:")
        marts = connector.list_available_marts()
        for mart in marts:
            print(f"  - {mart}")
        
        if marts:
            print(f"\nInfo for {marts[0]}:")
            info = connector.get_mart_info(marts[0])
            print(f"  Rows: {info['row_count']}")
            print(f"  Columns: {len(info['schema'])}") 