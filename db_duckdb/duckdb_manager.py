import duckdb
import boto3
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBManager:
    """Manages DuckDB connections and operations for the City Cycles analytics pipeline."""
    
    def __init__(self, db_path: str):
        """
        Initialize DuckDB manager.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.con = None
        self._setup_connection()
        self._setup_s3_access()
    
    def _setup_connection(self):
        """Set up DuckDB connection with proper configuration."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Connect to DuckDB
            self.con = duckdb.connect(self.db_path)
            
            # Set memory limits based on configuration (default: 8GB, can be overridden via DUCKDB_MEMORY_LIMIT)
            memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT', '8GB')
            self.con.execute(f"SET memory_limit='{memory_limit}'")
            self.con.execute("SET temp_directory='./temp'")
            
            # Install and load required extensions
            self.con.execute("INSTALL httpfs")
            self.con.execute("LOAD httpfs")
            self.con.execute("INSTALL s3")
            self.con.execute("LOAD s3")
            
            logger.info(f"Connected to DuckDB at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise
    
    def _setup_s3_access(self):
        """Configure S3 access for DuckDB."""
        load_dotenv()
        
        # Get AWS credentials from environment
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        
        if aws_access_key_id and aws_secret_access_key:
            # Set S3 credentials
            self.con.execute(f"SET s3_region='{aws_region}'")
            self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
            self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
            logger.info("S3 access configured")
        else:
            logger.warning("AWS credentials not found. S3 access may be limited.")
    
    def create_table(self, table_name: str, schema_sql: str):
        """
        Create a table in DuckDB.
        
        Args:
            table_name: Name of the table to create
            schema_sql: SQL CREATE TABLE statement
        """
        try:
            # Check if table already exists
            existing_tables = self.list_tables()
            if table_name in existing_tables:
                logger.info(f"Table {table_name} already exists, skipping creation")
                return
            
            self.con.execute(schema_sql)
            logger.info(f"Created table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def load_parquet_from_s3(self, s3_uri: str, table_name: str, replace: bool = True):
        """
        Load Parquet files directly from S3 into DuckDB table.
        
        Args:
            s3_uri: S3 URI pattern (e.g., 's3://bucket/path/*.parquet')
            table_name: Name of the table to load data into
            replace: If True, replace existing table; if False, append
        """
        try:
            if replace:
                # Drop existing table if it exists
                self.con.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table from Parquet files
            create_sql = f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM '{s3_uri}'
            """
            
            logger.info(f"Loading data from {s3_uri} into {table_name}")
            self.con.execute(create_sql)
            
            # Get row count
            row_count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Loaded {row_count:,} rows into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load data from {s3_uri} into {table_name}: {e}")
            raise
    
    def insert_parquet_from_s3(self, s3_uri: str, table_name: str):
        """
        Insert Parquet files from S3 into existing DuckDB table.
        
        Args:
            s3_uri: S3 URI pattern (e.g., 's3://bucket/path/*.parquet')
            table_name: Name of the table to insert data into
        """
        try:
            insert_sql = f"""
                INSERT INTO {table_name}
                SELECT * FROM '{s3_uri}'
            """
            
            logger.info(f"Inserting data from {s3_uri} into {table_name}")
            self.con.execute(insert_sql)
            
            # Get row count
            row_count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Total rows in {table_name}: {row_count:,}")
            
        except Exception as e:
            logger.error(f"Failed to insert data from {s3_uri} into {table_name}: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get information about a table.
        
        Args:
            table_name: Name of the table (can include schema prefix like 'marts.table_name')
            
        Returns:
            Dictionary with table information
        """
        try:
            # Get schema (avoid pandas DataFrame for memory efficiency)
            schema_result = self.con.execute(f"DESCRIBE {table_name}").fetchall()
            schema = [{"column_name": row[0], "column_type": row[1], "default": row[2]} for row in schema_result]
            
            # Get row count
            row_count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Get table size (approximate)
            size_query = f"""
                SELECT 
                    SUM(column_size) as total_size_bytes
                FROM (
                    SELECT 
                        column_name,
                        (CASE 
                            WHEN column_type LIKE '%VARCHAR%' THEN 100::BIGINT
                            WHEN column_type LIKE '%INTEGER%' THEN 8::BIGINT
                            WHEN column_type LIKE '%DOUBLE%' THEN 8::BIGINT
                            WHEN column_type LIKE '%TIMESTAMP%' THEN 8::BIGINT
                            ELSE 50::BIGINT
                        END * {row_count}::BIGINT) as column_size
                    FROM (
                        DESCRIBE {table_name}
                    )
                )
            """
            size_result = self.con.execute(size_query).fetchone()
            total_size_bytes = size_result[0] if size_result else 0
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "schema": schema,  # Already a list of dicts
                "size_mb": round(total_size_bytes / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            raise
    
    def list_tables(self, schema: str = None) -> List[str]:
        """
        List all tables in the database.
        
        Args:
            schema: Optional schema name to filter tables
            
        Returns:
            List of table names
        """
        try:
            if schema:
                # List tables in specific schema
                query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
                tables = self.con.execute(query).fetchall()
                return [table[0] for table in tables]
            else:
                # List all tables in current schema
                tables = self.con.execute("SHOW TABLES").fetchdf()
                return tables['name'].tolist()
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise
    
    def execute_query(self, query: str) -> Optional[Dict]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results as dictionary
        """
        try:
            # Use fetchall() instead of fetchdf() for memory efficiency
            result = self.con.execute(query).fetchall()
            if result:
                # Convert to list of dicts manually
                columns = [desc[0] for desc in self.con.description]
                return [dict(zip(columns, row)) for row in result]
            return []
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def close(self):
        """Close the DuckDB connection."""
        if self.con:
            self.con.close()
            logger.info("DuckDB connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 