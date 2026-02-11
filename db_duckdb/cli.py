"""
DuckDB Unified CLI

Provides a unified command-line interface for all DuckDB ETL operations.
"""

import click
import logging
from typing import Optional
from pathlib import Path

from .duckdb_manager import DuckDBManager
from .operations import DuckDBOperations
from .pipeline import DuckDBPipeline, run_full_pipeline, check_pipeline_status

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _cli_error(operation_name: str, error: Exception = None, message: str = None):
    """Log a CLI operation failure and raise a ClickException."""
    detail = str(error) if error else (message or "Unknown error")
    logger.error("=" * 60)
    logger.error(f"✗ {operation_name} FAILED: {detail}")
    logger.error("=" * 60)
    raise click.ClickException(detail)


@click.group()
@click.option('--db-path', type=click.Path(), help='Custom database path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, db_path: Optional[str], verbose: bool):
    """
    DuckDB ETL Pipeline CLI
    
    Provides commands for managing the DuckDB-based ETL pipeline for City Cycles analytics.
    """
    # Set up context
    ctx.ensure_object(dict)
    ctx.obj['db_path'] = db_path
    ctx.obj['verbose'] = verbose
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command()
@click.option('--verify/--no-verify', default=True, help='Verify tables after creation')
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
@click.pass_context
def init(ctx, verify: bool, dry_run: bool):
    """
    Initialize raw tables in DuckDB.
    
    Creates the four raw tables that serve as the foundation for the dbt transformation pipeline:
    - raw_nyc_legacy: NYC bike share data (legacy format)
    - raw_nyc_modern: NYC bike share data (modern format)
    - raw_london_legacy: London bike share data (legacy format)
    - raw_london_modern: London bike share data (modern format)
    """
    db_path = ctx.obj['db_path']
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - DUCKDB RAW TABLE INITIALIZATION")
    logger.info("=" * 60)
    
    try:
        operations = DuckDBOperations(db_path)
        
        if dry_run:
            logger.info("DRY RUN MODE - No tables will be created")
            logger.info("Would initialize raw tables with verification" if verify else "Would initialize raw tables without verification")
        else:
            results = operations.init_tables(verify=verify)
            
            if results['create_tables']:
                logger.info("✓ Raw table initialization completed successfully!")
                
                if verify and results['verify_tables']:
                    logger.info("✓ Table verification completed successfully!")
                elif verify and not results['verify_tables']:
                    logger.error("✗ Table verification failed!")
                    raise click.ClickException("Table verification failed")
            else:
                logger.error("✗ Raw table initialization failed!")
                raise click.ClickException("Raw table initialization failed")
        
        logger.info("=" * 60)
        logger.info("✓ INITIALIZATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("INITIALIZATION", error=e)


@cli.command()
@click.option('--table', '-t', help='Load specific table (optional)')
@click.option('--dry-run', is_flag=True, help='Show what would be loaded without actually loading')
@click.option('--append', is_flag=True, help='Append to existing tables instead of replacing')
@click.pass_context
def load(ctx, table: Optional[str], dry_run: bool, append: bool):
    """
    Load data from S3 Parquet files into DuckDB tables.
    
    Loads the four raw tables from S3 Parquet files:
    - raw_nyc_legacy: NYC bike share data (legacy format)
    - raw_nyc_modern: NYC bike share data (modern format)
    - raw_london_legacy: London bike share data (legacy format)
    - raw_london_modern: London bike share data (modern format)
    """
    db_path = ctx.obj['db_path']
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - S3 TO DUCKDB DATA LOADING")
    logger.info("=" * 60)
    
    try:
        operations = DuckDBOperations(db_path)
        
        if table:
            logger.info(f"Loading specific table: {table}")
        else:
            logger.info("Loading all tables")
        
        results = operations.load_data(
            table_name=table,
            dry_run=dry_run,
            replace=not append
        )
        
        if results:
            successful_loads = sum(1 for success in results.values() if success)
            total_loads = len(results)
            
            if successful_loads == total_loads:
                logger.info("=" * 60)
                logger.info("✓ DATA LOADING COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
                logger.info("Next steps:")
                logger.info("1. Run 'db_duckdb verify' to validate data integrity")
                logger.info("2. Test dbt pipeline with: dbt run --select staging")
            else:
                _cli_error("DATA LOADING", message=f"{successful_loads}/{total_loads} tables loaded successfully")
        else:
            _cli_error("DATA LOADING", message="No results returned")

    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("DATA LOADING", error=e)


@cli.command()
@click.option('--table', '-t', help='Verify specific table (optional)')
@click.option('--detailed', is_flag=True, help='Include detailed quality checks')
@click.pass_context
def verify(ctx, table: Optional[str], detailed: bool):
    """
    Verify data integrity and provide summary statistics.
    
    Performs comprehensive data validation including:
    - Schema validation
    - Quality checks (null values, duplicates, date ranges)
    - Summary statistics
    """
    db_path = ctx.obj['db_path']
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - DATA VERIFICATION")
    logger.info("=" * 60)
    
    try:
        operations = DuckDBOperations(db_path)
        
        if table:
            logger.info(f"Verifying specific table: {table}")
        else:
            logger.info("Verifying all tables")
        
        results = operations.verify_data(
            table_name=table,
            detailed=detailed
        )
        
        if results:
            failed_verifications = sum(1 for result in results.values() 
                                     if result.get('status') == 'FAIL')
            total_verifications = len(results)
            
            if failed_verifications == 0:
                logger.info("=" * 60)
                logger.info("✓ DATA VERIFICATION COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
                logger.info("Next steps:")
                logger.info("1. Test dbt pipeline with: dbt run --select staging")
                logger.info("2. Run full dbt pipeline: dbt run")
            else:
                _cli_error("DATA VERIFICATION", message=f"{failed_verifications}/{total_verifications} tables failed verification")
        else:
            _cli_error("DATA VERIFICATION", message="No results returned")

    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("DATA VERIFICATION", error=e)


@cli.command()
@click.option('--include-intermediate', is_flag=True, help='Also export intermediate and unified tables')
@click.option('--table', '-t', help='Export specific table (optional)')
@click.option('--dry-run', is_flag=True, help='Show what would be exported without actually exporting')
@click.pass_context
def export(ctx, include_intermediate: bool, table: Optional[str], dry_run: bool):
    """
    Export dbt mart tables to S3 as Parquet files.
    
    Exports mart tables from DuckDB to S3 as Parquet files for dashboard consumption.
    """
    db_path = ctx.obj['db_path']
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - MART EXPORT TO S3")
    logger.info("=" * 60)
    
    try:
        operations = DuckDBOperations(db_path)
        
        if table:
            logger.info(f"Exporting specific table: {table}")
        else:
            logger.info("Exporting all mart tables")
            if include_intermediate:
                logger.info("Including intermediate tables")
        
        results = operations.export_marts(
            include_intermediate=include_intermediate,
            table_name=table,
            dry_run=dry_run
        )
        
        if results:
            successful_exports = sum(1 for success in results.values() if success)
            total_exports = len(results)
            
            if successful_exports == total_exports:
                logger.info("=" * 60)
                logger.info("✓ EXPORT COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
                logger.info("Next steps:")
                logger.info("1. Use httpfs_connector.py to query the exported Parquet files")
                logger.info("2. Update dashboard to use S3 Parquet files instead of local database")
            else:
                _cli_error("EXPORT", message=f"{successful_exports}/{total_exports} tables exported successfully")
        else:
            _cli_error("EXPORT", message="No results returned")

    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("EXPORT", error=e)


@cli.command()
@click.option('--tables', is_flag=True, help='List available tables')
@click.option('--exports', is_flag=True, help='List existing exports in S3')
@click.option('--marts', is_flag=True, help='List available mart tables in database')
@click.option('--verbose', is_flag=True, help='Show detailed information')
@click.pass_context
def list(ctx, tables: bool, exports: bool, marts: bool, verbose: bool):
    """
    List available tables, exports, or marts.
    
    Provides information about the current state of the database and S3 exports.
    """
    db_path = ctx.obj['db_path']
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - LIST INFORMATION")
    logger.info("=" * 60)
    
    try:
        operations = DuckDBOperations(db_path)
        
        if tables or not any([tables, exports, marts]):
            logger.info("Available tables in database:")
            logger.info("=" * 50)
            
            table_info = operations.list_tables()
            available_tables = table_info['available_tables']
            
            if available_tables:
                for table_name in available_tables:
                    if verbose and table_name in table_info['table_details']:
                        details = table_info['table_details'][table_name]
                        if 'error' not in details:
                            logger.info(f"  {table_name} ({details['row_count']:,} rows, {details['size_mb']} MB)")
                        else:
                            logger.info(f"  {table_name} (error: {details['error']})")
                    else:
                        logger.info(f"  {table_name}")
            else:
                logger.info("  No tables found")
            
            logger.info("")
        
        if exports:
            logger.info("Existing exports in S3:")
            logger.info("=" * 50)
            try:
                import boto3
                from .config.duckdb_config import S3_BUCKET
                s3_client = boto3.client('s3')
                response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix='marts/')
                if 'Contents' in response:
                    for obj in response['Contents']:
                        size_mb = obj['Size'] / (1024 * 1024)
                        last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"  s3://{S3_BUCKET}/{obj['Key']} ({size_mb:.1f} MB, {last_modified})")
                else:
                    logger.info("  No exports found in S3")
            except Exception as e:
                logger.warning(f"  Could not list S3 exports: {e}")
            logger.info("")

        if marts:
            logger.info("Available mart tables in database:")
            logger.info("=" * 50)
            try:
                with DuckDBManager(db_path=db_path) as db:
                    mart_tables = db.list_tables(schema='main_marts')
                if mart_tables:
                    for table_name_item in sorted(mart_tables):
                        if verbose:
                            try:
                                with DuckDBManager(db_path=db_path) as db:
                                    info = db.get_table_info(f"main_marts.{table_name_item}")
                                    logger.info(f"  {table_name_item} ({info['row_count']:,} rows, {info['size_mb']} MB)")
                            except Exception:
                                logger.info(f"  {table_name_item}")
                        else:
                            logger.info(f"  {table_name_item}")
                else:
                    logger.info("  No mart tables found (run dbt first)")
            except Exception as e:
                logger.warning(f"  Could not list mart tables: {e}")
            logger.info("")
        
        logger.info("=" * 60)
        logger.info("✓ LIST COMPLETED")
        logger.info("=" * 60)
        
    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("LIST", error=e)


@cli.command()
@click.option('--skip-verify', is_flag=True, help='Skip data verification step')
@click.option('--skip-export', is_flag=True, help='Skip mart export step')
@click.option('--dry-run', is_flag=True, help='Run in dry-run mode (no actual changes)')
@click.pass_context
def pipeline(ctx, skip_verify: bool, skip_export: bool, dry_run: bool):
    """
    Run the complete ETL pipeline.
    
    Executes all stages of the ETL pipeline in sequence:
    1. Initialize raw tables
    2. Load data from S3
    3. Verify data integrity (optional)
    4. Export marts to S3 (optional)
    """
    db_path = ctx.obj['db_path']
    
    try:
        results = run_full_pipeline(
            skip_verify=skip_verify,
            skip_export=skip_export,
            dry_run=dry_run,
            db_path=db_path
        )
        
        logger.info("=" * 60)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("PIPELINE", error=e)


@cli.command()
@click.pass_context
def status(ctx):
    """
    Check the current status of the pipeline.
    
    Provides information about the current state of tables, data loading, and marts.
    """
    db_path = ctx.obj['db_path']
    
    logger.info("=" * 60)
    logger.info("CITY CYCLES - PIPELINE STATUS")
    logger.info("=" * 60)
    
    try:
        status_info = check_pipeline_status(db_path)
        
        logger.info("Pipeline Status:")
        logger.info("=" * 50)
        
        logger.info(f"Tables exist: {'✓' if status_info['tables_exist'] else '✗'}")
        logger.info(f"Tables loaded: {'✓' if status_info['tables_loaded'] else '✗'}")
        logger.info(f"Data verified: {'✓' if status_info['data_verified'] else '✗'}")
        logger.info(f"Marts available: {'✓' if status_info['marts_available'] else '✗'}")
        
        if 'details' in status_info:
            details = status_info['details']
            
            if 'total_rows' in details:
                logger.info(f"Total rows: {details['total_rows']:,}")
            
            if 'existing_tables' in details:
                logger.info(f"Existing tables: {', '.join(details['existing_tables'])}")
            
            if 'missing_tables' in details and details['missing_tables']:
                logger.info(f"Missing tables: {', '.join(details['missing_tables'])}")
        
        if 'error' in status_info:
            logger.error(f"Error: {status_info['error']}")
        
        logger.info("=" * 60)
        logger.info("✓ STATUS CHECK COMPLETED")
        logger.info("=" * 60)
        
    except click.ClickException:
        raise
    except Exception as e:
        _cli_error("STATUS CHECK", error=e)


if __name__ == '__main__':
    cli()
