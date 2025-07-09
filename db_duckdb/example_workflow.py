#!/usr/bin/env python3
"""
Example workflow demonstrating the complete S3 export and HTTPFS query process.

This script shows how to:
1. Export dbt mart tables to S3 as Parquet files
2. Query the exported Parquet files using HTTPFS
3. Use the data in a dashboard-like application
"""

import sys
from pathlib import Path
import logging

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from db_duckdb.export_to_s3 import export_all_marts, list_s3_exports
from db_duckdb.httpfs_connector import HTTPFSConnector, get_daily_metrics, get_hourly_patterns, get_station_growth

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def example_export_workflow():
    """Example of exporting mart tables to S3."""
    
    logger.info("=" * 60)
    logger.info("EXAMPLE: EXPORTING MARTS TO S3")
    logger.info("=" * 60)
    
    # Step 1: List existing exports
    logger.info("Step 1: Checking existing S3 exports...")
    list_s3_exports()
    
    # Step 2: Export all marts (dry run first)
    logger.info("\nStep 2: Exporting mart tables to S3...")
    results = export_all_marts(dry_run=True)  # Set to False to actually export
    
    logger.info(f"Export results: {results}")
    
    return results

def example_query_workflow():
    """Example of querying S3 Parquet files using HTTPFS."""
    
    logger.info("\n" + "=" * 60)
    logger.info("EXAMPLE: QUERYING S3 PARQUET FILES")
    logger.info("=" * 60)
    
    try:
        with HTTPFSConnector() as connector:
            
            # Step 1: List available marts
            logger.info("Step 1: Listing available mart tables...")
            marts = connector.list_available_marts()
            logger.info(f"Available marts: {marts}")
            
            if not marts:
                logger.warning("No mart tables found in S3. Run export_to_s3.py first.")
                return
            
            # Step 2: Get info about a specific mart
            logger.info(f"\nStep 2: Getting info about {marts[0]}...")
            info = connector.get_mart_info(marts[0])
            logger.info(f"Mart info: {info}")
            
            # Step 3: Query daily metrics
            logger.info("\nStep 3: Querying daily metrics...")
            daily_data = get_daily_metrics(location='nyc', year=2023)
            logger.info(f"Found {len(daily_data)} daily metrics records for NYC 2023")
            
            if daily_data:
                logger.info(f"Sample record: {daily_data[0]}")
            
            # Step 4: Query hourly patterns
            logger.info("\nStep 4: Querying hourly patterns...")
            hourly_data = get_hourly_patterns(location='nyc', day_type='weekday')
            logger.info(f"Found {len(hourly_data)} hourly pattern records for NYC weekdays")
            
            # Step 5: Query station growth
            logger.info("\nStep 5: Querying station growth...")
            growth_data = get_station_growth(location='nyc')
            logger.info(f"Found {len(growth_data)} station growth records for NYC")
            
            if growth_data:
                logger.info(f"Sample growth record: {growth_data[0]}")
            
            # Step 6: Custom query example
            logger.info("\nStep 6: Custom query example...")
            custom_query = """
                SELECT 
                    location,
                    year,
                    COUNT(*) as total_rides,
                    AVG(avg_duration_minutes) as avg_duration
                FROM '{s3_uri}'
                WHERE year >= 2020
                GROUP BY location, year
                ORDER BY location, year
            """
            
            custom_data = connector.execute_custom_query(
                custom_query, 
                {"s3_uri": f"s3://{connector.s3_bucket}/marts/mart_daily_metrics.parquet"}
            )
            logger.info(f"Custom query results: {custom_data}")
    
    except Exception as e:
        logger.error(f"Error in query workflow: {e}")

def example_dashboard_integration():
    """Example of how to integrate with a dashboard application."""
    
    logger.info("\n" + "=" * 60)
    logger.info("EXAMPLE: DASHBOARD INTEGRATION")
    logger.info("=" * 60)
    
    # Simulate dashboard data loading
    logger.info("Loading dashboard data from S3...")
    
    try:
        # Load data for different dashboard components
        daily_metrics = get_daily_metrics()
        hourly_patterns = get_hourly_patterns()
        station_growth = get_station_growth()
        
        # Calculate summary statistics
        total_rides = sum(record.get('total_rides', 0) for record in daily_metrics)
        avg_duration = sum(record.get('avg_duration_minutes', 0) for record in daily_metrics) / len(daily_metrics) if daily_metrics else 0
        
        logger.info(f"Dashboard Summary:")
        logger.info(f"  Total rides: {total_rides:,}")
        logger.info(f"  Average duration: {avg_duration:.1f} minutes")
        logger.info(f"  Daily metrics records: {len(daily_metrics)}")
        logger.info(f"  Hourly patterns records: {len(hourly_patterns)}")
        logger.info(f"  Station growth records: {len(station_growth)}")
        
        # Example of filtering data for specific dashboard views
        nyc_2023_data = [r for r in daily_metrics if r.get('location') == 'nyc' and r.get('year') == 2023]
        logger.info(f"  NYC 2023 records: {len(nyc_2023_data)}")
        
    except Exception as e:
        logger.error(f"Error in dashboard integration: {e}")

def main():
    """Main function to run the complete example workflow."""
    
    logger.info("CITY CYCLES - S3 EXPORT & HTTPFS QUERY WORKFLOW")
    logger.info("=" * 60)
    
    try:
        # Run export workflow
        export_results = example_export_workflow()
        
        # Run query workflow
        example_query_workflow()
        
        # Run dashboard integration example
        example_dashboard_integration()
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ EXAMPLE WORKFLOW COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info("Next steps:")
        logger.info("1. Run 'python db_duckdb/export_to_s3.py' to export marts")
        logger.info("2. Update your dashboard to use HTTPFS connector")
        logger.info("3. Test with real data queries")
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ EXAMPLE WORKFLOW FAILED: {e}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main() 