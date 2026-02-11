#!/usr/bin/env python3
"""
CLI Interface for City Cycles Pipeline Orchestrator

Provides command-line interface for running the orchestrator with various options.
"""

import sys
import argparse
import logging
from pathlib import Path
from .main import CityBikesOrchestrator, _log_section

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """Configure logging based on verbosity level."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='City Cycles Pipeline Orchestrator - End-to-end ETL automation',
        epilog="""
Examples:
  # Run complete pipeline
  python -m orchestrator.cli run
  
  # Run with full dbt refresh
  python -m orchestrator.cli run --dbt-full-refresh
  
  # Skip extraction (use existing S3 files)
  python -m orchestrator.cli run --skip-extraction
  
  # Run individual stage
  python -m orchestrator.cli stage extraction
  python -m orchestrator.cli stage dbt --full-refresh
  
  # Test with verification skip
  python -m orchestrator.cli run --skip-extraction --skip-verify
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Run command - full pipeline
    run_parser = subparsers.add_parser('run', help='Run the complete pipeline')
    run_parser.add_argument(
        '--skip-extraction',
        action='store_true',
        help='Skip data extraction phase (use existing S3 files)'
    )
    run_parser.add_argument(
        '--skip-verify',
        action='store_true',
        help='Skip data verification after DuckDB load'
    )
    run_parser.add_argument(
        '--skip-export',
        action='store_true',
        help='Skip mart export to S3'
    )
    run_parser.add_argument(
        '--dbt-full-refresh',
        action='store_true',
        help='Run dbt with --full-refresh flag (rebuild all incremental models)'
    )
    run_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    # Stage command - run individual stage
    stage_parser = subparsers.add_parser('stage', help='Run a specific pipeline stage')
    stage_parser.add_argument(
        'stage_name',
        choices=['extraction', 'file_management', 'database_load', 'dbt', 'export'],
        help='Name of the stage to run'
    )
    stage_parser.add_argument(
        '--skip-verify',
        action='store_true',
        help='Skip verification (for database_load stage)'
    )
    stage_parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Full refresh (for dbt stage)'
    )
    stage_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    # Status command - check pipeline status
    status_parser = subparsers.add_parser('status', help='Check pipeline status')
    status_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Show help if no command specified
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Setup logging
    setup_logging(verbose=getattr(args, 'verbose', False))
    
    # Initialize orchestrator
    orchestrator = CityBikesOrchestrator()
    
    # Execute command
    try:
        if args.command == 'run':
            success = orchestrator.run(
                skip_extraction=args.skip_extraction,
                skip_verify=args.skip_verify,
                skip_export=args.skip_export,
                dbt_full_refresh=args.dbt_full_refresh
            )
            sys.exit(0 if success else 1)
        
        elif args.command == 'stage':
            success = orchestrator.run_stage(
                args.stage_name,
                skip_verify=getattr(args, 'skip_verify', False),
                full_refresh=getattr(args, 'full_refresh', False)
            )
            sys.exit(0 if success else 1)
        
        elif args.command == 'status':
            check_pipeline_status(orchestrator)
            sys.exit(0)
        
        else:
            parser.print_help()
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        sys.exit(1)


def check_pipeline_status(orchestrator: CityBikesOrchestrator):
    """Check and display pipeline status."""
    from db_duckdb.pipeline import check_pipeline_status

    _log_section("CITY CYCLES PIPELINE STATUS")

    try:
        status = check_pipeline_status()

        logger.info("\nDatabase Status:")
        logger.info(f"  Tables exist: {'✓' if status['tables_exist'] else '✗'}")
        logger.info(f"  Tables loaded: {'✓' if status['tables_loaded'] else '✗'}")
        logger.info(f"  Marts available: {'✓' if status['marts_available'] else '✗'}")

        if 'details' in status:
            details = status['details']

            if 'total_rows' in details:
                logger.info("\nData Volume:")
                logger.info(f"  Total rows: {details['total_rows']:,}")

            if 'existing_tables' in details:
                logger.info("\nExisting Tables:")
                for table in details['existing_tables']:
                    logger.info(f"  - {table}")

            if 'missing_tables' in details and details['missing_tables']:
                logger.info("\nMissing Tables:")
                for table in details['missing_tables']:
                    logger.info(f"  - {table}")

            if 'mart_tables' in details and details['mart_tables']:
                logger.info("\nAvailable Marts:")
                for table in details['mart_tables']:
                    logger.info(f"  - {table}")

        logger.info("")

    except Exception as e:
        logger.error(f"Error checking status: {e}")


if __name__ == '__main__':
    main()

