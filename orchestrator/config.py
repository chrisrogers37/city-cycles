"""
Configuration for City Cycles Pipeline Orchestrator

This module provides configuration management for the orchestrator.
"""

import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DBT_DIR = PROJECT_ROOT / "dbt_city_cycles"
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# AWS Configuration
AWS_CONFIG = {
    'region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
    's3_bucket': os.getenv('S3_BUCKET', 'city-cycles-data-ctr37'),
    'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
}

# DuckDB Configuration
DUCKDB_CONFIG = {
    'db_path': str(DATA_DIR / 'city_cycles.duckdb'),
    'memory_limit': os.getenv('DUCKDB_MEMORY_LIMIT', '8GB'),
    'threads': int(os.getenv('DUCKDB_THREADS', '4')),
}

# Extraction Configuration
EXTRACTION_CONFIG = {
    'nyc_start_year': int(os.getenv('NYC_START_YEAR', '2019')),
    'enable_nyc': os.getenv('ENABLE_NYC_EXTRACTION', 'true').lower() == 'true',
    'enable_london': os.getenv('ENABLE_LONDON_EXTRACTION', 'true').lower() == 'true',
}

# dbt Configuration
DBT_CONFIG = {
    'profiles_dir': os.getenv('DBT_PROFILES_DIR', str(Path.home() / '.dbt')),
    'target': os.getenv('DBT_TARGET', 'prod'),
}

# Notification Configuration (Optional)
NOTIFICATION_CONFIG = {
    'enable_notifications': os.getenv('ENABLE_NOTIFICATIONS', 'false').lower() == 'true',
    'sns_topic_arn': os.getenv('SNS_TOPIC_ARN'),
    'email_recipients': os.getenv('EMAIL_RECIPIENTS', '').split(',') if os.getenv('EMAIL_RECIPIENTS') else [],
}

# Pipeline Configuration
PIPELINE_CONFIG = {
    'skip_extraction_on_error': os.getenv('SKIP_EXTRACTION_ON_ERROR', 'false').lower() == 'true',
    'continue_on_stage_failure': os.getenv('CONTINUE_ON_STAGE_FAILURE', 'false').lower() == 'true',
    'default_dbt_full_refresh': os.getenv('DEFAULT_DBT_FULL_REFRESH', 'false').lower() == 'true',
}

# Logging Configuration
LOGGING_CONFIG = {
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'log_file': str(LOGS_DIR / 'orchestrator.log'),
    'max_log_size_mb': int(os.getenv('MAX_LOG_SIZE_MB', '100')),
    'log_retention_days': int(os.getenv('LOG_RETENTION_DAYS', '30')),
}


def get_config() -> Dict[str, Any]:
    """
    Get complete configuration dictionary.
    
    Returns:
        Dictionary with all configuration sections
    """
    return {
        'project_root': str(PROJECT_ROOT),
        'data_dir': str(DATA_DIR),
        'dbt_dir': str(DBT_DIR),
        'logs_dir': str(LOGS_DIR),
        'aws': AWS_CONFIG,
        'duckdb': DUCKDB_CONFIG,
        'extraction': EXTRACTION_CONFIG,
        'dbt': DBT_CONFIG,
        'notifications': NOTIFICATION_CONFIG,
        'pipeline': PIPELINE_CONFIG,
        'logging': LOGGING_CONFIG,
    }


def validate_config() -> bool:
    """
    Validate required configuration values.
    
    Returns:
        True if configuration is valid, False otherwise
    """
    errors = []
    
    # Check AWS credentials
    if not AWS_CONFIG['access_key_id']:
        errors.append("AWS_ACCESS_KEY_ID not set")
    if not AWS_CONFIG['secret_access_key']:
        errors.append("AWS_SECRET_ACCESS_KEY not set")
    if not AWS_CONFIG['s3_bucket']:
        errors.append("S3_BUCKET not set")
    
    # Check paths
    if not PROJECT_ROOT.exists():
        errors.append(f"Project root not found: {PROJECT_ROOT}")
    if not DBT_DIR.exists():
        errors.append(f"dbt directory not found: {DBT_DIR}")
    
    # Report errors
    if errors:
        print("Configuration validation errors:")
        for error in errors:
            print(f"  ✗ {error}")
        return False
    
    print("✓ Configuration validated successfully")
    return True


def print_config(show_secrets: bool = False):
    """
    Print current configuration.
    
    Args:
        show_secrets: Whether to show secret values (AWS keys, etc.)
    """
    config = get_config()
    
    print("=" * 80)
    print("CITY CYCLES ORCHESTRATOR CONFIGURATION")
    print("=" * 80)
    
    print(f"\nProject Paths:")
    print(f"  Root: {config['project_root']}")
    print(f"  Data: {config['data_dir']}")
    print(f"  dbt: {config['dbt_dir']}")
    print(f"  Logs: {config['logs_dir']}")
    
    print(f"\nAWS Configuration:")
    print(f"  Region: {config['aws']['region']}")
    print(f"  S3 Bucket: {config['aws']['s3_bucket']}")
    if show_secrets:
        print(f"  Access Key: {config['aws']['access_key_id']}")
    else:
        print(f"  Access Key: {'*' * 20 if config['aws']['access_key_id'] else 'NOT SET'}")
    
    print(f"\nDuckDB Configuration:")
    print(f"  Database: {config['duckdb']['db_path']}")
    print(f"  Memory Limit: {config['duckdb']['memory_limit']}")
    print(f"  Threads: {config['duckdb']['threads']}")
    
    print(f"\nExtraction Configuration:")
    print(f"  NYC Enabled: {config['extraction']['enable_nyc']}")
    print(f"  London Enabled: {config['extraction']['enable_london']}")
    print(f"  NYC Start Year: {config['extraction']['nyc_start_year']}")
    
    print(f"\ndbt Configuration:")
    print(f"  Profiles Dir: {config['dbt']['profiles_dir']}")
    print(f"  Target: {config['dbt']['target']}")
    
    print(f"\nPipeline Configuration:")
    print(f"  Skip Extraction on Error: {config['pipeline']['skip_extraction_on_error']}")
    print(f"  Continue on Stage Failure: {config['pipeline']['continue_on_stage_failure']}")
    print(f"  Default dbt Full Refresh: {config['pipeline']['default_dbt_full_refresh']}")
    
    print(f"\nNotifications:")
    print(f"  Enabled: {config['notifications']['enable_notifications']}")
    if config['notifications']['sns_topic_arn']:
        print(f"  SNS Topic: {config['notifications']['sns_topic_arn']}")
    if config['notifications']['email_recipients']:
        print(f"  Email Recipients: {', '.join(config['notifications']['email_recipients'])}")
    
    print(f"\nLogging:")
    print(f"  Level: {config['logging']['log_level']}")
    print(f"  Log File: {config['logging']['log_file']}")
    
    print("=" * 80)


if __name__ == '__main__':
    import sys
    
    if '--validate' in sys.argv:
        validate_config()
    elif '--show-secrets' in sys.argv:
        print_config(show_secrets=True)
    else:
        print_config(show_secrets=False)

