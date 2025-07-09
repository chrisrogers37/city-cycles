# S3 Export and HTTPFS Query System

This directory now includes functionality to export dbt mart tables to S3 as Parquet files and query them directly using DuckDB's HTTPFS extension.

## Overview

The system enables a lightweight dashboard architecture where:
1. **dbt** creates mart tables in local DuckDB
2. **Export scripts** send mart tables to S3 as Parquet files
3. **Dashboard** queries S3 Parquet files directly using HTTPFS

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   dbt Pipeline  │    │   S3 Export     │    │   Dashboard     │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Mart Tables │ │───▶│ │ Parquet     │ │───▶│ │ HTTPFS      │ │
│ │ (DuckDB)    │ │    │ │ Files (S3)  │ │    │ │ Queries     │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Files

### Core Modules

- **`export_to_s3.py`** - Export dbt mart tables to S3 as Parquet files
- **`httpfs_connector.py`** - Query S3 Parquet files using DuckDB's HTTPFS extension
- **`example_workflow.py`** - Complete example demonstrating the workflow

### Configuration

- **`config/duckdb_config.py`** - S3 bucket and database configuration
- **`duckdb_manager.py`** - Reused for database operations

## Usage

### 1. Export Mart Tables to S3

```bash
# Export all mart tables
python db_duckdb/export_to_s3.py

# Export specific table
python db_duckdb/export_to_s3.py --table mart_daily_metrics

# Dry run (see what would be exported)
python db_duckdb/export_to_s3.py --dry-run

# List existing exports
python db_duckdb/export_to_s3.py --list
```

### 2. Query S3 Parquet Files

```python
from db_duckdb.httpfs_connector import HTTPFSConnector, get_daily_metrics

# Use convenience functions
daily_data = get_daily_metrics(location='nyc', year=2023)

# Or use the connector directly
with HTTPFSConnector() as connector:
    data = connector.query_mart('mart_daily_metrics', 'SELECT * FROM "{s3_uri}" LIMIT 10')
```

### 3. Dashboard Integration

```python
# In your dashboard app.py
from db_duckdb.httpfs_connector import get_daily_metrics, get_hourly_patterns

def load_dashboard_data():
    """Load data for dashboard components."""
    daily_metrics = get_daily_metrics()
    hourly_patterns = get_hourly_patterns()
    return daily_metrics, hourly_patterns
```

## Benefits

### Performance
- ✅ **Fast queries** - Parquet is columnar and compressed
- ✅ **Selective loading** - Only read needed columns
- ✅ **No full database** - Dashboard doesn't need local DuckDB

### Scalability
- ✅ **S3 handles load** - No database connection limits
- ✅ **Pay per query** - Only pay for data accessed
- ✅ **Global access** - S3 available from anywhere

### Cost
- ✅ **No database hosting** - S3 is cheaper than RDS
- ✅ **No compute overhead** - HTTPFS is lightweight
- ✅ **Storage efficient** - Parquet compression

## Configuration

### Environment Variables

```bash
# Required for S3 access
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1

# S3 bucket (defaults to city-cycles-data-ctr37)
S3_BUCKET=your-bucket-name
```

### S3 Structure

```
s3://your-bucket/
├── marts/
│   ├── mart_daily_metrics.parquet
│   ├── mart_hourly_patterns.parquet
│   ├── mart_nyc_member_analysis.parquet
│   ├── mart_station_growth.parquet
│   └── mart_daily_metrics_long.parquet
└── extracted_bike_ride_parquet/
    ├── nyc/
    └── london/
```

## Example Workflow

```bash
# 1. Run dbt to create mart tables
cd dbt_city_cycles
dbt run --select marts

# 2. Export marts to S3
cd ..
python db_duckdb/export_to_s3.py

# 3. Test HTTPFS queries
python db_duckdb/example_workflow.py

# 4. Update dashboard to use HTTPFS
# Edit dashboard/app.py to use httpfs_connector
```

## Troubleshooting

### Common Issues

1. **AWS credentials not found**
   - Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables

2. **S3 bucket access denied**
   - Ensure IAM permissions include S3 read/write access

3. **HTTPFS extension not available**
   - DuckDB will automatically install HTTPFS when needed

4. **Parquet files not found**
   - Run `export_to_s3.py` first to create the files

### Debug Commands

```bash
# Check S3 exports
python db_duckdb/export_to_s3.py --list

# Test HTTPFS connection
python db_duckdb/example_workflow.py

# Check environment
echo $AWS_ACCESS_KEY_ID
echo $S3_BUCKET
```

## Next Steps

1. **Export your marts** to S3 using `export_to_s3.py`
2. **Update your dashboard** to use `httpfs_connector.py`
3. **Test the workflow** with `example_workflow.py`
4. **Deploy** your lightweight dashboard

This architecture provides a scalable, cost-effective solution for your bike share analytics dashboard! 