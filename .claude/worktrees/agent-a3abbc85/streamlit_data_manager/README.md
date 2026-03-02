# streamlit_data_manager

This module ensures that all required Parquet files for the City Cycles dashboard are present in the local `data/` directory. If any required file is missing, it will be automatically downloaded from S3 when the dashboard starts.

- **parquet_file_manager.py**: Checks for required Parquet files and downloads them from S3 if needed.

## Managed Mart Files

The following 10 Parquet files are downloaded from `s3://{S3_BUCKET}/marts/`:

| File | Description |
|------|-------------|
| `mart_daily_metrics.parquet` | Daily ride counts and metrics by city |
| `mart_hourly_rides.parquet` | Hourly ride aggregations |
| `mart_hourly_patterns_summary.parquet` | Hourly usage patterns |
| `mart_nyc_member_analysis.parquet` | NYC member vs casual rider analysis |
| `mart_station_growth.parquet` | Station count growth over time |
| `mart_daily_metrics_long.parquet` | Long-format daily metrics for flexible charting |
| `mart_weather_ride_correlation.parquet` | Hourly weather-ride correlation data |
| `mart_weather_impact_summary.parquet` | Weather impact statistics vs clear baseline |
| `mart_station_directory.parquet` | Station reference table with coordinates |
| `mart_station_weather_performance.parquet` | Per-station ridership change by weather condition |

Files are downloaded gracefully on dashboard startup — missing files trigger a warning rather than a crash.