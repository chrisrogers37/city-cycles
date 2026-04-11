# Changelog

All notable changes to the City Cycles Analytics project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Analytics Deep Dive — Phase 5A** - Two new pages: Ride Analytics (`/analytics`) with key metrics cards, monthly trend chart (year overlay + avg/total toggle), duration trends, hourly bar chart (current hour highlight), member percentage (NYC only), station growth; Weather Deep Dive (`/weather`) with temperature vs rides, precipitation impact, weather condition impact (horizontal RdYlGn bars), hourly weather impact (rain/snow/fog lines). Shared infrastructure: chart theme constants, ChartContainer, MetricCard, DataTable (sortable), DatePresetBar (year presets). All charts highlight today's conditions via existing useInsights hook. NavBar updated with Analytics and Weather links.
- **City Comparison Mode (Phase 04)** - New `/compare` page with side-by-side NYC vs London analysis: dual weather header with biking scores, comparison stats table (rides, duration, peak hours, member/casual split computed from hourly data), cross-city narrative insight (detects same/different weather), dual hourly chart with 4 lines (similar-day + overall for each city) and absolute/normalized Y-axis toggle. NavBar updated with Compare link and `alwaysVisible` prop for secondary pages.
- **Canvas Weather Particles (Phase 2B)** - Canvas-based particle system for immersive weather effects: rain drops (60-100 particles with wind drift), drizzle (lighter 30-50 particles), heavy rain/thunderstorm (120-150 particles), snow (50-80 with sinusoidal drift), branching lightning bolts with glow and screen shake. Mobile-optimized with halved particle counts on viewports < 768px.
- **"Days Like Today" Visualizations (Phase 03)** - Enhanced landing page with richer similar-day data: DurationInsight component (ride duration comparison with directional arrows), MemberCasualSplit component (horizontal stacked bar computed from hourly data), enhanced SimilarDayCard with precipitation context and limited-data warnings, HourlyPatternChart with peak hour annotation and line-draw animation. Shared `format.ts` helpers for consistent number formatting across components.
- **Weather Experience Frontend (Phase 2A)** - Next.js 16 + TypeScript + Tailwind CSS frontend (`frontend/`) with immersive weather landing page. Features: time-of-day sky gradients, CSS weather effects (fog, clouds, sun glow), city silhouette SVGs, animated biking score gauge, "Days Like Today" card, hourly ride pattern chart (Recharts), insight cards, 24-hour forecast strip, city toggle (NYC/London), Zustand state management, SWR data fetching with 5-minute revalidation. Canvas particle effects (rain, snow, lightning) deferred to Phase 2B.
- **FastAPI Backend** - New API layer (`api/`) with 17 endpoints wrapping existing weather service, recommendation engine, and DuckDB parquet queries. Endpoints: weather (2), insights (1), similar-day (2), analytics (10), health (1), plus auto-generated OpenAPI docs
- **Weather Extraction Service** - Decoupled weather extraction from the monthly pipeline into a standalone entrypoint (`scripts/weather_entrypoint.sh`) with dedicated Railway config (`railway-weather.toml`) for independent 6-hour cron scheduling, enabling near-real-time weather data for dashboard recommendations

### Changed
- **dbt Views Conversion** - Converted staging, intermediate, and unified dbt models from incremental tables to views, eliminating 23+ hour full-refresh hangs on 216M+ row datasets; marts remain as physical tables

### Fixed
- **Missing Mart Export** - Added `mart_similar_day_stats` to the `MART_TABLES` export list in `db_duckdb/operations.py`, fixing the mart never being exported to S3 despite dbt producing it

### Improved
- **Insight Card Styling** - Neutral/info cards get subtle blue tint and all cards get colored left border matching severity (green/blue/amber/red)
- **Bar Chart Gradients** - Station growth and hourly bar charts use dark-to-light atmospheric blue gradient instead of flat color, making data values visually apparent
- **Chart Descriptions** - Added descriptive captions below each chart section heading on Ride Analytics page for first-time visitor context
- **Forecast Chart Polish** - Enabled legend, added Y-axis labels (Temp °C, Precip mm), and increased chart height to accommodate legend on landing page forecast
- **Forecast Context** - Added caption explaining temperature line vs precipitation bars on landing page
- **Chart Axis Labels** - Added human-readable axis labels to hourly bar chart, station growth chart, and member percentage chart (replacing raw column names like `metric_value`, `member_percentage`)
- **Member Percentage X-Axis** - Formatted monthly ticks as "Jan 2023" instead of showing artificial 1st-of-month dates
- **Average Daily Rides Format** - Changed from decimal (85,111.7) to integer (85,112) since daily ride counts are whole numbers
- **Metrics Time Period Context** - Added date range caption below top-line metrics on Ride Analytics page

### Fixed
- **City Comparison Page Crash** - Cast `numpy.int64` to Python `int` when passing DuckDB query results back as parameters, fixing "Unable to transform python value of type '<class 'numpy.int64'>' to DuckDB LogicalType" crash
- **Hourly Patterns Raw Error** - Added `parquet_exists()` pre-check before querying `mart_hourly_patterns_summary.parquet`, replacing raw IO Error with user-friendly info message
- **Developer-Facing Empty States** - Replaced "Run the full pipeline to generate weather mart data" messages with user-friendly descriptions across Weather Deep Dive, Station Weather Performance, and Weather Impact Comparison sections

### Added
- **"Days Like Today" Contextual Insights** - Landing page now shows historical ride patterns for similar conditions
  - Queries mart_similar_day_stats by current month, day type, temperature band, and precipitation
  - Generates natural-language insights like "On similar February weekdays with light rain, NYC averaged 12,400 rides"
  - Includes trip duration comparisons and peak activity hour insights
  - Graceful degradation when mart data is unavailable
- **Similar Day Statistics Mart** - New `mart_similar_day_stats` dbt model for "days like today" weather queries
  - Pre-computes ride statistics by (location, month, day_type, temperature_band, precipitation_intensity)
  - Dual granularity: daily totals and hourly patterns in a single table
  - Includes pct_change_vs_overall, duration_pct_change_vs_overall, peak_hour_start/peak_hour_end
  - Full schema.yml documentation and test coverage
- **Weather Deep Dive Empty States** - Added user-friendly empty-state UI when weather mart data is missing
  - Shows informational message instead of blank page or red error boxes
  - Per-section feedback when a query returns no rows for the selected city
  - Added `parquet_exists()` helper to `dashboard/utils/query_helpers.py`

### Improved
- **Dashboard Empty States** -- Weather-dependent dashboard sections now show styled info messages when mart parquet files are missing or queries return no data, instead of rendering blank space or raw error messages
  - Hardened Station Weather Performance section in Ride Analytics page
  - Hardened Weather Impact section in City Comparison page
  - Fixed recommendation engine edge case where fully missing data showed confusing "0 days" message

### Fixed
- **Weather Deep Dive Column Bug** - Fixed `total_rides` -> `ride_count` column reference in temperature and precipitation queries
  - Queries now match the actual `mart_weather_ride_correlation` schema
  - Previously caused silent failures (empty charts) even with populated data

### Fixed
- **Weather Pipeline End-to-End** - Validated and fixed the weather data pipeline from extraction through mart export
  - Added `source_file` column to weather parquet output for lineage tracking consistency with bike data pipelines
  - Fixed validation query for `raw_weather_hourly` to handle transitional schema (removed `source_file` reference)
  - Verified all 3 weather marts build successfully: mart_weather_ride_correlation, mart_weather_impact_summary, mart_station_weather_performance
- **dbt Full-Refresh 23h+ Hang** - Converted staging, intermediate, and unified models from incremental tables to SQL views
  - `stg_nyc_modern` (216M rows) hung for 23+ hours materializing as an incremental table during `--full-refresh`
  - Row-level data was being materialized 3 times (staging → intermediate → unified) before any aggregation
  - Views are instant (zero materialization) — DuckDB computes through them on-the-fly when marts aggregate
  - Marts remain as physical tables, aggregating ~300M rows down to thousands
  - Removed all `{{ config(materialized='incremental') }}` overrides and `{% if is_incremental() %}` blocks from 8 model files
  - Changed `dbt_project.yml` intermediate/unified from `+materialized: incremental` to `+materialized: view`
  - Removed index definitions from `int_nyc_rides` and `int_london_rides` (views don't support indexes)
  - Total `dbt run` time dropped from 23h+ (hung) to ~7 minutes

### Fixed
- **Playwright Version Mismatch in Docker** - Updated base image from `v1.52.0-noble` to `v1.58.0-noble` to match pip package `playwright==1.58.0`
  - London extraction was failing because browser binaries didn't match the installed Python package
- **DuckDB Out-of-Memory in Railway** - Increased memory limits for containerized pipeline execution
  - `PRAGMA memory_limit`: 512MB → 1GB → 3GB → 8GB → 32GB (dbt_project.yml)
  - `DUCKDB_MEMORY_LIMIT` env var: 2GB → 8GB (Dockerfile, used by db_duckdb data loading)
  - Reduced dbt concurrency from 2 threads to 1 to prevent concurrent model OOM
  - Added `PRAGMA preserve_insertion_order=false` and `PRAGMA max_temp_directory_size='10GB'` for memory efficiency
  - Moved database load to subprocess execution so DuckDB's C++ allocator memory is fully released by the OS before dbt starts
  - This gives dbt the full 32GB for committing stg_nyc_modern (216M rows) instead of ~24GB after in-process retention
- **Staging Model Index Removal** - Removed all indexes from staging models to prevent multi-hour index builds
  - stg_nyc_modern (216M rows) hung for 12+ hours building unique index on ride_id at 30GB memory
  - Stripped indexes from all 5 staging models (stg_nyc_modern, stg_nyc_legacy, stg_london_legacy, stg_london_modern, stg_weather_hourly)
  - Staging is a pass-through layer; indexes only add value on query-target tables (marts)
- **stg_nyc_modern bike_id Index Error** - Removed index on nonexistent `bike_id` column (NYC modern schema uses `rideable_type`)
- **stg_weather_hourly source_file Error** - Removed references to `source_file` column not present in weather parquet files
  - Changed incremental logic to use `weather_record_id` instead of `source_file`
- **stg_weather_hourly Duplicate Key Violation** - Added deduplication step for overlapping weather parquet files
  - Raw weather data can have duplicate city+hour rows from overlapping backfill/incremental extractions
  - Added `QUALIFY ROW_NUMBER()` partitioned by city and hour to keep one row per weather_record_id

## [2.0.0] - 2026-02-13

### Added
- **Atmospheric UI & Dashboard Redesign** - Multi-page architecture with weather animations and time-of-day theming
  - Restructured 779-line monolithic `app.py` into lean 51-line entrypoint using `st.navigation` / `st.Page`
  - New `dashboard/pages/` — 4 page modules: landing, ride_analytics, weather_deep_dive, comparison
  - New `dashboard/components/` — 6 reusable components: city_toggle, weather_hero, recommendation_cards, forecast_strip, biking_score_gauge, chart_factory
  - New `dashboard/theme/` — time-of-day period calculation, atmospheric Plotly template with dark theme
  - New `dashboard/utils/` — extracted query helpers, CSS injection utilities
  - New `dashboard/static/` — rain (40 drops), snow (30 flakes), clear (sun pulse), cloudy (drift) CSS animations
  - Time-of-day gradient backgrounds (night, dawn, morning, day, golden, dusk) keyed to city timezone
  - `.streamlit/config.toml` with dark theme configuration
  - Weather Deep Dive page: temperature vs rides, precipitation impact, condition breakdown, hourly weather impact
  - 52 new tests: theme, CSS injector, components, chart factory
- **Station-Level Weather Analysis** - Station weather resilience metrics and map visualization
  - New `mart_station_directory` dbt mart — reference table for all stations with NYC lat/lng coordinates
  - New `mart_station_weather_performance` dbt mart — ridership change per station per weather condition vs clear baseline
  - Dashboard: weather resilience ranking table with condition/hour filters
  - Dashboard: NYC scatter map (OpenStreetMap, no token) colored by weather impact
  - Dashboard: cross-city weather impact comparison bar chart on Comparison page
  - 4 new dashboard query tests, updated mart list tests
- **Weather-Informed Recommendation Engine** - Biking score and natural language insights
  - New `dashboard/recommendation_engine.py` — pure Python module with zero Streamlit imports
  - WMO code classifier, temperature/wind/precipitation classifiers with enum-based categories
  - Weighted biking score (0-100) from temperature, precipitation, wind, and weather conditions
  - Historical data lookup via `mart_weather_impact_summary.parquet` with category mapping
  - Template-based insight generator with severity ranking (warning > positive > caution > neutral)
  - Dashboard integration: biking score gauge and recommendation cards in weather panel
  - 50 new tests covering classifiers, scoring, historical lookup, insight generation, and integration
- **Real-time Weather Dashboard** - Live weather panel with 15-minute auto-refresh
  - New `dashboard/weather_service.py` module fetching current conditions and 48-hour forecast from Open-Meteo
  - `CurrentWeather`, `HourlyForecastEntry`, `CityWeather` frozen dataclasses with unit conversion properties
  - WMO weather code mappings (descriptions, categories, emojis) for all 27 Open-Meteo codes
  - `@st.fragment(run_every="15m")` for partial page rerun (no third-party dependency needed)
  - `@st.cache_data(ttl=900)` for 15-minute response caching with graceful error handling
  - Forecast chart (Plotly dual-axis: temperature + precipitation probability) and 12-hour summary table
  - 30 new tests covering WMO codes, API parsing, fetch error handling, and coordinate validation
- **Weather Data Pipeline** - End-to-end weather data extraction pipeline using Open-Meteo API
  - New `extraction/weather.py` module with backfill (by year) and incremental (date-stamped) modes
  - New `data_models/weather.py` with `HourlyWeatherRecord` dataclass for schema validation
  - New `stg_weather_hourly` dbt staging model with derived fields: weather_condition (WMO codes), precipitation_intensity, temperature_band, wind_category
  - DuckDB `raw_weather_hourly` table configuration (schema, S3 URI, validation, quality checks)
  - Weather extraction integrated into orchestrator pipeline (non-blocking) and available as standalone stage
  - 11 new tests covering API fetching, backfill idempotency, incremental updates, and data model validation

- **Hourly Ride-Weather Analytics Marts** - New dbt mart models for weather-ride correlation analysis
  - `mart_hourly_rides` — granular hourly ride metrics with date dimension (replaces mart_hourly_patterns)
  - `mart_hourly_patterns_summary` — backward-compatible aggregate (same schema as old mart_hourly_patterns)
  - `mart_weather_ride_correlation` — inner join of hourly rides and weather data
  - `mart_weather_impact_summary` — pre-computed weather impact statistics with clear-weather baseline comparison
  - Dashboard updated to use mart_hourly_patterns_summary (backward-compatible)
  - MARTS list expanded from 5 to 8 entries across streamlit_data_manager and db_duckdb
  - Fixed pre-existing missing mart_daily_metrics_long in pipeline.py status check

### Changed
- **BaseDataRecord Rename** - Renamed `BaseBikeShareRecord` → `BaseDataRecord` in `data_models/base.py`
  - The base class is a generic schema-validation mixin with no bike-specific logic
  - Updated all consumers: nyc_bike.py, london_bike.py, extracted_file_manager, and tests

### Technical Improvements
- **Dependency Updates** - Updated outdated Python packages across three risk tiers
  - Tier 1 (safe): Updated 15 security/utility packages (certifi, urllib3, requests, click, GitPython, psutil, pillow, PyYAML, etc.)
  - Tier 2 (moderate): Updated core dependencies (boto3 1.38→1.42, duckdb 1.3→1.4, pandas 2.2→2.3, numpy 2.2→2.4, pyarrow 20→23, streamlit 1.45→1.54, plotly 6.1→6.5, pydantic 2.11→2.12, playwright 1.52→1.58)
  - Tier 3 (high risk): Updated dbt ecosystem from 1.9 to 1.10 (dbt-core 1.9.6→1.10.19, dbt-duckdb 1.9.4→1.10.0), updated deepdiff 7.0→8.6
  - All 132 existing tests pass after updates (0 regressions)
  - dbt compile succeeds (deprecation warnings for test argument format, non-blocking)

### Fixed
- **SQL Injection in DuckDB Credential Setup** - Added input validation for AWS credentials before SQL interpolation
  - New `_validate_aws_credential()` ensures only safe characters in credential values
  - Prevents potential SQL injection via malformed environment variables
- **Bare Exception in Export** - Added exception details to error log in `_export_table_to_s3`
- **SQL Injection in Dashboard** - Parameterized all user-controlled values in DuckDB queries
  - Replaced f-string interpolation with `$1, $2, ...` positional parameters across 22 queries
  - Added `run_query_params` helper for parameterized query execution
  - Location, date, and year values now passed safely as query parameters
- **Hardcoded S3 Bucket** - `streamlit_data_manager/parquet_file_manager.py` now reads `S3_BUCKET` from environment variable with fallback to default

### Changed
- **Data Quality Checks** - Refactored `_run_data_quality_checks` from 122-line if-elif chain to metadata-driven approach
  - Added `TABLE_QUALITY_CONFIG` dictionary defining null-check columns, duplicate keys, and date columns per table
  - Reduced function from 122 lines to ~50 lines with identical behavior
- **Dashboard Code Quality** - Decomposed repeated patterns into helper functions
  - Extracted `get_date_range()` helper, reducing 30 lines of duplicate code to 8
  - Consolidated session state initialization into dict-driven `set_default_state()`
  - Replaced `date_filter_sql()` f-string helper with clamped date parameter values

### Improved
- **File Manager Memory Efficiency** - Fixed `_stream_csv_to_parquet` loading entire CSV into memory
  - Now downloads CSV to temp file and uses `pd.read_csv(chunksize=50_000)` for true streaming
  - Memory usage is now proportional to chunk size, not total file size
  - Removed hardcoded PyArrow schema that only worked for NYC Modern data; schema is now inferred from model transformation output
- **S3 Existence Check Performance** - Replaced O(n*m) HEAD requests with cached S3 listing
  - `_parquet_exists_for_csv` now uses a single paginated `list_objects_v2` call instead of up to 8 HEAD requests per CSV
  - Reduces ~1,600 S3 API calls to ~2 for a typical pipeline run
- **CLI Error Handling** - Consolidated duplicated error handling across 7 CLI commands
  - Extracted `_cli_error()` helper function for consistent error logging and exception raising
  - Added proper `click.ClickException` re-raise to prevent double-wrapping

### Added
- **Test Coverage Expansion** - Added 44 new tests across 5 files covering previously untested modules
  - `tests/conftest.py`: Shared fixtures (temp DuckDB, mock S3, sample DataFrames for all 4 schemas)
  - `tests/test_extraction.py`: 13 tests for extraction/utils.py, nyc.py, and london.py
  - `tests/test_db_duckdb_operations.py`: 19 tests for DuckDBManager, DuckDBOperations, DuckDBPipeline, and utils
  - `tests/test_dashboard.py`: 6 tests for dashboard query patterns (DuckDB aggregation, date filtering, Parquet reads)
  - `tests/test_streamlit_data_manager.py`: 6 tests for ensure_local_parquet_files() with mocked S3
  - Total tests: 79 → 135 (132 pass, 3 skip)
- **CLI List Command** - Implemented previously stubbed `--exports` and `--marts` flags
  - `--exports` lists mart Parquet files in S3 with sizes and timestamps
  - `--marts` lists mart tables in the database with optional row counts
- **dbt Schema Documentation** - Added schema.yml files for all model layers
  - Documented all 4 staging models with column descriptions
  - Documented 2 intermediate models (int_nyc_rides, int_london_rides)
  - Documented unified_rides model with full column descriptions
  - Documented all 5 mart models with column descriptions
  - Documented population seed with column descriptions
- **dbt Data Tests** - Added data quality tests across all model layers
  - unique and not_null tests on ride_id for staging, intermediate, and unified models
  - accepted_values tests on location, user_type, day_type, and metric_name columns
  - not_null tests on key mart columns (location, date, ride_count, etc.)
  - not_null tests on source table primary keys
- **dbt Source Freshness** - Added freshness monitoring to all 4 raw source tables
  - warn_after: 45 days, error_after: 90 days
  - loaded_at_field configured for each source table
- **dbt Project Config** - Added intermediate and unified model layer configuration to dbt_project.yml

### Changed
- **dbt Macros** - Extracted repeated SQL logic into reusable Jinja macros
  - `day_type(timestamp_column)` macro for weekday/weekend classification (used in all 4 staging models)
  - `user_type_mapping(column_name)` macro for legacy Subscriber/Customer to member/casual mapping
- **stg_nyc_legacy Unique Key** - Changed unique_key from composite 4-column key to `ride_id` for consistency with all other staging models
- **Data Models Base Class** - Consolidated duplicate `validate_schema` methods into `BaseBikeShareRecord`
  - Replaced 4 identical subclass implementations with a single base class method
  - Replaced `EXTRACTED_FILE_MANAGER_DEBUG` env-var-gated `print()` calls with `logging.debug()`
  - Subclasses now inherit validation behavior via `_required_columns` class attribute

### Improved
- **mart_station_growth SQL** - Refactored repeated `lag()` window function (4 occurrences) into a single CTE
- **dbt_project.yml Portability** - Replaced hardcoded `/tmp` temp directory with `env_var("DBT_TEMP_DIR", "/tmp")`
- **Orchestrator Logging** - Consolidated 26+ repeated separator patterns into `_log_section` and `_log_step` helpers
  - Reduced boilerplate in `orchestrator/main.py` by ~20 lines
  - Pipeline step headers are now generated consistently from a single function

### Fixed
- **London Extraction UnboundLocalError** - Moved `local_path` assignment before `try` block in `extraction/london.py`
  - Previously, if an exception occurred before `local_path` was assigned, the `finally` block would raise `UnboundLocalError`

### Technical Improvements
- **Extracted Duplicated ZIP Upload Logic** - Consolidated identical CSV-from-ZIP upload code into `_upload_csv_from_zip_entry` helper
  - Removed ~50 lines of duplicated code between `_extract_zip_using_filetree` and `_process_nested_zip`
- **Logging Consistency** - Replaced `print()` with `logging` across orchestrator and extraction modules
  - `orchestrator/cli.py`: `check_pipeline_status` now uses `logger` (18 print calls replaced)
  - `extraction/nyc.py`: All 11 `print()` calls replaced with `logger.info()` / `logger.error()`
  - `extraction/london.py`: All 9 `print()` calls replaced with `logger.info()` / `logger.error()`
  - `extraction/utils.py`: `print()` and `logging.error()` calls replaced with named `logger`
- **Exception Handling** - Narrowed bare `except Exception` to specific exception types
  - `extraction/nyc.py`: Now catches `(ClientError, ConnectionError, OSError)`
  - `extraction/london.py`: Now catches `(RequestException, ConnectionError, OSError)`

- **Dead Code Cleanup** - Removed unused imports, dead variables, and placeholder files
  - Removed unused `sys`, `numpy`, `boto3`, `pyarrow.csv`, `re`, `Dict`, `Any`, `datetime` imports across 5 files
  - Removed unused `ZipFileNode` and `walk_folder` filetree imports from file manager
  - Removed redundant local `import time` in `_cleanup_memory()`
  - Deleted empty placeholder `extraction/weather.py`
  - Removed dead variable `start_time` in `extraction/london.py`

### Added
- **Railway Cron Job Deployment** - Migrated pipeline from EC2 to Railway for ~$125+/month savings
  - Created `Dockerfile` using Playwright v1.52.0 base image with all pipeline dependencies
  - Created `scripts/railway_entrypoint.sh` wrapper for ephemeral cron execution
  - Created `railway.toml` with monthly cron schedule (3rd of each month at 2AM UTC)
  - Created `.dockerignore` to keep build context minimal
  - Created `.env.example` documenting all required/optional environment variables
  - Ephemeral architecture: fresh DuckDB each run, no persistent volume needed
  - Container runs `--dbt-full-refresh` every time since DB is rebuilt from S3
  - Verified end-to-end: full pipeline ran successfully in Docker (301M+ rows loaded, all 12 dbt models built, 5 marts exported to S3)

### Changed
- **`.gitignore` Update** - Added exception for `railway.toml` (was blocked by `*.toml` rule)

### Technical Improvements
- **Docker stdout buffering fix** - Added `PYTHONUNBUFFERED=1` to Dockerfile for real-time log output in containers (extraction modules use `print()` which buffers in non-TTY environments)

- **Changelog Workflow Integration** - Integrated changelog maintenance into development workflow
  - Added "Changelog Maintenance" section to CLAUDE.md with version bump rules and entry categories
  - Updated `/quick-commit` command to verify CHANGELOG.md is updated before committing
  - Updated `/commit-push-pr` command to reference CHANGELOG entries in PR descriptions
  - Added CHANGELOG.md to Important Files section in CLAUDE.md

- **db_duckdb/README.md** - Created comprehensive module documentation (~270 lines)
  - CLI reference for all 7 commands with options
  - Raw table definitions with S3 paths
  - Programmatic usage examples
  - Configuration (environment variables, database location)
  - Data loading strategy explanation (full replace design decision)
  - Pipeline integration context
  - Troubleshooting guide (OOM, S3 access, database issues, full reset)

- **Archived Documentation** - Moved stale one-time documents to `documentation/archive/`
  - `FEATURE-SUMMARY.md` - Historical record of incremental processing feature (marked as historical)
  - `INCREMENTAL_STATUS.md` - One-time announcement of incremental config completion

- **Project Planning Documentation** - Created comprehensive planning docs in `docs/planning/`
  - `ROADMAP.md` - Full project enhancement roadmap with phases and timelines
  - `COST-OPTIMIZATION.md` - Detailed AWS cost reduction strategies ($130 → <$10/month)
  - `QUICK-START-PRIORITIES.md` - Immediate action items for quick wins

- **System Guide** - Created `docs/SYSTEM-GUIDE.md` with comprehensive operational documentation
  - Quick start commands for running tests and pipeline
  - Summary of recent refactoring changes
  - Complete commands reference for all modules
  - Troubleshooting guide for common issues
  - Environment setup instructions

### Changed
- **CLAUDE.md Documentation Accuracy** - Fixed multiple file references to match actual codebase
  - Fixed orchestrator stage name from `extract` to `extraction`
  - Fixed export stage name from `mart_export` to `export`
  - Updated module file names (nyc.py, london.py, nyc_bike.py, etc.)
  - Added undocumented db_duckdb CLI commands (verify, list, pipeline, status)
  - Updated module table with correct key files
  - Fixed extracted_file_manager CLI commands (`extract_zips` → `extract`, `convert_csvs` → `convert`)
  - Updated Documentation References to reflect actual existing files

- **Documentation Audit** - Comprehensive review and correction of all project docs
  - Fixed SYSTEM-GUIDE.md incorrect claims about deleted intermediate models (models still exist)
  - Updated ROADMAP.md with actual implementation status (checkboxes marked complete/pending)
  - Archived FEATURE-SUMMARY.md as historical document with note pointing to SYSTEM-GUIDE.md
  - Updated INCREMENTAL_STATUS.md with current review date
- **Full Documentation Review & Handoff Preparation** (February 2026) - Verified all doc assertions against code
  - Fixed CLAUDE.md: `S3_BUCKET_NAME` → `S3_BUCKET`, bucket name to `city-cycles-data-ctr37`, corrected S3 parquet paths, fixed `file_processor.py` → `manager.py`
  - Fixed README.md: removed nonexistent `resources/` reference, added missing module README links, fixed London data date ranges (Legacy 2010-2022, Modern 2022-present), corrected DuckDB loading description to "Direct S3 Loading via httpfs / Full Replace"
  - Fixed incremental-processing-guide.md: Stage 2 file tracking mechanism (uses S3 existence checks, not DB table), Stage 3 loading strategy (full replace, not incremental)
  - Fixed incremental-pipeline-architecture.md: `int_nyc_rides` unique_key is `ride_id`, not "None (append-only)"
  - Fixed data_models/README.md: replaced reference to nonexistent `db_duckdb/init_raw_tables.py` with `db_duckdb/operations.py`
  - Fixed extracted_file_manager/README.md: removed deprecated methods section listing 4 methods that don't exist in code
  - Updated ROADMAP.md: orchestrator test coverage from "None" to "Good (34 tests)", marked documentation review as completed
  - Updated SYSTEM-GUIDE.md: refreshed date, fixed S3 parquet path format to `{city}/{model_class_name}/`

### Fixed
- **Bare except clause** in `extracted_file_manager/manager.py` - Changed to specific `ClientError` exception handling
- **Incorrect type hints** in `db_duckdb/pipeline.py` - Changed `Dict[str, any]` to `Dict[str, Any]` with proper import
- **CHANGELOG.md Error** - Removed incorrect claim that intermediate dbt models were deleted (they still exist and are used)

### Removed
- **No-op column renames** in `data_models/nyc_bike.py` - Removed self-referential renames like `"ride_id": "ride_id"` that served no purpose

### Technical Improvements
- **Test Suite Enhancement** - Improved test coverage from 49 to 83 tests
  - Added 34 new tests for `orchestrator` module (config, main, CLI)
  - Fixed S3 exception handling tests to use proper `ClientError` instead of generic `Exception`
  - All 83 tests now pass (3 skipped for dry-run output capture issues)

---

## [1.0.0] - 2026-01-26

### Added
- **Claude Code Power User Setup** - Comprehensive Claude Code configuration following Boris Cherny's best practices
  - Created CLAUDE.md with 10KB+ project-specific instructions
  - Added .claude/settings.json with pre-allowed permissions (40+ commands) and PostToolUse hook for auto-formatting
  - Created 6 slash commands: quick-commit, commit-push-pr, test-and-fix, run-pipeline, validate-data, review-changes
  - Created 5 specialized agents: verify-app, code-simplifier, data-quality-validator, pipeline-troubleshooter, code-architect
  - Added .claude/README.md usage guide
  - Enables no-permission-prompt workflows, automated code formatting, and built-in verification loops

- **Changelog Maintenance** - Added CHANGELOG.md (this file) following Keep a Changelog format
  - Integrated changelog maintenance workflow into CLAUDE.md
  - Semantic versioning for version bumps (MAJOR.MINOR.PATCH)
  - Entry categories: Added, Changed, Fixed, Improved, Technical Improvements

### Technical Improvements
- Configured PostToolUse hook to auto-format Python files with black/autopep8
- Pre-allowed 40+ safe commands to streamline development workflow
- Added safety denials for destructive commands (S3 deletion, rm -rf, DROP TABLE)

---

## Version History Summary

- **2.0.0** (2026-02-13) - Weather data pipeline, atmospheric dashboard redesign, recommendation engine, Railway deployment, 135+ tests
- **1.0.0** (2026-01-26) - Initial release with Claude Code power user setup and changelog

---

## Versioning Guide

- **MAJOR** (X.0.0) - Breaking changes (incompatible API changes, major schema changes, pipeline restructuring)
- **MINOR** (x.Y.0) - New features (new data sources, pipeline stages, dashboard features, dbt models)
- **PATCH** (x.y.Z) - Bug fixes, performance improvements, documentation updates, minor refactoring

## Entry Categories

- **Added** - New features, data sources, pipeline stages, dbt models
- **Changed** - Changes in existing functionality, behavior modifications
- **Fixed** - Bug fixes, error corrections
- **Improved** - Performance improvements, optimizations
- **Technical Improvements** - Refactoring, test improvements, infrastructure changes, developer experience
- **Deprecated** - Features marked for removal in future versions
- **Removed** - Removed features or functionality
- **Security** - Security-related fixes or improvements

---

_Keep this changelog up to date with every significant change. Every PR should include a CHANGELOG.md update._

[unreleased]: https://github.com/chrisrogers37/city-cycles/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/chrisrogers37/city-cycles/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/chrisrogers37/city-cycles/releases/tag/v1.0.0
