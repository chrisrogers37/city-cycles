# Changelog

All notable changes to the City Cycles Analytics project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
- **Data Models Base Class** - Consolidated duplicate `validate_schema` methods into `BaseBikeShareRecord`
  - Replaced 4 identical subclass implementations with a single base class method
  - Replaced `EXTRACTED_FILE_MANAGER_DEBUG` env-var-gated `print()` calls with `logging.debug()`
  - Subclasses now inherit validation behavior via `_required_columns` class attribute

### Technical Improvements
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

[unreleased]: https://github.com/chrisrogers37/city-cycles/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/chrisrogers37/city-cycles/releases/tag/v1.0.0
