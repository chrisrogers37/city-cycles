# Changelog

All notable changes to the City Cycles Analytics project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Changelog Workflow Integration** - Integrated changelog maintenance into development workflow
  - Added "Changelog Maintenance" section to CLAUDE.md with version bump rules and entry categories
  - Updated `/quick-commit` command to verify CHANGELOG.md is updated before committing
  - Updated `/commit-push-pr` command to reference CHANGELOG entries in PR descriptions
  - Added CHANGELOG.md to Important Files section in CLAUDE.md

- **Project Planning Documentation** - Created comprehensive planning docs in `docs/planning/`
  - `ROADMAP.md` - Full project enhancement roadmap with phases and timelines
  - `COST-OPTIMIZATION.md` - Detailed AWS cost reduction strategies ($130 → <$10/month)
  - `QUICK-START-PRIORITIES.md` - Immediate action items for quick wins

### Changed
- **CLAUDE.md Documentation Accuracy** - Fixed multiple file references to match actual codebase
  - Fixed orchestrator stage name from `extract` to `extraction`
  - Fixed export stage name from `mart_export` to `export`
  - Updated module file names (nyc.py, london.py, nyc_bike.py, etc.)
  - Added undocumented db_duckdb CLI commands (verify, list, pipeline, status)
  - Updated module table with correct key files

### Fixed
- **Bare except clause** in `extracted_file_manager/manager.py` - Changed to specific `ClientError` exception handling
- **Incorrect type hints** in `db_duckdb/pipeline.py` - Changed `Dict[str, any]` to `Dict[str, Any]` with proper import

### Removed
- **Vestigial intermediate dbt models** - Deleted `int_nyc_rides.sql` and `int_london_rides.sql` from `dbt_city_cycles/models/intermediate/`
- **Intermediate table references** - Removed from `db_duckdb/operations.py` INTERMEDIATE_TABLES list
- **No-op column renames** in `data_models/nyc_bike.py` - Removed self-referential renames like `"ride_id": "ride_id"` that served no purpose

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
