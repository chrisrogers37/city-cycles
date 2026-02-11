# City Cycles - Technical Debt Inventory

**Scan Date:** 2026-02-10
**Session:** full-scan-feb-2026
**Scanned By:** Automated tech debt analysis (Claude Code)

---

## Executive Summary

A comprehensive scan of the City Cycles codebase identified **16 categories of technical debt** across Python source code (~4,537 lines), dbt models (12 models), and test infrastructure (86 tests). The most critical findings are: zero dbt tests, SQL injection risks in the dashboard, 70+ overly-broad exception handlers, and significant test coverage gaps in core pipeline modules.

---

## Complete Inventory

### HIGH SEVERITY

| ID | Finding | Location | Blast Radius | Complexity | Risk |
|----|---------|----------|-------------|------------|------|
| H1 | SQL injection in DuckDB credential setup | `db_duckdb/duckdb_manager.py:64-66` | Security | Low | Med |
| H2 | SQL injection in dashboard queries | `dashboard/app.py` (15+ locations) | Security | Med | High |
| H3 | Zero dbt tests defined | `dbt_city_cycles/tests/` | Data quality | Med | High |
| H4 | Missing test coverage (5 of 8 modules untested) | `tests/` | Reliability | High | High |
| H5 | 70+ bare `except Exception` catches | Codebase-wide | Debugging | Med | Med |
| H6 | Unimplemented TODO stubs exposed in CLI | `db_duckdb/cli.py:325,332` | User-facing | Low | Low |

### MEDIUM SEVERITY

| ID | Finding | Location | Blast Radius | Complexity | Risk |
|----|---------|----------|-------------|------------|------|
| M1 | Schema validation duplicated 4x | `data_models/nyc_bike.py`, `london_bike.py` | Maintainability | Low | Low |
| M2 | Data quality checks: 122-line if-elif chain | `db_duckdb/operations.py:254-375` | Maintainability | Med | Low |
| M3 | 6 large functions (>80 lines each) | `manager.py`, `operations.py`, `app.py`, `main.py` | Readability | Med | Low |
| M4 | CLI error handling duplicated 6x | `db_duckdb/cli.py` | Maintainability | Low | Low |
| M5 | Download-and-store pattern duplicated | `extraction/nyc.py`, `london.py` | Maintainability | Low | Low |
| M6 | 26+ logging separator patterns | `db_duckdb/cli.py`, `orchestrator/main.py` | Maintainability | Low | Low |
| M7 | S3 file existence check duplicated | `extraction/utils.py`, `manager.py` | Maintainability | Low | Low |
| M8 | dbt: zero documentation (12 models undocumented) | `dbt_city_cycles/models/` | Onboarding | Med | Low |
| M9 | dbt: empty macros directory, repeated SQL | `dbt_city_cycles/` | Maintainability | Med | Low |
| M10 | dbt: missing schema config for intermediate/unified | `dbt_city_cycles/dbt_project.yml` | Organization | Low | Low |
| M11 | dbt: inconsistent unique key (stg_nyc_legacy) | `dbt_city_cycles/models/staging/` | Data quality | Low | Med |
| M12 | Debug print() instead of logging (20+ instances) | `data_models/`, `extracted_file_manager/` | Observability | Low | Low |
| M13 | 28+ outdated dependencies | `requirements.txt` | Security/compat | Med | Med |
| M14 | Hardcoded S3 bucket in streamlit_data_manager | `parquet_file_manager.py:12` | Portability | Low | Low |
| M15 | Memory inefficiency: full CSV loaded then chunked | `manager.py:446` | Performance | Low | Med |

### LOW SEVERITY

| ID | Finding | Location | Blast Radius | Complexity | Risk |
|----|---------|----------|-------------|------------|------|
| L1 | 10 unused imports across data_models, db_duckdb, manager | Multiple files | Cleanliness | Low | None |
| L2 | Unused variable `start_time` | `extraction/london.py:31` | Cleanliness | Low | None |
| L3 | Placeholder file `extraction/weather.py` | `extraction/weather.py` | Cleanliness | Low | None |
| L4 | Unused aliased imports (ZipFileNode, walk_folder) | `manager.py:26` | Cleanliness | Low | None |
| L5 | Redundant `import time` inside function | `manager.py:127` | Cleanliness | Low | None |
| L6 | Inconsistent separator widths (60 vs 80) | Multiple CLI files | Style | Low | None |
| L7 | Dashboard session state boilerplate | `dashboard/app.py:43-58` | Readability | Low | None |
| L8 | dbt: undocumented population seed | `dbt_city_cycles/seeds/` | Documentation | Low | None |
| L9 | dbt: hardcoded `/tmp` in dbt_project.yml | `dbt_city_cycles/dbt_project.yml:46` | Portability | Low | Low |

---

## Prioritized Remediation Order

| Phase | PR Title | Effort | Risk | Depends On | Blocks |
|-------|----------|--------|------|------------|--------|
| 01 | Dead code & unused imports cleanup | Small | None | - | 04 |
| 02 | dbt documentation, tests & configuration | Medium | Low | - | 03 |
| 03 | dbt macros & SQL refactoring | Medium | Low | 02 | - |
| 04 | Data models base class refactor | Small | Low | 01 | 09 |
| 05 | Dashboard hardening & refactor | Large | Med | - | 09 |
| 06 | DuckDB layer refactor | Large | Med | - | 09 |
| 07 | Extracted file manager refactor | Large | Med | - | 09 |
| 08 | Orchestrator & extraction cleanup | Medium | Low | - | 09 |
| 09 | Test coverage expansion | Large | Low | 04-08 | 10 |
| 10 | Dependency updates | Medium | High | 09 | - |

---

## Dependency Matrix

```
Phase 01 (Dead Code)          ──blocks──> Phase 04 (Data Models)
Phase 02 (dbt Docs)           ──blocks──> Phase 03 (dbt Macros)
Phase 04 (Data Models)        ──blocks──> Phase 09 (Tests)
Phase 05 (Dashboard)          ──blocks──> Phase 09 (Tests)
Phase 06 (DuckDB)             ──blocks──> Phase 09 (Tests)
Phase 07 (File Manager)       ──blocks──> Phase 09 (Tests)
Phase 08 (Orchestrator)       ──blocks──> Phase 09 (Tests)
Phase 09 (Tests)              ──blocks──> Phase 10 (Deps)
```

### Parallel Execution Groups

**Group A (can run in parallel):** Phases 01, 02, 05, 06, 07, 08
- These touch disjoint file sets and have no mutual dependencies.

**Group B (sequential after Group A):** Phase 03 (after 02), Phase 04 (after 01)

**Group C (after all above):** Phase 09

**Group D (last):** Phase 10

---

## Files Modified Per Phase (Conflict Matrix)

| File | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 |
|------|----|----|----|----|----|----|----|----|----|----|
| `data_models/base.py` | X | | | X | | | | | | |
| `data_models/nyc_bike.py` | X | | | X | | | | | | |
| `data_models/london_bike.py` | X | | | X | | | | | | |
| `db_duckdb/duckdb_manager.py` | X | | | | | X | | | | |
| `extracted_file_manager/manager.py` | X | | | | | | X | | | |
| `extraction/weather.py` | X | | | | | | | | | |
| `extraction/london.py` | X | | | | | | | X | | |
| `dbt_city_cycles/**` | | X | X | | | | | | | |
| `dashboard/app.py` | | | | | X | | | | | |
| `streamlit_data_manager/*` | | | | | X | | | | | |
| `db_duckdb/operations.py` | | | | | | X | | | | |
| `db_duckdb/cli.py` | | | | | | X | | | | |
| `db_duckdb/pipeline.py` | | | | | | X | | | | |
| `extracted_file_manager/*` | | | | | | | X | | | |
| `orchestrator/*` | | | | | | | | X | | |
| `extraction/nyc.py` | | | | | | | | X | | |
| `extraction/utils.py` | | | | | | | | X | | |
| `tests/*` | | | | | | | | | X | |
| `requirements.txt` | | | | | | | | | | X |

**Phases 01 and 04 share files** (`data_models/`), hence 04 depends on 01.
**Phases 01 and 06 share** `db_duckdb/duckdb_manager.py` (01 removes unused import, 06 fixes SQL injection). These are non-conflicting changes but should merge 01 first.
**Phases 01 and 07 share** `extracted_file_manager/manager.py` (01 removes unused imports, 07 refactors functions). Non-conflicting but merge 01 first.

---

## Remediation Plan Documents

- [01_dead-code-cleanup.md](./01_dead-code-cleanup.md)
- [02_dbt-documentation-tests.md](./02_dbt-documentation-tests.md)
- [03_dbt-macros-refactor.md](./03_dbt-macros-refactor.md)
- [04_data-models-base-class.md](./04_data-models-base-class.md)
- [05_dashboard-hardening.md](./05_dashboard-hardening.md)
- [06_duckdb-layer-refactor.md](./06_duckdb-layer-refactor.md)
- [07_file-manager-refactor.md](./07_file-manager-refactor.md)
- [08_orchestrator-extraction-cleanup.md](./08_orchestrator-extraction-cleanup.md)
- [09_test-coverage-expansion.md](./09_test-coverage-expansion.md)
- [10_dependency-updates.md](./10_dependency-updates.md)
