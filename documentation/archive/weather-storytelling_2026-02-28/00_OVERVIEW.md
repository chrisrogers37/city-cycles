# Weather Storytelling — Enhancement Session Overview

**Session:** weather-storytelling
**Date:** 2026-02-28
**Scope:** Weather data pipeline + "days like today" historical insights
**Goal:** Get weather pipeline working end-to-end, then surface contextual ride insights based on historically similar weather days

## User Intent

> "I want live weather and I want it to surface insight on past days that look like the weather by time of day and date/month. So like you come and see the weather and citybike data that relates... the weather and what that means for citibiking in the area!"

The dashboard should be a portfolio storytelling piece where users arrive, see live weather, and immediately get contextual insight like "On similar February weekdays with light rain, NYC averaged 12,400 rides — 23% below typical."

## Enhancements Planned: 5

| Phase | Title | Impact | Effort | Risk | Dependencies |
|-------|-------|--------|--------|------|--------------|
| 01 | Fix Weather Pipeline End-to-End | High | Low-Medium | Medium | None |
| 02 | Build "Similar Day" Mart | High | Medium | Low | None (but needs 01 for data) |
| 03 | Surface Contextual Landing Insights | High | Medium | Low | Phase 02 |
| 04 | Fix Weather Deep Dive Page | Medium | Low | Low | None (but needs 01 for data) |
| 05 | Harden Dashboard Empty States | Low | Medium | Low | None |

## Dependency Graph

```
Phase 01 (fix pipeline) ──────────────────────── unlocks data for all phases
     │
Phase 02 (similar day mart) ───── Phase 03 (landing page insights)
     │
Phase 04 (fix deep dive) ──────── independent (code changes standalone)
     │
Phase 05 (harden empty states) ── independent (can run in parallel)
```

## Parallelization

**Group A — Can run in parallel (touch disjoint files):**
- Phase 01: `db_duckdb/config/duckdb_config.py`, `dbt_city_cycles/models/staging/sources.yml`
- Phase 02: `dbt_city_cycles/models/marts/mart_similar_day_stats.sql` (new), `dbt_city_cycles/models/marts/schema.yml`, `db_duckdb/operations.py`, `streamlit_data_manager/parquet_file_manager.py`
- Phase 04: `dashboard/pages/weather_deep_dive.py`
- Phase 05: `dashboard/pages/ride_analytics.py`, `dashboard/pages/comparison.py`, `dashboard/recommendation_engine.py`, `dashboard/utils/query_helpers.py`

**Sequential requirement:**
- Phase 03 must wait for Phase 02 (needs `mart_similar_day_stats` to exist)
- Phase 03 touches: `dashboard/recommendation_engine.py`, `dashboard/components/recommendation_cards.py`, `dashboard/pages/landing.py`

**Note:** Phase 05 and Phase 03 both modify `dashboard/recommendation_engine.py`. If running in parallel, Phase 03 changes (adding `SimilarDayStats`, `lookup_similar_day_stats`, `generate_similar_day_insights`) are additive and should not conflict with Phase 05 changes (improving `_insight_missing_data` messaging). However, to be safe, run Phase 03 before Phase 05, or verify no merge conflicts.

## Recommended Implementation Order

1. **Phase 01** — Fix the pipeline first (everything else needs data)
2. **Phase 02** + **Phase 04** + **Phase 05** — Run in parallel (disjoint files)
3. **Phase 03** — After Phase 02 completes (depends on the new mart)

## Total Estimated Effort

~15-24 hours across all 5 phases

## Design Docs

```
documentation/planning/phases/weather-storytelling_2026-02-28/
├── 00_OVERVIEW.md          (this file)
├── 01_fix-weather-pipeline.md
├── 02_similar-day-mart.md
├── 03_contextual-landing-insights.md
├── 04_fix-weather-deep-dive.md
└── 05_harden-empty-states.md
```
