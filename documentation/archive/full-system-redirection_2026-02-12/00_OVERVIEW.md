# Full System Redirection: Weather-Informed Bike Riding Dashboard

## Session Context

- **Date:** 2026-02-12
- **Session Name:** full-system-redirection
- **Scope:** Weather data integration, real-time dashboard, recommendation engine, atmospheric UI
- **Focus Areas:** Data sourcing (Open-Meteo API), ETL pipeline, dbt analytics, Streamlit dashboard redesign

## User's Stated Goals

Transform City Cycles from a historical analytics dashboard into a **real-time, weather-informed bike riding recommendation tool**. The core question the product answers: **"Should I bike today?"**

### Key Requirements
- Ingest hourly/daily weather data (historical + real-time + forecast) for NYC and London
- Correlate weather conditions with historical ride patterns at hourly granularity
- Generate recommendations: "It's raining at 9am — historically that means X% fewer rides"
- Beautiful, atmospheric UI: rain/snow animations, time-of-day gradients, city toggle
- 15-minute refresh cadence for current weather conditions
- Deep-dive analytics pages for historical exploration
- Station-level weather analysis (which stations maintain ridership in bad weather?)
- Design (not build) live station availability and "near me" features for future

### Weather Strategy
- **API:** Open-Meteo (free, no API key, hourly data since 1940, real-time + forecast)
- **Reference Points:** Central Park (NYC: 40.7128, -74.0060), City of London (London: 51.5074, -0.1278)
- **v1:** One weather point per city
- **Future:** Zone-based weather when "near me" feature is built

---

## Phase Summary

| Phase | Title | Type | Status | PR |
|-------|-------|------|--------|-----|
| 01 | Weather Data Pipeline (Foundation) | Build | ✅ COMPLETE | #36 |
| 02 | Hourly Ride-Weather Analytics Marts | Build | ✅ COMPLETE | #37 |
| 03 | Real-time Weather Dashboard Layer | Build | ✅ COMPLETE | #38 |
| 04 | Recommendation Engine | Build | ✅ COMPLETE | #39 |
| 05 | Atmospheric UI & Dashboard Redesign | Build | ✅ COMPLETE | #41 |
| 06 | Station-Level Weather Analysis | Build | ✅ COMPLETE | #40 |
| 07 | Live Station Data (Design Only) | Design | ✅ COMPLETE | — |
| 08 | "Near Me" Feature (Design Only) | Design | ✅ COMPLETE | — |

---

## Dependency Graph

```
Phase 01 (Weather Pipeline) ─────┬──→ Phase 02 (Analytics Marts) ──┬──→ Phase 04 (Recommendations)
                                  │                                  │
                                  ├──→ Phase 03 (Real-time Layer) ──┤──→ Phase 05 (Atmospheric UI)
                                  │                                  │
                                  └──→ Phase 06 (Station Analysis) ─┘

Phase 07 (Live Station Design) ──── Independent (design only)
Phase 08 ("Near Me" Design) ─────── Independent (design only)
```

### Parallel Execution Groups

**Group A (no dependencies, can start immediately):**
- Phase 01 — Weather Data Pipeline
- Phase 07 — Live Station Data (Design Only)
- Phase 08 — "Near Me" Feature (Design Only)

**Group B (depends on Phase 01):**
- Phase 02 — Hourly Ride-Weather Analytics Marts
- Phase 03 — Real-time Weather Dashboard Layer
- Phase 06 — Station-Level Weather Analysis

**Group C (depends on Phases 02 + 03):**
- Phase 04 — Recommendation Engine

**Group D (depends on Phases 03 + 04):**
- Phase 05 — Atmospheric UI & Dashboard Redesign

### Recommended Implementation Order
1. Phase 01 (foundation — everything depends on this)
2. Phases 02, 03, 06 in parallel (disjoint file sets)
3. Phase 04 (needs 02 + 03)
4. Phase 05 (needs 03 + 04, largest phase)
5. Phases 07, 08 anytime (design only, no code changes)

---

## File Impact Matrix

Shows which phases touch which files to verify no parallel conflicts:

| File / Area | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 |
|---|---|---|---|---|---|---|---|---|
| `extraction/weather.py` (NEW) | C | | | | | | | |
| `data_models/weather.py` (NEW) | C | | | | | | | |
| `data_models/__init__.py` | M | | | | | | | |
| `db_duckdb/operations.py` | M | M | | | | | | |
| `db_duckdb/pipeline.py` | M | | | | | | | |
| `dbt: stg_weather_hourly.sql` (NEW) | C | | | | | | | |
| `dbt: mart_hourly_rides.sql` (NEW) | | C | | | | | | |
| `dbt: mart_hourly_patterns_summary.sql` (NEW) | | C | | | | | | |
| `dbt: mart_weather_ride_correlation.sql` (NEW) | | C | | | | | | |
| `dbt: mart_weather_impact_summary.sql` (NEW) | | C | | | | | | |
| `dbt: mart_hourly_patterns.sql` | | D | | | | | | |
| `dbt: schema.yml` (marts) | | M | | | M | M | | |
| `dashboard/weather_service.py` (NEW) | | | C | | | | | |
| `dashboard/recommendation_engine.py` (NEW) | | | | C | | | | |
| `dashboard/app.py` | | M* | | | M** | M | | |
| `dashboard/static/` (NEW) | | | | | C | | | |
| `dbt: mart_station_weather_performance.sql` (NEW) | | | | | | C | | |
| `dbt: mart_station_directory.sql` (NEW) | | | | | | C | | |
| `streamlit_data_manager/parquet_file_manager.py` | | M | | | | M | | |
| `orchestrator/main.py` | M | | | | | | | |
| `orchestrator/config.py` | M | | | | | | | |
| `requirements.txt` | M | | M | | M | | | |
| `.streamlit/config.toml` (NEW) | | | | | C | | | |
| `tests/` | M | M | M | M | M | M | | |

**Legend:** C = Create, M = Modify, D = Delete/Replace, M* = Minor change, M** = Major rewrite

### Parallel Safety Verification
- **Phases 02, 03, 06** touch disjoint files — safe to run in parallel after Phase 01
- **Phase 05** is the only phase that does a major rewrite of dashboard/app.py — must run AFTER 02, 03, 04
- **Phase 02** makes a minor backward-compatible change to dashboard/app.py (parquet filename) — must complete before Phase 05
- **Phases 07, 08** are design docs only — no code changes, safe anytime

---

## Total Estimated Effort

| Phase | Estimated Effort |
|-------|-----------------|
| 01 - Weather Pipeline | 2-3 days |
| 02 - Analytics Marts | 1-2 days |
| 03 - Real-time Layer | 1-2 days |
| 04 - Recommendation Engine | 1-2 days |
| 05 - Atmospheric UI | 3-5 days |
| 06 - Station Analysis | 1-2 days |
| 07 - Live Station Design | 0.5 day |
| 08 - Near Me Design | 0.5 day |
| **Total** | **~10-17 days** |

---

## Design Documents

- `00_OVERVIEW.md` — This file
- `01_weather-data-pipeline.md` — Weather extraction, data models, DuckDB, dbt staging
- `02_hourly-ride-weather-analytics.md` — Granular hourly marts, weather correlation, impact summaries
- `03_realtime-weather-dashboard-layer.md` — Live weather fetch, auto-refresh, current conditions
- `04_recommendation-engine.md` — Condition classifier, insight generator, biking score
- `05_atmospheric-ui-dashboard-redesign.md` — Animations, gradients, page restructure, beautiful viz
- `06_station-level-weather-analysis.md` — Station weather performance, station directory
- `07_live-station-data-design.md` — GBFS integration architecture (design only)
- `08_near-me-feature-design.md` — Address input, geocoding, proximity (design only)

---

## Future Enhancements (Beyond This Session)

Ideas surfaced during discovery that are out of scope but worth tracking:

- **Zone-based weather:** Weather at user's location, not just city center (ties to Phase 08)
- **ML-based predictions:** Replace simple correlation with trained models for ride prediction
- **Push notifications:** "It's about to clear up — great time to bike in 30 minutes"
- **Social features:** "X people are biking right now in your area"
- **Multi-city expansion:** Add more bike share systems (Chicago Divvy, Paris Velib, etc.)
- **Route weather:** Weather along a specific route, not just at origin
