# Weather Experience Rebuild — Project Overview

**Created:** 2026-04-09
**Status:** Planning
**Scope:** Replace the Streamlit dashboard with an immersive, weather-driven frontend backed by the existing data layer

---

## Project Context

City Cycles is a bike share analytics project comparing NYC (CitiBike) and London (Santander Cycles). The existing system has a strong data foundation — a DuckDB + dbt pipeline that produces 11 mart parquet files, a recommendation engine that classifies weather conditions and queries historical ride patterns, and live weather data from the Open-Meteo API. The data layer is solid. The presentation layer is not.

The current Streamlit dashboard has fundamental limitations:

1. **CSS/Layout hacking.** 14 instances of `unsafe_allow_html=True`, fragile `data-testid` CSS selectors, and fixed-position DOM overlays for rain/snow particles. Unmaintainable.
2. **Full-page re-run model.** Every interaction triggers a complete script re-execution. Weather animations restart, CSS re-injects, queries re-execute. Visible flicker and 2-5 second lag.
3. **State fragmentation.** City selection stored in three independent session state keys (`selected_city`, `analytics_city`, `weather_city`). Pages do not stay in sync.
4. **No component interactivity.** Charts are isolated. No cross-filtering, no expand/collapse, no interactive scrubbing.
5. **Scaling ceiling.** Each session loads all parquet files independently. No shared data layer.

The data story — millions of historical rides cross-referenced with weather, season, and time of day — deserves a presentation layer that matches its depth.

---

## Vision Statement

An immersive weather experience that displays current weather conditions in NYC or London, backed by millions of data points showing what city biking has historically looked like at this given time of day, weather, season, and city. The weather is not a sidebar widget — it IS the interface. The user opens the app and sees the weather, and the data wraps around it.

---

## Architecture Overview

```
                        ┌─────────────────────────────────┐
                        │   React / Next.js Frontend       │
                        │   (Weather Experience + Analytics)│
                        └──────────────┬──────────────────┘
                                       │ HTTP / JSON
                        ┌──────────────▼──────────────────┐
                        │   FastAPI Backend (Python)        │
                        │   /api/weather, /api/insights,   │
                        │   /api/similar-day, /api/analytics│
                        └──────────────┬──────────────────┘
                                       │ imports
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          ┌─────────────┐   ┌──────────────────┐  ┌──────────────┐
          │ weather_     │   │ recommendation_  │  │ DuckDB       │
          │ service.py   │   │ engine.py        │  │ (in-memory)  │
          │ (Open-Meteo) │   │ (classification, │  │ → parquet    │
          └─────────────┘   │  scoring, insight)│  │   queries    │
                            └──────────────────┘  └──────┬───────┘
                                                         │ reads
                                                  ┌──────▼───────┐
                                                  │ /data/*.parquet │
                                                  │ (11 mart files) │
                                                  └──────┬───────┘
                                                         │ monthly sync
                                                  ┌──────▼───────┐
                                                  │ S3 + Railway  │
                                                  │ (cron export) │
                                                  └──────────────┘
```

**Key design principle:** The FastAPI layer wraps existing Python modules (weather_service.py, recommendation_engine.py, query_helpers.py patterns) without rewriting them. The frontend consumes JSON endpoints. The data pipeline, dbt models, S3 storage, and Railway cron are untouched.

---

## Phase Dependency Graph

```
Phase 01: API Layer ─────────────────────────────────────────────────┐
  (foundation, no dependencies)                                      │
       │                                                             │
       ├────────────────────┐                                        │
       ▼                    ▼                                        │
Phase 02: Weather     Phase 03: "Days Like                           │
  Experience            Today" Data                                  │
  Frontend              Visualization                                │
  (depends on 01)       (depends on 01,                              │
       │                 parallel with 02)                            │
       │                    │                                        │
       ├────────────────────┤                                        │
       ▼                    ▼                                        │
Phase 04: City Comparison Mode                                       │
  (depends on 02 + 03)                                               │
                                                                     │
Phase 05: Analytics Deep Dive Migration ◄────── Phase 02             │
  (depends on 02)                                                    │
                                                                     │
Phase 06: Deployment & Hosting ◄──────── Phase 01 + Phase 02 ───────┘
  (can start after 01+02 are functional)
```

**Parallel execution opportunities:**
- Phases 02 and 03 can run in parallel after Phase 01
- Phase 06 groundwork (infra, CI/CD) can start once 01+02 reach a deployable state
- Phase 05 is independent of 03 and 04

---

## Effort Estimates

| Phase | Scope | Size | Est. Duration |
|-------|-------|------|---------------|
| **01: API Layer** | FastAPI + 17 endpoints wrapping existing code | **S** | COMPLETE |
| **02A: Weather Frontend Core** | Next.js app, CSS weather effects, data sections, responsive layout | **M** | COMPLETE |
| **02B: Weather Canvas Particles** | Canvas particle system: rain, snow, lightning | **S** | COMPLETE |
| **03: Days Like Today Visualization** | Data viz enhancements: DurationInsight, MemberCasualSplit, chart annotations | **S** | COMPLETE |
| **04: City Comparison Mode** | Side-by-side NYC/London with shared controls | **M** | COMPLETE |
| **05A: Analytics Deep Dive (Charts)** | Ride Analytics + Weather Deep Dive pages, shared infra | **M** | COMPLETE |
| **05B: Station Explorer** | Mapbox station map, filters, table toggle | **S** | IN PROGRESS |
| **06: Deployment & Hosting** | Vercel/Railway, CI/CD, domain, monitoring | **S** | 1-2 weeks |
| **Total** | | | **11-17 weeks** |

---

## What Stays (Unchanged)

| Component | Location | Why It Stays |
|-----------|----------|-------------|
| Data pipeline (orchestrator) | `orchestrator/` | Monthly batch processing works; Railway cron is reliable |
| dbt models (staging → marts) | `dbt_city_cycles/models/` | 11 mart models are well-designed for the use case |
| Parquet export to S3 | `db_duckdb/`, Railway cron | Monthly export cycle is sufficient |
| Data models (pydantic) | `data_models/` | Schema validation is solid |
| recommendation_engine.py | `dashboard/recommendation_engine.py` | Pure Python, no Streamlit imports, fully testable — import directly into API |
| weather_service.py (core) | `dashboard/weather_service.py` | `fetch_city_weather()` is Streamlit-free |
| S3 bucket structure | `s3://city-cycles-data-ctr37/marts/` | Storage layer is clean |
| Extraction scripts | `extraction/` | Web scraping / API fetch is independent |

---

## What Changes

| Component | Current State | Future State |
|-----------|--------------|-------------|
| **Frontend** | Streamlit (Python, `dashboard/`) | React / Next.js (TypeScript, `frontend/`) |
| **API layer** | None (Streamlit reads parquet directly) | FastAPI (Python, `api/`) |
| **Data access** | `query_helpers.py` with `@st.cache_data` | FastAPI dependencies with in-memory TTL cache |
| **Weather caching** | `@st.cache_data(ttl=900)` | FastAPI in-memory cache (15min TTL) |
| **CSS/Theming** | 14x `unsafe_allow_html`, fragile selectors | Tailwind CSS + CSS modules |
| **Weather animations** | Fixed CSS particle divs (40 rain, 30 snow) | Canvas/WebGL with React control |
| **State management** | 3 independent `st.session_state` keys | Zustand, single source of truth |
| **Hosting** | Streamlit Cloud | Vercel (frontend) + Railway (API) |
| **Charts** | Plotly via `st.plotly_chart` (isolated) | Recharts (interactive, cross-filtered) |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| recommendation_engine.py mapping mismatches (7 temp bands vs 5 in mart) | Already exists | Medium | `_TEMP_BAND_TO_MART` mapping handles it |
| Parquet files missing locally on fresh deploy | High | Low | API downloads from S3 on startup (same as current app) |
| DuckDB concurrency under load | Medium | Medium | Per-request connections in FastAPI |
| Frontend build complexity (new toolchain) | Medium | Low | Next.js is well-documented; keep scope tight |
