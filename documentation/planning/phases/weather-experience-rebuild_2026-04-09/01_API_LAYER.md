# Phase 01: API Layer — Implementation Plan

**Created:** 2026-04-09
**Status:** IN PROGRESS
**Started:** 2026-04-09
**Challenge Round:** 2026-04-09 — CORS via env var, friendlier JSON field names, HTTP-layer tests only
**Depends on:** Nothing (foundation phase)
**Enables:** Phase 02 (Frontend), Phase 03 (Data Viz), Phase 06 (Deployment)

---

## Purpose

Decouple data access from Streamlit so any frontend can consume weather, insights, and analytics data via HTTP. The API wraps existing Python modules (`weather_service.py`, `recommendation_engine.py`, `query_helpers.py` patterns) without rewriting business logic.

---

## Technology

**FastAPI** (Python 3.10+) — same ecosystem, direct imports from existing code, async support, automatic OpenAPI docs.

---

## Endpoints

### Weather Endpoints

#### 1. `GET /api/weather/{city}`
Current weather for NYC or London.
- **Wraps:** `weather_service.fetch_city_weather()` from `dashboard/weather_service.py`
- **Cache:** 15 minutes TTL
- **Response:** temperature_c/f, apparent_temperature, humidity, precipitation_mm, weather_code, weather_description, weather_category, wind_speed_kmh, cloud_cover

#### 2. `GET /api/weather/{city}/forecast`
24-hour hourly forecast.
- **Wraps:** `weather_service.fetch_city_weather().forecast` (first 24 entries)
- **Cache:** Shared with `/api/weather/{city}`

### Insight Endpoints

#### 3. `GET /api/insights/{city}`
Full recommendation result — biking score, classified conditions, ranked insights.
- **Wraps:** `recommendation_engine.get_recommendations()`
- **Implementation:** Fetches weather from cache, constructs `WeatherConditions` (same bridge as `landing.py:22-34`), calls engine
- **Cache:** 15 minutes TTL

#### 4. `GET /api/similar-day/{city}`
Similar day stats for current conditions (daily grain).
- **Wraps:** `recommendation_engine.lookup_similar_day_stats()` with auto-detected month, day_type, weather classification
- **Cache:** 30 minutes TTL

#### 5. `GET /api/similar-day/{city}/hourly`
Hourly ride patterns for similar days (hourly grain from `mart_similar_day_stats`).
- **Implementation:** DuckDB query with `grain = 'hourly'` and same dimension filters as endpoint 4
- **Response:** Array of 24 objects with hour_of_day, avg_daily_rides, avg_duration_minutes, avg_member_rides, avg_casual_rides
- **Cache:** 30 minutes TTL

### Analytics Endpoints

#### 6. `GET /api/analytics/{city}/daily-metrics`
Daily ride metrics with date range filtering.
- **Query params:** `start_date`, `end_date`, `metric` (optional)
- **Data source:** `mart_daily_metrics.parquet`, `mart_daily_metrics_long.parquet`
- **Reuses patterns from:** `ride_analytics.py:72-98`

#### 7. `GET /api/analytics/{city}/hourly-patterns`
Ride distribution by hour of day.
- **Data source:** `mart_hourly_patterns_summary.parquet`
- **Reuses query from:** `ride_analytics.py:163`

#### 8. `GET /api/analytics/{city}/weather-correlation`
Weather vs rides — temperature bands, precipitation impact.
- **Query params:** `group_by` (optional: `temperature`, `precipitation`)
- **Data source:** `mart_weather_ride_correlation.parquet`
- **Reuses patterns from:** `weather_deep_dive.py:48-100`

#### 9. `GET /api/analytics/{city}/weather-impact`
Weather condition impact summary — % change vs clear per condition per hour.
- **Query params:** `conditions` (optional comma-separated)
- **Data source:** `mart_weather_impact_summary.parquet`
- **Reuses patterns from:** `weather_deep_dive.py:118-175`

#### 10. `GET /api/analytics/{city}/station-performance`
Station weather resilience rankings.
- **Query params:** `weather_condition` (required), `hour_start`, `hour_end`, `limit`
- **Data source:** `mart_station_weather_performance.parquet` + `mart_station_directory.parquet`
- **Reuses query from:** `ride_analytics.py:240-253`

#### 11. `GET /api/analytics/{city}/member-analysis`
NYC member vs casual percentage over time. Returns 404 for London.
- **Query params:** `start_date`, `end_date`
- **Data source:** `mart_nyc_member_analysis.parquet`

#### 12. `GET /api/analytics/{city}/station-growth`
Station count by year.
- **Query params:** `start_year`, `end_year` (optional)
- **Data source:** `mart_station_growth.parquet`

All analytics endpoints: **1 hour TTL** cache.

---

## Data Access Pattern

Read from local parquet files in `/data` directory (same as current Streamlit app). DuckDB in-memory queries per request.

```python
# api/dependencies.py
import duckdb, os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Fresh per-request connection (cheap, ~1ms, avoids concurrency issues)."""
    return duckdb.connect(":memory:")

def parquet_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)
```

**Startup:** Call `ensure_local_parquet_files()` from `streamlit_data_manager/parquet_file_manager.py` during FastAPI startup event.

---

## Key Code Reuse

### recommendation_engine.py — Direct Import
Pure Python, no Streamlit imports. Import directly:
```python
from dashboard.recommendation_engine import (
    get_recommendations, WeatherConditions, classify_conditions,
    compute_biking_score, lookup_similar_day_stats, lookup_historical_impact,
)
```

### weather_service.py — Wrap Core Function
`fetch_city_weather(city)` (line 209) is Streamlit-free. Only `get_city_weather_cached()` uses `@st.cache_data`. Wrap the pure function with API's own cache.

### Query Patterns — Lift SQL from Dashboard Pages
Every analytics endpoint's SQL can be lifted from `ride_analytics.py`, `weather_deep_dive.py`, and `comparison.py`.

---

## Caching Strategy

```python
# api/cache.py — simple in-memory TTL cache
import time
from typing import Any, Optional

_cache: dict[str, tuple[float, Any]] = {}

def get(key: str, ttl_seconds: int) -> Optional[Any]:
    if key in _cache:
        stored_at, value = _cache[key]
        if time.time() - stored_at < ttl_seconds:
            return value
        del _cache[key]
    return None

def set(key: str, value: Any) -> None:
    _cache[key] = (time.time(), value)
```

| Data Type | TTL | Rationale |
|-----------|-----|-----------|
| Current weather | 15 min | Matches current Streamlit cache |
| Forecast | 15 min | Same fetch as weather |
| Insights / recommendations | 15 min | Derived from weather |
| Similar-day stats | 30 min | Derived from weather + date |
| Analytics | 1 hour | Historical data, parquets update monthly |

---

## Project Structure

```
api/
├── __init__.py
├── main.py                    # FastAPI app, startup, CORS, router includes
├── dependencies.py            # DuckDB factory, parquet_path, city enum
├── cache.py                   # In-memory TTL cache
├── models/
│   ├── __init__.py
│   ├── weather.py             # Pydantic response models for weather
│   ├── insights.py            # Pydantic response models for insights
│   ├── similar_day.py         # Pydantic response models for similar-day
│   └── analytics.py           # Pydantic response models for analytics
├── routes/
│   ├── __init__.py
│   ├── weather.py             # /api/weather/{city}
│   ├── insights.py            # /api/insights/{city}
│   ├── similar_day.py         # /api/similar-day/{city}
│   └── analytics.py           # /api/analytics/{city}/*
└── services/
    ├── __init__.py
    ├── weather_bridge.py      # Wrapper around dashboard.weather_service
    └── query_service.py       # DuckDB query execution (sans Streamlit)
```

---

## City Path Parameter Validation

```python
from enum import Enum

class CityParam(str, Enum):
    nyc = "nyc"
    london = "london"
```

Automatic 422 for invalid cities, documented in OpenAPI spec.

---

## CORS Configuration

```python
origins = os.getenv("CORS_ORIGIN", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)
```

Read-only API — only `GET` methods needed. CORS origins configurable via `CORS_ORIGIN` env var (comma-separated), defaults to `http://localhost:3000`.

---

## Testing

**Framework:** pytest + httpx AsyncClient

```
tests/
├── test_api_weather.py
├── test_api_insights.py
├── test_api_similar_day.py
├── test_api_analytics.py
├── test_api_cache.py
└── conftest.py               # Shared fixtures (test client, mock data)
```

**Categories:** happy path, city validation (422), missing data (graceful), cache TTL behavior, weather API failure (502), NYC-only endpoints (404 for London).

---

## Verification

```bash
# Start
python -m uvicorn api.main:app --reload --port 8000

# Test endpoints
curl http://localhost:8000/api/weather/nyc | python -m json.tool
curl http://localhost:8000/api/insights/nyc | python -m json.tool
curl http://localhost:8000/api/similar-day/nyc | python -m json.tool
curl http://localhost:8000/api/similar-day/london/hourly | python -m json.tool
curl "http://localhost:8000/api/analytics/nyc/daily-metrics?start_date=2024-01-01&end_date=2024-12-31"
curl http://localhost:8000/api/analytics/nyc/hourly-patterns
curl http://localhost:8000/api/analytics/nyc/station-growth
curl http://localhost:8000/api/weather/paris  # Expect 422

# Swagger UI
open http://localhost:8000/docs
```

---

## Dependencies to Add

```
fastapi>=0.115.0
uvicorn[standard]>=0.34.0
httpx>=0.28.0
```

All other deps (duckdb, pandas, requests, boto3, pydantic) already in `requirements.txt`.

---

## Implementation Sequence

1. `api/` directory structure
2. `api/dependencies.py` — DuckDB factory, path resolution, city enum
3. `api/cache.py` — In-memory TTL cache
4. `api/services/query_service.py` — Port query_helpers.py sans Streamlit
5. `api/services/weather_bridge.py` — Wrap fetch_city_weather() with cache
6. `api/models/` — All Pydantic response models
7. `api/routes/weather.py` — Weather endpoints (simplest, test integration)
8. `api/routes/insights.py` — Insights endpoint (exercises recommendation engine)
9. `api/routes/similar_day.py` — Similar day endpoints (exercises DuckDB + classification)
10. `api/routes/analytics.py` — Analytics endpoints (lift SQL from dashboard pages)
11. `api/main.py` — Wire routers, CORS, startup event
12. Tests alongside each route module
13. Manual verification with curl + Swagger UI
