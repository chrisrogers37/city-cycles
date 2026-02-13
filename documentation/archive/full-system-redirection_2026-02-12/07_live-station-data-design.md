# Phase 07: Live Station Data -- Design Document

## 1. Executive Summary

This document defines the architecture for integrating real-time bike station availability data into the City Cycles dashboard. Both CitiBike (NYC) and Santander Cycles (London) expose station-level APIs that report how many bikes and docks are available at each station in near real-time. This design covers data sourcing, parsing, caching, unified modeling, dashboard rendering, and integration points with other planned phases (04, 06, 08).

**This is a design-only phase.** No code will be written during Phase 07. The purpose is to produce a specification complete enough that an implementer can pick this up months later and build it without ambiguity.

---

## 2. Data Sources & API Details

### 2.1 CitiBike (NYC) -- GBFS

CitiBike publishes a standard [GBFS (General Bikeshare Feed Specification)](https://gbfs.org/documentation/reference/) feed.

**Auto-discovery endpoint:**
```
https://gbfs.citibikenyc.com/gbfs/gbfs.json
```

This returns a JSON object listing all available feeds. The two feeds relevant to this feature:

**station_information** (mostly static, changes when stations are added/removed):
```
https://gbfs.citibikenyc.com/gbfs/en/station_information.json
```
Key fields per station:
- `station_id` (string) -- unique identifier
- `name` (string) -- human-readable station name
- `lat` (float) -- latitude
- `lon` (float) -- longitude
- `capacity` (integer) -- total number of docking points
- `short_name` (string, optional) -- short display name

**station_status** (real-time, refreshed every ~60 seconds):
```
https://gbfs.citibikenyc.com/gbfs/en/station_status.json
```
Key fields per station:
- `station_id` (string) -- joins to station_information
- `num_bikes_available` (integer) -- physical bikes present
- `num_docks_available` (integer) -- empty docks
- `num_ebikes_available` (integer) -- within num_bikes_available count
- `is_installed` (boolean) -- station physically installed
- `is_renting` (boolean) -- accepting rentals
- `is_returning` (boolean) -- accepting returns
- `last_reported` (integer) -- UNIX timestamp of last status update

**Rate limits:** No authentication required. No documented rate limit for GBFS feeds, but the specification recommends polling no more frequently than the `ttl` (time-to-live) value returned in the response (typically 60 seconds for station_status, 86400 seconds for station_information).

**Response wrapper:** Both feeds return `{"last_updated": int, "ttl": int, "data": {"stations": [...]}}`

### 2.2 Santander Cycles (London) -- TfL Unified API

London does not use GBFS. The [TfL Unified API](https://api.tfl.gov.uk/) provides a BikePoint endpoint.

**All stations:**
```
GET https://api.tfl.gov.uk/BikePoint
```

**Single station:**
```
GET https://api.tfl.gov.uk/BikePoint/{id}
```

**Stations by proximity:**
```
GET https://api.tfl.gov.uk/BikePoint?lat={lat}&lon={lon}&radius={meters}
```

The response is a JSON array of objects, each with:
- `id` (string) -- e.g., `"BikePoints_1"`
- `commonName` (string) -- e.g., `"River Street, Clerkenwell"`
- `lat` (float) -- latitude
- `lon` (float) -- longitude
- `additionalProperties` (array) -- key-value pairs containing:
  - `key: "NbBikes"`, `value: "12"` -- bikes available
  - `key: "NbEmptyDocks"`, `value: "8"` -- empty docks
  - `key: "NbDocks"`, `value: "20"` -- total capacity
  - `key: "Installed"`, `value: "true"` -- installed status
  - `key: "Locked"`, `value: "false"` -- locked status
  - `modified` -- ISO 8601 timestamp for each property

**Authentication:** Optional but recommended. Without an API key, the rate limit is approximately 50 requests per minute. With a registered `app_key`, the limit increases (approximately 500 requests per minute). Registration is free at [api-portal.tfl.gov.uk](https://api-portal.tfl.gov.uk/).

**Rate limit strategy:** Register for a TfL API key. Store it as `TFL_API_KEY` in `.env`. Pass it as a query parameter: `?app_key={key}`.

---

## 3. Data Architecture

### 3.1 Storage Decision: In-Memory Only (Dashboard-Side)

**Decision:** Live station data is NOT persisted to DuckDB or S3. It is fetched on-demand by the Streamlit dashboard and held in memory using `st.cache_data` with a short TTL.

**Rationale:**
1. Station status changes every 60 seconds. Persisting it to DuckDB/S3 would add pipeline complexity (a continuously-running ingestion process) with minimal analytical value.
2. The existing pipeline is batch-oriented (monthly runs). A streaming ingestion service is architecturally alien to the current design and would require new infrastructure (always-on EC2, cron job every minute, etc.).
3. Station information (metadata) is semi-static and changes rarely. It can be cached for 24 hours.
4. The primary use case is dashboard display ("show me what's available right now"), not historical analytics on availability.

**Future consideration:** If historical station availability analytics are desired later (e.g., "which stations run out of bikes most often?"), a separate periodic ingestion job could be added. That would be a separate phase, not covered here.

### 3.2 Data Flow Diagram

```
                    Dashboard Request (user loads Live tab)
                                    |
                                    v
                    +-------------------------------+
                    |   dashboard/live_stations.py  |
                    |   (new module)                |
                    +-------------------------------+
                         |                    |
                         v                    v
              +------------------+   +------------------+
              | GBFS Client      |   | TfL Client       |
              | (CitiBike NYC)   |   | (Santander LDN)  |
              +------------------+   +------------------+
                         |                    |
                         v                    v
              station_information     BikePoint (full list)
              station_status          
                         |                    |
                         v                    v
              +-------------------------------------------+
              |    Unified StationLiveStatus model         |
              |    (pydantic, in-memory only)              |
              +-------------------------------------------+
                                    |
                    +---------------+---------------+
                    |                               |
                    v                               v
           Live Availability Map            Status Table / Cards
           (pydeck ScatterplotLayer)        (st.dataframe / metrics)
```

### 3.3 Refresh Cadence

| Data Type | Source | Cache TTL | Refresh Trigger |
|-----------|--------|-----------|-----------------|
| Station information (NYC) | GBFS station_information | 24 hours | Daily or on cache miss |
| Station status (NYC) | GBFS station_status | 60 seconds | Auto-refresh (polling) |
| Station information + status (London) | TfL BikePoint | 120 seconds | Auto-refresh (polling) |

The TfL API returns both station metadata and live availability in one response, so there is no benefit in separating the information vs. status cache for London. A slightly longer TTL (120 seconds) compensates for the fact that TfL returns a larger payload (~800 stations in a single call).

### 3.4 Auto-Refresh Mechanism in Streamlit

Streamlit does not natively support WebSocket-based push updates. The recommended approach is polling via the `streamlit-autorefresh` component (already referenced in community patterns).

**Implementation approach:**
```python
from streamlit_autorefresh import st_autorefresh

# Auto-refresh every 60 seconds, max 1000 refreshes per session
refresh_count = st_autorefresh(interval=60_000, limit=1000, key="live_station_refresh")
```

This triggers a full Streamlit re-run every 60 seconds. Combined with `st.cache_data(ttl=60)`, the station status data will be re-fetched from the APIs on each refresh cycle.

**Dependency:** Add `streamlit-autorefresh` to `requirements.txt`.

---

## 4. Data Models (Pydantic)

### 4.1 New File: `data_models/station_live.py`

These models differ from the existing bike share record models in `data_models/`. Those models are dataclasses inheriting from `BaseBikeShareRecord` and are designed for CSV-to-Parquet pipeline processing. The live station models are pure pydantic models for API response validation, used only at the dashboard layer.

```python
"""
Live station data models for real-time bike availability.

These models are used ONLY by the dashboard layer for API response parsing.
They are NOT part of the batch ETL pipeline and do NOT inherit from
BaseBikeShareRecord.
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


class City(str, Enum):
    NYC = "nyc"
    LONDON = "london"


class StationInfo(BaseModel):
    """Static station metadata (location, name, capacity).
    
    Unified across both cities. Populated from:
    - NYC: GBFS station_information.json
    - London: TfL BikePoint API (top-level fields)
    """
    station_id: str
    name: str
    lat: float
    lon: float
    capacity: int
    city: City


class StationStatus(BaseModel):
    """Real-time station availability.
    
    Unified across both cities. Populated from:
    - NYC: GBFS station_status.json
    - London: TfL BikePoint API (additionalProperties)
    """
    station_id: str
    bikes_available: int
    docks_available: int
    ebikes_available: Optional[int] = None  # NYC only (GBFS provides this)
    is_installed: bool = True
    is_renting: bool = True
    is_returning: bool = True
    last_reported: Optional[datetime] = None


class StationLiveView(BaseModel):
    """Combined station info + live status for dashboard rendering.
    
    This is the primary model passed to visualization functions.
    One instance per station.
    """
    station_id: str
    name: str
    lat: float
    lon: float
    capacity: int
    city: City
    bikes_available: int
    docks_available: int
    ebikes_available: Optional[int] = None
    is_installed: bool = True
    is_renting: bool = True
    is_returning: bool = True
    last_reported: Optional[datetime] = None
    
    # Derived fields
    utilization_pct: float = Field(
        default=0.0,
        description="Percentage of capacity currently occupied by bikes"
    )
    is_active: bool = Field(
        default=True,
        description="True if station is installed, renting, and returning"
    )

    @classmethod
    def from_info_and_status(
        cls, info: StationInfo, status: StationStatus
    ) -> "StationLiveView":
        """Merge a StationInfo and StationStatus into a StationLiveView."""
        utilization = (
            status.bikes_available / info.capacity * 100
            if info.capacity > 0 else 0.0
        )
        is_active = (
            status.is_installed
            and status.is_renting
            and status.is_returning
        )
        return cls(
            station_id=info.station_id,
            name=info.name,
            lat=info.lat,
            lon=info.lon,
            capacity=info.capacity,
            city=info.city,
            bikes_available=status.bikes_available,
            docks_available=status.docks_available,
            ebikes_available=status.ebikes_available,
            is_installed=status.is_installed,
            is_renting=status.is_renting,
            is_returning=status.is_returning,
            last_reported=status.last_reported,
            utilization_pct=round(utilization, 1),
            is_active=is_active,
        )
```

### 4.2 Why Not Extend BaseBikeShareRecord?

The existing `BaseBikeShareRecord` in `/Users/chris/Projects/city-cycles/data_models/base.py` is designed for batch CSV processing: it has `staging_table`, `s3_prefix`, `_required_columns`, and `validate_schema(df)` / `to_dataframe(df, source_file)` methods. None of these concepts apply to a real-time API response. Using pydantic `BaseModel` directly is cleaner and avoids polluting the batch pipeline's registry (`BaseBikeShareRecord._registry`).

The live station models should NOT be registered in `data_models/registry.py` or `data_models/__init__.py` to avoid confusion with the ETL pipeline models.

---

## 5. API Integration Design

### 5.1 New File: `dashboard/live_stations.py`

This module contains all API client logic for fetching live station data. It is imported only by the dashboard, not by the ETL pipeline.

#### 5.1.1 GBFS Client (CitiBike NYC)

```python
import requests
import logging
from datetime import datetime
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

CITIBIKE_GBFS_BASE = "https://gbfs.citibikenyc.com/gbfs/en"
STATION_INFO_URL = f"{CITIBIKE_GBFS_BASE}/station_information.json"
STATION_STATUS_URL = f"{CITIBIKE_GBFS_BASE}/station_status.json"

REQUEST_TIMEOUT = 10  # seconds


def fetch_citibike_station_info() -> List[StationInfo]:
    """Fetch station metadata from GBFS station_information feed."""
    response = requests.get(STATION_INFO_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    
    stations = []
    for s in data["data"]["stations"]:
        stations.append(StationInfo(
            station_id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            capacity=s.get("capacity", 0),
            city=City.NYC,
        ))
    return stations


def fetch_citibike_station_status() -> List[StationStatus]:
    """Fetch real-time status from GBFS station_status feed."""
    response = requests.get(STATION_STATUS_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    
    statuses = []
    for s in data["data"]["stations"]:
        statuses.append(StationStatus(
            station_id=s["station_id"],
            bikes_available=s.get("num_bikes_available", 0),
            docks_available=s.get("num_docks_available", 0),
            ebikes_available=s.get("num_ebikes_available"),
            is_installed=bool(s.get("is_installed", 1)),
            is_renting=bool(s.get("is_renting", 1)),
            is_returning=bool(s.get("is_returning", 1)),
            last_reported=datetime.fromtimestamp(s["last_reported"])
                if s.get("last_reported") else None,
        ))
    return statuses
```

#### 5.1.2 TfL Client (Santander London)

```python
import os

TFL_BIKEPOINT_URL = "https://api.tfl.gov.uk/BikePoint"


def _get_tfl_params() -> Dict[str, str]:
    """Build query params, including API key if available."""
    params = {}
    api_key = os.environ.get("TFL_API_KEY")
    if api_key:
        params["app_key"] = api_key
    return params


def fetch_london_stations() -> Tuple[List[StationInfo], List[StationStatus]]:
    """Fetch all London station info and status in one API call.
    
    TfL returns both metadata and live availability in a single response,
    unlike GBFS which separates them into two feeds.
    """
    response = requests.get(
        TFL_BIKEPOINT_URL,
        params=_get_tfl_params(),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    
    infos = []
    statuses = []
    
    for point in data:
        # Extract station ID (e.g., "BikePoints_1" -> "1")
        station_id = point["id"].replace("BikePoints_", "")
        
        # Parse additionalProperties into a dict for easy lookup
        props = {
            p["key"]: p["value"]
            for p in point.get("additionalProperties", [])
        }
        
        infos.append(StationInfo(
            station_id=station_id,
            name=point["commonName"],
            lat=point["lat"],
            lon=point["lon"],
            capacity=int(props.get("NbDocks", 0)),
            city=City.LONDON,
        ))
        
        # Find the most recent modification timestamp
        modified_dates = [
            p.get("modified") for p in point.get("additionalProperties", [])
            if p.get("modified")
        ]
        last_reported = None
        if modified_dates:
            last_reported = datetime.fromisoformat(
                max(modified_dates).replace("Z", "+00:00")
            )
        
        statuses.append(StationStatus(
            station_id=station_id,
            bikes_available=int(props.get("NbBikes", 0)),
            docks_available=int(props.get("NbEmptyDocks", 0)),
            ebikes_available=None,  # TfL does not separate e-bikes
            is_installed=props.get("Installed", "").lower() == "true",
            is_renting=props.get("Locked", "").lower() != "true",
            is_returning=props.get("Locked", "").lower() != "true",
            last_reported=last_reported,
        ))
    
    return infos, statuses
```

#### 5.1.3 Unified Fetch Function with Caching

```python
import streamlit as st
import pandas as pd


@st.cache_data(ttl=86400)  # 24 hours for station info
def get_station_info(city: str) -> List[StationInfo]:
    """Cached station metadata fetch."""
    if city == "nyc":
        return fetch_citibike_station_info()
    elif city == "london":
        infos, _ = fetch_london_stations()
        return infos
    raise ValueError(f"Unknown city: {city}")


@st.cache_data(ttl=60)  # 60 seconds for live status
def get_station_status(city: str) -> List[StationStatus]:
    """Cached station status fetch."""
    if city == "nyc":
        return fetch_citibike_station_status()
    elif city == "london":
        _, statuses = fetch_london_stations()
        return statuses
    raise ValueError(f"Unknown city: {city}")


def get_live_stations(city: str) -> pd.DataFrame:
    """Get merged station info + status as a DataFrame for display.
    
    Returns a DataFrame with one row per active station, suitable for
    pydeck rendering and tabular display.
    """
    infos = get_station_info(city)
    statuses = get_station_status(city)
    
    # Build lookup by station_id
    status_map = {s.station_id: s for s in statuses}
    
    live_views = []
    for info in infos:
        status = status_map.get(info.station_id)
        if status is None:
            continue  # Station exists in info but not in status (rare)
        view = StationLiveView.from_info_and_status(info, status)
        if view.is_active:  # Only show active stations
            live_views.append(view.model_dump())
    
    return pd.DataFrame(live_views)
```

### 5.2 Error Handling & Fallback Behavior

| Failure Mode | Behavior |
|-------------|----------|
| API timeout (>10s) | Show stale cached data (if available) + warning banner |
| API HTTP error (4xx/5xx) | Show stale cached data + error banner with retry button |
| API returns empty data | Show "No station data available" message |
| Network completely down | Show placeholder message, do not crash dashboard |
| Partial city failure | Show data for the city that succeeded, error for the other |
| Malformed JSON response | Log error, show fallback message, do not crash |

**Implementation pattern:** Wrap all API calls in try/except. Use `st.warning()` for degraded states. Never let an API failure crash the entire dashboard.

```python
def safe_get_live_stations(city: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Fetch live station data with graceful error handling.
    
    Returns:
        Tuple of (DataFrame or None, error_message or None)
    """
    try:
        df = get_live_stations(city)
        if df.empty:
            return None, "No active stations found."
        return df, None
    except requests.exceptions.Timeout:
        return None, "Station data temporarily unavailable (timeout). Showing cached data if available."
    except requests.exceptions.HTTPError as e:
        return None, f"Station API error: {e.response.status_code}. Retrying on next refresh."
    except Exception as e:
        logger.error(f"Unexpected error fetching live stations for {city}: {e}")
        return None, "Unable to load live station data. Please try again."
```

### 5.3 Rate Limiting Considerations

| API | Limit | Our Usage | Safety Margin |
|-----|-------|-----------|---------------|
| GBFS (CitiBike) | No hard limit; TTL=60s | 2 requests/minute (info + status) | Well within spec |
| TfL BikePoint (unauthenticated) | ~50 req/min | 1 request/2 min | ~2% of limit |
| TfL BikePoint (with app_key) | ~500 req/min | 1 request/2 min | ~0.2% of limit |

Risk is minimal. Even with multiple concurrent dashboard users on Streamlit Cloud, `st.cache_data` ensures the API is called at most once per TTL period (the cache is shared across users in a Streamlit Cloud deployment).

---

## 6. Dashboard Integration

### 6.1 New Dashboard Section: "Live Availability"

This will be a new page in the sidebar navigation. Using the existing pattern from `/Users/chris/Projects/city-cycles/dashboard/app.py` where the sidebar radio selects between "NYC", "London", and "Comparison", a new option "Live" (or an expandable section on the NYC/London pages) will be added.

**Recommended approach:** Add "Live Availability" as a toggleable section within each city's page (NYC and London), not as a separate page. This keeps it contextually linked to the city the user is already viewing.

#### 6.1.1 Map Visualization (pydeck)

The `pydeck` library is already in `requirements.txt` (line 57: `pydeck==0.9.1`). It provides GPU-accelerated map rendering within Streamlit.

```python
import pydeck as pdk

def render_live_station_map(df: pd.DataFrame, city: str):
    """Render an interactive map of live station availability.
    
    Args:
        df: DataFrame with columns: lat, lon, bikes_available,
            docks_available, capacity, name, utilization_pct
        city: "nyc" or "london" (determines map center)
    """
    # Map center coordinates
    centers = {
        "nyc": {"lat": 40.7128, "lon": -74.0060, "zoom": 12},
        "london": {"lat": 51.5074, "lon": -0.1278, "zoom": 12},
    }
    center = centers[city]
    
    # Color by utilization: green (many bikes) -> yellow -> red (few bikes)
    # Using bikes_available / capacity to derive color
    df["fill_color"] = df.apply(
        lambda row: _utilization_color(row["utilization_pct"]), axis=1
    )
    
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_radius=40,  # meters
        get_fill_color="fill_color",
        pickable=True,
        auto_highlight=True,
    )
    
    tooltip = {
        "html": "<b>{name}</b><br/>"
                "Bikes: {bikes_available} / {capacity}<br/>"
                "Docks: {docks_available}<br/>"
                "Utilization: {utilization_pct}%",
        "style": {
            "backgroundColor": "steelblue",
            "color": "white",
        }
    }
    
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=center["lat"],
            longitude=center["lon"],
            zoom=center["zoom"],
            pitch=0,
        ),
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/light-v10",
    )
    
    st.pydeck_chart(deck)


def _utilization_color(utilization_pct: float) -> list:
    """Map utilization percentage to RGB color.
    
    0% (empty) -> red [255, 59, 48]
    50% -> yellow [255, 204, 0]
    100% (full) -> green [52, 199, 89]
    """
    if utilization_pct <= 50:
        t = utilization_pct / 50
        r = int(255)
        g = int(59 + (204 - 59) * t)
        b = int(48 - 48 * t)
    else:
        t = (utilization_pct - 50) / 50
        r = int(255 - (255 - 52) * t)
        g = int(204 - (204 - 199) * t)
        b = int(0 + 89 * t)
    return [r, g, b, 180]
```

#### 6.1.2 Summary Metrics

Above the map, show aggregate metrics:

```python
def render_live_summary(df: pd.DataFrame, city: str):
    """Show summary metrics for live station data."""
    col1, col2, col3, col4 = st.columns(4)
    
    total_stations = len(df)
    total_bikes = df["bikes_available"].sum()
    total_docks = df["docks_available"].sum()
    avg_utilization = df["utilization_pct"].mean()
    
    with col1:
        st.metric("Active Stations", f"{total_stations:,}")
    with col2:
        st.metric("Bikes Available", f"{total_bikes:,}")
    with col3:
        st.metric("Docks Available", f"{total_docks:,}")
    with col4:
        st.metric("Avg Utilization", f"{avg_utilization:.1f}%")
```

#### 6.1.3 Data Freshness Indicator

```python
def render_freshness_indicator(df: pd.DataFrame):
    """Show when the data was last refreshed."""
    if "last_reported" in df.columns and not df["last_reported"].isna().all():
        most_recent = df["last_reported"].max()
        age_seconds = (datetime.now() - most_recent).total_seconds()
        
        if age_seconds < 120:
            st.caption(f"Data is current (updated {int(age_seconds)}s ago)")
        elif age_seconds < 600:
            st.warning(f"Data may be stale (last update {int(age_seconds / 60)} minutes ago)")
        else:
            st.error(f"Data is stale (last update {int(age_seconds / 60)} minutes ago)")
    else:
        st.caption("Data freshness unknown")
```

### 6.2 Weather + Live Station Integration

When Phase 03 (Real-time Weather Dashboard Layer) is built, the live station map can be enhanced to show weather context. The design for this integration:

**Concept:** "It's raining -- these stations near you have bikes"

```python
def render_weather_station_combo(df: pd.DataFrame, weather: dict, city: str):
    """Render live stations with current weather context.
    
    Args:
        df: Live station DataFrame
        weather: Current weather dict from Phase 03's weather_service.py
            e.g. {"temp_c": 8, "condition": "rain", "description": "Light rain"}
        city: "nyc" or "london"
    """
    # Weather banner
    condition = weather.get("condition", "unknown")
    temp = weather.get("temp_c")
    
    if condition in ("rain", "drizzle", "thunderstorm"):
        st.info(
            f"Current conditions: {weather['description']} ({temp} C). "
            f"{df['bikes_available'].sum():,} bikes available across "
            f"{len(df):,} stations."
        )
    elif condition == "snow":
        st.warning(
            f"Snow reported. Exercise caution. "
            f"{df['bikes_available'].sum():,} bikes still available."
        )
    else:
        st.success(
            f"Good conditions for biking! {weather['description']} ({temp} C). "
            f"{df['bikes_available'].sum():,} bikes available."
        )
    
    # Render map normally
    render_live_station_map(df, city)
```

This function depends on Phase 03's `dashboard/weather_service.py` being available. When Phase 07 is implemented, check whether Phase 03 is complete and conditionally use weather data.

---

## 7. Infrastructure Considerations

### 7.1 Does This Need a Backend Service?

**No.** Streamlit can handle this directly. The reasons:

1. **Low request volume:** At most 2-3 API calls per minute (1 per city per refresh cycle), well within Streamlit Cloud's capabilities.
2. **No persistence needed:** Data is in-memory only with `st.cache_data`.
3. **No transformation needed:** The API responses are simple JSON, parsed directly into pydantic models.
4. **Shared cache:** `st.cache_data` on Streamlit Cloud is shared across all users viewing the same deployment, so even with multiple concurrent users, API calls are not duplicated.

A backend service (e.g., FastAPI) would only be needed if:
- Historical availability tracking is added (periodic ingestion to DuckDB)
- The number of concurrent users exceeds what Streamlit Cloud can handle
- Push notifications are implemented

None of these are in scope for the initial implementation.

### 7.2 Caching Strategy Summary

```
+-----------------------------------------------------+
|           Streamlit Cache Architecture               |
+-----------------------------------------------------+
|                                                       |
|  st.cache_data(ttl=86400)                            |
|  ┌──────────────────────────────────────────────┐    |
|  │ Station Information (NYC)  - 24h cache       │    |
|  │ Station Information (London) - 24h cache     │    |
|  └──────────────────────────────────────────────┘    |
|                                                       |
|  st.cache_data(ttl=60)                               |
|  ┌──────────────────────────────────────────────┐    |
|  │ Station Status (NYC)  - 60s cache            │    |
|  │ Station Status (London) - 120s cache*        │    |
|  └──────────────────────────────────────────────┘    |
|                                                       |
|  * London uses separate ttl because one API call     |
|    returns both info + status. Longer TTL reduces    |
|    API load for the larger payload.                  |
|                                                       |
+-----------------------------------------------------+
```

### 7.3 Environment Variables

New environment variables required (add to `.env` and `orchestrator/config.py`):

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `TFL_API_KEY` | No | None | TfL API key for higher rate limits |
| `LIVE_STATION_ENABLED` | No | `true` | Feature flag to enable/disable live station tab |
| `LIVE_STATION_REFRESH_SECONDS` | No | `60` | Auto-refresh interval in seconds |

### 7.4 New Dependencies

Add to `requirements.txt`:

```
streamlit-autorefresh==1.0.1
```

Note: `requests` (already present at line 65), `pydeck` (already present at line 57), and `pydantic` (already present at line 55) are already in the requirements.

---

## 8. Integration with Other Phases

### 8.1 Phase 06: Station-Level Weather Analysis

Phase 06 creates two new dbt models:
- `mart_station_directory.sql` -- a deduplicated station catalog from historical ride data
- `mart_station_weather_performance.sql` -- how each station performs under different weather conditions

**Integration point:** Phase 06's station directory provides the canonical list of station IDs and names derived from historical data. Live station data uses IDs from the GBFS/TfL APIs. These ID systems may not match perfectly:

- **NYC:** Historical ride data uses the same `station_id` values as GBFS. These should join directly.
- **London:** Historical ride data uses `start_station_id` / `end_station_id` values that correspond to TfL's BikePoint IDs (the numeric portion of `BikePoints_XXX`). These should also join.

**Joining historical and live data:** When the user views a station on the live map, the dashboard can look up that station's historical weather performance from `mart_station_weather_performance.parquet`:

```python
# Pseudocode for station detail panel
def show_station_detail(station_id: str, city: str):
    live_status = get_current_status(station_id)
    historical = query_station_weather_performance(station_id, city)
    
    st.write(f"Station: {live_status.name}")
    st.metric("Bikes Now", live_status.bikes_available)
    st.write("Historical weather performance:")
    st.dataframe(historical)  # e.g., avg rides per day by weather condition
```

### 8.2 Phase 08: "Near Me" Feature

Phase 08 designs an address-input or geolocation feature that finds nearby stations. Live station data is the natural data source for "Near Me" -- users want to see what's available near them right now, not what existed historically.

**Integration point:** Phase 08 will need:
1. The `get_live_stations(city)` function from this phase to get current availability.
2. A distance calculation from the user's location to each station.
3. Filtering to show only the nearest N stations.

**TfL proximity endpoint:** The TfL API supports `?lat=&lon=&radius=` parameters that return only stations within a radius. This can be used directly for London instead of fetching all stations and filtering client-side.

**GBFS does not have a proximity endpoint.** For NYC, all stations must be fetched, then filtered by distance in Python.

**Design recommendation for Phase 08:** The `get_live_stations()` function should accept an optional `center_lat, center_lon, radius_km` parameter. For NYC, this filters the full station list post-fetch. For London, this modifies the TfL API request to use the proximity endpoint.

### 8.3 Phase 04: Recommendation Engine

Phase 04 builds a recommendation engine that answers "Should I bike today?" The live station data can enhance recommendations:

- "Great biking weather, and 85% of stations near your route have bikes available"
- "Conditions are good, but availability is low in Midtown -- consider starting from Penn Station"

**Integration point:** Phase 04's `dashboard/recommendation_engine.py` can call `get_live_stations()` to include real-time availability context in recommendations. This is an optional enhancement -- the recommendation engine should work without live data (falling back to weather-only recommendations).

---

## 9. File Structure for Implementation

```
city-cycles/
  data_models/
    station_live.py              (NEW) -- Pydantic models for live station data
  dashboard/
    live_stations.py             (NEW) -- API clients, caching, fetch logic
    live_station_components.py   (NEW) -- Map, metrics, freshness UI components
    app.py                       (MODIFY) -- Add "Live Availability" section
  streamlit_data_manager/
    parquet_file_manager.py      (NO CHANGE) -- Live data is in-memory, not parquet
  tests/
    test_station_live_models.py  (NEW) -- Unit tests for pydantic models
    test_live_stations.py        (NEW) -- Unit tests for API clients (mocked)
  requirements.txt               (MODIFY) -- Add streamlit-autorefresh
  .env                           (MODIFY) -- Add TFL_API_KEY (optional)
```

---

## 10. Testing Strategy

### 10.1 Unit Tests for Pydantic Models

Test `StationInfo`, `StationStatus`, and `StationLiveView` validation:
- Valid construction
- Missing required fields raise `ValidationError`
- `StationLiveView.from_info_and_status()` correctly computes `utilization_pct` and `is_active`
- Edge cases: zero capacity, all fields at boundary values

### 10.2 Unit Tests for API Clients (Mocked)

Following the existing pattern in `/Users/chris/Projects/city-cycles/tests/test_extraction.py`, mock `requests.get` to return fixture JSON:

```python
# tests/test_live_stations.py

@patch("dashboard.live_stations.requests.get")
def test_fetch_citibike_station_info(mock_get):
    """fetch_citibike_station_info() should parse GBFS station_information."""
    mock_get.return_value.json.return_value = {
        "last_updated": 1700000000,
        "ttl": 60,
        "data": {
            "stations": [
                {
                    "station_id": "1",
                    "name": "Test Station",
                    "lat": 40.7128,
                    "lon": -74.0060,
                    "capacity": 20,
                }
            ]
        }
    }
    mock_get.return_value.raise_for_status = MagicMock()
    
    from dashboard.live_stations import fetch_citibike_station_info
    result = fetch_citibike_station_info()
    
    assert len(result) == 1
    assert result[0].station_id == "1"
    assert result[0].city == City.NYC
```

### 10.3 Integration Test (Manual)

A manual test script that calls the real APIs (not run in CI) to verify the parsing works with actual production data:

```python
# tests/manual/test_live_api.py (not in CI)
# Run: python -m tests.manual.test_live_api

def test_real_citibike():
    stations = fetch_citibike_station_info()
    assert len(stations) > 100  # NYC has ~2000 stations
    
    statuses = fetch_citibike_station_status()
    assert len(statuses) > 100
```

---

## 11. Implementation Estimate

| Task | Effort | Dependencies |
|------|--------|--------------|
| Pydantic models (`data_models/station_live.py`) | 1-2 hours | None |
| GBFS client for NYC | 2-3 hours | Models |
| TfL client for London | 2-3 hours | Models |
| Unified fetch + caching layer | 1-2 hours | Both clients |
| Map visualization component (pydeck) | 3-4 hours | Fetch layer |
| Summary metrics + freshness indicator | 1-2 hours | Fetch layer |
| Dashboard integration (app.py modifications) | 2-3 hours | All above |
| Error handling + fallback | 1-2 hours | All above |
| Unit tests (models + mocked API) | 2-3 hours | Models + clients |
| Auto-refresh integration (streamlit-autorefresh) | 1 hour | Dashboard |
| Weather integration hooks (if Phase 03 done) | 1-2 hours | Phase 03 |
| **Total** | **~2-3 days** | |

### Suggested Implementation Order

1. Pydantic models (no external dependencies)
2. GBFS client + unit tests
3. TfL client + unit tests
4. Unified fetch layer + caching
5. Dashboard map component
6. Summary metrics + freshness
7. App.py integration + auto-refresh
8. Error handling pass
9. Weather integration (if Phase 03 is available)

---

## 12. Open Questions for Implementer

1. **Mapbox API key:** `pydeck` uses Mapbox for base maps. Streamlit Cloud provides a default Mapbox token, but custom deployments may need `MAPBOX_API_KEY` in `.env`. Verify this works on the existing Streamlit Cloud deployment.

2. **Station ID matching between historical and live data:** Verify that `station_id` values from GBFS match those in `unified_rides.start_station_id` / `end_station_id`. If they do not, a mapping table may be needed.

3. **TfL API reliability:** The TfL API occasionally returns incomplete data or is slow. Determine through testing whether 120-second TTL is sufficient or if it should be longer.

4. **Streamlit Cloud resource limits:** With ~2,800 NYC stations and ~800 London stations, the pydeck map will render ~3,600 points. Verify this performs well on Streamlit Cloud's resource tier.

5. **Feature flag:** Should `LIVE_STATION_ENABLED` default to `true` or `false`? If the TfL API key is not configured, the London section will still work (unauthenticated) but with lower rate limits. Should the feature silently degrade or explicitly warn?

---

Sources:
- [GBFS Specification Reference](https://gbfs.org/documentation/reference/)
- [GBFS v2.2 on GitHub](https://github.com/NABSA/gbfs/blob/v2.2/gbfs.md)
- [TfL Unified API](https://api.tfl.gov.uk/)
- [TfL API Rate Limits](https://techforum.tfl.gov.uk/t/what-is-the-rate-limit-or-quota-for-the-api/22)
- [TfL API Portal](https://api-portal.tfl.gov.uk/)
- [Citi Bike System Data](https://citibikenyc.com/system-data)
- [streamlit-autorefresh on GitHub](https://github.com/kmcgrady/streamlit-autorefresh)
- [GBFS Definitions (Google)](https://developers.google.com/micromobility/reference/gbfs-definitions)
