# Phase 08: "Near Me" Feature -- Design Document

## 1. Overview and Product Vision

The "Near Me" feature transforms City Cycles from a historical analytics dashboard into a location-aware, personalized recommendation tool. A user provides their location (address, browser geolocation, or map click), and the system responds with:

- The N nearest bike share stations
- Current weather at their location
- Historical weather-ride correlation data for those specific stations
- Live bike/dock availability (if Phase 07 is implemented)
- A synthesized recommendation such as: "Station X is 0.3 mi away, has 8 bikes available, and historically maintains good ridership in current conditions"

This feature depends on data outputs from Phases 01, 02, 06, and optionally Phase 07. It is designed here but not implemented.

---

## 2. Address Input and Geocoding

### 2.1 Location Input Methods

Three methods should be supported, in order of implementation priority:

**Method A: Text Address Input (Primary)**
A `st.text_input` field where the user types an address or place name (e.g., "Times Square, NYC" or "King's Cross, London"). This is the simplest to implement and has no privacy concerns.

```
st.text_input("Enter your address or a nearby landmark", placeholder="e.g., Times Square, NYC")
```

**Method B: Browser Geolocation (Secondary)**
Use the `streamlit-geolocation` component (available on PyPI) which wraps the JavaScript `navigator.geolocation.getCurrentPosition()` API. This returns lat/lng directly, bypassing the need for geocoding. It requires explicit user consent via a browser permission prompt.

The component would be added to `requirements.txt`:
```
streamlit-geolocation>=1.0.0
```

Usage pattern:
```python
from streamlit_geolocation import streamlit_geolocation
location = streamlit_geolocation()
if location and location["latitude"] is not None:
    user_lat, user_lng = location["latitude"], location["longitude"]
```

**Method C: Map Click (Tertiary)**
Use `streamlit-folium` to render an interactive map. The user clicks a point, and `st_folium()` returns the clicked coordinates. This requires adding `streamlit-folium` to `requirements.txt`.

```python
from streamlit_folium import st_folium
import folium

m = folium.Map(location=[40.7128, -74.0060], zoom_start=12)
map_data = st_folium(m, width=700, height=400)
if map_data and map_data["last_clicked"]:
    user_lat = map_data["last_clicked"]["lat"]
    user_lng = map_data["last_clicked"]["lng"]
```

### 2.2 Geocoding Service Selection

**Recommended: Nominatim (OpenStreetMap) via `geopy`**

Rationale:
- Free for non-commercial use (City Cycles is a portfolio project)
- No API key required
- `geopy` is a well-maintained Python library that wraps Nominatim cleanly
- Rate limit: 1 request/second (acceptable -- the user geocodes once per session, not in a loop)
- Attribution to OpenStreetMap contributors is the only requirement

The `geopy` library would be added to `requirements.txt`.

Implementation pattern:
```python
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="city-cycles-dashboard")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

location = geocode("Times Square, New York, NY")
if location:
    lat, lng = location.latitude, location.longitude
```

**Alternative considered: Open-Meteo Geocoding API**

Open-Meteo provides its own geocoding endpoint at `https://geocoding-api.open-meteo.com/v1/search`. Since Phase 01 already uses Open-Meteo for weather data, this would reduce external dependencies. However, Nominatim has better address-level resolution and is the industry standard for OpenStreetMap data. The recommendation is to use Nominatim for address geocoding and let Open-Meteo handle only weather.

**Not recommended: Google Maps Geocoding API**

Requires an API key, billing account, and costs money after the free tier. Unnecessary for a portfolio project.

### 2.3 City Detection from Coordinates

After geocoding, the system must determine which city the user is in to scope the station search. Simple bounding box check:

- **NYC bounding box:** Lat 40.49-40.92, Lng -74.26 to -73.68
- **London bounding box:** Lat 51.28-51.69, Lng -0.51 to 0.33

If coordinates fall outside both boxes, display a message: "This feature is currently available for New York City and London only."

---

## 3. Station Directory (Prerequisite Data)

### 3.1 Current State of Station Coordinates

**NYC: Coordinates ARE available in ride data.**

Both NYC legacy and modern schemas include per-ride lat/lng:
- `stg_nyc_legacy.sql`: `start_station_latitude`, `start_station_longitude`, `end_station_latitude`, `end_station_longitude`
- `stg_nyc_modern.sql`: `start_lat/start_lng`, `end_lat/end_lng`
- `unified_rides.sql`: `start_latitude`, `start_longitude`, `end_latitude`, `end_longitude`

These coordinates are attached to every ride, not to a station master table. A station directory can be derived by aggregating distinct station_id/station_name combinations with their median or mode coordinates.

**London: Coordinates are NOT available in ride data.**

Both London schemas (`stg_london_legacy.sql`, `stg_london_modern.sql`) contain only `start_station_name`, `start_station_id`, `end_station_name`, `end_station_id`. No latitude or longitude fields exist. The `unified_rides.sql` model explicitly sets London coordinates to `NULL`.

### 3.2 Solving the London Station Coordinates Problem

**Recommended approach: TfL BikePoint API (one-time fetch + periodic refresh)**

The TfL Unified API at `https://api.tfl.gov.uk/BikePoint/` returns a JSON array of all current docking stations with their latitude and longitude. This endpoint:
- Is publicly accessible (free, though an app_id/app_key is recommended for higher rate limits)
- Returns station ID, common name, and coordinates for every active station
- Includes `NbBikes`, `NbDocks`, and other real-time data

The response contains an `id` field (e.g., `BikePoints_1`) and `commonName` field (e.g., `River Street, Clerkenwell`). The numeric station ID embedded in the `id` field can be matched to the `start_station_number` / `end_station_number` fields in the London ride data.

**Implementation strategy:**

1. Create a new extraction script `extraction/london_stations.py` that calls `https://api.tfl.gov.uk/BikePoint/` and saves the result as a JSON or CSV seed file
2. Add a dbt seed `dbt_city_cycles/seeds/london_station_coordinates.csv` containing columns: `station_id`, `station_name`, `latitude`, `longitude`
3. Alternatively, create a new raw table `raw_london_stations` loaded from a Parquet file in S3

Refreshing this data monthly (alongside the pipeline) is sufficient since stations rarely move.

**Matching strategy:**

London station IDs are numeric strings (e.g., `"000981"`, `"200163"`). The TfL API returns IDs like `"BikePoints_981"`. The match should strip the `BikePoints_` prefix and compare numeric values, with station name as a fallback for edge cases.

**Fallback approach: Geocode station names via Nominatim**

If the TfL API is unavailable or insufficient, station names like "British Museum, Bloomsbury" or "Waterloo Station 1, Waterloo" contain enough geographic context for Nominatim to geocode them. However, this would require geocoding hundreds of stations (at 1/second rate limit = several minutes) and may have lower accuracy than the TfL API. This should be treated as a secondary fallback.

### 3.3 Station Directory dbt Model

Phase 06 already plans to create `mart_station_directory.sql` (per the file impact matrix in `00_OVERVIEW.md`). This model should be designed to:

1. Aggregate NYC station coordinates from ride data (median lat/lng per station_id)
2. Join London station IDs to the TfL-sourced coordinate table
3. Output a unified station directory with columns:

```
station_id       VARCHAR   -- e.g., "6464.09" (NYC) or "000981" (London)
station_name     VARCHAR   -- e.g., "E 44 St & Lexington Ave"
latitude         DOUBLE
longitude        DOUBLE
location         VARCHAR   -- "nyc" or "london"
total_rides      BIGINT    -- lifetime ride count (starts + ends)
first_seen_date  DATE      -- earliest ride at this station
last_seen_date   DATE      -- most recent ride at this station
is_active        BOOLEAN   -- last_seen_date within 90 days of max(date)
```

This model is a prerequisite for the "Near Me" feature and should be built during Phase 06.

---

## 4. Proximity Calculation

### 4.1 Distance Formula: Haversine

**Recommendation: Haversine formula (straight-line, "as the crow flies")**

Rationale:
- Simple to implement with zero external dependencies
- Sufficient accuracy for finding nearby stations (we are comparing relative distances, not navigating)
- The `haversine` PyPI package or a 10-line pure Python implementation both work
- Walking distance APIs (Google Directions, OSRM) add complexity, cost, and latency for marginal benefit

The Haversine formula calculates the great-circle distance between two points on a sphere given their latitudes and longitudes. For distances under 10 km in a city context, the error vs. actual walking distance is typically 20-40% (walking distances are longer due to road geometry), but the ranking of "nearest stations" is preserved.

**Implementation pattern (pure Python, no external dependency):**

```python
import math

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c
```

Units: miles for NYC (American convention), kilometers for London (metric convention). The dashboard should display the unit appropriate to the city.

### 4.2 Search Strategy: K-Nearest with Radius Cap

**Recommendation: Return the K=5 nearest stations, capped at a maximum radius of 1 mile (1.6 km)**

Rationale:
- In dense areas (Midtown Manhattan, Zone 1 London), there may be 20+ stations within 0.5 miles. Showing 5 gives useful choice without overwhelming the user.
- In sparse areas (outer boroughs, Zone 3+), there may be no stations within 1 mile. The radius cap prevents showing stations that are unreasonably far.
- The user could optionally adjust K via a slider: `st.slider("Show nearest stations", 3, 15, 5)`

**Algorithm:**

1. Load the station directory for the detected city (from `mart_station_directory.sql`, cached with `@st.cache_data`)
2. Compute Haversine distance from user coordinates to every station
3. Sort by distance ascending
4. Return the top K stations where distance <= max_radius
5. If fewer than K stations within radius, return all that qualify

For NYC with approximately 2,000 stations, computing 2,000 Haversine distances takes under 1ms in Python. No spatial index is needed.

### 4.3 DuckDB Spatial Alternative

DuckDB supports spatial extensions (`INSTALL spatial; LOAD spatial;`). This would allow computing distances in SQL:

```sql
SELECT station_name, station_id, latitude, longitude,
       ST_Distance_Sphere(
           ST_Point(longitude, latitude),
           ST_Point(-73.975, 40.752)
       ) / 1609.34 as distance_miles
FROM mart_station_directory
WHERE location = 'nyc'
ORDER BY distance_miles
LIMIT 5
```

This is a viable alternative if the station directory is already loaded into DuckDB. However, for the dashboard running on Streamlit Cloud with Parquet files (not a persistent DuckDB instance), the Python approach is simpler.

---

## 5. Personalized Recommendations

### 5.1 Data Inputs for Recommendations

The recommendation engine combines four data layers:

| Data Layer | Source | Phase Dependency | Latency |
|---|---|---|---|
| Nearby stations + distance | Station directory + Haversine | Phase 06 | Instant (cached) |
| Current weather at user location | Open-Meteo API (live) | Phase 03 | ~200ms API call |
| Historical weather-ride patterns per station | `mart_station_weather_performance` | Phase 06 | Instant (cached Parquet) |
| Live bike/dock availability | GBFS feeds (NYC/London) | Phase 07 | ~500ms API call |

### 5.2 Recommendation Logic

For each of the K nearest stations, compute a composite recommendation:

**Step 1: Distance score**
- < 0.1 mi: "Right next to you"
- 0.1-0.25 mi: "A short walk (X min)"
- 0.25-0.5 mi: "A moderate walk (X min)"
- 0.5-1.0 mi: "A longer walk (X min)"
- Walking time estimate: distance_miles / 0.05 (assuming 3 mph walking speed = 0.05 mi/min)

**Step 2: Weather resilience score (from Phase 06 mart)**
- Query `mart_station_weather_performance` for each station
- Under current weather conditions (temperature bucket, precipitation level), what is this station's historical ride volume relative to its average?
- Example: "This station retains 85% of normal ridership in rainy conditions" (resilient) vs. "This station drops to 40% in rain" (weather-sensitive)

**Step 3: Current conditions context (from Phase 03/04)**
- Pull current temperature, precipitation, wind from Open-Meteo for the user's coordinates
- Map to the recommendation engine's condition categories (from Phase 04)
- Generate text: "Current conditions: 55F, light rain -- historically a moderate biking day"

**Step 4: Live availability (from Phase 07, if implemented)**
- Query GBFS `station_status` for each nearby station
- Display bikes available, docks available, e-bikes available
- Flag stations with 0 bikes or 0 docks

**Step 5: Composite recommendation output**

For each station, generate a card-style recommendation:

```
[Station Name] -- [Distance] ([Walk Time])
Bikes: 8 available | Docks: 12 available | E-bikes: 3
Weather resilience: High (retains 85% ridership in current conditions)
Recommendation: Good station choice for current weather
```

### 5.3 Recommendation Ranking

Stations should be ranked by a weighted composite score, not just distance:

```python
score = (w_distance * distance_score) + (w_weather * weather_resilience) + (w_availability * availability_score)
```

Default weights: `w_distance=0.5, w_weather=0.3, w_availability=0.2`

If Phase 07 (live availability) is not yet built, redistribute: `w_distance=0.6, w_weather=0.4`

The user could toggle between "Sort by: Distance | Best Overall | Most Bikes Available".

---

## 6. Zone-Based Weather

### 6.1 Current Architecture (Phase 01)

Phase 01 uses a single weather reference point per city:
- NYC: Central Park (40.7128, -74.0060)
- London: City of London (51.5074, -0.1278)

### 6.2 User-Location Weather

Open-Meteo's Forecast API accepts any lat/lng pair and returns weather for that exact location. When the user provides their coordinates for the "Near Me" feature, we can fetch weather at their specific location rather than using the city-center reference point.

**API call:**
```
https://api.open-meteo.com/v1/forecast?latitude={user_lat}&longitude={user_lng}&current=temperature_2m,precipitation,wind_speed_10m,weather_code
```

### 6.3 When Does This Matter?

For most use cases, weather within a single city is uniform enough that the city-center reference point suffices. Zone-based weather matters in edge cases:

- **Localized rain cells:** A thunderstorm over Brooklyn may not affect Manhattan. However, Open-Meteo's spatial resolution is typically 1-11 km, so adjacent neighborhoods may report the same weather anyway.
- **Coastal effects:** Stations near the ocean (Coney Island, Canary Wharf) may experience different wind/temperature than inland stations.
- **Elevation differences:** Minimal in NYC and London (both essentially flat).

**Recommendation:** Fetch weather at the user's location as a "nice to have" since the API call is free and fast. Display it alongside the city-center weather to show the user whether conditions differ. Do not over-engineer zone-based station-weather analytics in the marts -- the historical correlation data from Phase 06 uses city-center weather, and that is sufficient.

### 6.4 Display Strategy

Show two weather readings when they differ:

```
Weather at your location: 52F, light rain
City reference (Central Park): 54F, cloudy

Note: Conditions at your location differ slightly from the city reference used for historical patterns.
```

When conditions match (most of the time), show only one reading.

---

## 7. Map Visualization

### 7.1 Library Selection

**Recommendation: Folium via `streamlit-folium` for the "Near Me" feature**

Rationale:
- Folium provides interactive Leaflet.js maps with click handling, popups, marker clustering, and layer controls
- `streamlit-folium`'s `st_folium()` returns click coordinates, enabling the "map click" location input method
- Folium supports custom marker icons, colored markers, and circle markers natively
- No API key required (uses OpenStreetMap tiles by default)
- `pydeck` (already in `requirements.txt` at v0.9.1) is better for large-scale data visualization (thousands of points, 3D layers) but lacks the bidirectional click-to-coordinate capability that `st_folium` provides

Note: `pydeck` is already in requirements.txt. For city-wide station overview maps (Phase 06's station directory visualization), `pydeck` with its `ScatterplotLayer` is ideal. For the interactive "Near Me" feature where the user clicks and selects stations, Folium is the better choice.

**New dependency:** `streamlit-folium>=0.18.0`

### 7.2 Map Design

The map should show:

1. **User location marker** -- Blue pin or crosshair icon at the user's coordinates
2. **Nearby station markers** -- Colored by recommendation score:
   - Green: Strong recommendation (high availability, weather-resilient, close)
   - Yellow: Moderate recommendation
   - Red: Weak recommendation (low availability, weather-sensitive, or far)
3. **Distance circles** -- Optional concentric circles at 0.25 mi and 0.5 mi from user location
4. **Station popups** -- Click a station marker to see full details (name, distance, bikes, docks, weather resilience)
5. **Auto-zoom** -- Map should auto-zoom to contain the user marker and all K nearest stations

```python
import folium
from streamlit_folium import st_folium

m = folium.Map(location=[user_lat, user_lng], zoom_start=15)

# User location
folium.Marker(
    [user_lat, user_lng],
    popup="Your location",
    icon=folium.Icon(color="blue", icon="user", prefix="fa")
).add_to(m)

# Station markers
for station in nearby_stations:
    color = "green" if station.score > 0.7 else "orange" if station.score > 0.4 else "red"
    folium.Marker(
        [station.lat, station.lng],
        popup=f"{station.name}<br>Distance: {station.distance:.2f} mi<br>Bikes: {station.bikes}",
        icon=folium.Icon(color=color, icon="bicycle", prefix="fa")
    ).add_to(m)

# Distance circle
folium.Circle(
    [user_lat, user_lng],
    radius=402,  # 0.25 miles in meters
    color="blue", fill=False, opacity=0.3
).add_to(m)

st_folium(m, width=700, height=500)
```

### 7.3 Mobile-Friendly Considerations

Since Streamlit Cloud serves to mobile browsers:

- Use `width="100%"` or responsive width for the map
- Station cards below the map should stack vertically on narrow screens (Streamlit handles this natively with `st.columns` on mobile)
- Browser geolocation is especially useful on mobile (GPS accuracy is high)
- Touch-friendly popup sizes -- ensure popup text is readable on small screens
- Consider `st.expander` for detailed station info to save vertical space

---

## 8. Privacy Considerations

### 8.1 Browser Geolocation

- The `streamlit-geolocation` component triggers the browser's standard permission dialog
- The user must explicitly click "Allow" before any location data is shared
- If the user denies permission, fall back to text input gracefully
- No location data is stored server-side -- it exists only in the Streamlit session state for the duration of the session

### 8.2 Address Input

- Text addresses are sent to Nominatim for geocoding
- Nominatim's usage policy requires a User-Agent string identifying the application but does not log queries for public use
- The address is NOT stored in any database, S3, or log file
- After geocoding, only the lat/lng coordinates are retained in session state
- The text input should be cleared on page refresh

### 8.3 Data Sent to External APIs

| API | Data Sent | Privacy Impact |
|---|---|---|
| Nominatim (geocoding) | User's text address | Low -- public API, no login, no tracking |
| Open-Meteo (weather) | User's lat/lng | Low -- public API, no login, no tracking |
| GBFS feeds (availability) | None (we fetch station data, not send user data) | None |
| TfL BikePoint API | None (we fetch station data) | None |

### 8.4 Recommendation

Add a brief privacy notice on the "Near Me" page:

```
"Your location is used only to find nearby stations and fetch local weather. 
It is not stored or shared beyond this session."
```

---

## 9. Integration with Other Phases

### 9.1 Phase 01 (Weather Data Pipeline)

- Open-Meteo API client created in Phase 01 (`extraction/weather.py`) can be reused for fetching weather at the user's location
- The same API response schema applies; no new parsing is needed

### 9.2 Phase 03 (Real-time Weather Dashboard Layer)

- Phase 03 creates `dashboard/weather_service.py` with a `fetch_current_weather(lat, lng)` function
- The "Near Me" feature calls this same function but with the user's coordinates instead of the city-center reference point
- Phase 03's auto-refresh logic (15-minute cadence) can also refresh the "Near Me" weather if the user has the page open

### 9.3 Phase 04 (Recommendation Engine)

- Phase 04 creates `dashboard/recommendation_engine.py` with condition classification (ideal/good/fair/poor) and insight generation
- The "Near Me" feature reuses the same classifier but applies it per-station using station-level weather resilience data
- The text generation patterns from Phase 04 (e.g., "Historically, X% of rides happen in these conditions") apply directly

### 9.4 Phase 06 (Station-Level Weather Analysis)

- Phase 06 creates `mart_station_weather_performance.sql` -- the critical data source for station-level weather resilience scores
- Phase 06 creates `mart_station_directory.sql` -- the station coordinate master table that the "Near Me" feature queries
- **These two marts are hard prerequisites for the "Near Me" feature**

### 9.5 Phase 07 (Live Station Availability)

- Phase 07 designs the GBFS integration for real-time bike/dock counts
- If Phase 07 is implemented before "Near Me," the availability data enriches the recommendation
- If Phase 07 is NOT yet implemented, the "Near Me" feature works without it (omitting the availability layer from recommendations)
- The feature should be designed to gracefully degrade: check if live data is available, include it if so, skip it if not

---

## 10. New Files and Dependencies

### 10.1 New Files

| File | Purpose |
|---|---|
| `extraction/london_stations.py` | One-time + periodic fetch of London station coordinates from TfL BikePoint API |
| `dbt_city_cycles/seeds/london_station_coordinates.csv` | Seed file with London station ID, name, lat, lng (generated by extraction script) |
| `dashboard/near_me.py` | "Near Me" feature module: geocoding, proximity calculation, recommendation cards, map rendering |

### 10.2 New Dependencies

| Package | Version | Purpose |
|---|---|---|
| `geopy` | >=2.4.0 | Nominatim geocoding |
| `streamlit-folium` | >=0.18.0 | Interactive map with click handling |
| `streamlit-geolocation` | >=1.0.0 | Browser geolocation access |

### 10.3 Modified Files

| File | Change |
|---|---|
| `requirements.txt` | Add geopy, streamlit-folium, streamlit-geolocation |
| `dashboard/app.py` | Add "Near Me" page to sidebar navigation |
| `streamlit_data_manager/parquet_file_manager.py` | Add `mart_station_directory.parquet` and `mart_station_weather_performance.parquet` to MARTS list |
| `dbt_city_cycles/seeds/schema.yml` | Add schema for london_station_coordinates seed |

---

## 11. Implementation Estimate and Phasing

### Prerequisites (Must Be Complete)
- Phase 01: Weather data pipeline (for Open-Meteo API client)
- Phase 06: Station directory mart + station weather performance mart
- London station coordinates extraction (new work)

### Sub-Phase Breakdown

| Sub-Phase | Description | Effort | Dependencies |
|---|---|---|---|
| A | London station coordinate extraction (`extraction/london_stations.py`) + dbt seed | 0.5 day | None |
| B | Station directory mart enhancement (add London coords via seed join) | 0.5 day | Sub-Phase A, Phase 06 |
| C | Core "Near Me" module: geocoding, proximity, city detection | 1 day | Sub-Phase B |
| D | Recommendation card generation (combining distance, weather, availability) | 1 day | Sub-Phase C, Phases 03, 04 |
| E | Interactive map with Folium (user marker, station markers, popups) | 1 day | Sub-Phase C |
| F | Dashboard integration (new page, sidebar nav, session state) | 0.5 day | Sub-Phases D, E |
| G | Testing and polish (edge cases, mobile, error handling) | 0.5 day | Sub-Phase F |

**Total estimated effort: 5 days**

### Suggested Implementation Order

1. Sub-Phase A (London coords) -- can be done anytime, even before other phases
2. Sub-Phases B, C in sequence (data foundation, then core logic)
3. Sub-Phases D, E in parallel (recommendation text and map visualization are independent)
4. Sub-Phase F (integration)
5. Sub-Phase G (testing)

---

## 12. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| TfL BikePoint API changes or becomes unavailable | Low | High (no London coords) | Cache coordinates locally as dbt seed; refresh manually if API breaks |
| Nominatim rate limiting or downtime | Low | Medium (geocoding fails) | Fall back to browser geolocation; cache recent geocode results in session state |
| London station ID mismatch between ride data and TfL API | Medium | Medium (some stations unmapped) | Use fuzzy name matching as fallback; log unmatched stations for manual review |
| streamlit-folium compatibility issues with Streamlit Cloud | Low | Medium (map doesn't render) | Fall back to pydeck (already in requirements.txt) or st.map for basic display |
| User outside NYC/London | Certain | Low | Clear messaging: "Available for NYC and London only" |
| No stations within radius | Medium | Low | Expand radius progressively (0.5 -> 1.0 -> 2.0 miles) with messaging |

---

## 13. Future Enhancements (Beyond Initial Implementation)

- **Walking directions:** Integrate OSRM (free, open-source routing) to show actual walking routes on the map, not just straight-line distance
- **Favorite stations:** Allow users to bookmark stations for quick access
- **Historical comparison:** "You are near Station X. Last Tuesday at this time, it had Y bikes available."
- **Multi-modal transit:** Show nearby subway/tube stations alongside bike stations
- **Zone-based weather analytics:** Once the "Near Me" feature establishes user coordinates as a pattern, extend the weather marts to support zone-level historical analysis
- **Push notifications:** If the system is ever extended to native mobile, notify users when conditions improve at their saved stations

---

## Sources

- [Nominatim - OpenStreetMap Wiki](https://wiki.openstreetmap.org/wiki/Nominatim)
- [Nominatim Usage Policy](https://operations.osmfoundation.org/policies/nominatim/)
- [Open-Meteo Geocoding API](https://open-meteo.com/en/docs/geocoding-api)
- [Open-Meteo Weather Forecast API](https://open-meteo.com/en/docs)
- [TfL BikePoint API](https://api.tfl.gov.uk/BikePoint)
- [TfL BikePoint Documentation](https://docs.api.tfl.lu/v1/en/RESTAPIs/BikePoint/)
- [streamlit-geolocation on PyPI](https://pypi.org/project/streamlit-geolocation/)
- [streamlit-folium on GitHub](https://github.com/randyzwitch/streamlit-folium)
- [CitiBike GBFS System Data](https://citibikenyc.com/system-data)
- [haversine on PyPI](https://pypi.org/project/haversine/)
- [GBFS Specification Reference](https://gbfs.org/documentation/reference/)
- [General Bikeshare Feed Specification - TfL Tech Forum](https://techforum.tfl.gov.uk/t/general-bikeshare-feed-specification-gbfs/1829)

---
