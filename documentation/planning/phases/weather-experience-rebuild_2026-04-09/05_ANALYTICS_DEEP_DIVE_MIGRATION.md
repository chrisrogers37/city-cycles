# Phase 05: Analytics Deep Dive Migration — Implementation Plan

**Created:** 2026-04-09
**Status:** COMPLETE (Phase 5A) | IN PROGRESS (Phase 5B)
**Started:** 2026-04-11
**5A Completed:** 2026-04-11
**5B Started:** 2026-04-12
**Depends on:** Phase 01 (API endpoints), Phase 02 (Next.js app shell)
**Priority:** Lower than landing experience; completes the product

---

## Purpose

Migrate three Streamlit analytics pages to the new React frontend. These pages carry the analytical depth and must feel connected to the landing page — weather context carries through, charts highlight current conditions.

---

## Current State

| Page | Streamlit File | Sections | Queries |
|------|---------------|----------|---------|
| Ride Analytics | `ride_analytics.py` | Key metrics, monthly trends (year overlay), duration, hourly patterns, member % (NYC), station growth, station weather perf + map | 7 queries, 5 marts |
| Weather Deep Dive | `weather_deep_dive.py` | Temp vs rides, precip impact, condition impact, hourly weather impact | 4 queries, 2 marts |
| City Comparison | `comparison.py` | Side-by-side metrics, trends, duration, station growth, weather impact | 6 queries, 3 marts |

---

## Pages to Build

### 1. Ride Analytics (`/analytics`)

**Controls:** City selector (global), date range picker with presets ("Last Year", "All Time", per-year).

**Sections:**

1. **Key Metrics** — Total Rides, Average Daily, Average Duration (3 cards)
2. **Monthly Ride Trends** — Line chart, year overlay. Toggle: "Average Daily" vs "Total". **Enhancement:** highlight current month.
3. **Duration Trends** — Same year-overlay pattern. Caption: "True average: total minutes / total rides."
4. **Time of Day** — Horizontal bars, hours 0-23. **Enhancement:** highlight current hour.
5. **Member Percentage** — NYC only, line chart. Conditional render.
6. **Station Growth** — Bar chart, year vs station count.
7. **Station Weather Performance** — Weather condition dropdown, hour range slider (**debounced 300ms**). **Enhancement: map as primary view** (both NYC and London, not just NYC). Mapbox/react-map-gl scatter: size = rides, color = RdYlGn % change. Table as toggle.

**API calls:** daily-metrics, hourly-patterns, member-analysis, station-growth, station-performance.

### 2. Weather Deep Dive (`/weather`)

**Controls:** City selector (global).

**Sections:**

1. **Temperature vs Rides** — Bar chart by temp range. **Enhancement:** highlight today's temp band.
2. **Precipitation Impact** — Bar chart by precip category. **Enhancement:** highlight today's precip level.
3. **Weather Condition Impact** — Bar chart, % change vs clear. RdYlGn color scale. **Enhancement:** highlight today's condition.
4. **Hourly Weather Impact** — Multi-line: rain, snow, fog. **Enhancement:** vertical marker at current hour.

**"Today's conditions" integration:** Read current weather from React Context (populated by landing page). No extra API call.

**API calls:** weather-correlation, weather-impact.

### 3. Station Explorer (`/stations`)

**Promoted from a subsection to its own page.** Map-first UX.

**Controls:** City selector, weather condition multi-select, hour range slider (debounced), minimum rides threshold.

**Primary: Interactive Map**
- Mapbox GL JS / react-map-gl
- Scatter markers: size = total rides, color = RdYlGn (% change vs clear)
- Hover tooltip: station name, % change, rides, duration
- Click: detail card
- Both NYC and London supported

**Secondary: Ranked Table**
- Toggle between map and table
- Sortable columns, CSV export

---

## Shared Components

| Component | Purpose | Replaces |
|-----------|---------|----------|
| CitySelector | Global context, URL sync (`?city=nyc`) | 3 separate `st.session_state` keys |
| DateRangePicker | Start/end + preset buttons, URL sync | `st.date_input` (no presets) |
| ChartContainer | Consistent wrapper: title, subtitle, loading, empty, error states | Repeated `st.subheader` + `try/except` |
| DataTable | Sortable, formatted, CSV export | `st.dataframe` + `st.expander` |
| MetricCard | Large number + label + optional delta | `st.metric` |
| ChartHighlightContext | Provides current conditions to all charts for annotations | New concept |

---

## Chart Theme

```typescript
export const chartTheme = {
  colors: {
    primary: '#5DADE2',
    secondary: '#E74C3C',
    success: '#2ECC71',
    warning: '#F39C12',
    purple: '#9B59B6',
    teal: '#1ABC9C',
  },
  colorway: ['#5DADE2', '#E74C3C', '#2ECC71', '#F39C12', '#9B59B6', '#1ABC9C', '#E67E22', '#3498DB'],
  background: 'transparent',
  grid: 'rgba(255, 255, 255, 0.06)',
  text: 'rgba(255, 255, 255, 0.8)',
};
```

Maps from existing `ATMOSPHERIC_COLORS` in `dashboard/theme/plotly_template.py`.

---

## Implementation Sequence

### Step 1: Shared infrastructure (2-3 days)
- Chart theme constants
- ChartContainer, DataTable, MetricCard, DateRangePicker
- React Query hooks for all analytics endpoints
- ChartHighlightContext

### Step 2: Ride Analytics (3-4 days)
- Page layout and controls
- Monthly trend chart (year overlay)
- Duration, hourly, member, station growth charts
- Station weather performance (map + table)
- "Today" highlight annotations

### Step 3: Weather Deep Dive (2-3 days)
- Temperature, precipitation, condition, hourly impact charts
- Current weather highlight integration

### Step 4: Station Explorer (3-4 days)
- Mapbox integration
- Station scatter layer
- Filter controls + detail panel
- Table toggle

### Step 5: Polish (2-3 days)
- Navigation transitions
- URL state sync
- Responsive layout
- Accessibility

**Total: ~12-17 days**

---

## Challenge Round Decisions (2026-04-11)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Map library** | Mapbox / react-map-gl (in 5B) | Professional vector tiles, free tier covers this project. react-map-gl is the standard React wrapper. |
| **ChartHighlightContext** | Skip — use existing hooks | `useInsights(city)` already returns classified conditions (temp band, precip, weather category). SWR deduplicates. No extra abstraction needed. |
| **City URL sync** | Zustand only | Landing and compare pages use Zustand without URL params. Keep consistent. City resets on refresh (acceptable for exploration). |
| **Phase split** | Split 5A/5B | 5A: Shared infra + Ride Analytics + Weather Deep Dive (chart pages). 5B: Station Explorer (Mapbox + filters + table toggle). Keeps PRs reviewable. |
| **CSV export** | Defer | DataTable gets sorting/formatting only. CSV export adds complexity for a feature not yet requested. |
| **DateRangePicker** | Preset buttons only | Simple button group ('2024', '2023', 'All Time'). No calendar widget. Covers year-over-year comparison use cases. |

---

## Phase 5A Scope

**Shared infra + Ride Analytics + Weather Deep Dive.** No Mapbox (deferred to 5B).

### What's IN Phase 5A:
- TypeScript interfaces for all analytics API responses
- SWR hooks for analytics endpoints
- Chart theme constants
- ChartContainer, MetricCard, DataTable (sorting only), DatePresetBar shared components
- Ride Analytics page (`/analytics`) with all 7 sections
- Weather Deep Dive page (`/weather`) with all 4 sections
- "Today" highlight annotations using existing hooks
- NavBar updates (add /analytics, /weather links)
- Responsive layout

### What's DEFERRED to Phase 5B:
- Station Explorer page (`/stations`)
- Mapbox / react-map-gl dependency
- StationMap, StationMapMarker components
- Station filter controls (weather multi-select, hour range slider, min rides threshold)
- Map/table toggle
- NavBar /stations link
- CSV export for DataTable

---

## Phase 5B Challenge Round Decisions (2026-04-12)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Map library** | Mapbox GL + react-map-gl | Free tier (50k loads/month). Dark-v11 style matches app theme. User will create Mapbox account. |
| **Filter controls** | Single-select + client filter | API accepts single weather_condition. Hour range via selects. Min rides as client-side filter. No API changes needed. |
| **CSV export** | Deferred again | No user request. Keeps scope tight. Can add later. |
| **Marker approach** | Source + Circle Layer (not individual Markers) | Data-driven styling via Mapbox expressions. Performant for hundreds of stations. |

### Phase 5B Scope:
- Station Explorer page (`/stations`) — map-first UX
- Mapbox GL scatter map with data-driven circle sizing (rides) and coloring (RdYlGn % change)
- Filter controls: weather dropdown (single-select), hour range selects, min rides input (client-side)
- Map/table toggle with existing DataTable component
- Hover popup on map with station details
- NavBar `/stations` link
- useDebounce hook (300ms)
- Graceful fallback when NEXT_PUBLIC_MAPBOX_TOKEN not set
- Stations without lat/lng appear only in table view

---

## Verification

**Per-page:** Values match Streamlit output for same city/date range. Charts render correctly. "Today" highlights annotate correct bands.

**Cross-cutting:** City persists across pages. Loading/empty/error states work. Dark theme consistent.

---

## File List — Phase 5A

```
app/analytics/page.tsx
app/weather/page.tsx
components/charts/chart-theme.ts
components/charts/MonthlyTrendChart.tsx
components/charts/HourlyBarChart.tsx
components/charts/StationGrowthChart.tsx
components/charts/TemperatureRidesChart.tsx
components/charts/PrecipitationChart.tsx
components/charts/WeatherConditionChart.tsx
components/charts/HourlyWeatherImpactChart.tsx
components/charts/MemberPercentageChart.tsx
components/charts/DurationTrendChart.tsx
components/ui/ChartContainer.tsx
components/ui/DataTable.tsx
components/ui/MetricCard.tsx
components/ui/DatePresetBar.tsx
hooks/useAnalytics.ts
```

## File List — Phase 5B

```
app/stations/page.tsx
components/maps/StationMap.tsx
hooks/useDebounce.ts
components/layout/NavBar.tsx (modified — added /stations link)
```
