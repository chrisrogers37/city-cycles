# Phase 04: City Comparison Mode — Implementation Plan

**Created:** 2026-04-09
**Status:** Planning
**Depends on:** Phase 02 (Weather Frontend), Phase 03 (Days Like Today Viz)

---

## Overview

Side-by-side view showing how NYC and London respond to their current (potentially different) weather conditions. Narrative: **"It's raining in London and sunny in NYC — here's how each city responds."**

Same API endpoints as Phase 03, called for both cities simultaneously.

---

## Layout: Tabbed with Comparison Cards (Recommended)

Compact dual-weather header at top, comparison data panel below, shared chart.

**Why not split-screen:** Data differences matter more than visual weather differences. Single column works on mobile. Structured layout communicates comparisons more effectively.

---

## Components

### 1. Dual Weather Header

Side-by-side current conditions:

```
+---------------------------+---------------------------+
|     NYC                   |     London                |
|     22°C  Clear           |     12°C  Rain            |
|     Biking Score: 88      |     Biking Score: 42      |
|     [green dot]           |     [orange dot]          |
+---------------------------+---------------------------+
```

Data: `/api/weather/nyc` + `/api/weather/london` + `/api/insights/{city}` for scores.

### 2. Comparison Stats Table

Structured side-by-side metrics:

| Metric | NYC | London |
|--------|-----|--------|
| Avg daily rides | 14,200 | 18,400 |
| vs typical | +12% | -28% |
| Avg duration | 14.8 min | 16.2 min |
| Duration vs typical | +2% | -12% |
| Peak hours | 5-7 PM | 5-6 PM |
| Member/Casual split | 68/32% | 75/25% |
| Historical sample | 52 days | 23 days |
| Current conditions | mild/dry | mild/rain |

Color-coded: green for positive, red for negative. "Winner" per row highlighted.

### 3. Dual Hourly Chart

Four lines on one chart:
- NYC "days like today": solid `#5DADE2` (sky blue), width 2.5
- London "days like today": solid `#E74C3C` (warm red), width 2.5
- NYC overall: dashed `rgba(93,173,226,0.35)`, width 1
- London overall: dashed `rgba(231,76,60,0.35)`, width 1

Two vertical "Now" markers (one per city's local time — they're in different timezones).

**Y-axis normalization:** Default absolute counts + toggle to "% of daily total" for shape comparison. NYC may be 10x London in absolute volume — the normalized view enables direct pattern comparison.

### 4. "Same Weather, Different Cities" Insight

**Trigger:** Both cities' temperature_band AND precipitation_intensity match (or within one band).

**Same weather example:**
> "Both cities are enjoying mild, dry conditions. NYC sees 14,200 rides while London sees 18,400 — London's denser bike network handles 30% more rides under the same weather."

**Different weather example:**
> "London is dealing with rain (score: 42) while NYC enjoys clear skies (score: 88) — a tale of two cities."

---

## Data Flow

```
User navigates to /compare
  → Fetch weather for BOTH cities (parallel)
  → Classify conditions for both
  → Map to mart dimensions for both
  → Look up similar-day stats for both (parallel)
  → Look up hourly patterns for both (parallel)
  → Get biking scores for both
  → Render dual header, insight, stats table, chart
```

**Timezone edge case:** At certain hours, NYC might be "weekday" while London is "weekend" (or vice versa). Similar-day lookup uses each city's local day_type independently.

---

## Page Integration

Integrate into the `/compare` route as the primary section, with historical comparison charts below:

```
[Page Title: "NYC vs London"]
[── LIVE WEATHER COMPARISON ──]
[Dual Weather Header]
[Same Weather / Different Cities Insight]
[Comparison Stats Table]
[Dual Hourly Chart]
[── divider ──]
[── HISTORICAL COMPARISON (Phase 05) ──]
[Monthly Trends]
[Hourly Patterns]
[Station Growth]
```

---

## File List

| File | Purpose | New/Modify |
|------|---------|------------|
| `components/compare/DualWeatherHeader.tsx` | Side-by-side weather display | NEW |
| `components/compare/ComparisonStatsTable.tsx` | Structured comparison table | NEW |
| `components/compare/DualHourlyChart.tsx` | Four-line hourly chart + Y-axis toggle | NEW |
| `components/compare/CrossCityInsight.tsx` | Weather match/contrast narrative | NEW |
| `app/compare/page.tsx` | Comparison page composition | NEW |
| `hooks/useDualCity.ts` | Parallel SWR for both cities | NEW |

---

## Verification

**Different weather (common):** NYC 24°C clear (score 92), London 11°C rain (score 38). NYC green, London red. Chart shows NYC above baseline, London below. Insight highlights contrast.

**Same weather (occasional):** Both mild/dry. "Same weather" insight triggers. Normalized chart view reveals cultural/infrastructure differences.

**One city missing data:** Missing city shows "N/A". Chart shows only available city's lines.

**Timezone edge:** Friday evening NYC, Saturday morning London — verify day_type computed per-city.

---

## Implementation Sequence

1. Parameterize day_type detection by timezone
2. DualWeatherHeader (simplest, two weather summaries)
3. ComparisonStatsTable (HTML table with conditional styling)
4. CrossCityInsight (text generation with weather-match detection)
5. DualHourlyChart (most complex: four lines, two markers, Y-axis toggle)
6. Wire into /compare page
7. Test end-to-end with real weather for both cities
