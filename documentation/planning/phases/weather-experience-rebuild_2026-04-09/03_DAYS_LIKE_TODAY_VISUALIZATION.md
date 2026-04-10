# Phase 03: "Days Like Today" Visualization — Implementation Plan

**Created:** 2026-04-09
**Status:** Planning
**Depends on:** Phase 01 (API Layer), parallel with Phase 02 (Frontend)

---

## Overview

Transform the recommendation engine's analytical output into a visual data experience. The backend already classifies weather, queries `mart_similar_day_stats`, and generates insights. This phase builds the frontend components that render those results.

Core narrative: **"On days like today, here is what bike share ridership actually looks like."**

---

## Data Sources

| API Endpoint | Returns | Used By |
|-------------|---------|---------|
| `GET /api/similar-day/{city}` | Daily grain: avg_daily_rides, pct_change_vs_overall, avg_duration_minutes, duration_pct_change, peak_hour_start/end, sample_days, avg_member_rides, avg_casual_rides | Summary Card, Member Split, Duration Insight |
| `GET /api/similar-day/{city}/hourly` | 24 rows: hour_of_day, avg_daily_rides, avg_duration_minutes, avg_member_rides, avg_casual_rides + overall average | Hourly Pattern Chart |
| `GET /api/insights/{city}` | biking_score, recommendations (text, severity, metric, value), classified conditions | Insight Cards |
| `GET /api/weather/{city}` | Current conditions (for classification context and current hour marker) | Chart annotations |

**Mart data behind the API:**
- `mart_similar_day_stats` (grain='daily'): single row matching current (location, month_num, day_type, temperature_band, precipitation_intensity)
- `mart_similar_day_stats` (grain='hourly'): 24 rows for the same dimensions
- `mart_hourly_patterns_summary`: overall average baseline for comparison line

---

## Components to Build

### 1. Similar Day Summary Card

**Hero data card positioned below the weather scene.** Natural language sentence:

> "On [mild] [April] [weekday]s with [no rain], [NYC] averaged [12,400] rides — [23% below] typical"

| Sentence Token | API Field | Format |
|----------------|-----------|--------|
| `[mild]` | temperature_band | lowercase |
| `[April]` | current month | month name |
| `[weekday]s` | day_type | append 's' |
| `[no rain]` | precipitation_intensity | "no rain" for none, "light rain" for light, etc. |
| `[NYC]` | city param | display name |
| `[12,400]` | avg_daily_rides | comma-formatted |
| `[23% below]` | pct_change_vs_overall | abs value + "above"/"below" |

**Visual:**
- Background tint: green (pct >= 10), red (pct <= -10), neutral gray
- Confidence: "Based on 47 similar days" (from sample_days). "(limited data)" when < 10.
- Peak hours: "Peak activity: 5-7 PM" (from peak_hour_start/end)
- Frosted glass aesthetic matching the landing page

### 2. Hourly Ride Pattern Chart

**The signature visualization.** Two-series area chart:

- **"Days like today":** Filled area, primary color, from hourly grain of mart_similar_day_stats
- **"Overall average":** Dashed line, muted color, from mart_hourly_patterns_summary
- **Current hour marker:** Vertical dashed line at current hour with "Now" annotation
- **Peak hour annotation:** Arrow pointing to peak_hour_start
- **Area fill:** Between the two lines to emphasize the difference
- **Animated on load:** Lines draw in

X-axis: hours 0-23. Y-axis: average rides.

**Tooltip:** "8 AM: Days like today: 342 rides | Overall: 520 rides | -34%"

**API response shape (recommended for hourly endpoint):**
```json
{
  "hours": [
    {
      "hour_of_day": 0,
      "similar_day_avg_rides": 42.3,
      "overall_avg_rides": 78.1,
      "similar_day_avg_duration": 12.5,
      "member_rides": 30.1,
      "casual_rides": 12.2,
      "sample_days": 47
    }
  ],
  "classification": {
    "temperature_band": "mild",
    "precipitation_intensity": "none",
    "month_num": 4,
    "day_type": "weekday"
  }
}
```

### 3. Member vs Casual Split

Compact horizontal stacked bar:
- Member: `#3498DB` (blue) — "Members: 8,200 (66%)"
- Casual: `#E67E22` (orange) — "Casual: 4,200 (34%)"
- Data: avg_member_rides, avg_casual_rides from daily grain

### 4. Duration Insight

Compact stat with directional arrow:
- "Rides are 8% shorter on days like today" (red down-arrow)
- Subtitle: "avg 14.2 min vs typical 15.4 min"
- Threshold: abs(pct) < 3 → "Trip duration is typical" (neutral style)

### 5. Insight Cards

Existing component from `recommendation_cards.py` — the API already generates "days like today" insights via `generate_insights()` in the recommendation engine. No new logic needed, just render the API response.

---

## Page Layout

```
[City Toggle]
[Weather Hero]
[Biking Score | Temperature | Wind]         ← existing
[───── divider ─────]
[Similar Day Summary Card]                   ← NEW
[Hourly Ride Pattern Chart]                  ← NEW
[Duration Insight | Member/Casual Split]     ← NEW (2-col)
[───── divider ─────]
[Riding Insights]                            ← existing (now includes similar-day insights)
[───── divider ─────]
[Next 24 Hours]                              ← existing
```

---

## Verification Scenarios

**Clear April weekday in NYC (score ~90):**
- Card: "On mild April weekdays with no rain, NYC averaged 14,200 rides — 12% above typical" (green)
- Chart: "Days like today" above "Overall" at commute peaks
- Member/Casual: ~70/30
- Duration: "Trip duration is typical"

**Rainy April weekday in London (score ~35):**
- Card: "On mild April weekdays with moderate rain, London averaged 18,400 rides — 28% below typical" (red)
- Chart: "Days like today" well below "Overall" at all hours
- Member/Casual: ~80/20 (casuals disappear in rain)
- Duration: "Rides are 12% shorter" (red down-arrow)

**Snowy January weekday in NYC (score ~15):**
- Card: "On freezing January weekdays with heavy precipitation, NYC averaged 2,100 rides — 73% below typical" (red)
- Chart: flat, low line far below overall
- Low confidence warning if sample_days < 10

**Missing data (rare condition combo):**
- Card: "Historical comparison data for conditions like today is being built"
- Chart: only "Overall" line with note

---

## File List

| File | Purpose | New/Modify |
|------|---------|------------|
| `components/insights/SimilarDayCard.tsx` | Hero summary card | NEW |
| `components/insights/HourlyPatternChart.tsx` | Dual-line hourly chart | NEW |
| `components/insights/MemberCasualSplit.tsx` | Member/casual ratio bar | NEW |
| `components/insights/DurationInsight.tsx` | Duration comparison stat | NEW |
| `hooks/useSimilarDay.ts` | SWR hook for similar-day endpoints | NEW (or extend Phase 02) |
| `lib/format.ts` | Number/date formatting helpers | NEW |

---

## Implementation Sequence

1. Build SimilarDayCard (simplest, pure rendering)
2. Build DurationInsight (small, self-contained)
3. Build MemberCasualSplit (small chart)
4. Build HourlyPatternChart (most complex, two data sources)
5. Wire into landing page
6. Test with both cities under various weather conditions
