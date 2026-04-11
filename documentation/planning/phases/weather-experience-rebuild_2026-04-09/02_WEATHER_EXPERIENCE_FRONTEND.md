# Phase 02: Weather Experience Frontend — Implementation Plan

**Created:** 2026-04-09
**Status:** IN PROGRESS (Phase 2B)
**Started:** 2026-04-10 (Phase 2A) / 2026-04-11 (Phase 2B)
**Completed:** 2026-04-10 (Phase 2A)
**Depends on:** Phase 01 (API Layer) — COMPLETE
**Enables:** Phase 03 (Data Viz), Phase 04 (Comparison), Phase 05 (Analytics)
**Split:** Phase 2A (core + CSS effects + data sections) → Phase 2B (Canvas particles)

---

## Architecture

- **Framework:** Next.js 14+ (App Router) with TypeScript
- **Styling:** Tailwind CSS + CSS custom properties for weather theming
- **Animation:** Canvas for particle systems (rain, snow, lightning); CSS for gradients, fog, clouds, sun
- **Charts:** Recharts (lightweight, React-native, ~300KB vs Plotly's 3.5MB)
- **State:** Zustand (1KB, no provider wrapping, SSR-compatible)
- **Data Fetching:** SWR with 5-minute revalidation

---

## Landing Page Design

The landing page IS the product. It feels like a weather app backed by data, not a dashboard.

### Section 1: Full-Viewport Weather Scene (100vh)

Layers:
- **Base:** Time-of-day sky gradient (CSS custom properties, transitions on city switch)
- **Middle:** Weather effect overlay (Canvas particles or CSS overlays)
- **Foreground:** City silhouette SVG anchored to viewport bottom (~15-20% height)
- **Overlay text** (centered, ~35% from top):
  - Temperature: 5rem, font-weight 200, white with text shadow
  - Weather description: 1.2rem, uppercase tracking, 85% white
  - City name: 0.9rem, 60% white

The Canvas runs a continuous particle loop. The sky gradient shifts at time-period boundaries. The silhouette is distinct per city (Liberty skyline vs Parliament skyline).

### Section 2: City Toggle (floating, fixed position)

Pill-shaped toggle at top-center, z-index above weather scene. Switching triggers:
1. Crossfade on sky gradient (CSS transition, 800ms ease)
2. Canvas clears and reinitializes with new city's weather
3. Silhouette SVG crossfades
4. All data below re-fetches via SWR (city key changes)

### Section 3: Biking Score (overlaid on weather scene, ~60% vertical)

Large circular gauge, 0-100. Color: green (80+), amber (60-79), orange (40-59), red (<40). Score animates on load (counter tween from 0, 600ms). Label: "Excellent" / "Good" / "Fair" / "Poor". Glow matches score color.

### Section 4: "Days Like Today" Card (first content below fold)

Frosted-glass card (backdrop-filter blur, semi-transparent dark background). Content from `/api/similar-day/{city}`:
> "On mild April weekdays, NYC averages 12,400 rides — 23% below typical. Peak activity: 5-7 PM."

Severity color tints the card accent. Fallback: "Historical comparison data is being prepared."

### Section 5: Hourly Ride Pattern Chart

Recharts AreaChart:
- "Today's Expected Pattern" (filled area, primary color)
- "Overall Average" (dashed line, muted)
- Current hour: vertical reference line with dot
- Dark transparent background

### Section 6: Riding Insights Cards

3-5 cards, responsive grid (1 col mobile, 2-3 desktop). Each:
- Left border accent by severity (3px)
- Background: severity color at 10-15% opacity with backdrop blur
- Icon: Lucide icons (not emoji) — checkmark, info, alert triangle, alert octagon
- Ordered by severity (warnings first)

### Section 7: 24-Hour Forecast Strip

Horizontal row of hour-cells: weather icon, temperature, precipitation indicator. Desktop: all 24 visible. Mobile: horizontal scroll with snap points.

---

## Weather Animations

### Canvas Particle System

Single `WeatherCanvas` component wrapping `<canvas>` sized to viewport. Manages animation via `requestAnimationFrame`. On weatherCode change: particles fade out (500ms), new particles fade in.

| Condition | Codes | Implementation |
|-----------|-------|---------------|
| **Clear/Sunny** | 0-1 | CSS: warm radial gradient (sun) at upper-right, pulsing opacity 0.7-1.0 on 6s cycle. Shimmer overlay with mix-blend-mode. |
| **Partly Cloudy** | 2 | CSS: 2-3 cloud layers as semi-transparent gradients, drifting at 20s/35s/50s speeds, opacity 0.08-0.15 |
| **Cloudy** | 3 | CSS: heavy cloud gradient, slow 40s drift. Muted/desaturated sky. |
| **Fog** | 45, 48 | CSS: 3-4 horizontal fog bands, alternating drift directions, 10-15% white-gray opacity |
| **Rain** | 51-67, 80-82 | Canvas: 60-100 particles (varies by intensity). 2px wide, gradient to rgba(174,194,224,0.6), speed 600-1200px/s, length 8-20px. Darker sky gradient. |
| **Heavy Rain** | 65, 67, 82 | Canvas: 120-150 particles, faster (800-1500px/s), longer (15-25px). Darker sky. |
| **Snow** | 71-77, 85-86 | Canvas: 50-80 particles. Slow fall (30-80px/s), sinusoidal horizontal drift, filled white circles with blur, opacity 0.6-0.9. Cold blue-white palette. |
| **Thunderstorm** | 95-99 | Heavy rain + lightning. Lightning: branching jagged path from random top position, white with glow, 100ms hold, 200ms afterglow. Subtle screen shake (2px translate, 150ms). |

---

## Time-of-Day Theming

Sky gradient controlled by CSS custom properties based on city's local time:

| Period | Hours | Gradient |
|--------|-------|----------|
| Night | 22-05 | `#0a0e27` → `#1a1a3e` → `#0d1117` |
| Dawn | 05-07 | `#1a1a3e` → `#4a2040` → `#c97035` → `#e8a765` |
| Morning | 07-10 | `#1a2940` → `#2d4a6e` → `#3d6080` |
| Day | 10-16 | `#1a3050` → `#264a70` → `#2d5580` → `#1a3050` |
| Golden | 16-19 | `#4a3520` → `#6b4a15` → `#4a3020` → `#2a2020` |
| Dusk | 19-22 | `#2a1535` → `#3d2050` → `#1e2840` → `#0d1117` |

Weather modifies base: rain darkens by mixing `#0a0e1a` at 30%, snow shifts blue-white, fog desaturates.

---

## Color Token System

```css
--color-positive: #2ECC71;
--color-neutral: #5DADE2;
--color-caution: #F39C12;
--color-warning: #E74C3C;
--color-text-primary: rgba(255, 255, 255, 0.9);
--color-text-secondary: rgba(255, 255, 255, 0.7);
--color-text-muted: rgba(255, 255, 255, 0.5);
--color-surface: rgba(30, 35, 45, 0.7);
--color-surface-border: rgba(255, 255, 255, 0.08);
```

Consolidated from 5 existing Streamlit files into one source.

---

## Navigation

- `/` — Landing page (weather experience, default)
- `/analytics` — Ride Analytics (lazy loaded)
- `/weather` — Weather Deep Dive (lazy loaded)
- `/compare` — City Comparison (lazy loaded)

Minimal floating nav bar. Landing page: nav hidden until scroll past weather scene (fade in). Secondary pages: nav always visible.

---

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Particles** | Canvas | Current Streamlit creates 40 individual DOM elements. Canvas = single GPU layer, dynamic counts, effects impossible in CSS. |
| **City silhouettes** | SVG | Crisp at any width, CSS-tintable, animatable. Raster would pixelate on HiDPI. |
| **Charts** | Recharts | Only chart on landing is a simple area (24 points). Plotly is 3.5MB. Recharts is 300KB, React-native. |
| **State** | Zustand | Two pieces of shared state (city + weather cache). 1KB, no provider, SSR-safe. Redux is overkill, Context causes unnecessary rerenders. |

---

## Project Structure

```
frontend/
  src/
    app/
      layout.tsx                     # Root layout: providers, fonts, global CSS
      page.tsx                       # Landing page (weather experience)
      analytics/page.tsx             # Ride Analytics (Phase 05)
      weather/page.tsx               # Weather Deep Dive (Phase 05)
      compare/page.tsx               # City Comparison (Phase 04)
    components/
      weather/
        WeatherScene.tsx             # Full-viewport container
        WeatherCanvas.tsx            # Canvas particle system
        SkyGradient.tsx              # Time-of-day gradient
        CityToggle.tsx               # NYC/London toggle pill
        CitySilhouette.tsx           # SVG skyline
        WeatherHero.tsx              # Temp, description, city overlay
        BikingScore.tsx              # Animated score gauge
        FogOverlay.tsx               # CSS fog effect
        CloudOverlay.tsx             # CSS cloud drift
        SunOverlay.tsx               # CSS sun glow
      insights/
        SimilarDayCard.tsx           # "Days Like Today" bridge card
        HourlyPatternChart.tsx       # Recharts area chart
        InsightCards.tsx             # Recommendation cards grid
        ForecastStrip.tsx            # 24-hour forecast
      layout/
        NavBar.tsx                   # Floating minimal nav
        PageShell.tsx                # Dark-themed wrapper
    hooks/
      useWeather.ts                  # SWR: /api/weather/{city}
      useForecast.ts                 # SWR: /api/weather/{city}/forecast
      useInsights.ts                 # SWR: /api/insights/{city}
      useSimilarDay.ts              # SWR: /api/similar-day/{city}
      useTimePeriod.ts              # Time period from city timezone
      useScrollPosition.ts          # Scroll tracking for nav
    lib/
      api.ts                         # API base URL, fetch wrapper
      types.ts                       # TypeScript interfaces
      weather-codes.ts              # WMO code mappings
      colors.ts                      # Score/severity color helpers
      particles.ts                   # Rain, Snow, Lightning classes
      time-of-day.ts                # Port of time_of_day.py
    styles/
      globals.css                    # Tailwind, CSS custom properties
      weather-effects.css           # @keyframes: fog-drift, cloud-drift, sun-pulse
  public/
    assets/
      nyc-skyline.svg
      london-skyline.svg
  next.config.ts
  tailwind.config.ts
  tsconfig.json
  package.json
```

**Total: ~41 files** (20 components, 6 hooks, 5 lib modules, 2 style files, 2 SVG assets, config files)

---

## Challenge Round Decisions (2026-04-10)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Phase split** | Split 2A/2B | 2A = core + CSS effects + data sections. 2B = Canvas particles. Ships value faster, unblocks Phase 03. |
| **Chart library** | Recharts | Phase 05 needs many charts; 300KB justified. Avoids throwaway inline SVG. |
| **State management** | Zustand | 1KB, SSR-safe, scales to Phase 04 dual-city and Phase 05 filters. |
| **City silhouettes** | Simple geometric SVG | Quick to code, CSS-tintable, replaceable later with detailed versions. |
| **Placeholder pages** | Defer | No dead routes. Phases 04/05 bring their own pages and nav items. |

### Phase 2A Scope
Core structure, CSS weather effects (sky gradients, fog, clouds, sun), all data sections, Zustand + SWR, TypeScript interfaces, responsive layout. ~35 files.

### Phase 2B Scope (IN PROGRESS)
Canvas particle system (`WeatherCanvas.tsx`, `particles.ts`), rain/snow/lightning effects, thunderstorm screen shake, heavy rain variant.

#### Phase 2B Challenge Round Decisions (2026-04-11)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Lightning** | Full branching algorithm + glow + screen shake | More cinematic; ~80 LOC justified for the hero weather effect |
| **Drizzle** | Light rain config (fewer, shorter, slower) | Reuses rain particle class with lighter params; no new abstraction |
| **Canvas layer** | Absolute behind content, pointer-events:none | Simplest integration; particles behind text for readability |
| **Mobile perf** | Halve particle counts on viewport < 768px | Meaningful perf gain on low-end devices with minimal code |

---

## Verification (Phase 2A)

1. **Weather scene renders:** Sky gradient matches current time in the selected city
2. **CSS weather effects:** Fog bands, cloud drift, sun glow display for appropriate weather categories
3. **City toggle transitions:** Smooth gradient transition (800ms), data refetches, silhouette crossfades
4. **Data sections load:** Similar day card, hourly chart, insight cards, forecast strip all render
5. **Responsive:** Mobile (375px) — weather scene fills viewport, cards stack, forecast scrolls horizontally
6. **Performance:** Lighthouse 90+, no layout shift
7. **Error states:** API down → loading skeleton → non-destructive error, sky gradient still renders
8. **TypeScript:** `npx tsc --noEmit` passes with no errors
