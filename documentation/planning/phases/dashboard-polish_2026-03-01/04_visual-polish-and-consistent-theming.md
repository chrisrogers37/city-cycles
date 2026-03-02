# Phase 04: Visual Polish and Consistent Theming

**Status:** COMPLETE
**Started:** 2026-03-01
**Completed:** 2026-03-01
**PR:** #51
**Impact:** Medium | **Effort:** Medium | **Risk:** Low
**Files modified:** `dashboard/components/recommendation_cards.py`, `dashboard/components/chart_factory.py`, `dashboard/pages/ride_analytics.py`

## Context

Several visual inconsistencies remain after the previous phases fix errors, labels, and descriptions:

1. **Insight cards** — The neutral/info severity cards appear as plain gray text on dark backgrounds, looking like placeholders rather than intentional content. The positive card (green) and biking score card look polished by comparison.
2. **Station Growth chart** — Uses a flat `#5DADE2` blue that doesn't match the atmospheric gradient feel of other charts. The bar chart should use the atmospheric color palette for visual consistency.
3. **Chart color consistency** — The hourly bar chart also uses the same flat `#5DADE2`. While the atmospheric template defines an 8-color palette (`ATMOSPHERIC_COLORS`), single-series bar charts bypass it by hardcoding `color_discrete_sequence`.

## Detailed Implementation Plan

### Step 1: Improve neutral/info insight card styling

**File:** `dashboard/components/recommendation_cards.py`

The neutral cards currently use `rgba(255, 255, 255, 0.08)` — nearly invisible on the dark background. Give them a slightly more visible appearance with a subtle blue tint that feels intentional rather than empty.

**Before (lines 13-17):**
```python
_SEVERITY_BG_COLOR = {
    Severity.POSITIVE: "rgba(46, 204, 113, 0.15)",
    Severity.NEUTRAL: "rgba(255, 255, 255, 0.08)",
    Severity.CAUTION: "rgba(243, 156, 18, 0.15)",
    Severity.WARNING: "rgba(231, 76, 60, 0.15)",
}
```

**After:**
```python
_SEVERITY_BG_COLOR = {
    Severity.POSITIVE: "rgba(46, 204, 113, 0.15)",
    Severity.NEUTRAL: "rgba(93, 173, 226, 0.10)",
    Severity.CAUTION: "rgba(243, 156, 18, 0.15)",
    Severity.WARNING: "rgba(231, 76, 60, 0.15)",
}
```

The new neutral color `rgba(93, 173, 226, 0.10)` uses the atmospheric sky blue (`#5DADE2`) at low opacity. This gives the card a subtle blue tint that reads as "informational" rather than "empty."

Also add a left border accent to make cards feel more structured:

**Before (lines 20-25):**
```python
CARD_STYLE = (
    "padding:0.75em 1em; border-radius:10px; "
    "margin-bottom:0.5em; font-size:0.9em; "
    "border: 1px solid rgba(255,255,255,0.08); "
    "backdrop-filter: blur(8px);"
)
```

**After:**
```python
CARD_STYLE = (
    "padding:0.75em 1em; border-radius:10px; "
    "margin-bottom:0.5em; font-size:0.9em; "
    "border: 1px solid rgba(255,255,255,0.1); "
    "backdrop-filter: blur(8px);"
)
```

Update the card rendering to add a colored left border for each severity:

**Before (lines 41-48):**
```python
    for rec in result.recommendations:
        emoji = _SEVERITY_EMOJI[rec.severity]
        bg = _SEVERITY_BG_COLOR[rec.severity]
        st.markdown(
            f"<div style='{CARD_STYLE} background:{bg};'>"
            f"{emoji} {rec.text}</div>",
            unsafe_allow_html=True,
        )
```

**After:**
```python
    for rec in result.recommendations:
        emoji = _SEVERITY_EMOJI[rec.severity]
        bg = _SEVERITY_BG_COLOR[rec.severity]
        border_color = _SEVERITY_BORDER_COLOR[rec.severity]
        st.markdown(
            f"<div style='{CARD_STYLE} background:{bg}; "
            f"border-left: 3px solid {border_color};'>"
            f"{emoji} {rec.text}</div>",
            unsafe_allow_html=True,
        )
```

Note: `_SEVERITY_BORDER_COLOR` is defined at module level (alongside `_SEVERITY_BG_COLOR` and `_SEVERITY_EMOJI`) rather than inside the function, following the existing convention:

```python
_SEVERITY_BORDER_COLOR = {
    Severity.POSITIVE: "#2ECC71",
    Severity.NEUTRAL: "#5DADE2",
    Severity.CAUTION: "#F39C12",
    Severity.WARNING: "#E74C3C",
}
```

This adds a 3px colored left border that matches the severity — green for positive, blue for neutral/info, amber for caution, red for warning. The left border creates visual hierarchy and makes even neutral cards look intentional.

### Step 2: Use atmospheric gradient on Station Growth bars

**File:** `dashboard/components/chart_factory.py`, `station_growth_chart` function

Instead of flat `#5DADE2` for all bars, use a sequential color scale from the atmospheric palette that creates a subtle gradient effect across years.

**Before (lines 57-62):**
```python
def station_growth_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create a station count bar chart."""
    fig = px.bar(df, x='year', y='metric_value', title=title,
                 template='atmospheric',
                 labels={'metric_value': 'Station Count', 'year': 'Year'},
                 color_discrete_sequence=['#5DADE2'])
    return fig
```

**After:**
```python
def station_growth_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create a station count bar chart."""
    fig = px.bar(df, x='year', y='metric_value', title=title,
                 template='atmospheric',
                 labels={'metric_value': 'Station Count', 'year': 'Year'},
                 color='metric_value',
                 color_continuous_scale=[[0, '#2C3E50'], [0.5, '#3498DB'], [1, '#5DADE2']])
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    return fig
```

This creates a dark-to-light blue gradient based on station count, making the growth visually apparent while staying within the atmospheric color family. The color scale is hidden since the Y-axis already shows the values.

### Step 3: Apply consistent bar color to hourly chart

**File:** `dashboard/components/chart_factory.py`, `hourly_bar_chart` function

Apply the same gradient treatment to the hourly bar chart for consistency.

**Before (lines 28-34):**
```python
def hourly_bar_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create an hourly distribution bar chart."""
    fig = px.bar(df, x='hour_of_day', y='ride_count', title=title,
                 template='atmospheric',
                 labels={'ride_count': 'Number of Rides', 'hour_of_day': 'Hour of Day'},
                 color_discrete_sequence=['#5DADE2'])
    fig.update_layout(bargap=0.15)
    return fig
```

**After:**
```python
def hourly_bar_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create an hourly distribution bar chart."""
    fig = px.bar(df, x='hour_of_day', y='ride_count', title=title,
                 template='atmospheric',
                 labels={'ride_count': 'Number of Rides', 'hour_of_day': 'Hour of Day'},
                 color='ride_count',
                 color_continuous_scale=[[0, '#2C3E50'], [0.5, '#3498DB'], [1, '#5DADE2']])
    fig.update_layout(bargap=0.15, showlegend=False, coloraxis_showscale=False)
    return fig
```

This makes peak hours visually pop (brighter blue) while off-peak hours recede (darker), creating an intuitive visual encoding of activity level.

## Test Plan

1. **Insight cards:** Navigate to Dashboard landing page, verify:
   - Neutral/info cards have a subtle blue tint (not invisible gray)
   - All cards have a colored left border matching their severity
   - Positive card has green left border, neutral has blue, etc.
2. **Station Growth chart:** Navigate to Ride Analytics, scroll to Station Growth:
   - Bars show a dark-to-light blue gradient based on station count
   - No floating color scale bar visible
3. **Hourly bar chart:** If data available, verify gradient treatment
4. **No regressions:** `venv/bin/python -m pytest tests/ -v`
5. **Recommendation engine tests:** `venv/bin/python -m pytest tests/test_recommendation_engine.py tests/test_similar_day_insights.py -v`

## Verification Checklist

- [ ] Neutral insight cards are visibly distinct from the background
- [ ] All insight cards have colored left borders
- [ ] Station Growth bars use gradient coloring
- [ ] No color scale legend visible on bar charts
- [ ] Hourly bar chart (if available) uses gradient
- [ ] All existing tests pass
- [ ] Dashboard visual consistency improved across pages

## What NOT To Do

- Do NOT change the biking score card styling — it already looks polished
- Do NOT change the atmospheric template itself — changes should be at the chart/component level
- Do NOT add animations or transitions — keep it simple
- Do NOT change multi-series charts (monthly trends, comparison) — the colorway already works well for those
- Do NOT change the Weather Deep Dive charts — they already use proper labels and the atmospheric template correctly
