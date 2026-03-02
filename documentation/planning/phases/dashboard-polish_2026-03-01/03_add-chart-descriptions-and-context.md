# Phase 03: Add Chart Descriptions and Context

**Status:** COMPLETE
**Started:** 2026-03-01
**Completed:** 2026-03-01
**PR:** #50
**Impact:** High | **Effort:** Medium | **Risk:** Low
**Depends on:** Phase 01 (charts need to render without errors first)
**Files modified:** `dashboard/pages/ride_analytics.py`, `dashboard/pages/landing.py`, `dashboard/components/forecast_strip.py`

## Context

Charts across the dashboard lack explanatory context. A first-time visitor sees data but doesn't understand what it means or what to look for. The user explicitly asked for "better context on what charts mean."

This phase adds concise `st.caption()` descriptions under each chart section heading, and polishes the forecast chart with proper axis labels and a legend so users can interpret it.

## Detailed Implementation Plan

### Step 1: Add descriptions to Ride Analytics charts

**File:** `dashboard/pages/ride_analytics.py`

Add `st.caption()` lines after each `st.subheader()`. These provide one-line context about what the chart shows and what to look for.

**After line 110** (`st.subheader("Rides by Month (Overlayed by Year)")`), add:
```python
    st.caption("Monthly ridership overlayed by year to reveal seasonal patterns.")
```

**After line 131** (`st.subheader("Average Trip Duration by Month (Overlayed by Year)")`), add:
```python
    st.caption("Average trip length by month, overlayed by year.")
```

**After line 153** (`st.subheader("Time of Day Analysis")`), add:
```python
    st.caption("Ride distribution across hours of the day.")
```

**After line 173** (`st.subheader("Member Percentage Trend")`), add:
```python
        st.caption("Proportion of rides by annual members vs casual riders over time.")
```

Note: this caption is inside the `if location == 'nyc':` block, indented one level deeper.

**After line 186** (`st.subheader("Station Growth")`), add:
```python
    st.caption("Number of active bike share stations by year.")
```

**After line 202** (`st.subheader("Station Weather Performance")`), add:
```python
    st.caption("How individual stations perform during adverse weather.")
```

### Step 2: Polish the forecast chart

**File:** `dashboard/components/forecast_strip.py`

The forecast chart currently has no legend (`showlegend=False`), no Y-axis label, and users can't tell what the bars vs line represent.

**Before (lines 44-54):**
```python
    fig.update_layout(
        height=200,
        margin=dict(l=40, r=20, t=10, b=30),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='rgba(255,255,255,0.7)',
        showlegend=False,
        yaxis=dict(title='', gridcolor='rgba(255,255,255,0.05)'),
        yaxis2=dict(overlaying='y', side='right', showgrid=False, showticklabels=False),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
    )
```

**After:**
```python
    fig.update_layout(
        height=220,
        margin=dict(l=40, r=40, t=10, b=30),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='rgba(255,255,255,0.7)',
        showlegend=True,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='rgba(255,255,255,0.6)'),
        ),
        yaxis=dict(title='Temp (\u00b0C)', gridcolor='rgba(255,255,255,0.05)'),
        yaxis2=dict(
            title='Precip (mm)',
            overlaying='y', side='right', showgrid=False,
        ),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
    )
```

Changes:
- `showlegend=True` with horizontal legend above the chart
- Left Y-axis labeled "Temp (C)"
- Right Y-axis labeled "Precip (mm)" and made visible
- Height increased from 200 to 220 to accommodate legend
- Right margin increased to 40 for the right Y-axis label

### Step 3: Add forecast section context

**File:** `dashboard/pages/landing.py`, line 74

**Before:**
```python
    st.subheader("Next 24 Hours")
```

**After:**
```python
    st.subheader("Next 24 Hours")
    st.caption("Temperature forecast (line) with expected precipitation (bars)")
```

## Test Plan

1. **Visual verification per page:**
   - Ride Analytics: verify each chart section has a gray caption below its heading
   - Landing page: verify forecast chart has visible legend with "Temp (C)" and "Precip (mm)" entries
   - Landing page: verify forecast caption text appears below "Next 24 Hours" heading
2. **Caption text quality:** Read each caption aloud — it should make sense to someone who has never seen the dashboard before
3. **No regressions:** `venv/bin/python -m pytest tests/ -v`

## Verification Checklist

- [ ] Each Ride Analytics chart section has a descriptive caption
- [ ] Forecast chart shows horizontal legend above the chart
- [ ] Forecast chart left Y-axis reads "Temp (C)"
- [ ] Forecast chart right Y-axis reads "Precip (mm)"
- [ ] Forecast chart height accommodates legend without clipping
- [ ] Landing page shows forecast context caption
- [ ] All existing tests pass

## What NOT To Do

- Do NOT add lengthy multi-sentence descriptions — captions should be one line, ~10-15 words
- Do NOT change chart data or queries — only add display context
- Do NOT add tooltips or info icons — `st.caption()` is the right pattern for this codebase
- Do NOT add captions to the Weather Deep Dive page (it already has good `st.caption()` usage, e.g., line 46 and 116)
