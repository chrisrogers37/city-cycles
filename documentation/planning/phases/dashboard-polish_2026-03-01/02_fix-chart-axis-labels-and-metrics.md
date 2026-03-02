# Phase 02: Fix Chart Axis Labels and Metric Formatting

**Status:** PENDING
**Impact:** High | **Effort:** Low | **Risk:** Low
**Files modified:** `dashboard/pages/ride_analytics.py`, `dashboard/components/chart_factory.py`

## Context

Multiple charts display raw DataFrame column names as axis labels:
- Member Percentage chart: Y-axis shows `member_percentage`, X-axis shows `month`
- Station Growth chart: Y-axis shows `metric_value`, X-axis shows `year`
- Hourly bar chart: no axis labels at all (just `hour_of_day`, `ride_count`)

Additionally, the "Average Daily Rides" metric shows a misleading decimal ("85,111.7") for what should be a whole-number count.

These are small code changes with outsized visual improvement — every chart becomes immediately more readable.

## Detailed Implementation Plan

### Step 1: Fix Member Percentage chart axis labels

**File:** `dashboard/pages/ride_analytics.py`, lines 167-168

**Before:**
```python
            fig = px.line(member_df, x='month', y='member_percentage',
                          title="NYC Member Percentage Over Time", template='atmospheric')
```

**After:**
```python
            fig = px.line(member_df, x='month', y='member_percentage',
                          title="NYC Member Percentage Over Time", template='atmospheric',
                          labels={'member_percentage': 'Member %', 'month': 'Date'})
```

### Step 2: Fix Station Growth chart axis labels

**File:** `dashboard/components/chart_factory.py`, lines 57-62

The `station_growth_chart` function has no `labels` parameter, so axis labels default to raw column names.

**Before:**
```python
def station_growth_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create a station count bar chart."""
    fig = px.bar(df, x='year', y='metric_value', title=title,
                 template='atmospheric',
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
                 color_discrete_sequence=['#5DADE2'])
    return fig
```

### Step 3: Fix Hourly bar chart axis labels

**File:** `dashboard/components/chart_factory.py`, lines 28-34

**Before:**
```python
def hourly_bar_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create an hourly distribution bar chart."""
    fig = px.bar(df, x='hour_of_day', y='ride_count', title=title,
                 template='atmospheric',
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
                 color_discrete_sequence=['#5DADE2'])
    fig.update_layout(bargap=0.15)
    return fig
```

### Step 4: Fix Average Daily Rides decimal

**File:** `dashboard/pages/ride_analytics.py`, line 105

"85,111.7" should be "85,112" — daily ride counts should be integers.

**Before:**
```python
    col2.metric("Average Daily Rides", f"{avg_daily:,.1f}")
```

**After:**
```python
    col2.metric("Average Daily Rides", f"{avg_daily:,.0f}")
```

### Step 5: Add time period context to top metrics

**File:** `dashboard/pages/ride_analytics.py`, lines 103-106

The three metrics at the top of Ride Analytics have no indication of what time period they cover. Add the date range as context.

**Before:**
```python
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rides", f"{total_rides:,.0f}")
    col2.metric("Average Daily Rides", f"{avg_daily:,.0f}")
    col3.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")
```

**After:**
```python
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rides", f"{total_rides:,.0f}")
    col2.metric("Average Daily Rides", f"{avg_daily:,.0f}")
    col3.metric("Average Ride Duration", f"{avg_duration:.1f} minutes")
    st.caption(f"Metrics for {start_date} to {end_date}")
```

## Test Plan

1. **Visual verification:** Navigate to Ride Analytics page, verify:
   - Member Percentage chart Y-axis reads "Member %" (not `member_percentage`)
   - Member Percentage chart X-axis reads "Date" (not `month`)
   - Station Growth chart Y-axis reads "Station Count" (not `metric_value`)
   - Station Growth chart X-axis reads "Year" (not `year`)
   - Hourly bar chart (if data available) shows "Number of Rides" and "Hour of Day"
   - Average Daily Rides shows "85,112" (no decimal)
   - Time period caption appears below metrics
2. **No regressions:** `venv/bin/python -m pytest tests/ -v`

## Verification Checklist

- [ ] No raw column names visible as chart axis labels on any page
- [ ] Average Daily Rides displays as integer (no decimal)
- [ ] Time period caption visible below top metrics
- [ ] All existing tests pass
- [ ] Comparison page charts (which already have proper labels) still look correct

## What NOT To Do

- Do NOT change query column aliases — only add `labels={}` to Plotly calls
- Do NOT change the station_growth query to alias `station_count as station_count` — fix it at the display layer
- Do NOT add labels to charts that already have correct labels (comparison_line_chart, monthly_trend_chart already pass y_label)
