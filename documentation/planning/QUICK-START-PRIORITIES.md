# Quick Start: Top Priority Implementations

**For:** Immediate action items to improve the project
**Time Required:** ~1-2 days total

---

## Priority 1: Stop EC2 (5 minutes)

This single action saves ~$120/month.

```bash
# Check current instance status
aws ec2 describe-instance-status --instance-ids i-xxxxxxxxx

# Stop the instance
aws ec2 stop-instances --instance-ids i-xxxxxxxxx

# Verify stopped
aws ec2 describe-instance-status --instance-ids i-xxxxxxxxx
```

**Note:** EBS volume persists (no data loss). Start anytime with:
```bash
aws ec2 start-instances --instance-ids i-xxxxxxxxx
```

---

## Priority 2: Set Up Billing Alerts (10 minutes)

Never get surprised by AWS costs again.

```bash
# Create billing alert for $10
aws cloudwatch put-metric-alarm \
  --alarm-name "Monthly-Cost-10" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=Currency,Value=USD \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:us-east-1:ACCOUNT:billing-alerts

# Create alert for $25
aws cloudwatch put-metric-alarm \
  --alarm-name "Monthly-Cost-25" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --threshold 25 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=Currency,Value=USD \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:us-east-1:ACCOUNT:billing-alerts
```

---

## Priority 3: Test Local Development (30 minutes)

Verify you can run the pipeline locally.

```bash
# 1. Ensure you have AWS credentials configured
aws sts get-caller-identity

# 2. Verify S3 access
aws s3 ls s3://city-cycles-data-ctr37/

# 3. Test orchestrator locally (skip extraction to save time)
cd /path/to/city-cycles
python -m orchestrator.cli run --skip-extraction

# 4. Verify dbt models
cd dbt_city_cycles
dbt run --select stg_nyc_modern
dbt test
```

---

## Priority 4: Dashboard Quick Wins (2-4 hours)

Add high-impact features to the dashboard.

### 4a. Add YoY Growth to Metrics

Edit `dashboard/app.py`:

```python
# Add this function
def calculate_yoy_growth(df, date_col, metric_col, current_start, current_end):
    """Calculate year-over-year growth percentage."""
    current = df[(df[date_col] >= current_start) & (df[date_col] <= current_end)][metric_col].sum()

    # Previous year same period
    prev_start = current_start - pd.DateOffset(years=1)
    prev_end = current_end - pd.DateOffset(years=1)
    previous = df[(df[date_col] >= prev_start) & (df[date_col] <= prev_end)][metric_col].sum()

    if previous > 0:
        return ((current - previous) / previous) * 100
    return None

# Use in metrics display
yoy = calculate_yoy_growth(df, 'date', 'total_rides', start_date, end_date)
st.metric("Total Rides", f"{total_rides:,.0f}", delta=f"{yoy:+.1f}% YoY" if yoy else None)
```

### 4b. Add Day-of-Week Chart

```python
# Add after existing charts
st.subheader("Rides by Day of Week")

dow_df = df.groupby(df['date'].dt.dayofweek)['total_rides'].mean().reset_index()
dow_df['day_name'] = dow_df['date'].map({
    0: 'Monday', 1: 'Tuesday', 2: 'Wednesday',
    3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'
})

fig = px.bar(dow_df, x='day_name', y='total_rides', title='Average Rides by Day of Week')
st.plotly_chart(fig, use_container_width=True)
```

### 4c. Add Download Button

```python
# Add at bottom of each page
st.download_button(
    "Download Data (CSV)",
    df.to_csv(index=False),
    f"city_cycles_{selected_city}.csv",
    "text/csv"
)
```

---

## Priority 5: Update CHANGELOG (5 minutes)

Document changes you've made:

```markdown
## [Unreleased]

### Improved
- **Cost Optimization** - Documented strategies to reduce AWS spend from $130/month to <$10/month
- **Documentation Accuracy** - Fixed file references in CLAUDE.md to match actual codebase

### Added
- **Planning Documents** - Created docs/planning/ with roadmap and cost optimization guides
```

---

## Verification Checklist

After completing priorities:

- [ ] EC2 instance is stopped (check AWS console)
- [ ] Billing alerts configured (check CloudWatch alarms)
- [ ] Pipeline runs successfully locally
- [ ] Dashboard changes deployed to Streamlit Cloud
- [ ] CHANGELOG.md updated

---

## Next Steps

Once these are complete, refer to:
- `docs/planning/ROADMAP.md` for full enhancement roadmap
- `docs/planning/COST-OPTIMIZATION.md` for detailed cost strategies
- `docs/dashboard-review-and-enhancements.md` for dashboard features

---

**Estimated Immediate Savings: $120/month**
**Time Investment: ~2-4 hours**
**Portfolio Impact: High**
