# City Cycles: Project Roadmap & Enhancement Plan

**Created:** January 27, 2026
**Last Updated:** February 17, 2026
**Status:** Active Planning Document

---

## Executive Summary

City Cycles is a well-architected data engineering portfolio project demonstrating end-to-end analytics pipeline capabilities. The codebase shows solid engineering fundamentals with modular design, schema validation, incremental processing, and production deployment patterns.

**Key Strengths:**
- Clean separation of concerns across modules
- Idempotent operations with file-based tracking
- Incremental dbt strategy (37% runtime savings)
- Comprehensive documentation
- Full orchestration with single entry point

**Areas for Enhancement:**
- Cost optimization for portfolio-appropriate AWS spend
- Dashboard feature depth
- Test coverage expansion
- Monitoring and alerting capabilities

This roadmap prioritizes **cost reduction** and **portfolio polish** while maintaining production-grade quality.

---

## Part 1: Current State Assessment

### 1.1 Architecture Maturity

| Component | Maturity | Notes |
|-----------|----------|-------|
| Orchestration | ★★★★★ | CLI with 6 stages incl. weather, Railway deployment |
| Data Extraction | ★★★★★ | NYC S3 + London scraping + Open-Meteo weather API |
| Schema Validation | ★★★★★ | Clean pydantic models, registry pattern, weather dataclass |
| File Processing | ★★★★☆ | Memory management, chunking, S3 integration |
| Database Layer | ★★★★☆ | DuckDB with S3, 5 raw tables incl. weather |
| dbt Transformations | ★★★★★ | 10 marts incl. weather correlation and station analysis |
| Dashboard | ★★★★★ | Multi-page atmospheric UI, weather integration, recommendations |
| Testing | ★★★★★ | 283 tests passing, covers all modules incl. weather + dashboard |
| Monitoring | ★★☆☆☆ | Basic logging only, no alerting |

### 1.2 Cost Analysis (Current State)

**Monthly EC2 Costs (t3.xlarge running full time):**
- Instance: $0.1664/hr × 730 hrs = ~$121/month
- EBS Storage (100GB gp3): ~$8/month
- **Total if always running: ~$129/month**

**Actual Pipeline Usage:**
- Monthly run: ~1 hour (incremental)
- Quarterly full refresh: ~6 hours
- Effective monthly EC2 time needed: ~2-3 hours

**S3 Storage Costs:**
- Current data volume: ~50-100GB
- Storage: ~$2-3/month
- Data transfer: ~$1-2/month

**Current Problem:** EC2 instance likely running 24/7 for ~2 hours of actual monthly work.

---

## Part 2: Cost Optimization Strategy

### 2.1 Immediate Savings (Portfolio-Friendly Approach)

#### Option A: Stop EC2 When Not in Use (Recommended)
```bash
# Stop instance after pipeline completes
aws ec2 stop-instances --instance-ids i-xxxxxxxxx

# Start instance for monthly run
aws ec2 start-instances --instance-ids i-xxxxxxxxx
```

**Estimated Savings:** $121 → ~$0.50/month (only pay when running)

#### Option B: Use Smaller Instance
- Switch from t3.xlarge ($0.1664/hr) to t3.medium ($0.0416/hr)
- Still adequate for monthly incremental runs
- **Savings:** 75% reduction when running

#### Option C: Spot Instances for Non-Critical Runs
- Spot instances are 60-90% cheaper
- Acceptable for portfolio project (can retry if interrupted)
- **Savings:** Additional 70% off on-demand pricing

### 2.2 Recommended Cost-Optimized Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  PORTFOLIO-GRADE ARCHITECTURE               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ┌──────────────┐      ┌──────────────┐                   │
│   │ Local Dev    │      │ Streamlit    │                   │
│   │ (Your Mac)   │      │ Cloud (FREE) │                   │
│   └──────┬───────┘      └──────────────┘                   │
│          │                     ↑                            │
│          │                     │ Reads Parquet              │
│          ▼                     │                            │
│   ┌──────────────────────────────────────────────┐         │
│   │                    AWS S3                     │         │
│   │  - Raw data (ZIPs, CSVs, Parquet)           │         │
│   │  - Mart exports (for dashboard)              │         │
│   │  - Cost: ~$3-5/month                        │         │
│   └──────────────────────────────────────────────┘         │
│                                                             │
│   EC2: STOPPED (only start for monthly pipeline)           │
│   Estimated monthly cost: $5-10                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Local Development Strategy

For a portfolio project, consider running the pipeline locally:

**Benefits:**
- Zero EC2 costs
- Faster iteration during development
- Same codebase works locally and on EC2

**Local Requirements:**
- DuckDB: Works natively on macOS/Linux
- AWS CLI: Configured with credentials
- Python environment: Same requirements.txt
- Playwright: For London extraction

**Workflow:**
1. Develop and test locally
2. Push changes to GitHub
3. Run pipeline locally for updates
4. Mart exports go to S3 automatically
5. Streamlit Cloud reads from S3

### 2.4 Cost Comparison Summary

| Architecture | Monthly Cost | Use Case |
|--------------|--------------|----------|
| EC2 Always On (current) | ~$130 | Not recommended |
| EC2 On-Demand (stopped when idle) | ~$1-5 | Light usage |
| EC2 Spot + Stopped | ~$0.50-2 | Budget conscious |
| Local Development Only | ~$3-5 (S3 only) | **Portfolio recommended** |
| GitHub Actions + S3 | ~$5-10 | Automated but simple |

---

## Part 3: Production-Grade Improvements

### 3.1 High Priority Enhancements

#### A. Automated Instance Management

Create a simple Lambda function to start/stop EC2:

```python
# lambda_function.py
import boto3

def handler(event, context):
    ec2 = boto3.client('ec2')
    action = event.get('action', 'start')
    instance_id = 'i-xxxxxxxxx'

    if action == 'start':
        ec2.start_instances(InstanceIds=[instance_id])
    elif action == 'stop':
        ec2.stop_instances(InstanceIds=[instance_id])
```

Schedule with EventBridge:
- Start: 1st of month at 1:55 AM
- Stop: 1st of month at 3:00 AM (after pipeline completes)

#### B. Pipeline Failure Notifications

Add SNS notifications for failures:

```python
# In orchestrator/main.py
import boto3

def notify_failure(stage: str, error: str):
    sns = boto3.client('sns')
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:ACCOUNT:city-cycles-alerts',
        Subject=f'City Cycles Pipeline Failed: {stage}',
        Message=f'Stage {stage} failed with error:\n{error}'
    )
```

#### C. Health Check Endpoint

Add a simple status API:

```python
# status_api.py (optional FastAPI)
from fastapi import FastAPI
import duckdb

app = FastAPI()

@app.get("/health")
def health():
    try:
        conn = duckdb.connect('data/city_cycles.duckdb', read_only=True)
        result = conn.execute("SELECT MAX(date) FROM unified_rides").fetchone()
        return {"status": "healthy", "latest_data": str(result[0])}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

### 3.2 Medium Priority Enhancements

#### A. Data Quality Framework

Implement dbt tests:

```yaml
# dbt_city_cycles/models/staging/schema.yml
version: 2

models:
  - name: stg_nyc_modern
    columns:
      - name: ride_id
        tests:
          - unique
          - not_null
      - name: started_at
        tests:
          - not_null
      - name: ended_at
        tests:
          - not_null
```

Add Great Expectations for advanced validation:

```python
# expectations/unified_rides.py
import great_expectations as gx

context = gx.get_context()

expectation_suite = context.add_expectation_suite("unified_rides")

expectation_suite.add_expectation(
    expectation_type="expect_column_values_to_not_be_null",
    column="ride_id"
)

expectation_suite.add_expectation(
    expectation_type="expect_column_values_to_be_between",
    column="duration_minutes",
    min_value=0,
    max_value=1440  # Max 24 hours
)
```

#### B. Incremental Raw Table Loading

Current: Raw tables fully reload each time
Proposed: Track processed parquet files

```python
# db_duckdb/operations.py enhancement
def load_raw_table_incremental(self, table_name: str, s3_prefix: str):
    """Load only new parquet files not already in the table."""

    # Get already processed files
    existing = self.conn.execute(f"""
        SELECT DISTINCT source_file FROM {table_name}
    """).fetchall()
    existing_files = {row[0] for row in existing}

    # Get all available files
    available_files = self.s3_client.list_files(s3_prefix)

    # Load only new files
    new_files = [f for f in available_files if f not in existing_files]

    for file_path in new_files:
        self.conn.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM read_parquet('{file_path}')
        """)
```

**Impact:** Reduce database load stage from 30 minutes to ~2 minutes for monthly runs.

#### C. Parallel Extraction

Run NYC and London extraction concurrently:

```python
# orchestrator/main.py enhancement
import concurrent.futures

def run_extraction(self):
    """Run extraction with parallel NYC and London downloads."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        nyc_future = executor.submit(self.extract_nyc)
        london_future = executor.submit(self.extract_london)

        # Wait for both to complete
        nyc_result = nyc_future.result()
        london_result = london_future.result()

    return nyc_result and london_result
```

**Impact:** Reduce extraction from 25-30 minutes to ~15-20 minutes.

### 3.3 Low Priority Enhancements

#### A. Metrics Collection

Add Prometheus/CloudWatch metrics:

```python
# orchestrator/metrics.py
import time
from dataclasses import dataclass

@dataclass
class PipelineMetrics:
    stage: str
    duration_seconds: float
    rows_processed: int
    success: bool

    def to_cloudwatch(self):
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='CityBikes',
            MetricData=[
                {
                    'MetricName': f'{self.stage}_duration',
                    'Value': self.duration_seconds,
                    'Unit': 'Seconds'
                }
            ]
        )
```

#### B. Configuration Management

Move to YAML-based configuration:

```yaml
# config/pipeline.yml
pipeline:
  name: city-cycles
  version: 1.0.0

extraction:
  nyc:
    enabled: true
    start_year: 2019
  london:
    enabled: true

dbt:
  target: prod
  full_refresh_schedule: quarterly

notifications:
  sns_topic: arn:aws:sns:us-east-1:ACCOUNT:alerts
  email: christophertrogers37@gmail.com
```

---

## Part 4: Dashboard Enhancement Roadmap

Based on `docs/dashboard-review-and-enhancements.md`, prioritized for portfolio impact:

### 4.1 ✅ Completed Dashboard Enhancements (v2.0.0)

| Feature | Status | Notes |
|---------|--------|-------|
| Weather Correlation Overlay | ✅ COMPLETED | Full weather-ride correlation marts and dashboard page |
| Geospatial Station Map | ✅ COMPLETED | OpenStreetMap scatter map colored by weather impact |
| Automated Insights | ✅ COMPLETED | Recommendation engine with biking score and natural language |
| Multi-page Architecture | ✅ COMPLETED | 4 pages: landing, ride analytics, weather, comparison |
| Atmospheric UI/Dark Theme | ✅ COMPLETED | Time-of-day styling, weather animations |
| Real-time Weather | ✅ COMPLETED | Open-Meteo API with 15-min auto-refresh |

### 4.2 Remaining Dashboard Opportunities

1. **Day-of-Week / Hour x Day Heatmap** — Temporal analysis visualizations
2. **YoY Growth Metrics** — Year-over-year trending
3. **Download Data Button** — CSV export for users
4. **Enhanced Filtering** — Day type, season, member type filters

### 4.3 Dashboard Technical Improvements

```python
# Add to dashboard/app.py

# 1. Better caching
@st.cache_data(ttl=3600)  # 1 hour cache
def load_metrics():
    return duckdb.query("SELECT * FROM mart_daily_metrics").df()

# 2. Add download button
def add_download(df, filename):
    csv = df.to_csv(index=False)
    st.download_button(
        "Download CSV",
        csv,
        filename,
        "text/csv"
    )

# 3. Add map visualization (PyDeck)
import pydeck as pdk

def render_station_map(station_df):
    layer = pdk.Layer(
        'ScatterplotLayer',
        data=station_df,
        get_position='[lon, lat]',
        get_radius='rides / 100',
        get_fill_color='[200, 30, 0, 160]',
        pickable=True
    )
    return pdk.Deck(layers=[layer])
```

---

## Part 5: Testing & Quality Roadmap

### 5.1 Current Test Coverage (283 tests, 3 skips — as of Feb 2026)

| Module | Coverage | Status |
|--------|----------|--------|
| data_models | Good | Integration tests present |
| extraction | Moderate | Weather extraction tests (11 tests) |
| extracted_file_manager | Moderate | Has current tests |
| db_duckdb | Basic | CLI + operations tests |
| orchestrator | None | Needs unit tests |
| dashboard | ★★★★★ | 52 tests: theme, CSS, components, chart factory |
| weather_service | Good | 30 tests: WMO codes, API, fetch, coordinates |
| recommendation_engine | Good | 50 tests: classifiers, scoring, insights |

### 5.2 Testing Roadmap

**Phase 1: Unit Test Coverage (Priority)**

```python
# tests/test_orchestrator.py
import pytest
from unittest.mock import Mock, patch
from orchestrator.main import CityBikesOrchestrator

@pytest.fixture
def orchestrator():
    with patch.dict('os.environ', {'S3_BUCKET': 'test-bucket'}):
        return CityBikesOrchestrator()

def test_stage_isolation(orchestrator):
    """Each stage should be runnable independently."""
    with patch.object(orchestrator, 'run_extraction') as mock:
        mock.return_value = True
        result = orchestrator.run_stage('extraction')
        assert result == True
        mock.assert_called_once()

def test_dbt_full_refresh_flag(orchestrator):
    """Full refresh flag should propagate to dbt."""
    with patch.object(orchestrator, '_run_dbt') as mock:
        orchestrator.run_stage('dbt', full_refresh=True)
        mock.assert_called_with(full_refresh=True)
```

**Phase 2: Integration Tests**

```python
# tests/integration/test_pipeline.py
import pytest
import duckdb

@pytest.mark.integration
def test_end_to_end_pipeline():
    """Test complete pipeline with sample data."""
    # 1. Load sample parquet
    # 2. Run dbt transformations
    # 3. Verify mart outputs
    pass
```

**Phase 3: Data Quality Tests**

```yaml
# dbt_city_cycles/models/schema.yml
models:
  - name: unified_rides
    tests:
      - dbt_utils.recency:
          datepart: day
          field: date
          interval: 45  # Alert if no data in 45 days
    columns:
      - name: duration_minutes
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1440
```

---

## Part 6: Implementation Timeline

### Phase 1: Cost Optimization ✅ COMPLETED

- [x] Migrated from EC2 ($121/mo) to Railway (<$5/mo)
- [x] Ephemeral Docker architecture (no always-on compute)
- [x] Local development workflow documented

### Phase 2: Dashboard Enhancements ✅ COMPLETED (v2.0.0)

- [x] Station map visualization (OpenStreetMap scatter map)
- [x] Automated insights (recommendation engine with biking score)
- [x] Multi-page atmospheric UI with weather animations
- [x] Real-time weather integration with forecasts
- [ ] Enhanced filtering (day type, season, member type)

### Phase 3: Pipeline Improvements (Partially Complete)

- [x] Weather data pipeline (Open-Meteo API integration)
- [ ] Implement incremental raw table loading
- [ ] Add parallel extraction
- [ ] Set up failure notifications
- [ ] Add pipeline metrics

### Phase 4: Quality & Testing ✅ LARGELY COMPLETED

- [x] Expanded test coverage to 283 tests (from ~30)
- [x] Weather, dashboard, recommendation engine fully tested
- [ ] Add dbt data quality tests
- [ ] Set up CI/CD pipeline

### Phase 5: Documentation & Polish (In Progress)

- [x] Update all READMEs with current state (Feb 2026 audit)
- [ ] Create architecture diagrams
- [ ] Record demo video
- [ ] Prepare portfolio presentation

---

## Part 7: Success Metrics

### Technical Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Monthly AWS Cost | ~$130 | ✅ <$5 (Railway + S3) |
| Monthly Pipeline Runtime | ~68 min | <30 min |
| Test Coverage | ~30% | ✅ 283 tests passing |
| Dashboard Load Time | ~3s | <1s |
| Data Freshness | Monthly | Weekly capable |

### Portfolio Metrics

| Aspect | Current | Target |
|--------|---------|--------|
| Visual Appeal | Good | ✅ Excellent (atmospheric UI, animations) |
| Feature Depth | Basic | ✅ Advanced (weather, recommendations, maps) |
| Documentation | Good | Comprehensive |
| Demo-ability | Manual | Automated |
| Uniqueness | Multi-city comparison | ✅ + Weather analytics & recommendations |

---

## Part 8: Risk Assessment

### Low Risk
- Cost optimization changes
- Dashboard enhancements
- Documentation updates

### Medium Risk
- Incremental raw table loading (test thoroughly)
- Parallel extraction (handle failures gracefully)
- Testing infrastructure changes

### High Risk (Avoid for now)
- Major schema changes
- Database migration
- Real-time data integration

---

## Appendix A: Quick Reference Commands

### Cost Management
```bash
# Stop EC2 instance
aws ec2 stop-instances --instance-ids i-xxxxxxxxx

# Start EC2 instance
aws ec2 start-instances --instance-ids i-xxxxxxxxx

# Check instance status
aws ec2 describe-instance-status --instance-ids i-xxxxxxxxx

# Check monthly costs
aws ce get-cost-and-usage --time-period Start=2026-01-01,End=2026-01-31
```

### Local Development
```bash
# Full pipeline locally
python -m orchestrator.cli run

# Skip extraction (use existing S3 data)
python -m orchestrator.cli run --skip-extraction

# Test dbt only
python -m orchestrator.cli stage dbt

# Run tests
python -m pytest tests/ -v
```

### Dashboard Development
```bash
# Run dashboard locally
streamlit run dashboard/app.py

# Test with sample data
python -m pytest tests/test_dashboard.py -v
```

---

## Appendix B: Architecture Decision Records

### ADR-001: Local-First Development

**Context:** EC2 costs are high for a portfolio project.

**Decision:** Adopt local-first development with S3 as the only always-on cloud resource.

**Consequences:**
- Significant cost reduction
- Faster iteration cycles
- Same codebase works everywhere
- May need to document EC2 setup for "production" story

### ADR-002: Dashboard as Portfolio Focus

**Context:** Dashboard is the most visible component to employers.

**Decision:** Prioritize dashboard enhancements over pipeline optimizations.

**Consequences:**
- Better portfolio presentation
- May delay some infrastructure improvements
- Geospatial features add significant visual impact

### ADR-003: Incremental Over Full Refresh

**Context:** Full pipeline takes 6+ hours on full refresh.

**Decision:** Maintain incremental strategy, only full-refresh quarterly.

**Consequences:**
- 95% cost reduction
- 37% faster routine runs
- Slight complexity in debugging data issues

---

**End of Roadmap Document**

_This is a living document. Update as priorities change and features are completed._
