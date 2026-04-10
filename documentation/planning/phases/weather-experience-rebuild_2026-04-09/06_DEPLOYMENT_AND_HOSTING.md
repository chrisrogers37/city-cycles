# Phase 06: Deployment & Hosting — Implementation Plan

**Created:** 2026-04-09
**Status:** Planning
**Depends on:** Phase 01 (API), Phase 02 (Frontend) for initial deploy; all phases for full deploy
**Can begin:** Infrastructure setup in parallel with frontend development

---

## Current Deployment

| Component | Platform | Schedule |
|-----------|----------|----------|
| Streamlit dashboard | Streamlit Cloud (`city-cycles.streamlit.app`) | Always-on |
| Data pipeline | Railway (Docker, Pro plan) | Cron: `0 2 3 * *` (3rd of month) |
| Weather extraction | Railway (Docker) | Cron: `0 */6 * * *` (every 6 hours) |
| Data storage | AWS S3 (`city-cycles-data-ctr37`) | N/A |

---

## New Architecture

```
GitHub Repo
     │
     ├──── Vercel (Frontend: Next.js)
     │       └── Auto-deploy on push to main
     │
     ├──── Railway (API: FastAPI) ──── reads ──── S3 (Parquet)
     │       └── Auto-deploy on push to main        ▲
     │                                               │
     └──── Railway (Pipeline: existing) ─── exports ─┘
              └── Cron: monthly + 6-hourly weather
```

---

## Component Decisions

### Frontend: Vercel (Recommended)

- Native Next.js support (Vercel created Next.js)
- Automatic preview deployments on every PR
- Free tier: 100GB bandwidth, serverless functions
- Edge network, zero-config
- Built-in Web Vitals analytics

### API: Railway (Recommended)

- Already on Pro plan, same project as pipeline
- Shared billing, shared dashboard
- Docker deployment, auto-deploy from GitHub
- Built-in health checks and restart policies

### API Data Access: S3 Download on Startup (Recommended)

Same pattern as `streamlit_data_manager/parquet_file_manager.py`:
1. Download all 11 mart parquets from S3 to local `/data` on startup
2. Query locally with DuckDB in-memory
3. Railway's local disk persists for deployment lifetime
4. Refresh: restart API after monthly pipeline completes

**Why not query S3 directly:** Simpler, proven pattern, faster queries. Startup latency (~30-60s) is acceptable since the API runs continuously.

### Data Pipeline: No Changes

Monthly cron + 6-hourly weather extraction stay as-is on Railway.

---

## Environment Variables

### Frontend (Vercel)

| Variable | Value |
|----------|-------|
| `NEXT_PUBLIC_API_URL` | Railway API URL or custom domain |

### API (Railway — new service)

| Variable | Value | Notes |
|----------|-------|-------|
| `AWS_ACCESS_KEY_ID` | (existing) | Same as pipeline |
| `AWS_SECRET_ACCESS_KEY` | (existing) | Same as pipeline |
| `AWS_DEFAULT_REGION` | us-east-1 | |
| `S3_BUCKET` | city-cycles-data-ctr37 | |
| `CORS_ORIGIN` | Vercel frontend URL | |
| `ENVIRONMENT` | production | |

No new secrets needed — reuse existing AWS credentials.

---

## CI/CD

### GitHub Actions (new)

**`frontend-ci.yml`** — lint + type-check + test on PR (paths: `frontend/**`). Vercel handles deploy.

**`api-ci.yml`** — lint + test on PR (paths: `api/**`). Railway handles deploy.

**`data-refresh.yml`** (optional) — Cron `0 6 3 * *` (4 hours after pipeline). Triggers Railway API redeploy via webhook so it picks up fresh parquets.

### Deployment Flow

```
Feature branch push → GitHub Actions CI → Vercel preview deploy
PR merged to main → Vercel production deploy + Railway API deploy
3rd of month → Railway pipeline exports parquets → API restart picks up new data
```

---

## DNS Strategy

### Phase 1: Parallel operation (no DNS changes)

| URL | Target |
|-----|--------|
| `city-cycles.streamlit.app` | Streamlit (keep running) |
| `city-cycles.vercel.app` | Vercel frontend (auto-generated) |
| `<service>.up.railway.app` | Railway API (auto-generated) |

### Phase 2: Custom domain (when ready)

Option A: Buy domain (e.g., `citycycles.app`) — root → Vercel, `api.` → Railway. ~$12/year.
Option B: Use Vercel subdomain only. Free. Fine for portfolio.

### Phase 3: Cutover

1. Confirm new system works end-to-end
2. Update bookmarks/portfolio links
3. Add deprecation notice to Streamlit app
4. After 30 days: shut down Streamlit Cloud

---

## Monitoring

**API:** Railway metrics (CPU, memory, network). Health endpoint: `GET /health` → `{"status": "healthy", "marts_loaded": 11, "data_last_updated": "2026-04-03T02:45:00Z"}`. UptimeRobot free tier pinging `/health` every 5 min.

**Frontend:** Vercel Web Vitals (LCP, FID, CLS). Error boundary components.

---

## Rollback Plan

| Scenario | Action | Time |
|----------|--------|------|
| Frontend broken | Vercel: promote previous deployment | < 1 min |
| API broken | Railway: redeploy previous deployment | ~2 min |
| Full rollback | Streamlit app still running, update links back | Immediate |
| Data corruption | S3 has timestamped exports; download previous, restart API | ~5 min |

---

## Cost Estimate

| Component | Monthly Cost |
|-----------|-------------|
| Frontend (Vercel free) | $0 |
| API (Railway, shared project) | ~$5-10 |
| Pipeline (Railway, existing) | ~$2-5 |
| Weather extraction (Railway) | ~$1-2 |
| S3 storage | ~$3-5 |
| Domain (optional) | ~$1 |
| **Total** | **~$7-23/month** |

---

## Railway API Service Config

```toml
# railway-api.toml
[build]
builder = "DOCKERFILE"
dockerfilePath = "api/Dockerfile"

[deploy]
restartPolicyType = "ON_FAILURE"
restartPolicyMaxRetries = 3
healthcheckPath = "/health"
healthcheckTimeout = 30
```

---

## Migration Checklist

### Pre-deployment
- [ ] API Dockerfile built and tested locally
- [ ] Frontend builds (`npm run build`)
- [ ] All API endpoints verified against Streamlit query outputs
- [ ] CORS configured (frontend origin)
- [ ] Health check responds
- [ ] Environment variables ready

### Deployment
- [ ] Create Railway API service
- [ ] Configure env vars
- [ ] Deploy API, verify `/health`
- [ ] Connect GitHub to Vercel
- [ ] Configure `NEXT_PUBLIC_API_URL`
- [ ] Deploy frontend, verify landing page
- [ ] Test all pages end-to-end

### Post-deployment
- [ ] Uptime monitoring active
- [ ] Vercel analytics collecting
- [ ] Test data refresh flow
- [ ] Update README.md and CLAUDE.md
- [ ] Share new URL

### Cutover
- [ ] 2+ weeks stable on new platform
- [ ] Portfolio links updated
- [ ] Streamlit deprecation notice posted
- [ ] After 30 days: disconnect Streamlit Cloud

---

## Files to Create

```
api/Dockerfile
api/requirements.txt
.github/workflows/frontend-ci.yml
.github/workflows/api-ci.yml
.github/workflows/data-refresh.yml
railway-api.toml
```
