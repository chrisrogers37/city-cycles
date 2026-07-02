# Full System Review & Tech-Debt Triage — 2026-07-02

**Status:** Active tracker
**Scope:** End-to-end review of the City Cycles Analytics system — API backend, data
pipeline (extraction → file manager → DuckDB → dbt), frontend, and infrastructure/CI.
**Method:** Read-only code review of every major subsystem. Findings below are **new**
(not already covered by the ~60 open GitHub issues, which are catalogued in the
[Existing backlog](#existing-open-backlog) section for completeness).

> How to use this tracker: each new finding has a stable ID (e.g. `NEW-API-01`). When an
> item is promoted to a GitHub issue or fixed, note the issue/PR number next to it and
> check it off. This document is the single source of truth for the 2026-07-02 audit pass.

---

## Executive summary

The system is broadly well-structured, but this pass surfaced a cluster of **correctness
and data-integrity bugs** that are more serious than typical style tech-debt because they
silently produce wrong analytics or mask failures:

- **Silent failure masking across the pipeline.** The orchestrator reports extraction and
  file-management phases as "success" even when individual sources fail
  (`NEW-PIPE-01/02`), download failures are miscounted as "already in S3"
  (`NEW-PIPE-05`), and DuckDB verification always returns `PASS` regardless of quality
  checks (`NEW-PIPE-03`). Bad or missing data can flow all the way to marts undetected.
- **Timezone mismatch corrupts "days like today" insights.** Real-time weather is fetched
  in GMT while historical marts key on city-local time, so insight/similar-day lookups
  match the wrong hour/month/weekday (`NEW-API-01`).
- **`mart_similar_day_stats` mis-aggregates daily ride totals** by counting only the hours
  matching a weather bucket, then averaging partial-day sums and comparing them against a
  true full-day baseline (`NEW-DBT-01/02`). The headline "similar day" numbers the API
  serves are systematically wrong on mixed-weather days.
- **CI covers only the API.** The data pipeline and dbt project have no CI gate, and the
  API job hardcodes its test file list, so most of the `tests/` suite and all dbt
  data-quality checks never run automatically (`NEW-INFRA-01/02`).

### New findings by severity

| Severity | Count |
|----------|-------|
| High     | 12    |
| Medium   | 45    |
| Low      | 30    |
| **Total**| **87**|

### Recommended top priorities

1. `NEW-DBT-01` / `NEW-DBT-02` — Fix `mart_similar_day_stats` daily-grain aggregation (wrong numbers in production API).
2. `NEW-API-01` — Align real-time weather timezone with marts (wrong insight matching).
3. `NEW-PIPE-01` / `NEW-PIPE-02` / `NEW-PIPE-03` — Stop masking extraction / file-management / verification failures.
4. `NEW-INFRA-01` — Add pipeline + dbt CI so the above classes of bug are caught.
5. `NEW-PIPE-04` — Fix broken DuckDB `--append` load mode.
6. `NEW-API-02` — Don't let a single S3 startup error crash the whole API.

---

## New findings — API backend (`api/`)

| ID | Sev | Cat | Title |
|----|-----|-----|-------|
| NEW-API-01 | High | bug | Real-time weather uses GMT while marts use city-local time |
| NEW-API-02 | High | bug | S3 startup errors abort the entire app |
| NEW-API-03 | Med | bug | DuckDB failures silently reported as "no historical data" |
| NEW-API-04 | Med | bug | Inconsistent missing-data behavior between similar-day endpoints |
| NEW-API-05 | Med | bug | `NaN`/`or 0` guard is ineffective in daily-metrics |
| NEW-API-06 | Med | bug | Partial date/year filters silently ignored |
| NEW-API-07 | Med | bug | Similar-day hourly route has no DuckDB error handling |
| NEW-API-08 | Med | perf | Sync route handlers block the ASGI event loop |
| NEW-API-09 | Med | perf | daily-metrics scans the same parquet three times per request |
| NEW-API-10 | Med | perf | Recommendation engine opens two DuckDB connections per insights call |
| NEW-API-11 | Med | tech-debt | Parquet files never refreshed after first download |
| NEW-API-12 | Med | perf | Unbounded cache growth on daily-metrics date ranges |
| NEW-API-13 | Low | tech-debt | Caching applied to only 1 of 11 analytics endpoints |
| NEW-API-14 | Low | bug | `monthly-trends` aggregation param accepts invalid values silently |
| NEW-API-15 | Low | bug | `station-performance` hour bounds unvalidated |
| NEW-API-16 | Low | perf | Health check performs filesystem I/O for all marts on every request |
| NEW-API-17 | Low | security | OpenAPI `/docs` + `/redoc` enabled in production |
| NEW-API-18 | Low | bug | CORS origin env parsing doesn't strip whitespace |
| NEW-API-19 | Low | tech-debt | Dead code `run_query()` + unused import in `query_service.py` |
| NEW-API-20 | Low | tech-debt | `print()` instead of logger in S3 loader |
| NEW-API-21 | Low | tech-debt | Outdated `weather_bridge.py` docstring references deleted `dashboard.` |
| NEW-API-22 | Low | architecture | Similar-day hourly SQL lives in the route layer |
| NEW-API-23 | Low | tech-debt | No test for similar-day hourly happy path |

**Details**

- **NEW-API-01 · High · bug** — `_build_api_url()` (`api/services/weather_service.py:196-206`)
  omits the `timezone` param, so Open-Meteo defaults to GMT, while batch extraction uses
  `America/New_York` / `Europe/London` (`extraction/weather.py:49-55`) and marts derive
  `hour_of_day` / `month_num` / `day_type` from local time (`stg_weather_hourly.sql:100-104`).
  Insight and similar-day lookups therefore match the wrong hour/month/weekday. Also affects
  `weather_bridge.py:43` and `recommendation_engine.py:1010-1014`.
- **NEW-API-02 · High · bug** — `ensure_local_parquet_files()`
  (`api/services/data_loader.py:45-50`) re-raises any non-404 `ClientError`; the lifespan
  handler (`api/main.py:46-47`) has no try/except, so missing creds / access-denied /
  network blips prevent the process from ever becoming ready.
- **NEW-API-03 · Med · bug** — `lookup_historical_impact()` / `lookup_similar_day_stats()`
  (`recommendation_engine.py:479-481, 570-572, 748-758`) catch all exceptions and return
  empty dataclasses; `/api/insights` then emits "No historical riding data… rare
  combination," which is misleading when the parquet exists but the query failed.
- **NEW-API-04 · Med · bug** — Daily `/api/similar-day/{city}` returns 200 with null fields
  when parquet is missing (`similar_day.py:69-75`), but the hourly variant raises 404 for
  the same condition (`:111-112`).
- **NEW-API-05 · Med · bug** — `float(df.iloc[0] or 0)` (`analytics.py:80-83`) does not
  coalesce `NaN` (`NaN or 0` stays `NaN`); the avg-duration branch uses `is not None` which
  also passes `NaN` through, producing invalid JSON floats.
- **NEW-API-06 · Med · bug** — `member-analysis` and `station-growth`
  (`analytics.py:449-459, 491-506`) only filter when *both* bounds are provided; supplying
  one bound silently falls through to the unfiltered query.
- **NEW-API-07 · Med · bug** — Hourly similar-day (`similar_day.py:114-147`) runs inline
  DuckDB SQL with no try/except; corrupt/missing parquet after the existence check surfaces
  as an unhandled 500.
- **NEW-API-08 · Med · perf** — All route handlers are synchronous `def` doing blocking
  `requests.get()` and DuckDB I/O (e.g. `weather.py:60-77`, `analytics.py:36+`), starving
  other async tasks on the worker under concurrency.
- **NEW-API-09 · Med · perf** — daily-metrics issues three separate `run_query_params()`
  calls (`analytics.py:72-75`, `query_service.py:18-21`), each opening a fresh in-memory
  DuckDB connection and re-reading the same parquet.
- **NEW-API-10 · Med · perf** — `get_recommendations()` passes `conn=None` to both lookups
  (`recommendation_engine.py:1031-1053, 437-438, 515-516`); each creates its own `:memory:`
  connection despite a shared `conn` parameter existing.
- **NEW-API-11 · Med · tech-debt** — Download is skipped when the local file exists
  (`data_loader.py:41-44`), so monthly S3 mart updates are never picked up until the
  container filesystem is wiped/redeployed. (The `data-refresh.yml` redeploy webhook is the
  de-facto refresh mechanism — fragile and undocumented.)
- **NEW-API-12 · Med · perf** — The daily-metrics cache (`api/cache.py:10-25`) is keyed by
  `{city}:{start}:{end}` with no max size/LRU; arbitrary ranges accumulate for the process
  lifetime.
- **NEW-API-13 · Low · tech-debt** — Only daily-metrics is cached; the other 10 analytics
  endpoints hit parquet on every request.
- **NEW-API-14 · Low · bug** — `monthly-trends` aggregation (`analytics.py:104-107`) selects
  `SUM` for anything that isn't exactly `"avg"`, so `"average"` silently changes semantics.
- **NEW-API-15 · Low · bug** — `hour_start`/`hour_end` (`analytics.py:382-383`) accept any
  int (negative, >23, inverted) with no FastAPI constraints; invalid input returns empty
  results rather than 422.
- **NEW-API-16 · Low · perf** — `/health` (`main.py:85-89`) does `os.path.isfile` for all 11
  marts on every probe.
- **NEW-API-17 · Low · security** — `FastAPI()` (`main.py:52-57`) keeps default `/docs` and
  `/redoc`, exposing full schema on production Railway.
- **NEW-API-18 · Low · bug** — `CORS_ORIGIN.split(",")` (`main.py:60`) doesn't `strip()`,
  producing origins like `" http://prod.example.com"`.
- **NEW-API-19 · Low · tech-debt** — `run_query()` unused; `parquet_path` imported but unused
  (`query_service.py:9, 12-15`).
- **NEW-API-20 · Low · tech-debt** — Download progress uses `print()` (`data_loader.py:43`)
  while the rest of the module uses `logger`.
- **NEW-API-21 · Low · tech-debt** — `weather_bridge.py:2-3` docstring still references
  `dashboard.weather_service.fetch_city_weather()` post-decoupling.
- **NEW-API-22 · Low · architecture** — Similar-day hourly SQL is inlined in the router
  (`similar_day.py:114-147`) instead of `query_service`/`recommendation_engine`.
- **NEW-API-23 · Low · tech-debt** — `TestSimilarDayHourlyEndpoint`
  (`tests/test_api_similar_day.py:49-52`) only asserts 422; the query/mapping path is
  untested (distinct from #120).

---

## New findings — Data pipeline (`extraction/`, `extracted_file_manager/`, `data_models/`, `db_duckdb/`, `orchestrator/`)

| ID | Sev | Cat | Title |
|----|-----|-----|-------|
| NEW-PIPE-01 | High | bug | Extraction phase reported "success" despite per-source failures |
| NEW-PIPE-02 | High | bug | File-management failures not propagated to orchestrator |
| NEW-PIPE-03 | High | bug | `verify_data()` always marks tables PASS regardless of quality checks |
| NEW-PIPE-04 | High | bug | DuckDB `--append` load mode is broken (still does CREATE TABLE AS) |
| NEW-PIPE-05 | Med | bug | Download failures counted as "already in S3" (NYC + London) |
| NEW-PIPE-06 | Med | bug | Failed/empty CSV conversion can upload empty Parquet |
| NEW-PIPE-07 | Med | bug | ZIP extraction marked successful on partial inner failures |
| NEW-PIPE-08 | Med | bug | Cross-schema Parquet existence check can block reconversion |
| NEW-PIPE-09 | Med | bug | `LondonModernBikeShareRecord` required-columns list is incomplete |
| NEW-PIPE-10 | Med | architecture | Weather Parquet bypasses `HourlyWeatherRecord` validation |
| NEW-PIPE-11 | Med | correctness | Incremental weather loads 16-day forecast rows into raw history |
| NEW-PIPE-12 | Med | idempotency | Current-year weather backfill never refreshes after first upload |
| NEW-PIPE-13 | Med | bug | `load_data()` fail-fast leaves partial DB state |
| NEW-PIPE-14 | Med | bug | `export_marts()` stops on first failure (mixed old/new S3 marts) |
| NEW-PIPE-15 | Med | bug | `check_pipeline_status()` never sets `data_verified=True` |
| NEW-PIPE-16 | Med | bug | `marts_available` checks wrong schema (`main` vs `main_marts`) |
| NEW-PIPE-17 | Med | perf | Full dbt stdout buffered in memory, defeating streaming design |
| NEW-PIPE-18 | Med | robustness | TfL HTTP downloads have no timeout |
| NEW-PIPE-19 | Med | tech-debt | Pipeline status ignores `raw_weather_hourly` table |
| NEW-PIPE-20 | Low | bug | S3 export listing in DuckDB CLI is unpaginated (>1000 keys) |
| NEW-PIPE-21 | Low | tech-debt | Divergent mart-name lists between export and status |
| NEW-PIPE-22 | Low | tech-debt | `DUCKDB_THREADS` config is read but never applied |
| NEW-PIPE-23 | Low | tech-debt | No schema migration for existing raw tables |
| NEW-PIPE-24 | Low | robustness | London Playwright discovery relies on fixed 30-scroll heuristic |
| NEW-PIPE-25 | Low | enhancement | NYC/London web downloads lack transient-error retry |
| NEW-PIPE-26 | Low | perf | Unconditional per-ZIP member enumeration in hot path |

**Details**

- **NEW-PIPE-01 · High · bug** — `orchestrator/main.py:137-160` logs and swallows NYC/London/
  weather failures, then unconditionally sets `self.results['extraction'] = {'status':
  'success'}`. Downstream steps run on missing upstream data with no failure signal.
- **NEW-PIPE-02 · High · bug** — Conversion/extraction failures return `{file: False}` dicts
  without raising (`orchestrator/main.py:183-186`,
  `extracted_file_manager/simplified_pipeline.py:94-127`); the orchestrator always logs
  "✓ File management phase completed."
- **NEW-PIPE-03 · High · bug** — `_verify_table_data()` (`db_duckdb/operations.py:272-276`)
  hardcodes `"status": "PASS"`; detailed null/duplicate/date checks from
  `_run_data_quality_checks()` are collected but never affect status.
- **NEW-PIPE-04 · High · bug** — `load_parquet_from_s3(..., replace=False)`
  (`db_duckdb/duckdb_manager.py:133-144`) still runs `CREATE TABLE … AS SELECT` and never
  calls `insert_parquet_from_s3()`, so append mode fails if the table already exists.
- **NEW-PIPE-05 · Med · bug** — Any `False` from `download_and_store_*` is counted as
  `skipped_count` / "Files already in S3" (`extraction/nyc.py:111-120`,
  `extraction/london.py:119-129`), hiding invalid ZIPs, HTTP errors, and upload failures.
- **NEW-PIPE-06 · Med · bug** — If `pd.read_csv` yields no chunks or fails before the writer
  is created (`extracted_file_manager/manager.py:485-490`), `writer` stays `None` yet the
  temp Parquet is still uploaded, leaving a corrupt/empty object that idempotency checks then
  treat as done.
- **NEW-PIPE-07 · Med · bug** — Nested ZIP/CSV errors are `continue`d but the outer loop still
  sets `results[zip_file] = True` (`manager.py:152-157, 281-283`), so partial extraction is
  reported as success.
- **NEW-PIPE-08 · Med · bug** — `_parquet_exists_for_csv()` (`manager.py:519-541`) returns
  true if the filename exists under *any* of four schema prefixes, so a file converted under
  the wrong schema is skipped permanently.
- **NEW-PIPE-09 · Med · bug** — `LondonModernBikeShareRecord._required_columns`
  (`data_models/london_bike.py:74-84`) omits `"Bike number"` and `"Total duration (ms)"` that
  `to_dataframe()` later requires (`:90-95, 123`); validation passes, conversion then fails.
- **NEW-PIPE-10 · Med · architecture** — Weather is written directly from API JSON to Parquet
  (`extraction/weather.py:193-234, 315-328`) with no `validate_schema()`/`to_dataframe()`
  step, unlike bike data, so schema drift isn't caught before DuckDB load.
- **NEW-PIPE-11 · Med · correctness** — `fetch_forecast_weather()` always requests
  `forecast_days=16` and incremental uploads land in `raw_weather_hourly`
  (`extraction/weather.py:164, 309`) without separating forecast vs observed rows.
- **NEW-PIPE-12 · Med · idempotency** — Existing `weather_{city}_{year}.parquet` keys are
  skipped entirely (`extraction/weather.py:264-276`); the current-year file (built with a
  5-day-lag end date) stays stale until manually deleted.
- **NEW-PIPE-13 · Med · bug** — `load_data()` (`db_duckdb/operations.py:193-198`) fails fast
  on the first table error after earlier tables were already dropped/recreated, leaving
  partial DB state.
- **NEW-PIPE-14 · Med · bug** — `export_marts()` (`operations.py:534-538`) breaks the loop on
  the first failed export, leaving a mix of old/new S3 marts with no rollback.
- **NEW-PIPE-15 · Med · bug** — `check_pipeline_status()` (`db_duckdb/pipeline.py:158-206`)
  initializes `data_verified: False` and never updates it, so CLI status
  (`db_duckdb/cli.py:419`) always reports verification failed.
- **NEW-PIPE-16 · Med · bug** — Status uses `list_tables()` (default/`main` schema)
  (`pipeline.py:172-199`) but exports query `main_marts.{table}` (`operations.py:524`); after
  dbt, marts live in `main_marts`, so `marts_available` is usually false.
- **NEW-PIPE-17 · Med · perf** — Every dbt line is appended to `output_lines` and stored in
  `self.results['dbt']['output']` (`orchestrator/main.py:295-311`), defeating the streaming
  subprocess design for long runs.
- **NEW-PIPE-18 · Med · robustness** — `requests.get(file_url)` for TfL
  (`extraction/london.py:83`) has no timeout and can hang indefinitely (weather uses
  `timeout=120/60`).
- **NEW-PIPE-19 · Med · tech-debt** — `expected_tables` (`pipeline.py:169-170`) lists only the
  four bike tables; `raw_weather_hourly` (loaded via `S3_URIS`) is excluded from
  existence/load checks.
- **NEW-PIPE-20 · Low · bug** — `list_objects_v2` without a paginator
  (`db_duckdb/cli.py:317-322`) truncates at 1000 keys (same class as #114, different path).
- **NEW-PIPE-21 · Low · tech-debt** — Export includes `mart_similar_day_stats` but status
  omits it (`operations.py:476-488` vs `pipeline.py:193-197`).
- **NEW-PIPE-22 · Low · tech-debt** — `DUCKDB_THREADS` is in `DB_CONFIG`
  (`db_duckdb/config/duckdb_config.py:126`) but `DuckDBManager` only applies `memory_limit`
  (`duckdb_manager.py:62-65`).
- **NEW-PIPE-23 · Low · tech-debt** — `create_table()` (`duckdb_manager.py:112-115`) skips
  creation when the name exists without checking the live schema against `TABLE_SCHEMAS`.
- **NEW-PIPE-24 · Low · robustness** — File discovery depends on 30 one-second scrolls plus a
  single selector wait (`extraction/london.py:34-40`) with no completeness check.
- **NEW-PIPE-25 · Low · enhancement** — NYC/London downloads (`nyc.py:75-94`,
  `london.py:79-101`) fail permanently on the first network blip, unlike the file manager's
  `retry_on_transient_error` wrapper.
- **NEW-PIPE-26 · Low · perf** — Every ZIP logs its full member list on every run
  (`manager.py:252-254`).

---

## New findings — dbt project (`dbt_city_cycles/`)

| ID | Sev | Cat | Title |
|----|-----|-----|-------|
| NEW-DBT-01 | High | bug | `mart_similar_day_stats` daily grain counts partial-day rides, not full-day totals |
| NEW-DBT-02 | High | bug | `pct_change_vs_overall` compares incompatible denominators |
| NEW-DBT-03 | High | bug | `ride_id` uniqueness scoped globally, not per city (collision risk) |
| NEW-DBT-04 | Med | bug | NYC legacy synthetic `ride_id` can collide on identical fingerprints |
| NEW-DBT-05 | Med | bug | Weather-correlated marts silently drop ride hours without weather (INNER JOIN) |
| NEW-DBT-06 | Med | bug | `mart_daily_metrics_long` omits `unknown_user_type_rides` + has dead filters |
| NEW-DBT-07 | Med | bug | Hourly grain reuses misleading column name `avg_daily_rides` |
| NEW-DBT-08 | Med | architecture | Legacy/modern unions have no cutover deduplication |
| NEW-DBT-09 | Med | architecture | `rideable_type` / `bike_model` dropped before unified layer |
| NEW-DBT-10 | Med | architecture | Weather–ride join logic duplicated instead of centralized |
| NEW-DBT-11 | Med | architecture | All 11 marts are full rebuilds over ~216M-row views |
| NEW-DBT-12 | Med | testing | No composite-grain uniqueness tests on marts |
| NEW-DBT-13 | Med | testing | No `relationships` (FK) tests between layers |
| NEW-DBT-14 | Med | testing | No ride data-quality tests (duration / timestamp sanity) |
| NEW-DBT-15 | Med | testing | No custom singular tests or dbt unit tests despite configured `test-paths` |
| NEW-DBT-16 | Low | tech-debt | `mart_weather_impact_summary` duplicates near-identical CTEs |
| NEW-DBT-17 | Low | testing | Staging `ride_id` uniqueness tests miss union collisions |
| NEW-DBT-18 | Low | tech-debt | Fragile positional `GROUP BY 1,2,3,4,11` in `mart_daily_metrics` |
| NEW-DBT-19 | Low | bug | `is_precipitation` vs `precipitation_intensity` can disagree |
| NEW-DBT-20 | Low | tech-debt | Weather dedup picks arbitrary row on `(city, hour)` duplicates |
| NEW-DBT-21 | Low | tech-debt | `dbt_project.yml` forces single-threaded DuckDB (`PRAGMA threads=1`) |
| NEW-DBT-22 | Low | enhancement | Intermediate layer documentation is sparse |
| NEW-DBT-23 | Low | enhancement | No dbt project README or `exposures:` for API consumers |

**Details**

- **NEW-DBT-01 · High · bug** — `daily_totals` (`mart_similar_day_stats.sql:58-71`) groups
  hourly `mart_weather_ride_correlation` rows by `(date, temperature_band,
  precipitation_intensity)`, so `daily_rides` counts only hours in that bucket. A
  mixed-weather day yields multiple partial rows, and `daily_stats` (`:75-84`) averages those
  partial sums. The API serves this as full-day `avg_daily_rides`
  (`recommendation_engine.py:519-526`).
- **NEW-DBT-02 · High · bug** — `overall_baseline` (`:40-54`) computes true daily totals via
  `sum(ride_count) … group by location, date`, but `pct_change_vs_overall` (`:88-95`)
  compares that against `avg(d.daily_rides)` from the partial-day buckets, biasing
  percent-change low on multi-weather days.
- **NEW-DBT-03 · High · bug** — `unified_rides` and both `int_*_rides` enforce `unique` on
  `ride_id` alone (`unified/schema.yml:11-15`, `intermediate/schema.yml:11-15, 38-42`).
  London legacy uses `rental_id`, modern uses `number`; numeric IDs can collide across eras
  and between NYC/London when unioned (`unified_rides.sql:30-63`).
- **NEW-DBT-04 · Med · bug** — `stg_nyc_legacy.sql:8-11` builds `ride_id` from `bikeid`,
  `start_station_id`, and timestamps only (no `end_station_id`); trips with the same bike,
  origin and times but different destinations collapse to one ID.
- **NEW-DBT-05 · Med · bug** — `mart_weather_ride_correlation.sql:31-35`,
  `mart_station_weather_performance.sql:15-19` and downstream marts `INNER JOIN`
  `stg_weather_hourly`; ride hours lacking weather coverage are dropped, biasing weather
  impact and similar-day stats.
- **NEW-DBT-06 · Med · bug** — Wide mart computes `unknown_user_type_rides`
  (`mart_daily_metrics.sql:12`) but the long pivot never unpivots it
  (`mart_daily_metrics_long.sql:5-71`). `WHERE member_rides IS NOT NULL` /
  `casual_rides IS NOT NULL` (`:35, :44`) never filter because upstream uses
  `SUM(CASE … ELSE 0 END)` (returns 0, not NULL).
- **NEW-DBT-07 · Med · bug** — Hourly rows populate `avg_daily_rides` with `avg(c.ride_count)`
  (`mart_similar_day_stats.sql:196`); the dual meaning is documented
  (`marts/schema.yml:395-398`) but the column name will confuse consumers.
- **NEW-DBT-08 · Med · architecture** — `int_nyc_rides.sql:63-67` and
  `int_london_rides.sql:49-53` are blind `UNION ALL` with no date bounds or dedup; overlap at
  schema transitions can duplicate rides.
- **NEW-DBT-09 · Med · architecture** — `rideable_type` / `bike_model` are staged
  (`stg_nyc_modern.sql:9`, `stg_london_modern.sql:10`) but never carried through
  `int_*_rides`/`unified_rides`, blocking e-bike/model analytics.
- **NEW-DBT-10 · Med · architecture** — The `(location, date, hour_of_day)` weather join is
  repeated in multiple marts with no shared "weather-hour spine" intermediate.
- **NEW-DBT-11 · Med · architecture** — No mart uses incremental materialization or
  partitioning (`dbt_project.yml:44-46`); every run re-scans `unified_rides` for every mart.
- **NEW-DBT-12 · Med · testing** — No composite-grain uniqueness tests (`(location, date)`,
  `(location, date, hour_of_day)`, `(location, station_id)`, `(location, month)`); only
  single-column tests exist.
- **NEW-DBT-13 · Med · testing** — No `relationships` tests between layers (e.g.
  `mart_daily_metrics.location` → population; station IDs → `mart_station_directory`).
- **NEW-DBT-14 · Med · testing** — No `duration_seconds > 0`, `start_time <= stop_time`, or
  bounds tests anywhere in staging/intermediate/unified.
- **NEW-DBT-15 · Med · testing** — `test-paths: ["tests"]` is set (`dbt_project.yml:16`) but no
  `dbt_city_cycles/tests/` directory and no unit-test YAMLs exist for critical logic
  (similar-day, weather joins, member %).
- **NEW-DBT-16 · Low · tech-debt** — `by_weather_condition`
  (`mart_weather_impact_summary.sql:18-54`) and `by_precipitation_temp` (`:56-94`) differ only
  in grouping; ~40 lines of baseline/pct-change logic are copy-pasted.
- **NEW-DBT-17 · Low · testing** — Per-table `unique` on `ride_id` (`staging/schema.yml`)
  passes independently; collisions only surface at the union level with no `(location,
  ride_id)` test.
- **NEW-DBT-18 · Low · tech-debt** — `GROUP BY 1, 2, 3, 4, 11` (`mart_daily_metrics.sql:20`)
  breaks silently if SELECT columns are reordered.
- **NEW-DBT-19 · Low · bug** — `is_precipitation` (`stg_weather_hourly.sql:65-68`) checks
  precipitation/rain/snowfall but `precipitation_intensity` (`:71-77`) uses only
  `precipitation`; rows with rain/snow but `precipitation = 0` get `is_precipitation = true`
  and intensity `'none'`.
- **NEW-DBT-20 · Low · tech-debt** — `stg_weather_hourly.sql:10-13` dedups on `(city, hour)`
  ordering by `timestamp` only, so conflicting measurements keep a nondeterministic winner.
- **NEW-DBT-21 · Low · tech-debt** — `on-run-start` sets `PRAGMA threads=1`
  (`dbt_project.yml:50`), limiting mart build parallelism.
- **NEW-DBT-22 · Low · enhancement** — `intermediate/schema.yml` documents ~6 columns/model;
  `duration_seconds`, coordinates, `day_type`, `hour_of_day` are undocumented.
- **NEW-DBT-23 · Low · enhancement** — No `dbt_city_cycles/README.md` and no `exposures:`
  documenting API/dashboard consumers.

---

## New findings — Frontend (`frontend/`)

| ID | Sev | Cat | Title |
|----|-----|-----|-------|
| NEW-FE-01 | High | bug | No API error handling anywhere (SWR `error` never used) |
| NEW-FE-02 | High | bug | "Today" temperature bar highlight never matches (band vs range) |
| NEW-FE-03 | Med | bug | Station map does not recenter when city changes |
| NEW-FE-04 | Med | architecture | `apiFetch` bypasses Next.js `/api` rewrites |
| NEW-FE-05 | Med | bug | `CrossCityInsight` vanishes during loading (no isLoading check) |
| NEW-FE-06 | Med | bug | Invalid/non-standard Tailwind utility classes |
| NEW-FE-07 | Med | bug | Monthly "Now" marker ignores selected date range |
| NEW-FE-08 | Med | bug | Stations hour-range filter allows invalid ranges |
| NEW-FE-09 | Med | bug | `CrossCityInsight` division-by-zero in ride comparison |
| NEW-FE-10 | Med | a11y | No `prefers-reduced-motion` support |
| NEW-FE-11 | Med | a11y | DataTable sort controls are mouse-only |
| NEW-FE-12 | Med | a11y | NavBar mobile menu missing expanded-state semantics |
| NEW-FE-13 | Med | perf | WeatherCanvas keeps animating off-screen |
| NEW-FE-14 | Med | perf | Mapbox loaded synchronously without code splitting |
| NEW-FE-15 | Med | architecture | All route pages fully client-rendered (no RSC/loading/error) |
| NEW-FE-16 | Low | enhancement | London analytics silently omits Member Percentage chart |
| NEW-FE-17 | Low | enhancement | Weather charts always show Celsius labels (ignore tempUnit) |
| NEW-FE-18 | Low | tech-debt | SimilarDayCard peak hours use inconsistent 24h format |
| NEW-FE-19 | Low | tech-debt | Duplicate member/casual aggregation logic |
| NEW-FE-20 | Low | bug | InsightCards sort breaks on unknown severities |
| NEW-FE-21 | Low | bug | DurationInsight baseline divides by zero at −100% |
| NEW-FE-22 | Low | tech-debt | Unused CSS animation utilities |
| NEW-FE-23 | Low | enhancement | No frontend `.env.example` |
| NEW-FE-24 | Low | tech-debt | No standalone `tsc --noEmit` npm script |

**Details**

- **NEW-FE-01 · High · bug** — SWR hooks (`src/hooks/*.ts`) destructure only `{ data,
  isLoading }`; a repo-wide search finds zero `error`/`isError` usage. Failed requests
  silently render empty states / em dashes / `null` with no retry or feedback (e.g.
  `src/app/analytics/page.tsx:24-57`).
- **NEW-FE-02 · High · bug** — `TemperatureRidesChart.tsx:27-34` compares `temp_range`
  (ranges like `"15-20°C"`) with `insights.classified.temperature_band` (`"mild"`, `"warm"`,
  …) via substring `.includes()`, so `isToday` is never true.
- **NEW-FE-03 · Med · bug** — `StationMap.tsx:114` uses `initialViewState={CITY_CENTERS[city]}`;
  Mapbox applies it only on mount, so toggling city on `/stations` updates data but not the
  viewport.
- **NEW-FE-04 · Med · architecture** — `apiFetch` (`src/lib/api.ts:3-6`) always hits
  `${NEXT_PUBLIC_API_URL}${path}`, so the `next.config.ts:4-11` rewrite proxy is unused,
  coupling the client to the backend origin and relying on backend CORS.
- **NEW-FE-05 · Med · bug** — `CrossCityInsight.tsx:16-17` returns `null` while data is loading
  (no `isLoading` check), causing a layout gap/flash vs the skeletons in `DualWeatherHeader`.
- **NEW-FE-06 · Med · bug** — Non-standard classes `border-l-3`, `duration-800`,
  `scrollbar-hide` (`SimilarDayCard.tsx:24/59`, `CrossCityInsight.tsx:60`, `SkyGradient.tsx:25`,
  `ForecastStrip.tsx:45`) aren't defined, so those styles don't apply.
- **NEW-FE-07 · Med · bug** — `ReferenceLine x={currentMonth}`
  (`MonthlyTrendChart.tsx:51,127-132`, `DurationTrendChart.tsx:50,101-106`) always marks the
  current calendar month even when a past year is selected.
- **NEW-FE-08 · Med · bug** — `hourStart`/`hourEnd` (`stations/page.tsx:59-66,112-133`) are
  independent with no validation; `hourStart > hourEnd` yields SQL `BETWEEN` returning zero
  rows and a generic empty state.
- **NEW-FE-09 · Med · bug** — `CrossCityInsight.tsx:38-41` divides by `lonRides`/`nycRides`
  without a zero guard, yielding `Infinity`/`NaN` in the narrative.
- **NEW-FE-10 · Med · a11y** — Continuous rAF particles and CSS drift/pulse/bounce animations
  (`WeatherCanvas.tsx:59-88`, `weather-effects.css:4-24`, `WeatherScene.tsx:72`) run
  unconditionally with no `prefers-reduced-motion` guard.
- **NEW-FE-11 · Med · a11y** — Sortable `<th onClick>` headers (`DataTable.tsx:58-68`) lack
  `aria-sort`, `role="button"`, keyboard handlers, and focus styles.
- **NEW-FE-12 · Med · a11y** — Hamburger (`NavBar.tsx:124-127,148-157`) has `aria-label` but no
  `aria-expanded`/`aria-controls`, no Escape-to-dismiss, no focus trap.
- **NEW-FE-13 · Med · perf** — `WeatherScene` stays mounted above `PageShell`
  (`WeatherScene.tsx:44-55,58`; `WeatherCanvas.tsx:59-88`); the rAF loop keeps running after
  the hero scrolls off-screen.
- **NEW-FE-14 · Med · perf** — `mapbox-gl` CSS + `react-map-gl` are static imports
  (`StationMap.tsx:4-8`) with no `next/dynamic`, pulling the full bundle into the critical
  path on map view.
- **NEW-FE-15 · Med · architecture** — All `src/app/*/page.tsx` start with `"use client"`; no
  Server Components, `loading.tsx`, `error.tsx`, or route error boundaries.
- **NEW-FE-16 · Low · enhancement** — `MemberPercentageChart.tsx:26` returns `null` for
  non-NYC with no placeholder, leaving an unexplained blank gap on `/analytics` for London.
- **NEW-FE-17 · Low · enhancement** — `TemperatureRidesChart.tsx` X-axis labels come from API
  Celsius buckets and ignore the global `tempUnit` toggle.
- **NEW-FE-18 · Low · tech-debt** — `SimilarDayCard.tsx:44-47` renders `"7:00–19:00"` while the
  rest of the app uses `formatHour12()` (`WeatherNarrative.tsx:67-68`).
- **NEW-FE-19 · Low · tech-debt** — Near-identical member/casual summation
  (`MemberCasualSplit.tsx:24-41` vs `ComparisonStatsTable.tsx:39-63`) is duplicated.
- **NEW-FE-20 · Low · bug** — `InsightCards.tsx:36-39` sorts via
  `SEVERITY_ORDER.indexOf(a.severity as Severity)`; values outside the union return `-1`,
  causing unstable ordering.
- **NEW-FE-21 · Low · bug** — `DurationInsight.tsx:33` computes `avgDuration / (1 + pct/100)`,
  yielding `Infinity` when `duration_pct_change_vs_overall === -100`.
- **NEW-FE-22 · Low · tech-debt** — `@keyframes score-fill` and `.animate-fade-in-up`
  (`weather-effects.css:27-46`) are defined but unreferenced.
- **NEW-FE-23 · Low · enhancement** — No `frontend/.env.example` documenting
  `NEXT_PUBLIC_API_URL` / `NEXT_PUBLIC_MAPBOX_TOKEN`.
- **NEW-FE-24 · Low · tech-debt** — `package.json:5-10` has no standalone `tsc --noEmit`
  script (CI runs it, but local devs have no easy check).

---

## New findings — Infrastructure, CI & config

| ID | Sev | Cat | Title |
|----|-----|-----|-------|
| NEW-INFRA-01 | High | testing/ci | No CI for the data pipeline or dbt project |
| NEW-INFRA-02 | Med | ci | `api-ci.yml` hardcodes its test-file list |
| NEW-INFRA-03 | Med | tech-debt | No Python lint/format/type-check tooling despite CLAUDE.md claims |
| NEW-INFRA-04 | Low | tech-debt | Dependency drift risk between root and `api/requirements.txt` |

**Details**

- **NEW-INFRA-01 · High · testing/ci** — Only `frontend-ci.yml` and `api-ci.yml` exist. The
  substantial `tests/` suite covering extraction, `db_duckdb`, orchestrator,
  `extracted_file_manager`, and `data_models` never runs in CI, and there is **no dbt CI**
  (`dbt parse`/`build`/`test`). Combined with `NEW-DBT-12..15`, data-quality regressions and
  the pipeline bugs above (`NEW-PIPE-*`) can merge undetected.
- **NEW-INFRA-02 · Med · ci** — `api-ci.yml:26` enumerates test files by name
  (`test_api_*.py`, `test_weather_service.py`, …), so any new backend test file is silently
  excluded from CI unless the workflow is edited.
- **NEW-INFRA-03 · Med · tech-debt** — No `ruff`/`black`/`flake8`/`mypy` config exists
  (`pyproject.toml`/`setup.cfg`/`mypy.ini` absent), yet `CLAUDE.md` references mypy type
  checking and a PostToolUse auto-format hook. Style/type enforcement is unverifiable in CI.
- **NEW-INFRA-04 · Low · tech-debt** — Root `requirements.txt` and `api/requirements.txt`
  independently pin overlapping packages (duckdb, pandas, pyarrow, numpy, boto3, botocore,
  pydantic, requests). Version drift between the two is likely over time; consider a shared
  constraints file.

---

## Existing open backlog

For completeness, the following issues were already open at the time of this audit and are
**not** re-filed above. Grouped by area:

**Security (open):** #75 exception detail leakage, #76 health path exposure, #77 CORS
wildcard+credentials, #78 no rate limiting, #81 unvalidated date params, #82 unbounded result
sets, #83 vulnerable Next.js, #84 Mapbox token exposure, #86 insecure `/tmp` usage, #87
Dockerfile runs as root, #113 Python dependency vulnerabilities.

**Backend / pipeline tech-debt (open):** #92 forked table/model enums, #93 split
`recommendation_engine.py`, #94 promote public classifier, #95 `query_or_500()` helper, #96
dedup unit conversions, #97/#116 dead code removal, #98 unwired `orchestrator/config.py`, #99
double extraction, #100 divergent S3-existence checks, #101 print→logging, #102 WMO code maps,
#108 hardcoded `S3_BUCKET`, #109 `.env.example` drift, #114 `_list_s3_files` pagination, #115
`verify_data()` unreachable summary, #117 unused mart downloads, #118 streaming-subprocess
helper, #119 hardcoded `days_back=35`, #120 analytics endpoint test gap, #121
`manager.py` mixed responsibilities, #122 low-severity backend roundup.

**dbt tech-debt (open):** #103 redundant `config(materialized='table')`, #104 hourly-patterns
compat shim, #105 `day_type()` macro reuse, #106 mart consistency/doc cleanups.

**Frontend tech-debt (open):** #65 `temperature_f` tempUnit audit, #66 `formatTemp()`, #67
`CITY_DISPLAY_NAMES`, #68 lint-rule docs, #69 shared Skeleton, #71 `formatHourShort`/
`getCurrentHour`, #72 shared Recharts tooltip, #73 frontend test coverage, #74 unsafe casts,
#107 Railway config clarity, #110 color palette, #111 frontend dead code, #112 minor DRY.

**Recently closed (verified fixed):** #64, #79 security headers, #80 LIMIT param, #85 table-name
allowlisting.

---

## Notes / cross-cutting themes

- **Failure observability is the biggest systemic risk.** Multiple layers (orchestrator,
  extraction counters, DuckDB verify, export loop, API startup) either swallow errors or
  report success unconditionally. A run can "pass" end-to-end while serving stale or wrong
  data. Prioritize `NEW-PIPE-01/02/03`, `NEW-PIPE-13/14`, and `NEW-API-02/03`.
- **The similar-day feature is the product's headline, and its numbers are wrong**
  (`NEW-DBT-01/02` + `NEW-API-01`). This should be treated as a correctness incident, not
  cosmetic tech-debt.
- **CI is the missing safety net** (`NEW-INFRA-01/02`). Adding pipeline + dbt CI would have
  caught several of the bugs above and prevents regressions on the fixes.
