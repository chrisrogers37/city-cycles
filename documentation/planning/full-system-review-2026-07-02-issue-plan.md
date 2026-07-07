# Full System Review — Issue Triage & Filing Plan (2026-07-02)

Companion to [`full-system-review-2026-07-02.md`](./full-system-review-2026-07-02.md). This
document is the **durable record of how the 87 review findings were prioritized (P0–P4 +
nice-to-have) and filed as GitHub issues** on 2026-07-07.

## Priority rubric

| Priority | Definition | Filing |
|----------|------------|--------|
| **P0** | Active data-corruption, failure-masking, or availability bugs. Wrong numbers reach users or failures are hidden. | One issue per finding |
| **P1** | High-impact correctness/quality bugs and the missing CI safety net. | One issue per finding |
| **P2** | Medium correctness/robustness/perf/testing issues worth fixing soon. | Clustered by subsystem |
| **P3** | Low-severity but substantive tech-debt and minor bugs. | Clustered by subsystem |
| **P4** | Trivial cleanups (dead code, logging, docstrings, unused CSS). | Single cluster |
| **Nice-to-have** | Enhancements and documentation. | Single cluster |

Labels used: `priority:high` (P0+P1), `priority:medium` (P2), `priority:low` (P3/P4/nice),
plus `bug` / `tech-debt` / `enhancement`. (The installation token used for filing can create
issues and apply existing labels, but cannot create new labels, so no `P0`/`P1` labels were
added — priority is encoded in the title prefix.)

---

## P0 — Individual issues (8)

| Issue | Finding | Title |
|-------|---------|-------|
| [#125](https://github.com/chrisrogers37/city-cycles/issues/125) | NEW-DBT-01 | `mart_similar_day_stats` counts partial-day rides, not full-day totals |
| [#126](https://github.com/chrisrogers37/city-cycles/issues/126) | NEW-DBT-02 | `pct_change_vs_overall` compares incompatible denominators |
| [#127](https://github.com/chrisrogers37/city-cycles/issues/127) | NEW-API-01 | Real-time weather uses GMT while marts use city-local time |
| [#128](https://github.com/chrisrogers37/city-cycles/issues/128) | NEW-PIPE-01 | Extraction phase reported success despite per-source failures |
| [#129](https://github.com/chrisrogers37/city-cycles/issues/129) | NEW-PIPE-02 | File-management failures not propagated to orchestrator |
| [#130](https://github.com/chrisrogers37/city-cycles/issues/130) | NEW-PIPE-03 | `verify_data()` always marks tables PASS regardless of quality checks |
| [#131](https://github.com/chrisrogers37/city-cycles/issues/131) | NEW-PIPE-04 | DuckDB `--append` load mode is broken (still runs `CREATE TABLE AS`) |
| [#132](https://github.com/chrisrogers37/city-cycles/issues/132) | NEW-API-02 | S3 startup errors abort the entire API |

## P1 — Individual issues (9)

| Issue | Finding | Title |
|-------|---------|-------|
| [#133](https://github.com/chrisrogers37/city-cycles/issues/133) | NEW-DBT-03 | `ride_id` uniqueness scoped globally, not per city — collision risk |
| [#134](https://github.com/chrisrogers37/city-cycles/issues/134) | NEW-INFRA-01 | No CI for the data pipeline or dbt project |
| [#135](https://github.com/chrisrogers37/city-cycles/issues/135) | NEW-FE-01 | Frontend has no API error handling (SWR `error` never used) |
| [#136](https://github.com/chrisrogers37/city-cycles/issues/136) | NEW-FE-02 | "Today" temperature bar highlight never matches (band vs range) |
| [#137](https://github.com/chrisrogers37/city-cycles/issues/137) | NEW-DBT-05 | Weather-correlated marts silently drop ride hours without weather |
| [#138](https://github.com/chrisrogers37/city-cycles/issues/138) | NEW-API-03 | DuckDB failures silently reported as "no historical data" |
| [#139](https://github.com/chrisrogers37/city-cycles/issues/139) | NEW-PIPE-05 | Download failures counted as "already in S3" (NYC + London) |
| [#140](https://github.com/chrisrogers37/city-cycles/issues/140) | NEW-PIPE-09 | `LondonModernBikeShareRecord` required-columns list is incomplete |
| [#141](https://github.com/chrisrogers37/city-cycles/issues/141) | NEW-DBT-06 | `mart_daily_metrics_long` omits `unknown_user_type_rides` + dead filters |

## P2 — Clustered issues (4)

| Issue | Cluster | Findings |
|-------|---------|----------|
| [#142](https://github.com/chrisrogers37/city-cycles/issues/142) | API robustness & performance | NEW-API-04, 05, 06, 07, 08, 09, 10, 11, 12 |
| [#143](https://github.com/chrisrogers37/city-cycles/issues/143) | Pipeline correctness, robustness & CI | NEW-PIPE-06, 07, 08, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19; NEW-INFRA-02 |
| [#144](https://github.com/chrisrogers37/city-cycles/issues/144) | dbt correctness, modeling & testing | NEW-DBT-04, 07, 08, 09, 10, 11, 12, 13, 14, 15 |
| [#145](https://github.com/chrisrogers37/city-cycles/issues/145) | Frontend bugs, a11y & performance | NEW-FE-03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15 |

## P3 — Clustered issues (3)

| Issue | Cluster | Findings |
|-------|---------|----------|
| [#146](https://github.com/chrisrogers37/city-cycles/issues/146) | Backend, pipeline & infra tech-debt | NEW-API-13, 14, 15, 16, 17, 18, 22, 23; NEW-PIPE-20, 23, 24; NEW-INFRA-03, 04 |
| [#147](https://github.com/chrisrogers37/city-cycles/issues/147) | dbt tech-debt | NEW-DBT-16, 17, 19 |
| [#148](https://github.com/chrisrogers37/city-cycles/issues/148) | Frontend tech-debt | NEW-FE-19, 20, 21 |

## P4 — Clustered issue (1)

| Issue | Cluster | Findings |
|-------|---------|----------|
| [#149](https://github.com/chrisrogers37/city-cycles/issues/149) | Trivial cleanups | NEW-API-19, 20, 21; NEW-PIPE-21, 22, 26; NEW-DBT-18, 20, 21; NEW-FE-18, 22, 24 |

## Nice-to-have — Clustered issue (1)

| Issue | Cluster | Findings |
|-------|---------|----------|
| [#150](https://github.com/chrisrogers37/city-cycles/issues/150) | Enhancements & documentation | NEW-PIPE-25; NEW-DBT-22, 23; NEW-FE-16, 17, 23 |

---

## Coverage check

All 87 findings are assigned exactly once:

- **API (23):** P0 ×2 (01,02→#125? no — API-01 #127, API-02 #132), P1 ×1 (03→#138), P2 ×9 (04–12→#142), P3 ×8 (13–18,22,23→#146), P4 ×3 (19–21→#149).
- **Pipeline (26):** P0 ×4 (01→#128, 02→#129, 03→#130, 04→#131), P1 ×2 (05→#139, 09→#140), P2 ×13 (06,07,08,10–19→#143), P3 ×3 (20,23,24→#146), P4 ×3 (21,22,26→#149), nice ×1 (25→#150).
- **dbt (23):** P0 ×2 (01→#125, 02→#126), P1 ×3 (03→#133, 05→#137, 06→#141), P2 ×10 (04,07–15→#144), P3 ×3 (16,17,19→#147), P4 ×3 (18,20,21→#149), nice ×2 (22,23→#150).
- **Frontend (24):** P1 ×2 (01→#135, 02→#136), P2 ×13 (03–15→#145), P3 ×3 (19,20,21→#148), P4 ×3 (18,22,24→#149), nice ×3 (16,17,23→#150).
- **Infra (4):** P1 ×1 (01→#134), P2 ×1 (02→#143), P3 ×2 (03,04→#146).

## Notes

- Existing pre-review open issues (#65–#122) were **not** re-filed; they are indexed in the
  main review document under "Existing open backlog."
- Issue [#124](https://github.com/chrisrogers37/city-cycles/issues/124) is a leftover
  capability-probe issue. The filing token cannot close or edit issues, so it should be
  closed manually.
