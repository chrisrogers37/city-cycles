# Phase 10: Dependency Updates

**Status:** ✅ COMPLETE
**Started:** 2026-02-11
**Completed:** 2026-02-11
**PR Title:** `chore: update outdated Python dependencies`
**Risk Level:** High
**Estimated Effort:** Medium (2-3 hours)
**Dependencies:** Phase 09 (need passing test baseline before updating deps)
**Blocks:** None -- this is the final phase

---

## Summary

Update the project's Python dependencies in `requirements.txt` using a tiered, test-after-each-group approach. The project currently pins 84 packages. At least 28 have newer versions available. The most impactful updates are the dbt ecosystem (1.9 to 1.11), duckdb (1.3 to 1.4), and boto3/botocore (security patches).

**This is the riskiest phase in the entire remediation plan** because dependency updates can introduce subtle behavioral changes, deprecations, and API incompatibilities. The mitigation strategy is to update in tiers, running the full test suite after each tier, and rolling back any update that breaks tests.

---

## Files Modified

| # | File | Action |
|---|------|---------|
| 1 | `requirements.txt` | Update version pins for outdated packages |

**No other files should be modified** unless a dependency update introduces a breaking change that requires a code fix. If code changes are needed, document them clearly in the PR description and keep them minimal.

---

## Current Dependency State

### Full `requirements.txt` Snapshot (as of 2026-02-10)

```
agate==1.9.1
altair==5.5.0
annotated-types==0.7.0
attrs==25.3.0
babel==2.17.0
beautifulsoup4==4.13.4
blinker==1.9.0
boto3==1.38.28
botocore==1.38.28
cachetools==5.5.2
certifi==2025.4.26
charset-normalizer==3.4.2
click==8.2.1
colorama==0.4.6
daff==1.4.2
dbt-adapters==1.15.3
dbt-common==1.25.0
dbt-core==1.9.6
dbt-extractor==0.6.0
dbt-postgres==1.9.0
dbt-protos==1.0.317
dbt-semantic-interfaces==0.7.4
deepdiff==7.0.1
gitdb==4.0.12
GitPython==3.1.44
greenlet==3.2.2
idna==3.10
importlib-metadata==6.11.0
isodate==0.6.1
Jinja2==3.1.6
jmespath==1.0.1
jsonschema==4.24.0
jsonschema-specifications==2025.4.1
leather==0.4.0
MarkupSafe==3.0.2
mashumaro==3.14
more-itertools==10.7.0
msgpack==1.1.0
narwhals==1.41.0
networkx==3.5
numpy==2.2.6
ordered-set==4.1.0
packaging==24.2
pandas==2.2.3
parsedatetime==2.6
pathspec==0.12.1
pillow==11.2.1
playwright==1.52.0
plotly==6.1.2
protobuf==5.29.5
psutil==7.0.0
pyarrow==20.0.0
duckdb==1.3.1
dbt-duckdb==1.9.4
pydantic==2.11.5
pydantic_core==2.33.2
pydeck==0.9.1
pyee==13.0.0
python-dateutil==2.9.0.post0
python-dotenv==1.1.0
python-slugify==8.0.4
pytimeparse==1.1.8
pytz==2025.2
PyYAML==6.0.2
referencing==0.36.2
requests==2.32.3
rpds-py==0.25.1
s3transfer==0.13.0
six==1.17.0
smmap==5.0.2
snowplow-tracker==1.1.0
soupsieve==2.7
sqlparse==0.5.3
streamlit==1.45.1
tenacity==9.1.2
text-unidecode==1.3
toml==0.10.2
tornado==6.5.1
typing-inspection==0.4.1
typing_extensions==4.14.0
tzdata==2025.2
urllib3==2.4.0
zipp==3.22.0
```

---

## Three-Tier Update Strategy

### Tier 1: Safe Updates (Low Risk)

Patch-level and minor utility updates with no known breaking changes. These are transport/security libraries, encoding libraries, and small utility packages.

| Package | Current | Action | Category |
|---------|---------|--------|----------|
| `certifi` | 2025.4.26 | Update to latest | Security -- CA certificate bundle |
| `charset-normalizer` | 3.4.2 | Update to latest 3.x | Encoding detection |
| `idna` | 3.10 | Update to latest 3.x | Internationalized domain names |
| `urllib3` | 2.4.0 | Update to latest 2.x | HTTP transport -- security patches |
| `attrs` | 25.3.0 | Update to latest | Utility |
| `click` | 8.2.1 | Update to latest 8.x | CLI framework |
| `gitdb` | 4.0.12 | Update to latest 4.x | Git internal |
| `GitPython` | 3.1.44 | Update to latest 3.x | Git utility |
| `smmap` | 5.0.2 | Update to latest 5.x | Git internal |
| `psutil` | 7.0.0 | Update to latest 7.x | System monitoring |
| `pillow` | 11.2.1 | Update to latest 11.x | Image processing |
| `six` | 1.17.0 | Update to latest | Python 2/3 compatibility |
| `packaging` | 24.2 | Update to latest | Version parsing |
| `typing_extensions` | 4.14.0 | Update to latest 4.x | Typing backports |
| `zipp` | 3.22.0 | Update to latest 3.x | Zipfile utility |
| `pytz` | 2025.2 | Update to latest | Timezone data |
| `tzdata` | 2025.2 | Update to latest | Timezone data |
| `requests` | 2.32.3 | Update to latest 2.x | HTTP client |
| `python-dotenv` | 1.1.0 | Update to latest 1.x | Env file reader |
| `Jinja2` | 3.1.6 | Update to latest 3.x | Templating (used by dbt) |
| `MarkupSafe` | 3.0.2 | Update to latest 3.x | HTML escaping (Jinja dependency) |
| `PyYAML` | 6.0.2 | Update to latest 6.x | YAML parser |

### Tier 2: Moderate Risk Updates (Medium Risk)

Minor version bumps on packages that are central to the project's functionality. These packages are usually backward compatible within a major version, but changes in behavior can occur.

| Package | Current | Action | Category |
|---------|---------|--------|----------|
| `boto3` | 1.38.28 | Update to latest 1.x | AWS SDK -- security + new features |
| `botocore` | 1.38.28 | Update to latest 1.x | AWS SDK core (must match boto3) |
| `s3transfer` | 0.13.0 | Update to latest 0.x | S3 transfer (must match boto3) |
| `duckdb` | 1.3.1 | Update to latest 1.x | Embedded analytics database |
| `pandas` | 2.2.3 | Update to latest 2.x | Data manipulation |
| `numpy` | 2.2.6 | Update to latest 2.x | Numerical computing |
| `pyarrow` | 20.0.0 | Update to latest | Arrow columnar format |
| `streamlit` | 1.45.1 | Update to latest 1.x | Dashboard framework |
| `plotly` | 6.1.2 | Update to latest 6.x | Charting library |
| `pydantic` | 2.11.5 | Update to latest 2.x | Data validation |
| `pydantic_core` | 2.33.2 | Update to latest (must match pydantic) | Pydantic core |
| `playwright` | 1.52.0 | Update to latest 1.x | Browser automation (London extraction) |

### Tier 3: High Risk Updates (Major Version Bumps)

These are major version upgrades that may include breaking API changes. Each must be evaluated against migration guides.

| Package | Current | Target | Category |
|---------|---------|--------|----------|
| `dbt-core` | 1.9.6 | Latest | BREAKING -- read migration guide |
| `dbt-duckdb` | 1.9.4 | Latest (must match dbt-core) | BREAKING -- adapter compatibility |
| `dbt-adapters` | 1.15.3 | Latest (must match dbt-core) | dbt dependency |
| `dbt-common` | 1.25.0 | Latest (must match dbt-core) | dbt dependency |
| `dbt-protos` | 1.0.317 | Latest (must match dbt-core) | dbt dependency |
| `dbt-postgres` | 1.9.0 | Latest (must match dbt-core) | dbt dependency |
| `dbt-semantic-interfaces` | 0.7.4 | Latest | dbt dependency |
| `dbt-extractor` | 0.6.0 | Latest | dbt dependency |
| `agate` | 1.9.1 | Latest (dbt dependency) | Tabular data library |
| `deepdiff` | 7.0.1 | Latest | Object comparison (check API changes) |

---

## Implementation Steps

### Step 0: Establish Baseline (CRITICAL -- Do This First)

Before changing ANYTHING, run the full test suite and save the output. This is your baseline.

```bash
# Activate the virtual environment
source venv/bin/activate

# Run the full test suite and save output
python -m pytest tests/ -v > test_baseline_output.txt 2>&1

# Print the summary line
tail -5 test_baseline_output.txt
```

**Expected result:** All tests from Phase 09 are passing (approximately 123 tests: ~79 old + ~44 new). Save the exact pass/skip/fail counts -- you will compare against these after each tier.

Also save the current pip freeze:
```bash
pip freeze > pip_freeze_before.txt
```

---

### Step 1: Update Tier 1 Packages (Low Risk)

These are safe to update all at once.

```bash
pip install --upgrade \
    certifi \
    charset-normalizer \
    idna \
    urllib3 \
    attrs \
    click \
    gitdb \
    GitPython \
    smmap \
    psutil \
    pillow \
    six \
    packaging \
    typing_extensions \
    zipp \
    pytz \
    tzdata \
    requests \
    python-dotenv \
    Jinja2 \
    MarkupSafe \
    PyYAML
```

**After installing, run the full test suite:**

```bash
python -m pytest tests/ -v
```

**Check the results:**
- If ALL tests pass: Continue to Step 2.
- If ANY test fails: Identify the failing package by checking the error message. Downgrade that specific package back to its previous version:
  ```bash
  pip install <package>==<previous_version>
  ```
  Then re-run tests. Document the incompatibility.

**Save the new versions:**
```bash
pip freeze > pip_freeze_after_tier1.txt
```

---

### Step 2: Update Tier 2 Packages (One Group at a Time)

**IMPORTANT:** Update these packages ONE GROUP at a time. Run the test suite after EACH group. If a group breaks tests, roll back that group before proceeding.

#### Group 2a: AWS SDK (boto3 + botocore + s3transfer)

These three packages MUST be updated together. boto3 and botocore versions must match.

```bash
pip install --upgrade boto3 botocore s3transfer
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

If tests pass, continue. If not, roll back:
```bash
pip install boto3==1.38.28 botocore==1.38.28 s3transfer==0.13.0
```

#### Group 2b: DuckDB

```bash
pip install --upgrade duckdb
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

**Additional verification:** DuckDB is the project's core database. After updating, also verify:
```bash
python -c "import duckdb; print(duckdb.__version__); conn = duckdb.connect(':memory:'); print(conn.execute('SELECT 42').fetchone())"
```

If tests pass, continue. If not, roll back:
```bash
pip install duckdb==1.3.1
```

**What to watch for:**
- DuckDB occasionally changes SQL syntax or function behavior between minor versions
- Check if `DESCRIBE`, `SHOW TABLES`, `information_schema` queries still work
- The `DuckDBManager` tests (from Phase 09) will catch most issues

#### Group 2c: pandas + numpy

These often need to be updated together since pandas depends on numpy.

```bash
pip install --upgrade pandas numpy
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

If tests pass, continue. If not, roll back:
```bash
pip install pandas==2.2.3 numpy==2.2.6
```

**What to watch for:**
- pandas 2.x sometimes changes default dtypes (e.g., nullable integer types)
- The `data_models` tests will catch schema validation issues
- The `test_extracted_file_manager_current.py` tests will catch DataFrame transformation issues

#### Group 2d: pyarrow

```bash
pip install --upgrade pyarrow
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

If tests pass, continue. If not, roll back:
```bash
pip install pyarrow==20.0.0
```

**What to watch for:**
- pyarrow schema inference changes
- Parquet read/write compatibility
- The `test_dashboard.py` Parquet file tests will catch read issues

#### Group 2e: streamlit + plotly

```bash
pip install --upgrade streamlit plotly
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

**Additional verification:** Manually start the dashboard and verify it loads:
```bash
streamlit run dashboard/app.py
```
Check that charts render, date pickers work, and no deprecation warnings appear in the terminal.

If tests pass, continue. If not, roll back:
```bash
pip install streamlit==1.45.1 plotly==6.1.2
```

**What to watch for:**
- Streamlit API changes (session state, layout, widgets)
- Plotly chart API changes
- The dashboard tests from Phase 09 are lightweight and may not catch all UI issues -- manual verification is important here

#### Group 2f: pydantic + pydantic_core

```bash
pip install --upgrade pydantic pydantic_core
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

If tests pass, continue. If not, roll back:
```bash
pip install pydantic==2.11.5 pydantic_core==2.33.2
```

#### Group 2g: playwright

```bash
pip install --upgrade playwright
python -m playwright install
```

**Note:** Playwright requires a separate browser binary download after updating the pip package. The `python -m playwright install` command handles this.

**Run tests:**
```bash
python -m pytest tests/ -v
```

If tests pass, continue. If not, roll back:
```bash
pip install playwright==1.52.0
python -m playwright install
```

---

### Step 3: Evaluate Tier 3 Packages (dbt Ecosystem)

**This step requires careful evaluation.** The dbt ecosystem update is the riskiest part of this entire remediation plan.

#### 3a: Research Before Updating

Before running any pip commands, read the following:

1. **dbt-core migration guide** -- Go to https://docs.getdbt.com/ and find the migration guide for your target version. Look specifically for:
   - Deprecated features that were removed
   - Changes to `dbt_project.yml` syntax
   - Changes to profiles.yml format
   - Changes to Jinja macro behavior
   - Changes to incremental model strategy

2. **dbt-duckdb compatibility** -- Check the dbt-duckdb GitHub repository (https://github.com/duckdb/dbt-duckdb) to verify which version of dbt-core the latest dbt-duckdb supports.

3. **Known issues** -- Search GitHub Issues on both repos for the target version.

#### 3b: Attempt the Update

**Only proceed if the migration guides indicate backward compatibility for our use case.** Our dbt usage is relatively standard (incremental models, staging/intermediate/marts pattern, DuckDB adapter).

```bash
pip install --upgrade dbt-core dbt-duckdb dbt-adapters dbt-common dbt-protos dbt-postgres dbt-semantic-interfaces dbt-extractor agate
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

**Run dbt compile to verify dbt configuration is still valid:**
```bash
cd dbt_city_cycles
dbt compile
```

**Expected:** `dbt compile` should succeed with no errors. If it shows deprecation warnings, document them but do not treat them as blockers.

**If you have a database available, run a full dbt build:**
```bash
cd dbt_city_cycles
dbt run --full-refresh
dbt test
```

#### 3c: What To Do If dbt Update Fails

If the dbt update breaks compilation, tests, or model execution:

1. **Roll back immediately:**
   ```bash
   pip install \
       dbt-core==1.9.6 \
       dbt-duckdb==1.9.4 \
       dbt-adapters==1.15.3 \
       dbt-common==1.25.0 \
       dbt-protos==1.0.317 \
       dbt-postgres==1.9.0 \
       dbt-semantic-interfaces==0.7.4 \
       dbt-extractor==0.6.0 \
       agate==1.9.1
   ```

2. **Document the failure** in the PR description:
   - What version was attempted
   - What error occurred
   - What the migration guide says about the issue

3. **Defer the dbt update to a separate PR** with its own investigation. This PR should still ship with the Tier 1 and Tier 2 updates.

#### 3d: deepdiff Update

```bash
pip install --upgrade deepdiff
```

**Run tests:**
```bash
python -m pytest tests/ -v
```

If tests pass, keep. If not, roll back:
```bash
pip install deepdiff==7.0.1
```

---

### Step 4: Update `requirements.txt`

After all tiers are complete and tests pass, generate the final requirements:

```bash
pip freeze > pip_freeze_final.txt
```

**Do NOT just copy `pip freeze` output directly into `requirements.txt`.** The project's `requirements.txt` contains only direct dependencies, while `pip freeze` includes all transitive dependencies. Instead:

1. Open the current `requirements.txt` in your editor.
2. For each package in `requirements.txt`, look up its version in `pip_freeze_final.txt`.
3. Update the version pin in `requirements.txt` to match.
4. Keep the exact same format: `package==version` (with `==` pinning).
5. Keep the same package ordering as the current file.

**Example diff for a single package:**
```diff
- certifi==2025.4.26
+ certifi==2026.1.31
```

**Verify the updated `requirements.txt` is installable:**
```bash
# Create a fresh virtual environment to test
python -m venv /tmp/test_venv
source /tmp/test_venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/ -v
deactivate
rm -rf /tmp/test_venv
```

---

### Step 5: Final Verification

Run the following checks to confirm everything works end-to-end:

#### 5a: Full Test Suite
```bash
python -m pytest tests/ -v
```
**Expected:** Same pass/skip/fail counts as the baseline from Step 0.

#### 5b: dbt Compile
```bash
cd dbt_city_cycles && dbt compile
```
**Expected:** Compilation succeeds. Warnings about future deprecations are acceptable.

#### 5c: Dashboard Loads
```bash
streamlit run dashboard/app.py
```
**Expected:** Dashboard starts without errors. Charts render. Date pickers work. Verify in browser manually for 1-2 minutes.

#### 5d: Import Smoke Tests
```bash
python -c "import duckdb; print(f'duckdb {duckdb.__version__}')"
python -c "import pandas; print(f'pandas {pandas.__version__}')"
python -c "import boto3; print(f'boto3 {boto3.__version__}')"
python -c "import streamlit; print(f'streamlit {streamlit.__version__}')"
python -c "import pydantic; print(f'pydantic {pydantic.__version__}')"
python -c "import dbt.version; print(f'dbt-core {dbt.version.installed}')"
```
**Expected:** All print their updated versions without errors.

#### 5e: No Deprecation Warnings
```bash
python -W all -m pytest tests/ -v 2>&1 | grep -i "deprecat"
```
**Expected:** Ideally zero deprecation warnings. If any appear, document them in the PR description as future work. They are NOT blockers for this PR.

---

## Rollback Plan

If this PR causes problems after merging:

1. **Revert the PR:**
   ```bash
   git revert <merge-commit-sha>
   ```

2. **Reinstall old dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify tests pass with old dependencies.**

The rollback is safe because this PR only modifies `requirements.txt` -- no source code changes.

---

## What NOT To Do

- **Do NOT update ALL packages at once** -- follow the tiered approach
- **Do NOT update dbt-core without reading the migration guide** for the target version
- **Do NOT force a Tier 3 update that breaks things** -- defer it to a separate PR
- **Do NOT remove any packages from `requirements.txt`** -- only update versions
- **Do NOT add new packages** -- this PR is version updates only
- **Do NOT change the `requirements.txt` format** -- keep `==` pinning
- **Do NOT change from `==` to `>=` or `~=`** -- strict pinning prevents drift
- **Do NOT upgrade to Python 3.14+** -- the project runs on Python 3.13.3
- **Do NOT skip running tests between tier groups** -- test-after-each-group is the safety net
- **Do NOT delete `pip_freeze_before.txt`** until the PR is merged and verified in production
- **Do NOT update `dbt-postgres`** if it creates a conflict with `dbt-duckdb` -- the project uses DuckDB, not Postgres; `dbt-postgres` is likely a transitive dependency that should match whatever dbt-core expects

---

## Decision Log Template

Use this table to track what happened during the update process. Fill it in as you go and include it in the PR description.

| Tier | Group | Package(s) | Previous Version | New Version | Tests Pass? | Notes |
|------|-------|-----------|-----------------|-------------|-------------|-------|
| 1 | All | certifi, charset-normalizer, ... | (various) | (various) | Yes/No | |
| 2 | 2a | boto3, botocore, s3transfer | 1.38.28 | ? | Yes/No | |
| 2 | 2b | duckdb | 1.3.1 | ? | Yes/No | |
| 2 | 2c | pandas, numpy | 2.2.3, 2.2.6 | ? | Yes/No | |
| 2 | 2d | pyarrow | 20.0.0 | ? | Yes/No | |
| 2 | 2e | streamlit, plotly | 1.45.1, 6.1.2 | ? | Yes/No | |
| 2 | 2f | pydantic, pydantic_core | 2.11.5, 2.33.2 | ? | Yes/No | |
| 2 | 2g | playwright | 1.52.0 | ? | Yes/No | |
| 3 | 3b | dbt-core, dbt-duckdb, ... | 1.9.6, 1.9.4 | ? | Yes/No | |
| 3 | 3d | deepdiff | 7.0.1 | ? | Yes/No | |

---

## PR Checklist

- [ ] Baseline test results saved (`test_baseline_output.txt`)
- [ ] Baseline pip freeze saved (`pip_freeze_before.txt`)
- [ ] Tier 1 packages updated and tests pass
- [ ] Tier 2 packages updated one group at a time, tests pass after each group
- [ ] Tier 3 packages evaluated (updated OR deferred with documentation)
- [ ] `requirements.txt` updated with new version pins
- [ ] `python -m pytest tests/ -v` passes (same counts as baseline)
- [ ] `dbt compile` succeeds (in `dbt_city_cycles/` directory)
- [ ] Dashboard loads and renders charts (manual check)
- [ ] Import smoke tests pass for all major packages
- [ ] Decision log filled in with results for each tier/group
- [ ] `git diff` shows only `requirements.txt` modified (unless code fixes were needed)
- [ ] CHANGELOG.md updated with entry under `[Unreleased]`

### CHANGELOG Entry

```markdown
### Technical Improvements
- **Dependency Updates** - Updated outdated Python packages across three risk tiers
  - Tier 1 (safe): Updated XX security/utility packages (certifi, urllib3, requests, etc.)
  - Tier 2 (moderate): Updated core dependencies (boto3, duckdb, pandas, streamlit, etc.)
  - Tier 3 (high risk): [Updated dbt ecosystem from 1.9 to X.X / Deferred dbt update to separate PR]
  - All existing tests pass after updates
  - No breaking changes introduced
```

(Replace `XX` and version numbers with actual values after completing the updates.)
