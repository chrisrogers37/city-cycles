---
description: "Run orchestrator pipeline with validation"
---

Execute the City Cycles data pipeline with comprehensive validation:

## Pre-Flight Checks

1. **Verify environment:**
   ```bash
   # Check Python environment
   which python
   python --version

   # Check required packages
   pip show dbt-duckdb boto3 pandas
   ```

2. **Check AWS credentials:**
   ```bash
   # Verify .env file exists (don't print contents)
   ls -la .env
   ```

## Pipeline Execution

3. **Choose execution mode:**

   **Option A: Full pipeline run (monthly)**
   ```bash
   python -m orchestrator.cli run
   ```

   **Option B: Full refresh (quarterly)**
   ```bash
   python -m orchestrator.cli run --dbt-full-refresh
   ```

   **Option C: Individual stage**
   ```bash
   # Run specific stage only
   python -m orchestrator.cli stage extraction
   python -m orchestrator.cli stage weather_extraction
   python -m orchestrator.cli stage file_management
   python -m orchestrator.cli stage database_load
   python -m orchestrator.cli stage dbt
   python -m orchestrator.cli stage export
   ```

## Post-Run Validation

4. **Check pipeline status:**
   ```bash
   python -m orchestrator.cli status
   ```

5. **Verify data quality:**
   - Check for log errors
   - Verify row counts make sense
   - Run dbt tests: `cd dbt_city_cycles && dbt test`

6. **Report results:**
   - Pipeline stage completion status
   - Any errors or warnings encountered
   - Data volume processed
   - Recommendations for next steps

If any stage fails, stop and report the error with context.
