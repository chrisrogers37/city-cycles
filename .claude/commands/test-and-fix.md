---
description: "Run tests and fix any failures"
---

Follow this workflow:

1. **Run the full test suite:**
   ```bash
   python -m pytest tests/ -v
   ```

2. **Analyze failures:**
   - Review error messages and stack traces
   - Identify root causes (logic errors, schema changes, etc.)
   - Check if test data needs updating

3. **Fix the issues:**
   - Make necessary code changes
   - Update tests if requirements changed
   - Add new tests for uncovered cases

4. **Verify fixes:**
   - Re-run tests: `python -m pytest tests/ -v`
   - Ensure all tests pass before completing

5. **Report results:**
   - Summarize what was broken
   - Explain what was fixed
   - Note any new tests added

## Common Test Categories

- **Data Model Tests:** Schema validation, field types, required columns
- **Pipeline Tests:** Orchestrator stages, file processing, idempotency
- **Integration Tests:** S3 operations, DuckDB queries, dbt transformations
- **Utility Tests:** Helper functions, data transformations

If tests still fail after fixes, explain why and ask for guidance.
