#!/usr/bin/env bash
set -euo pipefail

echo "========================================"
echo "City Cycles - Weather Extraction"
echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "========================================"

python -m orchestrator.cli stage weather_extraction && EXIT_CODE=0 || EXIT_CODE=$?

echo "========================================"
echo "Finished at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Exit code: $EXIT_CODE"
echo "========================================"

exit $EXIT_CODE
