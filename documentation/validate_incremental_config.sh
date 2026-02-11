#!/bin/bash
# Validate that all dbt models have proper incremental configuration

echo "======================================"
echo "Validating Incremental Configuration"
echo "======================================"
echo ""

cd "$(dirname "$0")/../dbt_city_cycles/models"

echo "✓ Checking staging models..."
for file in staging/*.sql; do
    if grep -q "materialized='incremental'" "$file"; then
        if grep -q "unique_key" "$file"; then
            echo "  ✓ $(basename $file): incremental with unique_key"
        else
            echo "  ⚠ $(basename $file): incremental but MISSING unique_key"
        fi
    else
        echo "  ✗ $(basename $file): NOT incremental"
    fi
done

echo ""
echo "✓ Checking intermediate models..."
for file in intermediate/*.sql; do
    if grep -q "materialized='incremental'" "$file"; then
        if grep -q "unique_key" "$file"; then
            echo "  ✓ $(basename $file): incremental with unique_key"
        else
            echo "  ⚠ $(basename $file): incremental but MISSING unique_key"
        fi
    else
        echo "  ✗ $(basename $file): NOT incremental"
    fi
done

echo ""
echo "✓ Checking unified model..."
for file in unified/*.sql; do
    if grep -q "materialized='incremental'" "$file"; then
        if grep -q "unique_key" "$file"; then
            echo "  ✓ $(basename $file): incremental with unique_key"
        else
            echo "  ⚠ $(basename $file): incremental but MISSING unique_key"
        fi
    else
        echo "  ✗ $(basename $file): NOT incremental"
    fi
done

echo ""
echo "✓ Checking mart models (should be 'table' not incremental)..."
for file in marts/*.sql; do
    if grep -q "materialized='table'" "$file"; then
        echo "  ✓ $(basename $file): table (correct for marts)"
    elif grep -q "materialized='incremental'" "$file"; then
        echo "  ⚠ $(basename $file): incremental (unexpected for mart)"
    else
        echo "  ? $(basename $file): unknown materialization"
    fi
done

echo ""
echo "======================================"
echo "✓ Validation Complete"
echo "======================================"
echo ""
echo "All models should have:"
echo "  - Staging/Intermediate/Unified: materialized='incremental' + unique_key"
echo "  - Marts: materialized='table' (they rebuild each time)"
echo ""

