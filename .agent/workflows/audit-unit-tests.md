---
description: Audit unit tests - run pytest suite and validate all tests pass
---

# Audit: Unit Tests

Run the full pytest suite and report results.

## 1. Run the test suite

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m pytest tests/ -v --tb=short 2>&1 | tail -60
```

## 2. Check for skipped or xfail tests

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -rn '@pytest.mark.skip\|@pytest.mark.xfail\|pytest.skip(' tests/ || echo "✓ No skipped tests found"
```

## 3. Check test coverage gaps

Identify adapter modules that have NO corresponding test file:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && for f in dbt/adapters/icebreaker/*.py; do
  base=$(basename "$f" .py)
  [[ "$base" == "__init__" || "$base" == "__version__" || "$base" == "__pycache__" ]] && continue
  if [ ! -f "tests/test_${base}.py" ]; then
    echo "⚠ Missing test file: tests/test_${base}.py (for $f)"
  fi
done
echo "---"
echo "Existing test files:"
ls tests/test_*.py
```

## 4. Report

Summarize:
- **Total tests**: count from pytest output
- **Passed / Failed / Skipped**: breakdown
- **Coverage gaps**: modules without test files
- **Assessment**: PASS if all tests pass, FAIL otherwise
