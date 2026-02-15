---
description: Audit transpiler - validate SQL transpilation correctness and coverage
---

# Audit: Transpiler

Verify the Snowflake→DuckDB SQL transpiler is correct and comprehensive.

## 1. Run transpiler tests

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m pytest tests/test_transpiler.py tests/test_variant_handling.py -v --tb=short 2>&1
```

## 2. List all transform methods defined in transpiler.py

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n 'def _transform_' dbt/adapters/icebreaker/transpiler.py
```

## 3. Check which transforms have test coverage

For each `_transform_*` method, check if there is a corresponding test:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== Transforms in transpiler.py ===" && grep 'def _transform_' dbt/adapters/icebreaker/transpiler.py | sed 's/.*_transform_//' | sed 's/(.*//'; echo -e "\n=== Test functions ===" && grep 'def test_' tests/test_transpiler.py tests/test_variant_handling.py | sed 's/.*def //' | sed 's/(.*//'; echo -e "\n=== Untested transforms ===" && comm -23 <(grep 'def _transform_' dbt/adapters/icebreaker/transpiler.py | sed 's/.*_transform_//' | sed 's/(.*//') <(grep -h 'def test_' tests/test_transpiler.py tests/test_variant_handling.py | sed 's/.*def test_//' | sed 's/(.*//' | sort -u) 2>/dev/null || echo "(comparison skipped)"
```

## 4. Verify DIALECT_MAP is complete

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -A5 'DIALECT_MAP' dbt/adapters/icebreaker/transpiler.py
```

## 5. Check blacklisted functions list

Ensure the blacklist is up-to-date and not overly broad:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -A30 'BLACKLISTED_FUNCTIONS\|_BLACKLISTED\|blacklist' dbt/adapters/icebreaker/transpiler.py | head -40
```

## 6. Verify FLATTEN is NOT in the blacklist

FLATTEN should have been removed from the blacklist since it's now transpiled to UNNEST:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -c "
from dbt.adapters.icebreaker.transpiler import Transpiler
t = Transpiler()
bl = t.detect_blacklisted_functions('SELECT f.value FROM t, FLATTEN(input => arr) f')
if 'FLATTEN' in [b.upper() for b in bl]:
    print('✗ FLATTEN is still blacklisted — should be removed')
else:
    print('✓ FLATTEN correctly removed from blacklist')
" 2>&1
```

## 7. Report

Summarize:
- **Test results**: pass/fail count
- **Transform coverage**: transforms with and without tests
- **Blacklist accuracy**: any functions that should/shouldn't be there
- **Assessment**: PASS if all transforms tested and blacklist accurate, WARN if gaps exist
