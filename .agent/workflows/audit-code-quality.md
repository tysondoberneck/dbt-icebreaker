---
description: Audit code quality - linting, type checking, dead code, and raw prints
---

# Audit: Code Quality

Static analysis across the adapter codebase.

## 1. Ruff linting

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m ruff check dbt/ tests/ --output-format=concise 2>&1 | tail -40 || echo "(ruff not installed — skip)"
```

## 2. Type checking with mypy

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m mypy dbt/adapters/icebreaker/ --ignore-missing-imports --no-error-summary 2>&1 | tail -40 || echo "(mypy not installed — skip)"
```

## 3. Raw print() calls (should use console.py)

Scan for `print()` calls outside of `console.py` and test files:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -rn 'print(' dbt/adapters/icebreaker/*.py | grep -v '__pycache__' | grep -v 'console.py' | grep -v '# noqa' || echo "✓ No raw print() calls found"
```

## 4. TODO / FIXME / HACK comments

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -rn 'TODO\|FIXME\|HACK\|XXX\|TEMP' dbt/adapters/icebreaker/*.py | grep -v '__pycache__' || echo "✓ No TODO/FIXME/HACK comments found"
```

## 5. Unused imports

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m ruff check dbt/adapters/icebreaker/ --select F401 --output-format=concise 2>&1 || echo "(ruff not installed — skip)"
```

## 6. Report

Summarize:
- **Ruff issues**: count and severity
- **Type errors**: count from mypy
- **Raw prints**: count of print() calls not using console.py
- **TODO/FIXME**: count and locations
- **Unused imports**: list
- **Assessment**: PASS if no critical issues, WARN if minor, FAIL if blocking
