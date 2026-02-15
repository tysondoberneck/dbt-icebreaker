---
description: Audit error handling - check exception coverage, bare excepts, and error propagation
---

# Audit: Error Handling

Verify error handling is robust and follows project conventions.

## 1. List all custom exception classes

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -rn 'class.*Error\|class.*Exception' dbt/adapters/icebreaker/errors.py
```

## 2. Check which custom errors are actually raised

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== Custom errors defined ===" && grep 'class ' dbt/adapters/icebreaker/errors.py | sed 's/class //' | sed 's/(.*//'; echo -e "\n=== Custom errors used ===" && grep -rh 'raise.*Error\|raise.*Exception' dbt/adapters/icebreaker/*.py | grep -v __pycache__ | grep -v 'errors.py' | sort -u
```

## 3. Find bare except clauses

Bare `except:` or overly broad `except Exception` without re-raise:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n 'except:$\|except Exception:$\|except Exception as' dbt/adapters/icebreaker/*.py | grep -v __pycache__ || echo "✓ No bare except clauses found"
```

## 4. Check exception handling patterns

Look for swallowed exceptions (except + pass/continue):

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -c "
import re, os, glob
for f in sorted(glob.glob('dbt/adapters/icebreaker/*.py')):
    if '__pycache__' in f:
        continue
    lines = open(f).readlines()
    for i, line in enumerate(lines):
        if re.search(r'except.*:', line.strip()):
            if i + 1 < len(lines):
                next_line = lines[i+1].strip()
                if next_line in ('pass', 'continue', '...'):
                    print(f'⚠ {f}:{i+1} — swallowed exception: {line.strip()} → {next_line}')
print('--- Done ---')
" 2>&1
```

## 5. Verify _is_duckdb_incompatibility patterns

Check the fallback detection is comprehensive:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && sed -n '/_is_duckdb_incompatibility/,/^    def /p' dbt/adapters/icebreaker/connections.py | head -30
```

## 6. Verify all errors have suggestions

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -c "
import ast, sys
tree = ast.parse(open('dbt/adapters/icebreaker/errors.py').read())
for node in ast.walk(tree):
    if isinstance(node, ast.ClassDef) and 'Error' in node.name:
        has_suggestion = any(
            isinstance(n, ast.keyword) and n.arg == 'suggestion'
            for n in ast.walk(node)
        )
        status = '✓' if has_suggestion else '⚠ missing suggestion'
        print(f'{status}: {node.name}')
" 2>&1
```

## 7. Report

Summarize:
- **Custom errors**: count defined vs count used
- **Orphaned errors**: defined but never raised
- **Bare excepts**: count and locations
- **Swallowed exceptions**: count and locations
- **Suggestion coverage**: errors without actionable suggestions
- **Assessment**: PASS / WARN / FAIL
