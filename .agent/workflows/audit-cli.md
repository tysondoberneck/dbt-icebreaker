---
description: Audit CLI - verify command reachability, help text, and console usage
---

# Audit: CLI

Verify the CLI is complete, well-documented, and uses the console module.

## 1. List all CLI commands

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n "def cmd_\|add_parser\|'[a-z]*'" dbt/adapters/icebreaker/cli.py | head -40
```

## 2. Verify CLI entry point

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== setup.cfg / pyproject.toml entry points ===" && grep -r 'console_scripts\|entry_points\|icebreaker' pyproject.toml setup.cfg setup.py 2>/dev/null || echo "(no entry point config found — check __init__.py)"; echo -e "\n=== __init__.py ===" && cat dbt/adapters/icebreaker/__init__.py
```

## 3. Check for raw print() in CLI

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n 'print(' dbt/adapters/icebreaker/cli.py | grep -v 'console\.' | grep -v '# noqa' || echo "✓ No raw print() calls in CLI"
```

## 4. Check argparse help text coverage

Verify all subcommands have help text:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -c "
import re
content = open('dbt/adapters/icebreaker/cli.py').read()
# Find add_parser calls without help=
parsers = re.findall(r'add_parser\([^)]+\)', content)
for p in parsers:
    if 'help=' not in p:
        print(f'⚠ Missing help text: {p}')
    else:
        print(f'✓ {p[:60]}...')
if not parsers:
    # Check for add_subparsers pattern
    subs = re.findall(r\"subparsers\.add_parser\(['\"](\w+)['\"]\", content)
    for s in subs:
        print(f'  Command: {s}')
print('--- Done ---')
" 2>&1
```

## 5. Check that all cmd_* functions are wired into main()

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== cmd_* functions defined ===" && grep 'def cmd_' dbt/adapters/icebreaker/cli.py | sed 's/def //' | sed 's/(.*//'; echo -e "\n=== cmd_* functions called in main() ===" && sed -n '/def main/,/^def /p' dbt/adapters/icebreaker/cli.py | grep 'cmd_' | sed 's/.*\(cmd_[a-z_]*\).*/\1/' | sort -u
```

## 6. Report

Summarize:
- **Commands found**: list of all CLI commands
- **Entry point**: configured correctly
- **Raw prints**: any print() bypassing console.py
- **Help text**: commands missing help descriptions
- **Unreachable commands**: cmd_* functions not wired into main()
- **Assessment**: PASS / WARN / FAIL
