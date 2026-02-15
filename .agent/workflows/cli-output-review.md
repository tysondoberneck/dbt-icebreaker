---
description: Review and clean up CLI output to be professional and modern using rich
---

# CLI Output Review & Cleanup

This workflow guides the process of auditing and improving the CLI output of dbt-icebreaker.

## 1. Audit Current Output

Grep for all raw `print()` calls across the adapter:

```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker
grep -rn 'print(' dbt/adapters/icebreaker/*.py | grep -v '__pycache__'
```

Categorize each print into:
- **User-facing** (sync results, caching status, errors) → keep, style via `console.py`
- **Debug/verbose** (transpilation logs, internal routing) → hide by default
- **Noise** (duplicate logs, overly detailed) → remove or collapse

## 2. Console Module (`console.py`)

The centralized output module lives at `dbt/adapters/icebreaker/console.py` and uses the `rich` library.

**Message levels:**

| Function | Symbol | Color | When to use |
|----------|--------|-------|-------------|
| `console.info()` | (dim) | dim | Background context (loaded manifest, loaded env) |
| `console.success()` | `✓` | green | Completed actions (cached, synced, connected) |
| `console.warn()` | `!` | yellow | Non-fatal issues (sync skip, missing config) |
| `console.error()` | `✗` | red | Failures (connection failed, cache failed) |
| `console.step()` | `›` | cyan | In-progress actions (downloading, transpiling) |

**Verbosity** is controlled by `ICEBREAKER_VERBOSITY` env var:
- `quiet` — errors and final summary only
- `normal` (default) — success, warnings, errors, summary
- `verbose` — everything including transpilation and debug

**Rich features to use:**
- `rich.console.Console` with custom `Theme` for styled output
- `rich.panel.Panel` for boxed summaries (run summary, savings report)
- `rich.table.Table` for structured data (routing breakdown, model results)
- `rich.progress.Progress` with spinner for source downloads

## 3. Replace Print Calls

For each file, replace `print()` with the appropriate `console` function:

### Priority 1 — Highest Impact (connections.py, source_cache.py)
- `connections.py`: ~25 print calls — this is where most runtime output comes from
- `source_cache.py`: ~10 print calls — caching output

### Priority 2 — Summaries (run_summary.py, savings.py)
- Replace hand-crafted ASCII art with `rich.panel.Panel` and `rich.table.Table`

### Priority 3 — Supporting Files
- `snowflake_helper.py`, `impl.py`, `sync_manager.py`, `warehouse_sync.py`
- `cli.py`, `catalog_reader.py`, `catalog_scanner.py`, `metadata.py`

## 4. Verify

Run existing tests:
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && pytest -v
```

Test with a real dbt run:
```bash
cd /Users/tysondoberneck/codebase/data-model && source env/bin/activate
dbt run -s +ad_aggregators --profile icebreaker_dev
```

Test verbose mode:
```bash
ICEBREAKER_VERBOSITY=verbose dbt run -s +ad_aggregators --profile icebreaker_dev
```

Confirm no raw prints remain:
```bash
grep -rn 'print(' dbt/adapters/icebreaker/*.py | grep -v '__pycache__' | grep -v 'console.py'
```

## 5. Commit
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker
git add -A && git commit -m "feat: redesign CLI output with rich-based console module"
```
