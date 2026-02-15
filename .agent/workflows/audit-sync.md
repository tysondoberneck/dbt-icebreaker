---
description: Audit sync - verify sync manager, ledger, verification, and retry logic
---

# Audit: Sync

Verify the sync subsystem is correct and handles edge cases.

## 1. Run sync-related tests

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m pytest tests/ -k "sync" -v --tb=short 2>&1 || echo "No sync-specific tests found"
```

## 2. Check overlap between sync_manager.py and warehouse_sync.py

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== warehouse_sync.py ===" && grep -n 'def \|class ' dbt/adapters/icebreaker/warehouse_sync.py; echo -e "\n=== sync_manager.py ===" && grep -n 'def \|class ' dbt/adapters/icebreaker/sync_manager.py; echo -e "\n=== Cross-references ===" && grep -n 'warehouse_sync\|sync_manager' dbt/adapters/icebreaker/*.py | grep -v __pycache__
```

## 3. Verify SyncLedger schema

Check the ledger creates all necessary columns:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && sed -n '/_ensure_db/,/^    def /p' dbt/adapters/icebreaker/sync_manager.py
```

## 4. Check retry logic parameters

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== SyncConfig defaults ===" && sed -n '/class SyncConfig/,/^$/p' dbt/adapters/icebreaker/sync_manager.py; echo -e "\n=== Retry loop ===" && grep -n 'retry\|attempt\|max_retries' dbt/adapters/icebreaker/sync_manager.py
```

## 5. Check topological sort handles cycles

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && sed -n '/_topological_sort/,/^    def \|^class /p' dbt/adapters/icebreaker/sync_manager.py | head -50
```

## 6. Check row count verification logic

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n 'verify\|row_count\|_get_row_count' dbt/adapters/icebreaker/sync_manager.py | head -15
```

## 7. Verify sync is properly integrated into connections.py

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n 'sync\|SyncManager\|warehouse_sync' dbt/adapters/icebreaker/connections.py | head -15
```

## 8. Report

Summarize:
- **Test coverage**: sync tests pass/fail or missing
- **Module overlap**: redundancy between warehouse_sync and sync_manager
- **Ledger schema**: all columns present
- **Retry logic**: reasonable parameters (max retries, delay)
- **Cycle handling**: topological sort handles DAG cycles
- **Verification**: row count checks present
- **Integration**: sync properly called from connections.py
- **Assessment**: PASS / WARN / FAIL
