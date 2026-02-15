---
description: Audit routing - verify routing logic consistency between auto_router and traffic
---

# Audit: Routing Logic

Verify the routing engine is correct, consistent, and complete.

## 1. Run routing tests

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && python -m pytest tests/test_auto_router.py tests/test_traffic.py -v --tb=short 2>&1
```

## 2. Compare CLOUD_ONLY_FUNCTIONS across modules

Check that cloud-only function lists are consistent:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== auto_router.py ===" && grep -A30 'CLOUD_ONLY_FUNCTIONS' dbt/adapters/icebreaker/auto_router.py | head -35; echo -e "\n=== traffic.py ===" && grep -A30 'CLOUD_ONLY\|BLACKLIST\|_UNSUPPORTED' dbt/adapters/icebreaker/traffic.py | head -35; echo -e "\n=== transpiler.py ===" && grep -A30 'BLACKLISTED\|_UNSUPPORTED' dbt/adapters/icebreaker/transpiler.py | head -35
```

## 3. Compare RoutingReason enums

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "=== auto_router.py RoutingReason ===" && sed -n '/class RoutingReason/,/^$/p' dbt/adapters/icebreaker/auto_router.py; echo -e "\n=== traffic.py RoutingReason ===" && sed -n '/class RoutingReason/,/^$/p' dbt/adapters/icebreaker/traffic.py
```

## 4. Identify which router is actually used at runtime

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && echo "--- Imports of auto_router ---" && grep -rn 'from.*auto_router import\|import.*auto_router' dbt/adapters/icebreaker/*.py | grep -v __pycache__; echo -e "\n--- Imports of traffic ---" && grep -rn 'from.*traffic import\|import.*traffic' dbt/adapters/icebreaker/*.py | grep -v __pycache__
```

## 5. Verify 6-gate completeness in traffic.py

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n '_gate_' dbt/adapters/icebreaker/traffic.py
```

## 6. Check for routing decision logging

Verify routing decisions are tracked/logged for debugging:

// turbo
```bash
cd /Users/tysondoberneck/codebase/dbt-icebreaker && grep -n 'console\.\|logging\.\|logger\.' dbt/adapters/icebreaker/traffic.py dbt/adapters/icebreaker/auto_router.py | head -20
```

## 7. Report

Summarize:
- **Test results**: pass/fail
- **Function list consistency**: are CLOUD_ONLY lists in sync across modules?
- **Enum duplication**: RoutingReason defined in multiple places
- **Active router**: which module is used at runtime
- **6-gate completeness**: all gates implemented
- **Assessment**: PASS / WARN / FAIL
