---
description: Run all audit workflows for dbt-icebreaker and produce a demo-readiness report
---

# Full Audit: dbt-icebreaker

Run all 8 audit workflows in sequence and produce a consolidated report.

> **Important**: This is the master orchestrator. Run `/audit-all` to execute everything below.

---

## Phase 1: Foundation

### 1.1 Unit Tests
Run the `/audit-unit-tests` workflow. Record results.

### 1.2 Code Quality
Run the `/audit-code-quality` workflow. Record results.

---

## Phase 2: Architecture & Design

### 2.1 Architecture
Run the `/audit-architecture` workflow. Record results.

### 2.2 Routing Logic
Run the `/audit-routing` workflow. Record results.

---

## Phase 3: Core Functionality

### 3.1 Transpiler
Run the `/audit-transpiler` workflow. Record results.

### 3.2 Sync
Run the `/audit-sync` workflow. Record results.

---

## Phase 4: Polish & Robustness

### 4.1 Error Handling
Run the `/audit-error-handling` workflow. Record results.

### 4.2 CLI
Run the `/audit-cli` workflow. Record results.

---

## Phase 5: Consolidated Report

After running all 8 audits, produce a **Demo Readiness Report** with:

| Audit | Status | Issues | Critical |
|-------|--------|--------|----------|
| Unit Tests | PASS/WARN/FAIL | count | count |
| Code Quality | PASS/WARN/FAIL | count | count |
| Architecture | PASS/WARN/FAIL | count | count |
| Routing | PASS/WARN/FAIL | count | count |
| Transpiler | PASS/WARN/FAIL | count | count |
| Sync | PASS/WARN/FAIL | count | count |
| Error Handling | PASS/WARN/FAIL | count | count |
| CLI | PASS/WARN/FAIL | count | count |

### Overall Assessment

- **Demo Ready**: ✓ if no FAIL results and critical count = 0
- **Demo Ready with Caveats**: ⚠ if WARN results but no FAILs
- **Not Demo Ready**: ✗ if any FAIL results

### Priority Fixes

List any critical issues that must be fixed before the demo, ordered by severity.

### Recommendations

List non-critical improvements to consider after the demo.
