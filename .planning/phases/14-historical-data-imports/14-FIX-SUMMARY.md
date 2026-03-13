---
phase: 14-historical-data-imports
plan: FIX
subsystem: api
tags: [rate-limit, retry, tenacity, backfill, resilience]

# Dependency graph
requires:
  - phase: 14-historical-data-imports
    provides: MFLClient._export_with_retry(), run_historical_backfill()
provides:
  - MFLClient retries on MFLRateLimitError with exponential backoff (4-60s, 7 attempts)
  - Backfill orchestrator commits per-year and continues on per-year failure
  - BackfillStatus updates incrementally as years complete
affects: [15-eligibility-audit, 17-regression-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-year-commit, graceful-degradation, exponential-backoff-rate-limit]

key-files:
  created: []
  modified:
    - src/app/mfl/client.py
    - src/app/services/historical_sync.py

key-decisions:
  - "Added MFLRateLimitError to tenacity retry tuple (alongside HTTPStatusError)"
  - "Increased backoff to multiplier=2, min=4, max=60 and 7 attempts for rate limit recovery"
  - "Per-year commit in backfill so partial progress survives failures"
  - "Per-year try/except continues backfill on individual year failure"

patterns-established:
  - "Per-item commit pattern: commit and update status after each logical unit, not all at once"

issues-created: []

# Metrics
duration: 4 min
completed: 2026-03-13
---

# Phase 14 FIX: Rate Limit Retry & Backfill Resilience Summary

**Fix 2 UAT blocker issues: MFL rate limit retry and per-year backfill resilience**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-13
- **Completed:** 2026-03-13
- **Tasks:** 2
- **Files modified:** 2

## Issues Fixed

- **UAT-001:** Backfill fails on MFL API rate limit with no retry/backoff
- **UAT-002:** No historical data actually imported (caused by UAT-001)

## Root Cause

The `@retry` decorator on `_export_with_retry` only retried `httpx.HTTPStatusError`, but the 429 handler on line 162 raises `MFLRateLimitError` before tenacity ever sees it. Rate limits were never retried.

## Accomplishments

1. **MFLClient retry fix:** Added `MFLRateLimitError` to tenacity's retry condition. Increased exponential backoff (multiplier=2, min=4s, max=60s) and max attempts from 5 to 7 for rate limit recovery.
2. **Backfill per-year resilience:** Orchestrator now commits after each year (not all at once), continues to next year on failure, updates `BackfillStatus` incrementally, and reports partial failures without aborting.

## Task Commits

1. **Task 1: Add MFLRateLimitError to tenacity retry in MFLClient** - `f9fbe19`
2. **Task 2: Add per-year commit and retry in backfill orchestrator** - `4ea8477`

## Files Modified

- `src/app/mfl/client.py` - Retry decorator now includes MFLRateLimitError, increased backoff and attempts
- `src/app/services/historical_sync.py` - Per-year commit, per-year try/except, incremental status updates

## Verification Checklist

- [x] MFLClient retries on 429/MFLRateLimitError with exponential backoff
- [x] Backfill orchestrator commits per-year (not all-at-once)
- [x] Backfill continues to next year if one year fails
- [x] BackfillStatus updates incrementally as years complete
- [x] Syntax verified for both modified files

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

---
*Phase: 14-historical-data-imports*
*Completed: 2026-03-13*
