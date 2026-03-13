---
phase: 14-historical-data-imports
plan: 01
subsystem: api
tags: [mfl, httpx, sqlalchemy, sync, historical-data]

# Dependency graph
requires:
  - phase: 03-mfl-api-integration
    provides: year-scoped client factory, SyncResult dataclass, sync_scores/sync_rosters functions
provides:
  - detect_score_gaps() for weekly score gap detection
  - sync_historical_scores() with per-week granularity (1-17 + YTD)
  - detect_contract_gaps() for contract history gap detection
  - run_historical_roster_sync() orchestrator for multi-season contract import
affects: [15-eligibility-audit, 17-regression-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: [gap-detection-before-fetch, sequential-rate-limited-sync]

key-files:
  created: []
  modified:
    - src/app/services/score_sync.py
    - src/app/services/roster_sync.py
    - src/app/services/sync_orchestrator.py

key-decisions:
  - "Gap detection queries existing data before fetching to avoid redundant API calls"
  - "Sequential year processing with rate-limit delays (MFL rate limits are per-IP)"

patterns-established:
  - "Gap detection pattern: query DB for existing data, compute missing set, fetch only gaps"

issues-created: []

# Metrics
duration: 3 min
completed: 2026-03-13
---

# Phase 14 Plan 01: Historical Sync Services Summary

**Weekly score sync (weeks 1-17 + YTD) and historical roster/contract sync with gap detection to avoid redundant MFL API calls**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-13T08:45:43Z
- **Completed:** 2026-03-13T08:48:23Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Extended `sync_historical_scores()` to fetch all 18 week variants (YTD + 1-17) per season instead of just YTD
- Added `detect_score_gaps()` to find missing (season, week) combos and skip already-fetched data
- Added `detect_contract_gaps()` to find seasons with no contract records
- Added `run_historical_roster_sync()` orchestrator following established patterns from `run_historical_sync()`

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend historical score sync with weekly scores and gap detection** - `c18c5bb` (feat)
2. **Task 2: Add historical roster/contract sync with gap detection** - `4a942c9` (feat)

## Files Created/Modified
- `src/app/services/score_sync.py` - Added `_ALL_WEEKS` constant, `detect_score_gaps()`, rewrote `sync_historical_scores()` with gap detection
- `src/app/services/roster_sync.py` - Added `detect_contract_gaps()` querying Contract table for missing seasons
- `src/app/services/sync_orchestrator.py` - Added `run_historical_roster_sync()` with year-scoped client factory and rate limiting

## Decisions Made
- Gap detection queries DB first to compute missing data set, preventing redundant API calls on re-runs
- Sequential year processing with inter-request delays to respect MFL per-IP rate limits

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## Next Phase Readiness
- Historical sync services ready for integration into startup backfill (14-02)
- Gap detection ensures idempotent re-runs without wasted API calls
- Ready for 14-02-PLAN.md (Startup Backfill & Status Tracking)

---
*Phase: 14-historical-data-imports*
*Completed: 2026-03-13*
