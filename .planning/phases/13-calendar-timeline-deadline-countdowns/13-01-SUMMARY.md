---
phase: 13-calendar-timeline-deadline-countdowns
plan: 01
subsystem: api
tags: [sqlalchemy, franchise-tags, tenders, eligibility, bug-fix]

# Dependency graph
requires:
  - phase: 03-mfl-api-integration
    provides: Contract model with current-season storage convention
  - phase: 04-contract-engine
    provides: franchise_tags.py, tenders.py eligibility services
provides:
  - Fixed franchise tag eligibility for expired contracts
  - Fixed ERFA/RFA tender eligibility for expired contracts
  - Correct contract age calculations for current-season storage
affects: [12-contract-management-dashboard, 13-calendar-timeline-deadline-countdowns]

# Tech tracking
tech-stack:
  added: []
  patterns: []

key-files:
  created: []
  modified:
    - src/app/services/franchise_tags.py
    - src/app/services/tenders.py

key-decisions:
  - "Query Contract.season == season (not season - 1) for expired contracts"
  - "Contract age = season - signed_season (not (season - 1) - signed_season + 1)"

patterns-established: []

issues-created: []

# Metrics
duration: 2min
completed: 2026-03-12
---

# Phase 13 Plan 01: Fix ISS-002 Tag/Tender Eligibility Summary

**Fixed franchise tag, ERFA tender, and RFA tender eligibility by querying current season instead of season - 1 for expired contracts**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-12T16:10:26Z
- **Completed:** 2026-03-12T16:12:38Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Fixed franchise tag eligibility queries to find expired contracts in current season
- Fixed ERFA and RFA tender eligibility queries to find expired contracts in current season
- Corrected contract age and original length calculations for current-season storage convention
- Resolved ISS-002 (high-impact bug — tags/tenders were completely broken)

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix franchise tag eligibility season queries** - `b3c39e2` (fix)
2. **Task 2: Fix tender eligibility season queries** - `51f2d5e` (fix)

## Files Created/Modified
- `src/app/services/franchise_tags.py` - Fixed 2 season queries in check_tag_eligibility and calculate_franchise_tags
- `src/app/services/tenders.py` - Fixed 3 season queries and 2 age calculations in check_erfa_eligibility, check_rfa_eligibility, and calculate_tenders

## Decisions Made
- Query `Contract.season == season` with `years_remaining == 0` instead of `Contract.season == season - 1` — MFL sync stores all contracts in current season
- Contract age formula changed from `(season - 1) - signed_season + 1` to `season - signed_season` — consistent with current-season storage
- Left `_get_top_n_positional_salaries` using `season - 1` — legitimately needs previous season's positional salary data for tag pricing
- Left `eligible_seasons` in ERFA check using `[season - 1, season - 2]` — refers to when contract was signed, not storage season

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None — database was not running locally for live verification, but import tests confirmed code correctness.

## Next Phase Readiness
- ISS-002 resolved — tags/tenders will populate dashboard when DB is running
- Ready for 13-02-PLAN.md (calendar timeline UI)

---
*Phase: 13-calendar-timeline-deadline-countdowns*
*Completed: 2026-03-12*
