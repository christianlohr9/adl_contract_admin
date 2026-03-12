---
phase: 10-period-detection-eligibility
plan: 02
subsystem: api
tags: [schemas, tools-api, window-status, api-response]

# Dependency graph
requires:
  - phase: 10-period-detection-eligibility
    plan: 01
    provides: WindowStatus dataclass, get_all_window_statuses(), window fields on EligibilityResult
provides:
  - WindowStatusSchema in Pydantic response schemas
  - window_statuses field on bundled PlayerToolsSchema response
  - window_status/window_closes fields on EligibilitySchema
affects: [11-roster-wide-eligibility, 12-contract-management-dashboard]

# Tech tracking
tech-stack:
  added: []
  patterns: []

key-files:
  created: []
  modified: [src/app/schemas/tools.py, src/app/api/tools.py, src/app/services/eligibility.py]

key-decisions:
  - "Window statuses surfaced in bundled endpoint only, not individual tool endpoints"
  - "Per-tool error isolation pattern applied to window status fetch"

patterns-established: []

issues-created: []

# Metrics
duration: 3min
completed: 2026-03-12
---

# Phase 10 Plan 02: API Response Enhancement with Window Status Summary

**Added window status schemas and surfaced window information in tools API responses, completing Phase 10**

## Performance

- **Duration:** 3 min
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added `window_status` and `window_closes` fields to `EligibilitySchema` mirroring the dataclass
- Created `WindowStatusSchema` with action, status, opens, closes, reason fields
- Added `window_statuses` dict field to `PlayerToolsSchema` for bundled responses
- Updated bundled `get_all_tools()` endpoint to call `get_all_window_statuses()` with error isolation
- Fixed ruff lint issues: import sorting in schemas/tools.py, TC003 in eligibility.py

## Task Commits

Each task was committed atomically:

1. **Task 1: Add window status schema and update tools API** - `3a2c898` (feat)
2. **Task 2: Lint and import fixes** - `e3f1fa9` (fix)

## Files Modified
- `src/app/schemas/tools.py` - Added WindowStatusSchema, window fields on EligibilitySchema, window_statuses on PlayerToolsSchema
- `src/app/api/tools.py` - Added window status fetch in bundled endpoint with error isolation
- `src/app/services/eligibility.py` - Moved date import into TYPE_CHECKING block (lint fix)

## Decisions Made
- Window statuses surfaced in bundled endpoint only; individual tool endpoints return calculation results, not eligibility
- Per-tool error isolation pattern applied to window status fetch (consistent with existing pattern)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## Verification Results
- ruff check passes on all 4 files
- No circular import errors
- FastAPI server starts without errors
- All schema fields verified programmatically

## Phase 10 Complete
Phase 10 (Period Detection & Date-Aware Eligibility) is now complete:
- Window status service operational (10-01)
- API responses enhanced with window status metadata (10-02)
- ISS-001 addressed at backend level

---
*Phase: 10-period-detection-eligibility*
*Completed: 2026-03-12*
