---
phase: 11-roster-wide-eligibility-api
plan: 01
subsystem: api
tags: [fastapi, eligibility, roster, aggregation, contract-tools]

# Dependency graph
requires:
  - phase: 10-period-detection-eligibility
    provides: window status service and eligibility gating
  - phase: 06-api-layer
    provides: per-tool error isolation pattern, bundled endpoint design
provides:
  - GET /api/teams/{team_id}/eligibility endpoint
  - Roster-wide contract action aggregation service
  - Per-action player grouping with headline values
affects: [12-contract-management-dashboard]

# Tech tracking
tech-stack:
  added: []
  patterns: ["roster-wide aggregation service", "action-grouped eligibility response"]

key-files:
  created:
    - src/app/services/roster_eligibility.py
    - src/app/schemas/roster_eligibility.py
  modified:
    - src/app/api/teams.py

key-decisions:
  - "Per-player error isolation in roster aggregation — one player failing doesn't skip entire action group"
  - "Sort players by headline_value descending within action groups (None last)"
  - "Only include action groups with at least 1 eligible player"

patterns-established:
  - "Roster-wide aggregation: load roster once, iterate per open action window"

issues-created: []

# Metrics
duration: 3min
completed: 2026-03-12
---

# Phase 11 Plan 01: Roster Eligibility Service, Schemas, and Endpoint Summary

**Roster-wide eligibility aggregation endpoint grouping all contract actions by type with headline calculated values per eligible player**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-12T13:46:07Z
- **Completed:** 2026-03-12T13:49:16Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Created roster eligibility service aggregating all 7 contract action types (extensions, franchise tags, ERFA/RFA tenders, buyout/restructure, 5YO, PPE) across entire team roster
- Built Pydantic schemas for action-grouped eligibility response with window status metadata
- Added `GET /api/teams/{team_id}/eligibility` endpoint to teams router

## Task Commits

Each task was committed atomically:

1. **Task 1: Create roster eligibility service** - `304e5c8` (feat)
2. **Task 2: Create schemas and API endpoint** - `06547d7` (feat)

## Files Created/Modified
- `src/app/services/roster_eligibility.py` - Service with 3 dataclasses and get_roster_eligibility() aggregation function
- `src/app/schemas/roster_eligibility.py` - Pydantic schemas for PlayerActionSummary, ActionGroup, RosterEligibility
- `src/app/api/teams.py` - Added eligibility endpoint on teams router

## Decisions Made
None - followed plan as specified

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## Next Phase Readiness
- Phase 11 complete (single plan phase), ready for phase transition
- Endpoint provides the data layer needed for Phase 12 contract management dashboard
- All contract action types aggregated with headline values for roster-wide visibility

---
*Phase: 11-roster-wide-eligibility-api*
*Completed: 2026-03-12*
