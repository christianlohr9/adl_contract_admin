---
phase: 09-league-calendar-data-model
plan: 01
subsystem: database, api
tags: [sqlalchemy, alembic, fastapi, pydantic, tanstack-query, crud]

# Dependency graph
requires:
  - phase: 02-foundation
    provides: TimestampMixin, Base, async DB, Alembic
  - phase: 06-api-layer
    provides: APIRouter patterns, SessionDep, Pydantic schema conventions
  - phase: 07-frontend-placeholder
    provides: React scaffold, API client, TanStack Query setup
provides:
  - SeasonCalendar model with 27 date fields for all league events
  - CRUD endpoints for admin calendar configuration
  - Frontend query/mutation hooks for calendar data
  - TypeScript types for SeasonCalendarSchema
affects: [10-period-detection, 11-roster-wide-eligibility, 12-contract-management-dashboard, 13-calendar-timeline]

# Tech tracking
tech-stack:
  added: []
  patterns: [admin CRUD with 409 conflict detection, partial update via setattr loop]

key-files:
  created:
    - src/app/models/season_calendar.py
    - src/app/schemas/calendar.py
    - src/app/api/calendar.py
    - frontend/src/api/queries/calendar.ts
    - migrations/versions/48ff388fedeb_add_season_calendar_table.py
  modified:
    - src/app/models/__init__.py
    - src/app/main.py
    - frontend/src/api/client.ts
    - frontend/src/api/types.ts

key-decisions:
  - "All 27 date fields nullable — commissioner fills progressively as dates are set"
  - "PUT uses setattr loop on non-None fields to allow partial updates without nulling others"
  - "409 Conflict on duplicate season POST, consistent with unique constraint"

patterns-established:
  - "Admin CRUD pattern: list/get/create/update with 404/409 error handling"
  - "Frontend mutation hooks with cache invalidation on success"
  - "API client post/put methods for write operations"

issues-created: []

# Metrics
duration: 5min
completed: 2026-03-12
---

# Phase 9 Plan 1: SeasonCalendar Model & CRUD Summary

**SeasonCalendar model with 27 date fields covering all league deadlines, auction windows, and season markers, plus full CRUD API and frontend hooks**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-12T10:19:56Z
- **Completed:** 2026-03-12T10:25:49Z
- **Tasks:** 2
- **Files modified:** 9

## Accomplishments
- SeasonCalendar SQLAlchemy model with 27 nullable date fields grouped by deadlines, auction windows, and season markers
- Alembic migration creating season_calendars table with unique season constraint
- 4 FastAPI CRUD endpoints (list, get, create, update) with proper error handling
- Pydantic schemas for response, create, and update operations
- Frontend API client extended with post/put methods
- TypeScript types and TanStack Query hooks (2 queries + 2 mutations) ready for UI consumption

## Task Commits

Each task was committed atomically:

1. **Task 1: SeasonCalendar model and migration** - `de30cfd` (feat)
2. **Task 2: Schemas, CRUD endpoints, and frontend hooks** - `f453bf6` (feat)

## Files Created/Modified
- `src/app/models/season_calendar.py` - SeasonCalendar model with all date fields
- `migrations/versions/48ff388fedeb_add_season_calendar_table.py` - Alembic migration
- `src/app/schemas/calendar.py` - 3 Pydantic schemas (response, create, update)
- `src/app/api/calendar.py` - FastAPI router with 4 CRUD endpoints
- `frontend/src/api/queries/calendar.ts` - TanStack Query hooks for calendar data
- `src/app/models/__init__.py` - Added SeasonCalendar import
- `src/app/main.py` - Registered calendar router
- `frontend/src/api/client.ts` - Added post and put methods
- `frontend/src/api/types.ts` - Added SeasonCalendarSchema interface

## Decisions Made
- All 27 date fields nullable — commissioner fills progressively as dates are set each season
- PUT endpoint uses setattr loop on non-None fields for partial updates without nulling others
- 409 Conflict returned on duplicate season POST, consistent with unique constraint

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Next Phase Readiness
- SeasonCalendar model ready for period detection logic in 09-02
- CRUD endpoints ready for admin UI in Phase 12
- Frontend hooks ready for calendar UI in Phase 13

---
*Phase: 09-league-calendar-data-model*
*Completed: 2026-03-12*
