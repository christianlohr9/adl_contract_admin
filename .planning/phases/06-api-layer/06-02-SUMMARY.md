---
phase: 06-api-layer
plan: 02
subsystem: api
tags: [fastapi, pydantic, contract-tools, rest-api]

# Dependency graph
requires:
  - phase: 04-contract-engine
    provides: Extension, tag, tender, buyout calculation services
  - phase: 05-salary-cap
    provides: Eligibility checks, PPE and 5YO calculations
  - phase: 06-01
    provides: Router patterns, schema patterns, DB session dependency
provides:
  - 8 REST endpoints under /api/tools for all contract tool calculations
  - Bundled "all tools" endpoint for single-call player evaluation
  - Pydantic schemas for all contract tool result types
affects: [08-frontend-ui]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-tool error isolation in bundled endpoint, model_validate with from_attributes for dataclass→schema conversion]

key-files:
  created: [src/app/schemas/tools.py, src/app/api/tools.py]
  modified: [src/app/main.py]

key-decisions:
  - "Used /{player_id}/all for bundled endpoint to avoid path conflicts with sub-routes"
  - "Per-tool error isolation: each service call wrapped in try/except so one failure doesn't block others"
  - "Schemas mirror service dataclass fields exactly, using model_validate(from_attributes=True) for conversion"

patterns-established:
  - "Error isolation pattern: bundled endpoints catch per-service errors, log them, return None for failed tools"
  - "Consistent endpoint shape: verify player → call service → model_validate → return"

issues-created: []

# Metrics
duration: 3min
completed: 2026-03-11
---

# Phase 6 Plan 02: Contract Tools Endpoints Summary

**8 REST endpoints exposing full contract engine — bundled "all tools" call, eligibility checker, and individual extension/tag/tender/buyout/5YO/PPE endpoints**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-11T15:13:41Z
- **Completed:** 2026-03-11T15:16:44Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- 14 Pydantic schemas mirroring all service-layer dataclasses (extensions, tags, tenders, buyouts, 5YO, PPE, eligibility)
- Bundled GET /{player_id}/all endpoint calling all 6 calculate services with per-tool error isolation
- Eligibility endpoint checking all 7+ action types in a single call
- 6 individual tool endpoints for targeted queries

## Task Commits

Each task was committed atomically:

1. **Task 1: Create Pydantic schemas for contract tool results** - `10d6478` (feat)
2. **Task 2: Create contract tools router with 8 endpoints** - `be10817` (feat)

## Files Created/Modified
- `src/app/schemas/tools.py` - 14 Pydantic schemas for all contract tool result types
- `src/app/api/tools.py` - Contract tools router with 8 endpoints (bundled, eligibility, 6 individual)
- `src/app/main.py` - Registered tools router

## Decisions Made
- Used `/{player_id}/all` for bundled endpoint to avoid FastAPI path conflicts with sub-routes
- Per-tool error isolation in bundled endpoint — each service call wrapped in try/except, failures logged and returned as None
- Schemas mirror service dataclass field names exactly, converted via `model_validate(from_attributes=True)`

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## Next Phase Readiness
- All contract tool calculations now exposed via REST API
- Ready for 06-03 (salary cap endpoints) to complete the API layer
- Frontend (Phase 8) can consume these endpoints for contract tools UI

---
*Phase: 06-api-layer*
*Completed: 2026-03-11*
