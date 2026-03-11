---
phase: 05-salary-cap-validation
plan: 02
subsystem: contracts
tags: [eligibility, allotments, validation, contract-tools]

# Dependency graph
requires:
  - phase: 04-contract-engine
    provides: extension, franchise_tag, tender, buyout eligibility checkers
  - phase: 05-salary-cap-validation
    plan: 01
    provides: contract classifier, cap penalties, team summary
provides:
  - Unified eligibility validation service (7 action types)
  - Allotment model and tracking service (per-team annual limits)
affects: [06-api-layer]

# Tech tracking
tech-stack:
  added: []
  patterns: [unified dispatch, deferred imports for circular avoidance, team_id resolution from contracts]

key-files:
  created:
    - src/app/services/eligibility.py
    - src/app/services/allotments.py
    - src/app/models/allotment.py
    - migrations/versions/a3f7e2c91d4b_add_allotments_table.py
  modified:
    - src/app/models/__init__.py

key-decisions:
  - "Allotment checks use deferred imports to avoid circular dependency with eligibility service"
  - "team_id resolved from player's contract (current or previous season depending on action type)"
  - "Tender allotments shared between RFA and ERFA under single 'tender' action_type"

patterns-established:
  - "Unified eligibility dispatch: single check_eligibility() entry point delegates to action-specific checkers"
  - "EligibilityResult dataclass with rule_citation field for bylaws traceability"
  - "Allotment limits defined as module constants matching contract_tools.yaml"

issues-created: []

# Metrics
duration: 6 min
completed: 2026-03-11
---

# Phase 5 Plan 2: Eligibility Validation & Allotment Tracking Summary

**Unified contract action eligibility validation with bylaws citations and per-team annual allotment tracking**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-11T12:58:19Z
- **Completed:** 2026-03-11T13:04:49Z
- **Tasks:** 2
- **Files created:** 4
- **Files modified:** 1

## Accomplishments
- Unified eligibility service covers all 7 contract action types with clear yes/no + bylaws rule citation
- Delegates to existing Phase 4 eligibility checkers (extensions, tags, tenders, buyouts) — no duplication
- Allotment model tracks per-team usage with unique constraint preventing double-recording
- Allotment service enforces limits: 1 franchise tag, 1 B/R, 2 tenders (shared RFA/ERFA), 2 July 1 tenders
- Eligibility service gates actions on allotment availability before approving

## Task Commits

1. **Task 1: Unified eligibility validation service** - `3368e62` (feat)
2. **Task 2: Allotment model and tracking service** - `fc731fa` (feat)

## Files Created/Modified
- `src/app/services/eligibility.py` - Unified check_eligibility() dispatching to 7 action-specific checkers
- `src/app/services/allotments.py` - get_remaining_allotments(), has_allotment(), record_allotment_use()
- `src/app/models/allotment.py` - Allotment model with AllotmentActionType StrEnum
- `migrations/versions/a3f7e2c91d4b_add_allotments_table.py` - Alembic migration for allotments table
- `src/app/models/__init__.py` - Registered Allotment and AllotmentActionType

## Decisions Made
- Allotment checks use deferred imports (inside function body) to avoid circular dependency between eligibility.py and allotments.py
- team_id is resolved from the player's contract record — previous season for expired-contract actions (tags, tenders), current season for active-contract actions (extension, B/R, 5YO, PPE)
- RFA and ERFA tenders share a single "tender" allotment type with limit of 2, matching bylaws

## Deviations from Plan

None. Both tasks completed as specified.

---

**Phase 5 complete:** contract classification, cap penalties, team summary, eligibility gates, allotment tracking all implemented.

---
*Phase: 05-salary-cap-validation*
*Completed: 2026-03-11*
