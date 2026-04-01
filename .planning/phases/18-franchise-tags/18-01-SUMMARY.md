---
phase: 18-franchise-tags
plan: 01
subsystem: api, ui
tags: [franchise-tags, eligibility, team-scoping, conference]

requires:
  - phase: 17-regression-testing-validation
    provides: validated eligibility baseline
provides:
  - team_id scoping for franchise tag eligibility checks
  - team_id threading through API and frontend navigation
  - numeric sorting fix for data tables
  - default page size 50

affects: [19-extensions, 20-tenders, 21-5yo-ppe, 22-buyout-restructure]

tech-stack:
  added: []
  patterns:
    - "team_id optional param pattern for conference-scoped queries"

key-files:
  created: [scripts/validate_ft_eligibility.py, .planning/phases/18-franchise-tags/18-ISSUES.md]
  modified:
    - src/app/services/franchise_tags.py
    - src/app/services/eligibility.py
    - src/app/services/roster_eligibility.py
    - src/app/api/tools.py
    - frontend/src/api/queries/tools.ts
    - frontend/src/pages/PlayerDetailPage.tsx
    - frontend/src/pages/RosterPage.tsx
    - frontend/src/pages/ContractManagementPage.tsx
    - frontend/src/pages/SalaryCapPage.tsx
    - frontend/src/components/roster/columns.tsx
    - frontend/src/components/contracts/useEligibilityTable.ts
    - frontend/src/components/data-table/DataTable.tsx

key-decisions:
  - "team_id as optional param (backward-compatible) — without it, all contracts are considered"
  - "Team context passed via URL query param ?team=X, not route param"
  - "Validation scripts built but user prefers verifying in production UI directly"

patterns-established:
  - "Conference-scoped queries: add optional team_id param, filter Contract.team_id when provided"
  - "Frontend team context: pass ?team=X in navigation, read via useSearchParams"

issues-created: [ISS-018-001]

duration: 45min
completed: 2026-04-01
---

# Phase 18-01: FT Eligibility Validation Summary

**Fixed franchise tag eligibility for dual-conference league by scoping all contract queries to team_id, plus numeric sorting and pagination fixes**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-04-01T14:00:00Z
- **Completed:** 2026-04-01T14:45:00Z
- **Tasks:** 4 (2 from plan + 2 deviation fixes)
- **Files modified:** 12

## Accomplishments
- FT eligibility correctly scoped by team/conference — Jordyn Brooks (and similar cross-conference players) now show correct eligibility per team
- team_id threading through full stack: service → API → frontend query → navigation
- Numeric sorting on salary and value columns (was string-sorting Decimals)
- Default table page size increased to 50

## Task Commits

1. **Task 1: Build FT eligibility validation script** — `63da60a` (feat)
2. **Task 2: Add FT eligibility validation report** — `244fdde` (docs)
3. **Task 3: Scope FT eligibility by team_id** — `fcda175` (fix)
4. **Task 4: Frontend team_id threading + UI fixes** — `2b32f7f` (fix)

## Files Created/Modified
- `src/app/services/franchise_tags.py` — Added team_id param to eligibility and calculation functions
- `src/app/services/eligibility.py` — Thread team_id through unified check_eligibility dispatcher
- `src/app/services/roster_eligibility.py` — Pass team_id to eligibility checks and tag headline extractor
- `src/app/api/tools.py` — Accept team_id query param on /all and /tags endpoints
- `frontend/src/api/queries/tools.ts` — Accept teamId, append to API URL
- `frontend/src/pages/PlayerDetailPage.tsx` — Read team from URL search params
- `frontend/src/pages/RosterPage.tsx` — Pass team in navigation + use getRosterColumns
- `frontend/src/pages/ContractManagementPage.tsx` — Pass team in navigation
- `frontend/src/pages/SalaryCapPage.tsx` — Pass team in navigation + column links
- `frontend/src/components/roster/columns.tsx` — Convert to getRosterColumns(teamId), add numeric sorting
- `frontend/src/components/contracts/useEligibilityTable.ts` — Convert Decimal strings to Numbers
- `frontend/src/components/data-table/DataTable.tsx` — Default pageSize 50
- `scripts/validate_ft_eligibility.py` — Validation script (created but user prefers UI verification)

## Decisions Made
- User rejected script-based validation approach — prefers verifying directly in the production web app
- team_id scoping is backward-compatible (optional param, defaults to all contracts)
- Team context passed as URL query param (?team=X) rather than changing route structure

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Conference-scoped eligibility checks**
- **Found during:** User testing (Jordyn Brooks showed ineligible despite yr=0)
- **Issue:** Player had contracts in both conferences; eligibility check found cross-conference active contract and falsely reported "already re-signed"
- **Fix:** Added team_id parameter throughout the eligibility chain (franchise_tags → eligibility → roster_eligibility → API → frontend)
- **Files modified:** 12 files across backend and frontend
- **Verification:** curl API with team_id=148 returns eligible=true, team_id=129 returns eligible=false (correct)
- **Committed in:** fcda175, 2b32f7f

**2. [Rule 3 - Blocking] Numeric sorting on Decimal columns**
- **Found during:** User testing — salary columns sorted as strings ("16.32" before "2.09")
- **Issue:** Pydantic serializes Decimal as strings; frontend never converted to Numbers
- **Fix:** Number() conversion in useEligibilityTable, sortingFn: "basic" on salary columns
- **Committed in:** 2b32f7f

### Deferred Enhancements

Logged to .planning/phases/18-franchise-tags/18-ISSUES.md:
- ISS-018-001: Incomplete roster data for teams 129-144 (Conference 1 partial import)

---

**Total deviations:** 2 auto-fixed (1 missing critical, 1 blocking), 1 deferred
**Impact on plan:** Plan pivoted from script-based validation to production UI fix. Core deliverable (correct FT eligibility) achieved through different approach.

## Issues Encountered
- Database connection from host requires localhost:5432, not db:5432 (Docker network vs host)
- User feedback redirected approach from validation scripts to fixing the production app directly

## Next Phase Readiness
- FT eligibility validated via production UI — ready for 18-02 (FT price validation)
- team_id scoping pattern established for other tools (extensions, tenders, etc.)
- ISS-018-001 (incomplete roster data) does not block price validation

---
*Phase: 18-franchise-tags*
*Completed: 2026-04-01*
