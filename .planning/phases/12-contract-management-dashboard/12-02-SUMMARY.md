---
phase: 12-contract-management-dashboard
plan: 02
subsystem: ui
tags: [react, tanstack-table, eligibility, data-table, shadcn]

# Dependency graph
requires:
  - phase: 12-contract-management-dashboard-01
    provides: ContractManagementPage shell, WindowStatusBar, eligibility query hook
  - phase: 08-frontend-ui
    provides: DataTable component, DataTableColumnHeader, formatSalary, onRowClick pattern
  - phase: 11-roster-wide-eligibility-api
    provides: GET /api/teams/{team_id}/eligibility with action_groups and window_statuses
provides:
  - Unified eligibility data table with per-action columns
  - Action-grouped to player-centric data transformation
  - Eligible-only toggle filter (default on)
  - Dynamic column visibility based on window status
affects: [13-calendar-timeline]

# Tech tracking
tech-stack:
  added: []
  patterns: [action-grouped-to-player-pivot, dynamic-column-visibility-by-window-status]

key-files:
  created:
    - frontend/src/components/contracts/useEligibilityTable.ts
    - frontend/src/components/contracts/eligibility-columns.tsx
  modified:
    - frontend/src/pages/ContractManagementPage.tsx

key-decisions:
  - "None - followed plan as specified"

patterns-established:
  - "Data pivot: API returns action-grouped data, frontend transforms to player-centric rows via Map<player_id, EligibilityRow>"
  - "Dynamic columns: only show action columns where window_status is open"

issues-created: [ISS-002]

# Metrics
duration: 43min
completed: 2026-03-12
---

# Phase 12 Plan 02: Unified Data Table with Action Columns and Eligible-Only Toggle Summary

**Player-centric eligibility data table with dynamic per-action columns (only open windows), salary formatting, eligible-only toggle, and row-click navigation to player detail**

## Performance

- **Duration:** 43 min
- **Started:** 2026-03-12T15:04:47Z
- **Completed:** 2026-03-12T15:47:25Z
- **Tasks:** 2 auto + 1 checkpoint
- **Files modified:** 3

## Accomplishments
- Data transformation hook that pivots action-grouped API data to player-centric rows using Map<player_id, EligibilityRow>
- Dynamic column definitions — only shows columns for actions with open windows
- Eligible-only toggle (default on) with full roster mode that merges non-eligible players
- Right-aligned salary headers and cells with formatSalary() formatting

## Task Commits

Each task was committed atomically:

1. **Task 1: Data transformation and column definitions** - `837e461` (feat)
2. **Task 2: Eligible-only toggle and page assembly** - `7ef139a` (feat)
3. **Fix: Right-align column headers** - `a4c6d34` (fix)

## Files Created/Modified
- `frontend/src/components/contracts/useEligibilityTable.ts` - EligibilityRow type, transformEligibilityData(), useEligibilityTable() hook with memoized pivot
- `frontend/src/components/contracts/eligibility-columns.tsx` - getEligibilityColumns(windowStatuses) builds dynamic column defs, only includes open action windows
- `frontend/src/pages/ContractManagementPage.tsx` - Full page assembly with DataTable, toggle, loading/empty states, row click navigation

## Decisions Made
None - followed plan as specified.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Right-aligned column headers for salary/action columns**
- **Found during:** Checkpoint verification
- **Issue:** Column headers were left-aligned while cell values were right-aligned, creating visual misalignment
- **Fix:** Wrapped DataTableColumnHeader in `<div className="flex justify-end">` for salary and action columns
- **Files modified:** frontend/src/components/contracts/eligibility-columns.tsx
- **Verification:** Visual inspection confirmed alignment
- **Committed in:** a4c6d34

### Deferred Enhancements

Logged to .planning/ISSUES.md for future consideration:
- ISS-002: Franchise tag / tender eligibility checks query wrong season for expired contracts (discovered during checkpoint verification)

---

**Total deviations:** 1 auto-fixed (1 bug), 1 deferred (ISS-002 — backend bug in tag/tender eligibility season query)
**Impact on plan:** Auto-fix was cosmetic alignment. ISS-002 is a pre-existing backend bug causing franchise_tag, erfa_tender, and rfa_tender to show no eligible players — deferred to Phase 13.

## Issues Encountered
- Franchise tag, ERFA tender, and RFA tender columns appear (windows are open) but show no values for any player. Root cause: eligibility checkers query `season - 1` for expired contracts, but MFL sync stores all contracts in current season. Logged as ISS-002, deferred to Phase 13.

## Next Phase Readiness
- Phase 12 complete — Contract Management Dashboard fully functional for buyout, 5YO, and PPE actions
- Franchise tag / tender columns will populate once ISS-002 is fixed in Phase 13
- Ready for Phase 13: Calendar/Timeline & Deadline Countdowns

---
*Phase: 12-contract-management-dashboard*
*Completed: 2026-03-12*
