---
phase: 08-frontend-ui
plan: 04
subsystem: ui
tags: [react, recharts, shadcn, tanstack-query, nuqs, dashboard, salary-cap]

# Dependency graph
requires:
  - phase: 08-03
    provides: player detail page with contract tools tabs
  - phase: 06-03
    provides: salary cap and allotment API endpoints
  - phase: 06-01
    provides: team snapshot endpoint
provides:
  - Salary cap page with stacked bar chart, summary cards, penalty DataTable, allotments
  - Action-driven dashboard surfacing free agents, expiring contracts, and available allotments
  - Complete 4-page frontend application
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "ChartContainer with ChartConfig for themed Recharts integration"
    - "Snapshot endpoint for unified dashboard data loading"
    - "Action items prioritized by urgency (free agents > expiring > allotments)"

key-files:
  created:
    - frontend/src/components/cap/CapSummaryCards.tsx
    - frontend/src/components/cap/CapChart.tsx
    - frontend/src/components/cap/AllotmentsCard.tsx
    - frontend/src/components/dashboard/ActionItems.tsx
    - frontend/src/components/dashboard/CapOverview.tsx
    - frontend/src/components/dashboard/RosterSummary.tsx
  modified:
    - frontend/src/pages/SalaryCapPage.tsx
    - frontend/src/pages/DashboardPage.tsx

key-decisions:
  - "Fixed chart height to 120px instead of aspect-video to keep page readable"
  - "Free agents (years_remaining=0) surfaced as top-priority action items for tags/tenders"
  - "Tags and tenders allotments prioritized above buyouts in action items"

patterns-established:
  - "ChartContainer with aspect override for compact chart rendering"
  - "Action items sorted by priority: free agents > expiring > tags > tenders > buyouts"

issues-created: [ISS-001]

# Metrics
duration: 13 min
completed: 2026-03-12
---

# Phase 8 Plan 4: Salary Cap & Dashboard Summary

**Salary cap page with stacked bar chart and penalty DataTable, plus action-driven dashboard surfacing free agents, expiring contracts, and available allotments**

## Performance

- **Duration:** 13 min
- **Started:** 2026-03-12T09:25:19Z
- **Completed:** 2026-03-12T09:37:57Z
- **Tasks:** 3 (2 auto + 1 checkpoint)
- **Files modified:** 8

## Accomplishments
- Salary cap page with CapChart (stacked bar by contract type), CapSummaryCards, penalty DataTable, and AllotmentsCard
- Action-driven dashboard with free agents and expiring contracts as top-priority items
- Cap overview and roster summary widgets with links to full pages
- Complete 4-page frontend app: Dashboard → Roster → Player Detail → Salary Cap

## Task Commits

Each task was committed atomically:

1. **Task 1: Salary cap page with chart, penalty table, and allotments** - `26bee6c` (feat)
2. **Task 2: Dashboard page with action items, cap overview, and roster summary** - `edf6302` (feat)
3. **Fix: Chart height and free agent action items** - `aea261f` (fix)

## Files Created/Modified
- `frontend/src/components/cap/CapSummaryCards.tsx` - Stat cards for total salary, penalty exposure, roster count
- `frontend/src/components/cap/CapChart.tsx` - Stacked horizontal bar chart (NG/SD/FG breakdown)
- `frontend/src/components/cap/AllotmentsCard.tsx` - Remaining allotments display
- `frontend/src/pages/SalaryCapPage.tsx` - Full cap page with team selector, chart, penalty DataTable, allotments
- `frontend/src/components/dashboard/ActionItems.tsx` - Free agents, expiring contracts, available allotments
- `frontend/src/components/dashboard/CapOverview.tsx` - Compact cap summary with link to /cap
- `frontend/src/components/dashboard/RosterSummary.tsx` - Roster count by position with link to /roster
- `frontend/src/pages/DashboardPage.tsx` - Dashboard with team snapshot, action items, cap/roster widgets

## Decisions Made
- Fixed chart height (120px) instead of responsive aspect-video — single stacked bar doesn't need 16:9 ratio
- Free agents (years_remaining=0) surfaced as highest-priority action items — they can be tagged/tendered in offseason
- Tags and tenders prioritized above buyouts in allotment action items

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Chart too tall with aspect-video default**
- **Found during:** Checkpoint verification
- **Issue:** ChartContainer's default aspect-video class made the single-bar chart disproportionately tall
- **Fix:** Override with h-[120px] and [&>div]:!aspect-auto
- **Files modified:** frontend/src/components/cap/CapChart.tsx
- **Verification:** Chart renders at compact height, page is readable
- **Committed in:** aea261f

**2. [Rule 1 - Bug] Action items missing free agents for tags/tenders**
- **Found during:** Checkpoint verification (user feedback)
- **Issue:** Only showed years_remaining=1 as expiring; missed years_remaining=0 free agents who are the primary offseason action items
- **Fix:** Added free agent detection (years_remaining=0) with highest priority, reordered allotments
- **Files modified:** frontend/src/components/dashboard/ActionItems.tsx
- **Verification:** Free agents appear at top of action items with "Free Agent" badge
- **Committed in:** aea261f

### Deferred Enhancements

Logged to .planning/ISSUES.md for future consideration:
- ISS-001: Extension window awareness — league calendar system for offseason/in-season signing periods (discovered during checkpoint)

---

**Total deviations:** 2 auto-fixed (2 bugs from UAT feedback), 1 deferred
**Impact on plan:** Fixes improved usability; no scope creep.

## Issues Encountered
None — both tasks built cleanly on first pass.

## Next Phase Readiness
- Phase 8 complete — all 4 plans done
- Full frontend application functional: Dashboard, Roster, Player Detail, Salary Cap
- Known data issues from Phase 8 Plan 3 (ISS-001/002 in 08-03-ISSUES.md) still open
- ISS-001 logged for future league calendar system

---
*Phase: 08-frontend-ui*
*Completed: 2026-03-12*
