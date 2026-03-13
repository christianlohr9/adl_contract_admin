---
phase: 13-calendar-timeline-deadline-countdowns
plan: 02
subsystem: ui
tags: [react, shadcn-ui, tailwind, deadline-countdown, contract-management]

# Dependency graph
requires:
  - phase: 10-period-detection-date-aware-eligibility
    provides: Window status service with opens/closes dates
  - phase: 12-contract-management-dashboard
    provides: ContractManagementPage with WindowStatusBar, eligibility data flow
provides:
  - DeadlineCountdown component with urgency-colored countdown cards
  - useDeadlineCards hook deriving card data from window statuses
  - Day-level urgency awareness (green >14d, yellow 7-14d, red ≤7d)
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Urgency color coding: green (>14d), yellow (7-14d), red (≤7d)"
    - "Local timezone date parsing: new Date(dateStr + 'T00:00:00')"

key-files:
  created:
    - frontend/src/components/contracts/DeadlineCountdown.tsx
    - frontend/src/components/contracts/useDeadlineCards.ts
  modified:
    - frontend/src/pages/ContractManagementPage.tsx

key-decisions:
  - "Opening windows always green urgency (informational, not urgent)"
  - "PPE omitted from countdown cards (always-open, no deadline)"
  - "WindowStatusBar fully deleted (replaced, not supplemented)"

patterns-established:
  - "Deadline countdown card pattern: hook derives cards, component renders grid"

issues-created: []

# Metrics
duration: 3min
completed: 2026-03-13
---

# Phase 13 Plan 02: Deadline Countdown Cards Summary

**Urgency-colored deadline countdown cards replacing WindowStatusBar — days remaining with green/yellow/red coding, eligible player counts, sorted by urgency**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-12T16:15:31Z
- **Completed:** 2026-03-13T07:56:41Z
- **Tasks:** 2 (+ 1 checkpoint)
- **Files modified:** 4 (2 created, 1 modified, 1 deleted)

## Accomplishments
- Built useDeadlineCards hook deriving countdown cards from window statuses and action groups
- Built DeadlineCountdown component with responsive grid of urgency-colored cards
- Replaced WindowStatusBar with DeadlineCountdown on ContractManagementPage
- Deleted WindowStatusBar.tsx (fully superseded)

## Task Commits

Each task was committed atomically:

1. **Task 1: Build DeadlineCountdown component and useDeadlineCards hook** - `560ae75` (feat)
2. **Task 2: Integrate DeadlineCountdown into ContractManagementPage** - `26d19ff` (feat)

## Files Created/Modified
- `frontend/src/components/contracts/useDeadlineCards.ts` - Custom hook deriving deadline cards with urgency, day counts, eligible counts
- `frontend/src/components/contracts/DeadlineCountdown.tsx` - Responsive card grid with urgency-colored borders/backgrounds
- `frontend/src/pages/ContractManagementPage.tsx` - Replaced WindowStatusBar with DeadlineCountdown
- `frontend/src/components/contracts/WindowStatusBar.tsx` - Deleted (fully replaced)

## Decisions Made
- Opening windows always use green urgency (informational "opens in X days", not urgent)
- PPE excluded from countdown cards (always-open, no deadline per Phase 10 decision)
- WindowStatusBar fully deleted rather than kept as fallback

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## Next Phase Readiness
- Phase 13 complete — all plans executed
- v1.1 milestone complete — League Calendar & Contract Management fully shipped

---
*Phase: 13-calendar-timeline-deadline-countdowns*
*Completed: 2026-03-13*
