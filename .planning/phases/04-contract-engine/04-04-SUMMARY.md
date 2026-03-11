---
phase: 04-contract-engine
plan: 04
subsystem: engine
tags: [buyout, restructure, 5yo, ppe, decimal, franchise-tags, tenders]

# Dependency graph
requires:
  - phase: 04-contract-engine/04-01
    provides: EPV calculations and rules loader
  - phase: 04-contract-engine/04-03
    provides: Franchise tag and tender price calculations
provides:
  - Buyout/Restructure salary calculations with GM option builder
  - 5th Year Option pricing via NEFT/TT/modified-TT tiers
  - Proven Performance Escalator salary via SRFA/ORFA tenders
  - Complete contract engine (all 6 service modules)
affects: [05-salary-cap, 06-api-layer, 08-frontend-ui]

# Tech tracking
tech-stack:
  added: []
  patterns: [modified-TT salary calculation with custom rank ranges, starter percentile ranking]

key-files:
  created: [src/app/services/buyouts.py]
  modified: []

key-decisions:
  - "All B/R, 5YO, and PPE in single buyouts.py module — complementary contract tools"
  - "Modified TT uses _get_top_n_positional_salaries with custom rank ranges for 25-75% and bottom-25% tiers"
  - "Starter percentile computed from PlayerScore YTD points vs position group"

patterns-established:
  - "Modified tag salary calculation: reuse franchise_tags helpers with custom rank parameters"
  - "Percentile-based tier routing for salary determination"

issues-created: []

# Metrics
duration: 4min
completed: 2026-03-11
---

# Phase 4 Plan 4: Buyouts, 5YO, and PPE Summary

**Buyout/restructure salary engine with 5% per-year discount, 5th Year Option pricing via NEFT/TT percentile tiers, and PPE escalator via SRFA/ORFA tender prices — completing all 6 contract engine service modules**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-11T11:23:35Z
- **Completed:** 2026-03-11T11:28:00Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Buyout/Restructure: opening bid, salary tiers (1-6 years), 4 GM options, eligibility checks for rookie/UDFA restrictions
- 5th Year Option: percentile-based tier determination (top 87.5% → NEFT, 75-87.5% → TT, 25-75% → modified TT 3rd-20th, bottom 25% → modified TT 3rd-25th)
- Proven Performance Escalator: Level 3 (75th+) → SRFA price, Level 1-2 → ORFA price; excludes PK/PN positions
- Phase 4 complete: all 6 service modules created (rules, epv, extensions, franchise_tags, tenders, buyouts)

## Task Commits

Both tasks committed together (single file creation):

1. **Task 1: Buyout/Restructure salary calculations** - `0ccc43d` (feat)
2. **Task 2: 5YO and PPE calculations** - `0ccc43d` (feat, same commit)

## Files Created/Modified
- `src/app/services/buyouts.py` (770 lines) - Buyout/restructure, 5YO, and PPE calculation services

## Decisions Made
- Combined all B/R, 5YO, and PPE into single `buyouts.py` module — they are complementary contract tools
- Modified TT salary calculation reuses `_get_top_n_positional_salaries` from franchise_tags.py with custom rank ranges
- Starter percentile computed from PlayerScore YTD points ranked against position group

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed ruff lint warnings**
- **Found during:** Task 2 (after full file written)
- **Issue:** Unused import and nested `if` statements flagged by ruff
- **Fix:** Removed unused import, collapsed nested conditionals
- **Files modified:** src/app/services/buyouts.py
- **Verification:** `ruff check` passes clean
- **Committed in:** 0ccc43d

---

**Total deviations:** 1 auto-fixed (1 bug), 0 deferred
**Impact on plan:** Minor lint cleanup. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- Phase 4 complete — all contract engine services built
- Ready for Phase 5: Salary Cap & Validation
- All 6 service modules importable: rules, epv, extensions, franchise_tags, tenders, buyouts

---
*Phase: 04-contract-engine*
*Completed: 2026-03-11*
