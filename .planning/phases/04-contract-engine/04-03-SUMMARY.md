---
phase: 04-contract-engine
plan: 03
subsystem: contract-engine
tags: [franchise-tags, tenders, erfa, rfa, decimal-arithmetic, free-agency]

# Dependency graph
requires:
  - phase: 04-01
    provides: rules loader, EPV calculation core
  - phase: 04-02
    provides: extensions engine patterns, EYS calculation
provides:
  - Franchise tag price calculations (EFT, NEFT, TT)
  - Opening bid calculations for NEFT/TT tags
  - Consecutive tag tracking and premium pricing
  - ERFA tender salary calculations
  - RFA tender bid calculations (FRFA, SRFA, ORFA, RRFA)
  - Tag and tender eligibility checks
affects: [05-salary-cap, 06-api-layer, 08-frontend-ui]

# Tech tracking
tech-stack:
  added: []
  patterns: [tag-option-dataclass, tender-option-dataclass, positional-salary-averaging]

key-files:
  created:
    - src/app/services/franchise_tags.py
    - src/app/services/tenders.py
  modified: []

key-decisions:
  - "NFL RFA prices left as parameters with Decimal('0') defaults — external values not yet in constants"
  - "Tag salary uses AVG(Top-N salaries) vs 1.20x prev_salary MAX — Cap% treated as positional average per bylaws"

patterns-established:
  - "TagOption/TenderOption result dataclasses for multi-option contract tools"
  - "Eligibility check returns tuple[bool, str | None] pattern"

issues-created: []

# Metrics
duration: 4 min
completed: 2026-03-11
---

# Phase 4 Plan 3: Franchise Tags & Tenders Summary

**Franchise tag (EFT/NEFT/TT) and tender (ERFA/RFA) price calculation services with positional salary averaging and bylaws-based eligibility checks**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-11T10:48:54Z
- **Completed:** 2026-03-11T10:52:25Z
- **Tasks:** 2
- **Files created:** 2

## Accomplishments
- Franchise tag service calculates EFT/NEFT/TT prices using top-5/top-10 positional salary averages
- Opening bid formula for NEFT/TT: MAX(CEIL_100K(SD_Min - 0.1), FLOOR_100K(tag_salary))
- Consecutive tag tracking with third-tag premium pricing
- ERFA tender: MAX(veteran_minimum, 1.10 × prev_salary)
- Four RFA tender bid calculators with correct multipliers and FLOOR_100K rounding
- Full eligibility checks for tags, ERFA, and RFA per bylaws rules

## Task Commits

Each task was committed atomically:

1. **Task 1: Franchise tag price calculations** - `bb8630a` (feat)
2. **Task 2: ERFA and RFA tender calculations** - `9ad20e3` (feat)

**Plan metadata:** (this commit) (docs: complete plan)

## Files Created/Modified
- `src/app/services/franchise_tags.py` - TagOption/FranchiseTagResult dataclasses, tag salary calculation, opening bid formula, consecutive tag premium, eligibility checks, main orchestrator
- `src/app/services/tenders.py` - TenderOption/TenderResult dataclasses, ERFA salary calculation, four RFA bid calculators, eligibility checks, main orchestrator

## Decisions Made
- NFL RFA prices (FRFA, SRFA, ORFA, RRFA) are external values not yet in constants — left as optional parameters with Decimal('0') defaults and TODO comments. Plan explicitly noted this as expected.
- Tag salary implemented as MAX(AVG(Top-N salaries), 1.20 × prev_salary) — the Cap% reference in bylaws treated as positional average per plan guidance.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## Next Phase Readiness
- Tag and tender services complete, ready for 04-04 (Buyouts and Restructures)
- All contract tools (X-A through X-D) now implemented
- NFL RFA prices need to be added to constants when available (non-blocking)

---
*Phase: 04-contract-engine*
*Completed: 2026-03-11*
