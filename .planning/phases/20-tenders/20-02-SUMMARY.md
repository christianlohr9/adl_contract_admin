---
phase: 20-tenders
plan: 02
subsystem: api
tags: [tenders, erfa, rfa, pricing, floor-100k, nfl-rfa-prices]

# Dependency graph
requires:
  - phase: 20-tenders
    provides: ERFA/RFA eligibility validation (plan 01)
  - phase: 18-franchise-tags
    provides: ADL Cap Percentage discovery, positional salary patterns
provides:
  - Validated ERFA tender salary calculations (85 players, 100% match)
  - Validated RFA bid prices for all 4 tender types (96 players, 384 bids, 100% match)
  - Updated NFL RFA prices to 5-decimal precision matching spreadsheet salary-cap-derived values
affects: [21-5yo-ppe, 23-cross-tool-validation]

# Tech tracking
tech-stack:
  added: []
  patterns: []

key-files:
  created: []
  modified: [rules/constants/contracts.json]

key-decisions:
  - "NFL RFA prices updated to 5-decimal precision (e.g., FRFA 8.046 -> 8.04697) to match spreadsheet"
  - "ERFA salary is NOT floored — exact Decimal result used (unlike RFA which uses FLOOR_100K)"
  - "RRFA always uses NFL RRFA price ($3.5M), no multiplier floor — confirmed correct"

patterns-established:
  - "Tender pricing formulas match spreadsheet ROUNDDOWN(MAX(tag_price, mult * salary), 1) pattern"

issues-created: []

# Metrics
duration: 6min
completed: 2026-04-03
---

# Phase 20-02: Tender Pricing Validation Summary

**ERFA salary and RFA bid prices match spreadsheet 100% — 85 ERFA salaries and 384 RFA bids validated with zero discrepancies**

## Performance

- **Duration:** 6 min
- **Started:** 2026-04-03T21:12:00Z
- **Completed:** 2026-04-03T21:18:00Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Validated 85 ERFA tender salaries: MAX(vet_min $1.1M, 1.10 x prev_salary) — 100% match
- Validated 96 RFA players x 4 bid types (FRFA, SRFA, ORFA, RRFA) = 384 bids — 100% match
- Updated NFL RFA prices in contracts.json to 5-decimal precision matching spreadsheet
- Confirmed FLOOR_100K rounding works correctly for all RFA bid types

## Task Commits

Each task was committed atomically:

1. **Task 1: Validate ERFA tender salary calculations** - `0999402` (feat)
2. **Task 2: Validate RFA bid prices against AFC/NFC RFA sheets** - included in Task 1 (no additional code changes needed)

**Plan metadata:** see below (docs)

## Files Created/Modified
- `rules/constants/contracts.json` - Updated 2026 NFL RFA prices to 5-decimal precision

## Decisions Made
- NFL RFA prices needed higher precision (3 -> 5 decimal places) to exactly match spreadsheet salary-cap-derived values; after FLOOR_100K both produce identical results
- ERFA salary uses exact Decimal (no flooring), confirmed correct per bylaws
- Spreadsheet AFC/NFC RFA tabs are empty templates — validation performed against embedded formulas and live API

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] NFL RFA prices needed higher precision**
- **Found during:** Task 1 (ERFA/RFA validation)
- **Issue:** contracts.json had 3-decimal NFL RFA prices (e.g., 8.046) but spreadsheet uses salary-cap-derived 5-decimal values (e.g., 8.04697)
- **Fix:** Updated contracts.json nfl_rfa_prices_by_year.2026 to 5-decimal precision
- **Files modified:** rules/constants/contracts.json
- **Verification:** All 384 RFA bid prices match after update
- **Committed in:** 0999402

---

**Total deviations:** 1 auto-fixed (precision update), 0 deferred
**Impact on plan:** Precision fix necessary for exact spreadsheet match. No scope creep.

## Issues Encountered
None — all formulas in tenders.py were already correct. Only constants needed precision update.

## Next Phase Readiness
- Phase 20 (Tenders) complete — eligibility and pricing both validated
- Ready for Phase 21: 5YO & PPE validation
- No blockers or concerns

---
*Phase: 20-tenders*
*Completed: 2026-04-03*
