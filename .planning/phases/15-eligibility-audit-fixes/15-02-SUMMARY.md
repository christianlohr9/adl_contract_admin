---
phase: 15-eligibility-audit-fixes
plan: 02
subsystem: eligibility
tags: [eligibility, extensions, franchise-tags, tenders, buyouts, rfa, erfa, validation]

# Dependency graph
requires:
  - phase: 15-01
    provides: bylaw-to-code audit catalog (15-AUDIT.md) identifying all discrepancies
  - phase: 14
    provides: historical player scores and contract data for eligibility checks
provides:
  - All 7 eligibility checks match bylaws (except NFL kickoff rule)
  - Full roster validation with zero anomalies across 879 players
  - UDFA extension max-years enforcement
  - PK/PN tag salary grouping
  - Real NFL RFA tender prices
  - Revert/Transfer prohibition for tagged players
  - PR Starter Floor calculation for buyouts/PPE
affects: [phase-16-nfl-kickoff, phase-17-regression-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: [position-group-mapping-for-tag-salary, season-based-nfl-rfa-price-lookup]

key-files:
  created: [.planning/phases/15-eligibility-audit-fixes/15-VALIDATION.md]
  modified: [src/app/services/extensions.py, src/app/services/franchise_tags.py, src/app/services/tenders.py, src/app/services/buyouts.py, rules/constants/contracts.json]

key-decisions:
  - "Used 2026 NFL CBA RFA tender prices from official sources rather than placeholder zeros"
  - "Added active-contract check to prevent tag/tender eligibility for re-signed players"

patterns-established:
  - "_resolve_position_filter(): PK/PN grouped as Kicker/Punter for salary queries"
  - "_get_nfl_rfa_prices(): season-based lookup for NFL RFA tender prices from contracts.json"

issues-created: []

# Metrics
duration: 20min
completed: 2026-03-13
---

# Phase 15 Plan 02: Fix Eligibility Discrepancies & Validate Summary

**Fixed 10+ eligibility discrepancies (UDFA extensions, PK/PN tag grouping, NFL RFA prices, re-signed player checks) and validated 879 players across all 32 teams with zero anomalies**

## Performance

- **Duration:** 20 min
- **Started:** 2026-03-13T15:09:51Z
- **Completed:** 2026-03-13T15:29:58Z
- **Tasks:** 3 (2 auto + 1 checkpoint)
- **Files modified:** 6

## Accomplishments
- Fixed every DISCREPANCY and MISSING item from 15-AUDIT.md across 5 service files
- Added UDFA contract max-years check (3 years) to extension eligibility
- Grouped PK/PN into single "Kicker/Punter" category for tag/5YO salary queries
- Added real 2026 NFL RFA tender prices replacing $0 placeholders
- Added previous-RFA ineligibility check and multi-year UFA original_length fix
- Prohibited Revert/Transfer for tagged players on expired contracts
- Implemented PR Starter Floor calculation for buyouts and PPE
- Full roster sweep: 32 teams, 879 players, 6,153 checks, zero anomalies

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix all eligibility discrepancies from audit** - `2d61310` (fix)
2. **Task 1 deviation: Block tag/tender for re-signed players** - `7318969` (fix)
3. **Task 2: Full roster sweep validation** - `0dbf9c5` (test)

**Plan metadata:** (pending)

## Files Created/Modified
- `src/app/services/extensions.py` - E2-D UDFA max-years check
- `src/app/services/franchise_tags.py` - F6-M PK/PN grouping, Q2-D expired contract filter, re-signed player check
- `src/app/services/tenders.py` - R4-D original_length, R6-D/R9-D NFL RFA prices, R10-D previous-RFA check, Q3-D filter, re-signed player check
- `src/app/services/buyouts.py` - B6-M revert/transfer prohibition, Y7-D/P7-D PR Starter Floor, P4-D PPE tag prices
- `rules/constants/contracts.json` - Added `nfl_rfa_prices_by_year` with 2025/2026 NFL RFA tender prices
- `.planning/phases/15-eligibility-audit-fixes/15-VALIDATION.md` - Full roster sweep results

## Decisions Made
- Used 2026 NFL CBA RFA tender prices (FRFA=$8.046M, SRFA=$5.767M, ORFA=$3.674M, RRFA=$3.520M) rather than placeholder zeros
- Added active-contract existence check to tag/tender eligibility — players who have been re-signed are not eligible for tags/tenders on their expired contract

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed tag/tender eligibility for re-signed players**
- **Found during:** Task 2 (Full roster sweep validation)
- **Issue:** 97 false positives — players with both expired and active contracts showed as tag/tender eligible. Q2-D/Q3-D correctly queried expired contracts but didn't check if the player had been re-signed.
- **Fix:** Added "active contract exists" check to `check_tag_eligibility()`, `check_erfa_eligibility()`, and `check_rfa_eligibility()`
- **Files modified:** src/app/services/franchise_tags.py, src/app/services/tenders.py
- **Verification:** Second roster sweep showed 0 anomalies
- **Commit:** `7318969`

---

**Total deviations:** 1 auto-fixed (bug), 0 deferred
**Impact on plan:** Essential fix for correctness — eliminated 97 false positives. No scope creep.

## Issues Encountered
None — all discrepancies were fixable as documented in the audit.

## Next Phase Readiness
- All 7 eligibility checks now match bylaws (except NFL kickoff rule — Phase 16)
- 879 players validated with zero anomalies
- Only known gap: NFL kickoff gating for rookie extensions (Phase 16)
- Ready for Phase 16: NFL Kickoff Rule

---
*Phase: 15-eligibility-audit-fixes*
*Completed: 2026-03-13*
