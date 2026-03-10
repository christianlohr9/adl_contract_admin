---
phase: 01-rules-extraction
plan: 01
subsystem: rules
tags: [json, bylaws, constants, contracts, roster, salary-cap]

# Dependency graph
requires:
  - phase: none
    provides: first phase
provides:
  - Verified contracts.json with salary minimums, contract types, tags, extensions, tenders, auctions
  - Verified league.json with league structure, finances, playoffs, schedule
  - Verified rosters.json with roster limits, lineup slots, taxi squad, waivers
  - Verified salary_cap.json with cap formula, penalties, snapshots, trade cash, deposits
affects: [02-foundation, 04-contract-engine, 05-salary-cap]

# Tech tracking
tech-stack:
  added: []
  patterns: [json-constants-lean-format]

key-files:
  created: []
  modified:
    - rules/constants/contracts.json
    - rules/constants/league.json
    - rules/constants/rosters.json
    - rules/constants/salary_cap.json

key-decisions:
  - "Used pay-in table value ($3,810) over payout header ($3,840) for prize pool due to bylaws inconsistency"
  - "Renamed neft_bid_error_penalty to bid_error_penalty since it applies to both NEFT and RFA auctions"
  - "Kept lean format throughout — no _note, _formula, or _rationale fields"

patterns-established:
  - "JSON constants: raw values only, grouped by use/contract type, no bylaw citations"

issues-created: []

# Metrics
duration: 5min
completed: 2026-03-10
---

# Phase 1 Plan 1: Verify Rule Constants Summary

**Cross-verified all 4 JSON constant files against ADL Bylaws — added ~200 missing values for tags, tenders, extensions, auctions, and cap penalties**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-10T10:19:24Z
- **Completed:** 2026-03-10T10:24:40Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Cross-referenced every numeric value in all 4 JSON files against bylaws source of truth
- Added substantial missing constants: franchise tags, extensions, RFA/ERFA tenders, buyouts, 5th year option, proven performance escalator, all auction parameters
- Removed all description fields (_note, _formula, _rationale) per lean format guidance
- Added NFL minimum salary tables, cap penalty acceleration details, future deposit thresholds

## Task Commits

Each task was committed atomically:

1. **Task 1: Verify contracts.json and league.json** - `7639389` (verify)
2. **Task 2: Verify rosters.json and salary_cap.json** - `61a0667` (verify)

## Files Created/Modified
- `rules/constants/contracts.json` - Salary minimums, contract types/limits, tags, extensions, tenders, buyouts, all auction params, signing windows
- `rules/constants/league.json` - League structure, conferences, schedule, playoffs, finances, commissioner info
- `rules/constants/rosters.json` - Roster limits by phase, lineup slots with min/max, taxi squad rules, waivers, IR eligibility
- `rules/constants/salary_cap.json` - Cap formula, snapshot weeks, penalty rates (NG/SD/FG), trade cash, bid error penalty, deposit thresholds

## Decisions Made
- Used pay-in table value ($3,810) for prize pool over payout header ($3,840) — bylaws has an internal inconsistency
- Renamed `neft_bid_error_penalty` to `bid_error_penalty` — applies to both NEFT and RFA auctions per bylaws VIII-C and VIII-D
- Kept lean format: removed all `_note`, `_formula`, `_rationale` description fields

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added ~200 missing constants across all 4 files**
- **Found during:** Tasks 1 and 2
- **Issue:** Original JSON files had basic values but were missing franchise tags, extensions, RFA/ERFA tenders, buyouts, 5th year option, proven performance escalator, all auction parameters, cap penalty acceleration details, and more
- **Fix:** Added all values directly from bylaws passages
- **Files modified:** All 4 JSON files
- **Verification:** Every added value traces to a specific bylaws passage

---

**Total deviations:** 1 auto-fixed (missing critical constants), 0 deferred
**Impact on plan:** Essential additions — these constants are required for the contract engine in Phase 4. No scope creep.

## Issues Encountered
- Bylaws has internal inconsistency in Section XIV: pay-in table yields $3,810 but payout header says $3,840. Used the pay-in table value. Not a blocking issue.

## Next Phase Readiness
- contracts.json and league.json verified and complete
- rosters.json and salary_cap.json verified and complete
- Ready for 01-02-PLAN.md (next plan in Phase 1)

---
*Phase: 01-rules-extraction*
*Completed: 2026-03-10*
