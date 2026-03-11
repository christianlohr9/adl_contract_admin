---
phase: 05-salary-cap-validation
plan: 01
subsystem: contracts
tags: [contract-types, cap-penalties, salary-cap, decimal-math, dataclass]

# Dependency graph
requires:
  - phase: 03-mfl-api-integration
    provides: roster_sync with contract data
  - phase: 04-contract-engine
    provides: EPV calculations, round_to_10k helpers, rules.py loaders
provides:
  - Contract type classifier (NG/SD/FG)
  - Cap penalty calculator (all 3 types with acceleration/suspension/retired)
  - Team cap summary service with per-player penalty breakdown
affects: [05-salary-cap-validation, 06-api-layer]

# Tech tracking
tech-stack:
  added: []
  patterns: [pure-function classifiers, penalty result dataclasses, batch-query aggregation]

key-files:
  created:
    - src/app/services/contract_classifier.py
    - src/app/services/cap_penalties.py
    - src/app/services/cap_summary.py
  modified:
    - src/app/services/roster_sync.py
    - src/app/services/rules.py
    - docker-compose.yml

key-decisions:
  - "FG multi-year split uses ceil_10k/floor_10k (not ceil_100k/floor_100k) to match bylaws examples"
  - "Draft round passed from Player model for more reliable classifier input"

patterns-established:
  - "Pure function classifier: all inputs as parameters, no DB access"
  - "PenaltyResult dataclass with notes field explaining calculation"
  - "Batch query + Python-side computation for team aggregation"

issues-created: []

# Metrics
duration: 6 min
completed: 2026-03-11
---

# Phase 5 Plan 1: Salary Cap Penalty Calculations Summary

**Contract type classifier (NG/SD/FG) with cap penalty calculator and team-level cap summary aggregation using Decimal-only math**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-11T12:27:00Z
- **Completed:** 2026-03-11T12:33:48Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments
- Contract type classifier correctly categorizes NG/SD/FG based on salary threshold, designation parsing (draft round, tags, 5YO, EXT), replacing Phase 3's NG placeholder
- Cap penalty calculator handles all 3 contract types with acceleration (pre-July 1 lump sum), suspension discount (50%), and retired/deceased ($0) modifiers
- Team cap summary service provides per-player penalty breakdown and aggregate totals by contract type using batch query pattern

## Task Commits

Each task was committed atomically:

1. **Task 1: Contract type classifier and roster sync integration** - `944a9c8` (feat)
2. **Task 2: Cap penalty calculator service** - `e97c9a2` (feat)
3. **Task 3: Team cap summary service** - `c521cb5` (feat)

## Files Created/Modified
- `src/app/services/contract_classifier.py` - Pure function classifying contracts as NG/SD/FG based on bylaws rules
- `src/app/services/cap_penalties.py` - Penalty calculators for all 3 contract types with modifiers
- `src/app/services/cap_summary.py` - Team-level cap aggregation with per-player penalty breakdown
- `src/app/services/roster_sync.py` - Updated to use classifier instead of hardcoded NG
- `src/app/services/rules.py` - Added get_cap_penalty_rates(), ceil_10k, floor_10k helpers
- `docker-compose.yml` - Added rules/ volume mount for Docker container

## Decisions Made
- FG multi-year split uses ceil_10k/floor_10k (not ceil_100k/floor_100k) — bylaws example ($21.85m → $10.93m/$10.92m) requires 10k precision matching salary_cap.json cap_penalty_rounding of 0.01
- Draft round passed from Player model to classifier for more reliable detection than designation-only parsing

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] FG multi-year split rounding precision**
- **Found during:** Task 2 (Cap penalty calculator)
- **Issue:** Plan specified ceil_100k/floor_100k for FG split, but bylaws example requires 10k precision
- **Fix:** Added ceil_10k and floor_10k helpers to rules.py, used for FG split
- **Verification:** FG $6.60m/3yr → $10.93m/$10.92m matches bylaws exactly
- **Committed in:** e97c9a2

**2. [Rule 3 - Blocking] Docker rules/ volume mount missing**
- **Found during:** Task 1 (Contract classifier verification)
- **Issue:** rules/ directory not mounted in Docker container, causing FileNotFoundError
- **Fix:** Added `./rules:/app/rules` volume mount to docker-compose.yml
- **Verification:** Import and classification tests pass in Docker
- **Committed in:** 944a9c8

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking), 0 deferred
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- Cap penalty calculations complete, ready for 05-02 (contract eligibility validation)
- All verification checks pass including bylaws example validation

---
*Phase: 05-salary-cap-validation*
*Completed: 2026-03-11*
