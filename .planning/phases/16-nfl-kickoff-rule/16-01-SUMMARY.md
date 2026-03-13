---
phase: 16-nfl-kickoff-rule
plan: 01
subsystem: eligibility
tags: [eligibility, extensions, rookie, udfa, nfl-kickoff, season-calendar]

# Dependency graph
requires:
  - phase: 15-eligibility-audit-fixes
    provides: audited eligibility checks confirming kickoff rule was only remaining gap
  - phase: 09-league-calendar
    provides: SeasonCalendar model with regular_season_start field
provides:
  - NFL kickoff eligibility gate for rookie/UDFA final-year extensions
  - Conservative blocking when calendar date not configured
  - UAT-001 resolution
affects: [phase-17-regression-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-player-date-gating-via-season-calendar]

key-files:
  created: [tests/test_extension_kickoff.py]
  modified: [src/app/services/extensions.py]

key-decisions:
  - "Kickoff check placed inside rookie/UDFA block only — veterans unaffected"
  - "NULL calendar blocks conservatively with clear message rather than silently allowing"
  - "Check ordered after max-years check to avoid redundant messaging"

patterns-established:
  - "Per-player date gating: query SeasonCalendar within eligibility check for contract-type-specific rules"

issues-created: []

# Metrics
duration: 4min
completed: 2026-03-13
---

# Phase 16 Plan 01: NFL Kickoff Rule Summary

**NFL kickoff eligibility gate blocking rookie/UDFA final-year extensions before regular_season_start, with 6 targeted test cases**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-13T16:14:32Z
- **Completed:** 2026-03-13T16:18:37Z
- **Tasks:** 2 auto + 1 checkpoint (verified programmatically)
- **Files modified:** 2

## Accomplishments
- Added kickoff eligibility gate to `check_extension_eligibility()` — only applies to rookie/UDFA contracts in final year
- Conservative NULL calendar handling blocks with clear message when season start date not configured
- 6 test cases covering all scenarios: blocked before kickoff, allowed after, UDFA variant, non-rookie unaffected, multi-year unaffected, NULL calendar
- Resolves UAT-001 from Phase 4 (missing NFL kickoff eligibility rule)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add NFL kickoff eligibility check** - `d82e036` (feat)
2. **Task 2: Add tests for kickoff eligibility rule** - `4a8178b` (test)

## Files Created/Modified
- `src/app/services/extensions.py` - Added kickoff gate inside rookie/UDFA block after max-years check
- `tests/test_extension_kickoff.py` - 6 test cases for kickoff eligibility rule

## Decisions Made
- Kickoff check placed inside existing `if is_rookie_contract or is_udfa_contract:` block — veterans completely unaffected
- NULL calendar date blocks conservatively with descriptive message rather than silently allowing extensions
- Check ordered after max-years check to avoid redundant/confusing messaging for short-term contracts

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## Next Phase Readiness
- Phase 16 complete (single plan phase)
- All eligibility rules now match bylaws including NFL kickoff gate
- Ready for Phase 17: Regression Testing & Validation

---
*Phase: 16-nfl-kickoff-rule*
*Completed: 2026-03-13*
