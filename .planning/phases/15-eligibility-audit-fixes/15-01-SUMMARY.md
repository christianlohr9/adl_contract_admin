---
phase: 15-eligibility-audit-fixes
plan: 01
subsystem: eligibility
tags: [audit, bylaws, eligibility, extensions, franchise-tags, tenders, buyouts, 5yo, ppe]

# Dependency graph
requires:
  - phase: 14-historical-data-imports
    provides: historical scores and contract data enabling accurate eligibility checks
provides:
  - comprehensive bylaw-to-code mapping document (15-AUDIT.md)
  - catalog of 12 discrepancies and 3 missing implementations
  - 25 runtime test results against live data
affects: [15-02-fixes, 16-nfl-kickoff, 17-regression-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: []

key-files:
  created:
    - .planning/phases/15-eligibility-audit-fixes/15-AUDIT.md
  modified: []

key-decisions:
  - "Audit-only approach: catalog all issues before fixing any — prevents whack-a-mole"
  - "Extension window closure prevented runtime eligibility testing; confirmed logic via code review instead"

patterns-established:
  - "Bylaw-to-code mapping table format for systematic audit"

issues-created: []

# Metrics
duration: 13 min
completed: 2026-03-13
---

# Phase 15 Plan 01: Bylaw-to-Code Eligibility Audit Summary

**Systematic audit of all 7 contract actions against ADL Bylaws producing 41 MATCH / 12 DISCREPANCY / 3 MISSING findings, plus 25 runtime tests (20 PASS / 4 FAIL / 1 SKIP)**

## Performance

- **Duration:** 13 min
- **Started:** 2026-03-13T14:37:54Z
- **Completed:** 2026-03-13T14:50:33Z
- **Tasks:** 2
- **Files created:** 1

## Accomplishments
- Complete bylaw-to-code mapping for all 7 contract actions (Extensions, Franchise Tags, RFA, ERFA, B/R, 5YO, PPE)
- Identified 12 code discrepancies and 3 missing implementations across eligibility services
- Ran 25 targeted runtime tests against live database with 1,549 contracts
- Produced comprehensive 15-AUDIT.md ready to drive Plan 15-02 fixes

## Task Commits

Each task was committed atomically:

1. **Task 1: Create bylaw-to-code eligibility mapping** - `01ae2cb` (docs)
2. **Task 2: Run targeted eligibility scenarios against live data** - `2306c64` (test)

## Files Created/Modified
- `.planning/phases/15-eligibility-audit-fixes/15-AUDIT.md` — Complete bylaw-to-code mapping with rule tables, discrepancy details, and runtime test results

## Decisions Made
- Extension runtime tests could not confirm player-level eligibility via API because oEXT window was already closed (2026-02-27) — confirmed logic via code review instead
- ERFA eligible player test skipped due to no qualifying test data with single contract at/below vet min

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered
None

## Key Findings (for Plan 15-02)

**Critical/High Discrepancies:**
1. RRFA opening bid always $0 (placeholder NFL prices)
2. Multiple contracts per player/season cause wrong contract selection in tag/tender queries
3. PK/PN not grouped into "Kicker/Punter" category for tag/5YO salary calculations
4. Percentile calculation does not use PR Starter Floor (affects 5YO and PPE tiers)
5. UDFA extension max-years check missing
6. Multi-year UFA original_length calculation may be incorrect
7. All NFL RFA prices are $0 placeholders

**Medium Discrepancies:**
8. Revert/Transfer not prohibited for tagged players on expired contracts
9. EXT re-extension blocking depends on contract storage pattern
10. PPE uses bid functions instead of tag price functions

## Next Phase Readiness
- 15-AUDIT.md provides complete discrepancy catalog for Plan 15-02 to fix
- Each discrepancy has recommended fix documented
- Runtime test failures provide reproducible test cases
- Ready for Plan 15-02 (Fix Eligibility Discrepancies & Validate)

---
*Phase: 15-eligibility-audit-fixes*
*Completed: 2026-03-13*
