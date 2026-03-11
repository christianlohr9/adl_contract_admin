---
phase: 05-salary-cap-validation
plan: 05-01-FIX
subsystem: contracts
tags: [contract-classifier, regex, designation-parsing, uat-fix]

# Dependency graph
requires:
  - phase: 05-salary-cap-validation
    provides: contract_classifier.py with tag/extension regex patterns
provides:
  - Fixed classifier handles all bylaws designation variants (NEFToff, TToff, iEXT, oEXT, numbered tags)
affects: [05-salary-cap-validation, 06-api-layer]

# Tech tracking
tech-stack:
  added: []
  patterns: [uppercase-aware regex for designation parsing]

key-files:
  created: []
  modified:
    - src/app/services/contract_classifier.py

key-decisions:
  - "Regex patterns use uppercase (OFF, IO) since classifier uppercases designation before matching"

patterns-established: []

issues-created: []

# Metrics
duration: 3 min
completed: 2026-03-11
---

# Phase 5 Plan 1 FIX: Classifier Regex Fixes Summary

**Fixed tag and extension regex patterns in contract classifier to handle all bylaws designation variants (NEFToff, TToff, iEXT, oEXT, numbered tags)**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-11T12:49:57Z
- **Completed:** 2026-03-11T12:53:15Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Fixed `_TAG_RE` to match `NEFToff`/`TToff` (offer sheet contracts) and numbered tags (`EFT1`, `NEFT2`) — uses uppercase `OFF` since classifier uppercases designations
- Fixed `_EXT_RE` to match `iEXT`/`oEXT` (in-season/offseason extensions) — uses uppercase `[IO]?EXT`
- Re-synced all 1,549 contracts with corrected classifier — Kelce moved from SD to FG

## Task Commits

1. **Task 1: Fix tag and extension regex patterns** - `5a91db9` (fix)
2. **Task 2: Re-sync contracts** - No file changes (runtime verification only)

## Files Created/Modified
- `src/app/services/contract_classifier.py` - Updated `_TAG_RE` and `_EXT_RE` regex patterns

## Decisions Made
- Used uppercase in regex patterns (`OFF`, `[IO]`) because `classify_contract_type` uppercases the designation string before matching — avoids case-sensitivity issues

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Regex patterns needed uppercase for case-insensitive matching**
- **Found during:** Task 1 (regex pattern update)
- **Issue:** Initial fix used lowercase `off` and `[io]`, but classifier calls `.upper()` on designation before regex search
- **Fix:** Changed to uppercase `OFF` and `[IO]` to match uppercased input
- **Verification:** All 8 classifier test cases pass
- **Committed in:** 5a91db9

---

**Total deviations:** 1 auto-fixed (1 bug), 0 deferred
**Impact on plan:** Quick fix, same commit. No scope creep.

## Issues Encountered
None

## UAT Issues Addressed
- **UAT-001 (Major):** NEFToff/TToff now recognized as tag contracts → FG classification
- **UAT-002 (Minor):** iEXT/oEXT now matched by EXT regex → SD classification even below salary threshold

## Next Phase Readiness
- All classifier regex issues fixed, ready for re-verification or 05-02

---
*Phase: 05-salary-cap-validation*
*Completed: 2026-03-11*
