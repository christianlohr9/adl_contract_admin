---
phase: 04-contract-engine
plan: 02
subsystem: api
tags: [decimal, extensions, epv, salary-smoothing, eligibility]

# Dependency graph
requires:
  - phase: 04-contract-engine/01
    provides: Rules loader, EPV calculation core, salary rounding helpers
  - phase: 01-rules-extraction
    provides: extensions.yaml formulas, contracts.json constants
provides:
  - EYS calculation (Extension Year Salary)
  - Salary smoothing with compound growth
  - Extension eligibility checking (all bylaws rules)
  - Full extension options calculator (1-year through max)
affects: [04-03 tags/tenders, 04-04 buyouts, 06-api-layer]

# Tech tracking
tech-stack:
  added: []
  patterns: [dataclass result types for contract tools, Decimal boundary conversion]

key-files:
  created: [src/app/services/extensions.py]
  modified: []

key-decisions:
  - "Implemented eligibility + options in single file (self-contained tool per CONTEXT.md)"
  - "5YO detection via '+' or '5YO' in contract designation string"
  - "Total value calculated with compound growth applied to smoothed salary"

patterns-established:
  - "Contract tool pattern: check eligibility → calculate EPV → generate options → return typed result"

issues-created: []

# Metrics
duration: 2min
completed: 2026-03-11
---

# Phase 4 Plan 2: Extensions Engine Summary

**EYS calculation with 5YO adjustment, compound salary smoothing, bylaws eligibility checks, and full extension options service**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-11T10:32:25Z
- **Completed:** 2026-03-11T10:34:48Z
- **Tasks:** 2
- **Files created:** 1

## Accomplishments
- EYS formula: MAX(EPVs, floor) × (1.15 - 0.05 × effective_years) with 5YO adjustment
- Salary smoothing using 10% compound growth across old + new contract years
- SD minimum floor applied after smoothing
- Eligibility checker enforcing all bylaws rules (years remaining, prior EXT, robust PR, rookie contract, max 6 years)
- Full extension options calculator returning typed ExtensionResult with all valid options

## Task Commits

Each task was committed atomically:

1. **Task 1: EYS calculation and salary smoothing** - `28c3deb` (feat)
2. **Task 2: Extension options service with eligibility checks** - included in `28c3deb` (logically coupled, single self-contained file)

## Files Created/Modified
- `src/app/services/extensions.py` — Complete extensions engine: dataclasses, EYS, smoothing, eligibility, options calculator (253 lines)

## Decisions Made
- Combined Tasks 1 and 2 into single file as they are tightly coupled — the extension service is self-contained per CONTEXT.md
- 5YO status detected from contract designation containing "+" or "5YO"
- Total contract value uses compound growth applied to smoothed salary across all years

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None

## Next Phase Readiness
- Extensions engine complete, ready for franchise/transition tags (04-03)
- Pattern established: eligibility → EPV → options → typed result
- All contract tools can follow same pattern

---
*Phase: 04-contract-engine*
*Completed: 2026-03-11*
