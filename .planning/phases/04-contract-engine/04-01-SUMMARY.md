---
phase: 04-contract-engine
plan: 01
subsystem: api
tags: [decimal, lru_cache, epv, sqlalchemy, pyyaml]

# Dependency graph
requires:
  - phase: 01-rules-extraction
    provides: JSON constants and YAML formulas in rules/
  - phase: 02-foundation
    provides: SQLAlchemy models (Contract, Player, PlayerScore)
provides:
  - Cached rules loader service (load_constants, load_formulas)
  - Decimal salary rounding helpers (round_to_10k, round_to_100k, floor/ceil)
  - Season-specific salary minimum lookups with year fallback
  - EPV calculation core (position rank, robust season, performance salary, EPV orchestrator)
affects: [04-02 extensions, 04-03 tags/tenders, 04-04 buyouts, 05-salary-cap]

# Tech tracking
tech-stack:
  added: []
  patterns: [lru_cache for file-based config, Decimal-only salary math, dense_rank window function]

key-files:
  created: [src/app/services/rules.py, src/app/services/epv.py]
  modified: []

key-decisions:
  - "Salaries in millions throughout (0.01 = $10k) matching Contract model Numeric(5,2)"
  - "Year fallback for season lookups — returns latest available if requested year missing"
  - "_sal_at_rank clamps to list bounds rather than erroring on out-of-range"

patterns-established:
  - "Rules access via cached loaders — all services import from rules.py, never read files directly"
  - "Decimal boundary conversion — DB floats converted to Decimal(str(val)) at query boundary"

issues-created: []

# Metrics
duration: 4min
completed: 2026-03-11
---

# Phase 4 Plan 1: Rules Loader & EPV Core Summary

**Cached rules loader with Decimal rounding helpers and async EPV calculation engine using dense_rank position ranking**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-11T10:04:12Z
- **Completed:** 2026-03-11T10:07:53Z
- **Tasks:** 2
- **Files created:** 2

## Accomplishments
- Rules loader service with `@lru_cache` for all JSON constants and YAML formulas
- Decimal-based salary rounding helpers (round_to_10k, round_to_100k, floor_100k, ceil_100k, round_to_nearest_4)
- Season-specific salary minimum getters with year fallback logic
- EPV calculation core: position rank via dense_rank, robust season check (≥8 weeks), performance salary formula, full EPV orchestrator with 75%/82.5% floor

## Task Commits

Each task was committed atomically:

1. **Task 1: Rules loader service and salary rounding helpers** - `a06c205` (feat)
2. **Task 2: EPV calculation core** - `96fe1cc` (feat)

## Files Created/Modified
- `src/app/services/rules.py` — Cached loaders, rounding helpers, season lookups (155 lines)
- `src/app/services/epv.py` — EPVResult dataclass, position rank, robust season, performance salary, EPV orchestrator (277 lines)

## Decisions Made
- Salaries represented in millions throughout (0.01 = $10k), matching Contract model Numeric(5,2)
- Season lookups fall back to latest available year if requested year not in dict
- `_sal_at_rank` clamps to list bounds rather than erroring on out-of-range rank indices

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None

## Next Phase Readiness
- Rules loader and EPV core ready for extensions engine (04-02)
- All contract tools can import from rules.py for constants/formulas
- EPV orchestrator ready to be called by extension/tag/tender calculators

---
*Phase: 04-contract-engine*
*Completed: 2026-03-11*
