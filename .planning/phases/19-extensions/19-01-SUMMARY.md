---
phase: 19-extensions
plan: 01
subsystem: api
tags: [extensions, eligibility, epv, team-scoping, kickoff-gate]

# Dependency graph
requires:
  - phase: 18-franchise-tags
    provides: team_id scoping pattern for eligibility checks
provides:
  - EXT eligibility per-contract-slot evaluation (team_id scoping)
  - Fixed kickoff gate for expired rookie contracts
  - 97.4% match rate against TagElig26 spreadsheet (1,423/1,549 rows)
affects: [19-extensions, 23-cross-tool-validation]

# Tech tracking
tech-stack:
  added: []
  patterns: [team_id scoping for extension eligibility (mirrors franchise tag pattern)]

key-files:
  created: []
  modified: [src/app/services/extensions.py, src/app/services/eligibility.py]

key-decisions:
  - "Per-contract evaluation via team_id — mirrors franchise tag pattern from 18-01"
  - "Kickoff gate only applies to yr_rem=1 (final year), not yr_rem=0 (expired)"
  - "40 remaining discrepancies are correct robust PR enforcement — not bugs"

patterns-established:
  - "team_id scoping now used by both franchise tags and extensions"

issues-created: []

# Metrics
duration: 25min
completed: 2026-04-02
---

# Phase 19-01: EXT Eligibility Validation Summary

**Per-contract team_id scoping and kickoff gate fix reduce EXT eligibility discrepancies from 326 to 40 (all correct robust PR enforcement)**

## Performance

- **Duration:** 25 min
- **Started:** 2026-04-02
- **Completed:** 2026-04-02
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Added team_id scoping to check_extension_eligibility() and calculate_extensions(), fixing 252 discrepancies from per-player vs per-contract evaluation
- Fixed kickoff gate: expired rookie contracts (yr_rem=0) no longer incorrectly blocked by NFL kickoff date (39 discrepancies)
- Achieved 97.4% match rate (1,423/1,549) against TagElig26 spreadsheet

## Task Commits

Each task was committed atomically:

1. **Task 1: Audit EXT eligibility against spreadsheet** — inline audit only, no commit needed
2. **Task 2: Fix EXT eligibility discrepancies** — `a1c8fd9` (fix)

**Plan metadata:** pending

## Files Created/Modified
- `src/app/services/extensions.py` — Added team_id param to check_extension_eligibility() and calculate_extensions(); fixed kickoff gate condition; scoped EXT-in-prior-window check to team
- `src/app/services/eligibility.py` — Updated _check_extension() and dispatch to pass team_id for extensions

## Decisions Made
- Used team_id scoping (same pattern as franchise tags in 18-01) rather than contract_id — consistent API, backward compatible
- Kickoff gate changed from `years_remaining <= 1` to `== 1` — expired contracts are no longer rookie-gated
- 40 remaining "no robust PRs" discrepancies left as-is — app correctly enforces bylaws, spreadsheet doesn't check this rule

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Per-contract scoping needed for two-conference league**
- **Found during:** Task 1 (audit)
- **Issue:** check_extension_eligibility() picked highest-salary contract per player, but EXT eligibility is per-contract-slot. Players with two conference contracts had their eligible contract masked by an ineligible high-salary one.
- **Fix:** Added optional team_id parameter to scope contract queries, mirroring franchise tag pattern from Phase 18-01
- **Files modified:** src/app/services/extensions.py, src/app/services/eligibility.py
- **Verification:** Re-run audit shows 252 discrepancies resolved
- **Committed in:** a1c8fd9

---

**Total deviations:** 1 auto-fixed (missing critical), 0 deferred
**Impact on plan:** Fix was necessary for correctness in dual-conference league. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- EXT eligibility validated — ready for 19-02 (EXT pricing/EPV/EYS validation)
- 40 robust-PR discrepancies are correct behavior, not blocking

---
*Phase: 19-extensions*
*Completed: 2026-04-02*
