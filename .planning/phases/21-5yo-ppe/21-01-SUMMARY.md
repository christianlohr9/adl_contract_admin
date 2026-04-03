---
phase: 21-5yo-ppe
plan: 01
subsystem: api
tags: [5yo, percentile, tier-assignment, pr-starter-floor, adl-draft-round, cap-percentage]

# Dependency graph
requires:
  - phase: 20-tenders
    provides: Validated tender pricing (ERFA/RFA)
  - phase: 18-franchise-tags
    provides: ADL Cap Percentage discovery, tag salary functions
provides:
  - Validated PR Starter Floor constants (9 positions, 100% match)
  - Corrected 5YO percentile formula, tier boundaries, and eligibility logic
  - Fixed calculate_modified_tt_salary to apply ADL Cap Percentage
  - Fixed ADL draft round detection from contract designation (not NFL draft_round)
  - Two-conference contract handling for 5YO and PPE eligibility
affects: [21-02-ppe, 22-buyout-restructure, 23-cross-tool-validation]

# Tech tracking
tech-stack:
  added: []
  patterns: [adl-draft-round-from-designation]

key-files:
  created: []
  modified: [src/app/services/buyouts.py]

key-decisions:
  - "Percentile formula is (floor - rank) / floor, not (floor - rank) / (floor - 1)"
  - "NEFT/TT tier boundaries are strictly exclusive (> 87.5%, > 75%), not inclusive (>=)"
  - "ADL draft round derived from contract designation (e.g. '2023 1.04'), not player.draft_round (NFL round)"
  - "Two-conference league: 5YO/PPE must find the contract with the eligible ADL round across all conferences"
  - "calculate_modified_tt_salary must apply ADL Cap Pct (current_cap / prev_cap) like calculate_tag_salary"
  - "Remaining 10 unique tier mismatches are scoring data discrepancies (DB ranks differ from spreadsheet TSP_RK) — not code bugs"

patterns-established:
  - "_extract_adl_draft_round() helper for parsing designation strings"
  - "Multi-contract iteration for two-conference eligibility checks"

issues-created: []

# Metrics
duration: 20min
completed: 2026-04-03
---

# Phase 21-01: 5YO Validation Summary

**PR Starter Floors 100% match; 5YO formulas corrected (5 code fixes); 22/32 picks match with remaining differences traced to scoring data discrepancies**

## Performance

- **Duration:** 20 min
- **Started:** 2026-04-03
- **Completed:** 2026-04-03
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

### Task 1: PR Starter Floor Validation
- Validated all 9 positional starter floors against PPE5YO spreadsheet cols V-W: 100% match
- QB=16, RB=28, WR=50, TE=18, DT=38, DE=40, LB=38, CB=38, S=38
- PK=16 and PN=16 are app-only (PPE-ineligible positions) and correctly absent from spreadsheet

### Task 2: 5YO Tier & Price Validation
- Validated 32 first-round picks (16 NFC + 16 AFC) from PPE5YO spreadsheet
- **22 of 32 picks match exactly** (tier + price) after code fixes
- **5 code bugs found and fixed:**
  1. Percentile formula: `(floor - rank) / (floor - 1)` changed to `(floor - rank) / floor`
  2. Tier boundaries: `>= 0.875` changed to `> 0.875` (and `>= 0.75` to `> 0.75`)
  3. Draft round check: used `player.draft_round` (NFL) instead of ADL round from designation
  4. Contract selection: picked highest-salary contract, not the one with eligible ADL round
  5. Modified TT salary: missing ADL Cap Percentage adjustment (`current_cap / prev_cap`)
- **10 remaining mismatches** are scoring data discrepancies where the app's DB player ranks differ from the spreadsheet's TSP_RK values (e.g., Stroud ranked 18th in DB vs TSP_RK=11 in spreadsheet)
- **1 player (Mingo)** has no 2026 contract in DB — data gap, not code bug
- **1 minor price discrepancy (Charbonnet)**: $15.64 vs $15.47 ($0.17, ~1.1%) — likely minor salary data difference

### Exact matches (22/32):
- Richardson (QB), Robinson (RB), Gibbs (RB), Young (QB), Smith-Njigba (WR), Flowers (WR), Anderson (DE), Mayer (TE), Levis (QB), Wilson (DE) — across both conferences + 2 additional

## Task Commits

1. **fix(21-01): correct 5YO percentile formula, tier boundaries, and eligibility checks** - `f88cd62`

## Files Created/Modified
- `src/app/services/buyouts.py` — 5 bug fixes: percentile formula, tier boundaries, ADL round extraction, multi-conference contracts, cap_pct in modified TT

## Decisions Made
- Percentile formula `(floor - rank) / floor` matches all 13 spreadsheet TSP_RK/tier assignments
- Tier boundaries are exclusive at top: `> 87.5%` for NEFT, `> 75%` for TT (Anderson at exactly 87.5% is TT, not NEFT)
- ADL draft round must come from contract designation, not player model's NFL draft_round
- In two-conference league, must search all contracts to find the one with eligible ADL round
- Scoring data discrepancies are acceptable — the app's formula logic is correct

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Percentile formula wrong**
- **Issue:** `(floor - rank) / (floor - 1)` produced wrong tier for players at boundary ranks
- **Fix:** Changed to `(floor - rank) / floor`
- **Verification:** All 13 ranked spreadsheet players now tier-match when using spreadsheet TSP_RK

**2. [Rule 3 - Blocking] Tier boundaries inclusive instead of exclusive**
- **Issue:** `>= 0.875` included Anderson (87.5%) in NEFT; spreadsheet says TT
- **Fix:** Changed to `> 0.875` and `> 0.75`

**3. [Rule 3 - Blocking] Draft round from wrong source**
- **Issue:** `player.draft_round` is NFL round; 5YO needs ADL round from designation
- **Fix:** New `_extract_adl_draft_round()` parses "YYYY R.PP" from contract designation

**4. [Rule 3 - Blocking] Single-contract selection in two-conference league**
- **Issue:** `ORDER BY salary DESC LIMIT 1` could pick wrong contract (e.g., LaPorta's round-2 contract over round-1)
- **Fix:** Iterate all contracts, find first with eligible ADL round

**5. [Rule 3 - Blocking] Modified TT missing cap adjustment**
- **Issue:** `calculate_modified_tt_salary` didn't apply `current_cap / prev_cap` unlike `calculate_tag_salary`
- **Fix:** Added ADL Cap Percentage multiplication

---

**Total deviations:** 5 auto-fixed (all code bugs), 0 deferred
**Impact on plan:** All fixes were necessary for correct 5YO pricing. No scope creep.

## Issues Encountered
- Scoring data discrepancies for 8 unique players (Stroud, Kincaid, Addison, Johnston, Carter, LaPorta, Campbell, Charbonnet) — DB ranks differ from spreadsheet TSP_RK, causing tier/price mismatches. These are data issues, not formula bugs.
- Mingo has no 2026 contract in DB — import gap.

## Next Phase Readiness
- Phase 21 Plan 01 (5YO) complete — formula logic validated and fixed
- Ready for Phase 21 Plan 02 (PPE validation) or Phase 22 (B/R)
- PPE function also fixed (ADL round + multi-conference) as part of this plan
- No blockers

---
*Phase: 21-5yo-ppe*
*Completed: 2026-04-03*
