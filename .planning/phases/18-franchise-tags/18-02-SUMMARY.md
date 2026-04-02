---
phase: 18-franchise-tags
plan: 02
subsystem: api, database
tags: [franchise-tags, pricing, salary-cap, positional-averages, validation]

requires:
  - phase: 18-franchise-tags
    provides: FT eligibility validation and team_id scoping (plan 01)
provides:
  - validated EFT/NEFT/TT positional salary averages for all 10 positions
  - ADL Cap Percentage adjustment in tag salary calculations
  - validated per-player tag prices and opening bids (606 players)
  - FT price validation script (scripts/validate_ft_prices.py)
affects: [19-extensions, 21-5yo-ppe, 23-cross-tool-validation]

tech-stack:
  added: []
  patterns:
    - "ADL Cap Percentage: (current_cap / previous_cap) multiplier on positional salary averages"

key-files:
  created: [scripts/validate_ft_prices.py, .planning/phases/18-franchise-tags/18-02-VALIDATION.md]
  modified:
    - src/app/services/franchise_tags.py
    - src/app/services/rules.py
    - rules/constants/salary_cap.json

key-decisions:
  - "Tag salaries use ADL Cap Percentage: (current_cap / prev_cap) × AVG(top_N) — discovered from spreadsheet formula"
  - "Feb 15 EFT* values use 2.5% placeholder multiplier; real EFT computed from July 1 salaries — app matches July 1 section"
  - "adl_salary_cap_by_year added to salary_cap.json for cap adjustment lookups"

patterns-established:
  - "Cap-adjusted positional averages: multiply raw average by cap growth ratio before rounding"

issues-created: []

duration: 8min
completed: 2026-04-01
---

# Phase 18-02: FT Price Validation Summary

**ADL Cap Percentage multiplier discovered and applied to tag salary calculations — 0 discrepancies across 30 positional averages and 606 per-player prices**

## Performance

- **Duration:** ~8 min (subagent execution)
- **Started:** 2026-04-01T15:00:00Z
- **Completed:** 2026-04-01T15:08:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Discovered the ADL Cap Percentage adjustment: tag salaries multiply positional averages by (current_cap / prev_cap), confirmed from spreadsheet formula `ROUND(($L$4/$L$6)*AVERAGE(...))`
- All 30 positional averages (10 positions × 3 tag types) match the spreadsheet's July 1 section
- All 606 per-player tag prices validated — MAX(positional_avg, 1.20 × prev_salary) correct for all
- Opening bids correct for all NEFT/TT eligible players

## Task Commits

1. **Task 1: Build FT price validation script** — `8c44723` (feat)
2. **Task 2: Fix FT price discrepancies** — `6ddc806` (fix)

## Files Created/Modified
- `scripts/validate_ft_prices.py` — Two-layer validation: positional averages + per-player prices
- `src/app/services/franchise_tags.py` — Applied cap_pct multiplier in calculate_tag_salary()
- `src/app/services/rules.py` — Added get_adl_salary_cap() helper
- `rules/constants/salary_cap.json` — Added adl_salary_cap_by_year with 2025/2026 values
- `.planning/phases/18-franchise-tags/18-02-VALIDATION.md` — Full validation report

## Decisions Made
- Tag salaries require cap adjustment factor — discovered from spreadsheet formula, not documented in bylaws
- App matches July 1 salary section (not Feb 15 EFT* which uses a 2.5% placeholder)
- Cap values stored in salary_cap.json for easy year-over-year updates

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] ADL Cap Percentage not applied to positional averages**
- **Found during:** Task 2 (price validation)
- **Issue:** All positional averages were ~7.87% too low — app computed raw averages without the cap growth multiplier (244/226.2 ≈ 1.0787)
- **Fix:** Added cap_pct = get_adl_salary_cap(season) / get_adl_salary_cap(season-1) in calculate_tag_salary(), applied before rounding
- **Files modified:** franchise_tags.py, rules.py, salary_cap.json
- **Verification:** Re-run shows 0/30 discrepancies
- **Committed in:** 6ddc806

---

**Total deviations:** 1 auto-fixed (1 missing critical), 0 deferred
**Impact on plan:** Essential fix — without cap adjustment, all tag prices would be wrong by ~8%

## Issues Encountered
None

## Next Phase Readiness
- Phase 18 complete — franchise tag eligibility and prices fully validated
- Cap adjustment pattern established for reuse in 5YO/PPE calculations (Phase 21)
- Ready for Phase 19 (Extensions validation)

---
*Phase: 18-franchise-tags*
*Completed: 2026-04-01*
