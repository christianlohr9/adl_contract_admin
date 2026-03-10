---
phase: 01-rules-extraction
plan: 02
subsystem: rules
tags: [yaml, formulas, bylaws, epv, verification]

# Dependency graph
requires:
  - phase: 01-01
    provides: verified JSON constants referenced by YAML formulas
provides:
  - verified extensions.yaml with EPV formulas and eligibility rules
  - verified contract_tools.yaml with tag/tender/buyout formulas
  - verified salary_cap.yaml with cap penalty formulas
  - verified free_agency.yaml with auction and tender formulas
affects: [contract-engine, salary-cap, validation]

# Tech tracking
tech-stack:
  added: []
  patterns: [epv-code-discrepancy-documentation, cross-file-consistency-checks]

key-files:
  created: []
  modified:
    - rules/formulas/extensions.yaml
    - rules/formulas/contract_tools.yaml
    - rules/formulas/salary_cap.yaml
    - rules/formulas/free_agency.yaml

key-decisions:
  - "Bylaws wins over old EPV code when they conflict (EPV code has 100% floor bug vs 75% in bylaws)"
  - "RRFA rounding made explicit in formula (FLOOR_100K) for consistency with other RFA tags"

patterns-established:
  - "Formula verification: every YAML formula must trace to specific bylaws passage"
  - "Cross-file consistency: formulas appearing in multiple YAML files must be identical"

issues-created: []

# Metrics
duration: 7min
completed: 2026-03-10
---

# Phase 1 Plan 02: Verify Rule Formulas Summary

**All 4 YAML formula files verified against bylaws with old EPV code cross-referenced — 5 corrections applied, 1 EPV code bug documented**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-10T11:25:23Z
- **Completed:** 2026-03-10T11:33:22Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Verified all extension formulas (PR, Performance Salary, EPV variants, EYS, smoothing, 5YO, PPE) against bylaws and old EPV code
- Verified all contract tool formulas (franchise tags, RFA/ERFA tenders, buyouts/restructures) against bylaws
- Verified salary cap formulas (cap calculation, rollover, NG/SD/FG penalties, acceleration) against bylaws
- Verified free agency formulas (UFA auction, RFA tenders, CFA rules) against bylaws
- Confirmed cross-file consistency — no contradictions between YAML files
- Confirmed cross-format consistency — YAML formulas reference valid JSON constants

## Task Commits

Each task was committed atomically:

1. **Task 1: Verify extensions.yaml and contract_tools.yaml** - `a447ee0` (verify)
2. **Task 2: Verify salary_cap.yaml and free_agency.yaml** - `64130d8` (verify)

## Files Created/Modified
- `rules/formulas/extensions.yaml` - Added 5YO interaction note for EXTyears multiplier; documented old EPV code bug (100% floor vs bylaws 75%)
- `rules/formulas/contract_tools.yaml` - Added explicit contract_salary_formula for NEFT and TT (5% per-year discount); added RFA contract_salary clarification (no multi-year discount)
- `rules/formulas/salary_cap.yaml` - Added sd_minimum_salary derivation formula (NFL_Salary_Cap / 150); clarified B/R penalty formula for buyout vs restructure cases
- `rules/formulas/free_agency.yaml` - Fixed rfa_rrfa_tender formula to explicitly show FLOOR_100K rounding

## Decisions Made
- Bylaws wins over old EPV code — documented `epv_code_discrepancy` in extensions.yaml where old code uses MAX(PrevSal, EPVs) (100% floor) instead of MAX(0.75 * PrevSal, EPVs) per bylaws
- Made RRFA rounding explicit in formula for consistency with other RFA tag formulas

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Old EPV code bug documented**
- **Found during:** Task 1 (extensions.yaml verification)
- **Issue:** `epv_calculations.py` line 179 uses MAX(PrevSal, EPVs) (100% floor) instead of MAX(0.75 * PrevSal, EPVs) per bylaws
- **Fix:** Documented as `epv_code_discrepancy` in extensions.yaml — bylaws wins
- **Committed in:** a447ee0

**2. [Rule 2 - Missing Critical] Added sd_minimum_salary derivation formula**
- **Found during:** Task 2 (salary_cap.yaml verification)
- **Issue:** SD penalty minimum salary derivation formula `NFL_Salary_Cap / 150` was missing
- **Fix:** Added to salary_cap.yaml
- **Committed in:** 64130d8

**3. [Rule 2 - Missing Critical] Added explicit contract salary formulas**
- **Found during:** Task 1 (contract_tools.yaml verification)
- **Issue:** NEFT and TT lacked explicit contract_salary_formula showing 5% per-year discount; RFA lacked clarification that salary = high bid with no discount
- **Fix:** Added explicit formula fields to contract_tools.yaml
- **Committed in:** a447ee0

**4. [Rule 1 - Bug] B/R penalty formula clarified**
- **Found during:** Task 2 (salary_cap.yaml verification)
- **Issue:** Original formula only covered restructure case; buyout has additional penalty for deleted years
- **Fix:** Expanded formula in salary_cap.yaml
- **Committed in:** 64130d8

**5. [Rule 1 - Bug] RRFA rounding made explicit**
- **Found during:** Task 2 (free_agency.yaml verification)
- **Issue:** Rounding was in variable description but not in formula itself
- **Fix:** Changed formula to FLOOR_100K(NFL_RRFA_price) for consistency
- **Committed in:** 64130d8

---

**Total deviations:** 5 auto-fixed (3 bugs, 2 missing critical), 0 deferred
**Impact on plan:** All fixes necessary for formula accuracy. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- All 4 YAML formula files verified and accurate
- Ready for 01-03-PLAN.md (extract rule formulas into YAML files)
- Phase 1 nearing completion — 2 of 3 plans done

---
*Phase: 01-rules-extraction*
*Completed: 2026-03-10*
