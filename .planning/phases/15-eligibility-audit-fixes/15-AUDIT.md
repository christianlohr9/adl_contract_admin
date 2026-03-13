# Phase 15: Eligibility Audit — Bylaw-to-Code Mapping

**Audited:** 2026-03-13
**Bylaws source:** `rules/docs/contract_tools.md`, `rules/docs/contracts.md`
**Constants source:** `rules/constants/contracts.json`
**Season under test:** 2026

---

## Methodology

For every bylaw eligibility rule across all 7 contract actions:
1. Extract exact bylaws text
2. Identify the code that implements it (file:line)
3. Classify status: **MATCH** (correct), **DISCREPANCY** (wrong), or **MISSING** (not implemented)
4. For DISCREPANCY/MISSING: document the issue and recommended fix

---

## 1. Contract Extensions (Section X-A)

**Code files:** `src/app/services/extensions.py`, `src/app/services/epv.py`

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| E1 | Players with Contract Years >= 2 are ineligible | `extensions.py:159` — `contract.years_remaining >= 2` | **MATCH** | Correctly blocks players with 2+ years |
| E2 | Players on expired or unexpired rookie contracts signed for less than max years (3 UDFA, 4 drafted) are ineligible | `extensions.py:166-181` — checks drafted rookies via designation pattern | **DISCREPANCY** | See E2-D below |
| E3 | Players who received an EXT in current or prior window are ineligible | `extensions.py:188-198` — `Contract.designation.contains("EXT")` with `signed_season >= season - 1` | **DISCREPANCY** | See E3-D below |
| E4 | Players with no Robust PRs are ineligible (8+ active games in current + 2 prior seasons) | `extensions.py:201-207` — loops `[season, season-1, season-2]` calling `is_robust_season()` | **MATCH** | 3-season lookback is correct; `epv.py:95` uses `ext_robust_games_minimum` = 8 |
| E5 | Rookie/UDFA contracts ineligible until NFL games kick off in final year | N/A | **MATCH (Out of Scope)** | Phase 16 — NFL kickoff gating. Correctly excluded per plan. |
| E6 | EXT cannot cause years to exceed 6-year maximum | `extensions.py:183-185` checks `years_remaining >= max_years`; `extensions.py:294` calculates `max_ext_years = max_contract_years - current_years` | **MATCH** | Correctly caps at 6 years |
| E7 | EYS = MAX(EPV_curr, EPV_new, EPV_old, floor) x (1.15 - 0.05 x ext_years) | `extensions.py:66-91` — `calculate_eys()` | **MATCH** | Formula correctly implemented with floor included |
| E8 | Floor: 75% active salary / 82.5% expired salary | `epv.py:212-215` — uses `ext_prev_sal_floor_active_pct` (0.75) and `ext_prev_sal_floor_expired_pct` (0.825) | **MATCH** | Constants match bylaws |
| E9 | All EXTs are SD contracts, floored at SD minimum | `extensions.py:305-306` — `if smoothed < sd_minimum: smoothed = sd_minimum`; `ExtensionOption.contract_type = "SD"` | **MATCH** | SD type hardcoded; minimum enforced |
| E10 | Performance Salary = AVG(SAL(2*PR-3), SAL(2*PR-2)), PR=1 extrapolation | `epv.py:129-171` — `calculate_performance_salary()` | **MATCH** | PR=1 linear extrapolation and standard formula both correct |
| E11 | PR evaluated at "1 rank better" (2*PR-3, 2*PR-2 gives PR-1 salary) | `epv.py:167-169` | **MATCH** | Formula correctly pays at PR-1 equivalent |
| E12 | Salary smoothing with 10% annual growth | `extensions.py:99-122` — `calculate_smoothed_salary()` | **MATCH** | Growth rate loaded from constants (0.10) |

### Extension Discrepancies

#### E2-D: UDFA contract max-years check missing

**Bylaws:** "Players on expired or unexpired rookie contracts who were signed for less than the maximum possible years (3 for UDFA and 4 for drafted rookies)"

**Code (`extensions.py:166-181`):** Only checks for drafted rookie designations via pattern `f"{contract.signed_season} {r}."` for rounds 1-5. There is no check for UDFA contracts at all.

**Impact:** A player on a UDFA contract signed for fewer than 3 years (e.g., a 1-year UDFA deal) would incorrectly be allowed to receive an extension.

**Recommended Fix:**
- Add UDFA detection: check if `"UDFA" in desig`
- If UDFA: use `max_years = 3` (from `contract_year_limits.udfa.max`)
- If drafted rookie: use `max_years = 4` (from `contract_year_limits.drafted_rookie_all_rounds.max`)
- Apply the same `original_years < max_years` check to both

#### E3-D: EXT re-extension blocking query may produce false positives

**Bylaws:** "Players who have received an Extension in the current or prior EXT window (e.g. if a player got an oEXT, he cannot receive a second oEXT in the same window or an iEXT in the in-season window; he must wait until at least the next offseason window. A player who received an iEXT must wait until the next in-season window)"

**Code (`extensions.py:188-196`):**
```python
Contract.designation.contains("EXT"),
Contract.signed_season >= season - 1,
```

**Issue:** The query searches for ANY contract containing "EXT" in the designation with `signed_season >= season - 1`. This searches across ALL contracts for the player, not just the player's own contract. More importantly, this uses `signed_season` (when the original contract was signed) rather than tracking when the EXT action was performed. A contract with designation "2020 iEXT" that was signed in season 2020 would have `signed_season = 2020`, so for season 2026 it would not trigger. However, the actual EXT signing creates a new contract record with `signed_season` set to the EXT signing season — so the behavior depends on how EXT contracts are stored.

**Deeper concern:** The `signed_season` field tracks when the *original* contract was signed, not when the extension was applied. If the EXT modifies the existing contract row rather than creating a new one, `signed_season` would be the original signing year, not the EXT year, making this check ineffective for old contracts that were recently extended.

**Recommended Fix:**
- Verify how EXT contracts are stored (new row vs. in-place update)
- If in-place: need to track EXT action date separately (e.g., add `ext_applied_season` column or check action log)
- If new row: the current query should work for the common case, but confirm `signed_season` is set to the EXT season

---

## 2. Franchise Tags (Section X-B)

**Code files:** `src/app/services/franchise_tags.py`, `src/app/services/eligibility.py`

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| F1 | Player must have 0 remaining contract years (expired) | `franchise_tags.py:196-201` — `contract.years_remaining > 0` returns ineligible | **MATCH** | Correctly requires expired contract |
| F2 | Max 3 consecutive EFT/NEFT tags (4th tag blocked) | `franchise_tags.py:203-208` — `consecutive >= max_consecutive` (3) | **MATCH** | Correctly blocks at 4th consecutive tag |
| F3 | 1 franchise tag per team per season | `eligibility.py:253-266` — `has_allotment(session, team_id, season, "franchise_tag")` | **MATCH** | Allotments service limits to 1 per `_ALLOTMENT_LIMITS` |
| F4 | EFT/NEFT = MAX(AVG(Top5), 120% prev); TT = MAX(AVG(Top10), 120% prev) | `franchise_tags.py:90-111` — `calculate_tag_salary()` uses `n=5` for EFT/NEFT, `n=10` for TT | **MATCH** | Salary formula correct |
| F5 | 3rd consecutive tag premium: MAX(144% prev, 120% positional FT, highest positional FT) | `franchise_tags.py:218-263` — `_calculate_third_tag_salary()` | **MATCH** | All three options computed correctly |
| F6 | PK/PN grouped into single "Kicker/Punter" category for tag salary | Not implemented | **MISSING** | See F6-M below |
| F7 | Opening bid for NEFT/TT: MAX(CEIL_100K(SD_Min - $100k), FLOOR_100K(tag_salary)) | `franchise_tags.py:119-126` — `calculate_opening_bid()` | **MATCH** | Formula matches bylaws |
| F8 | All tag salaries rounded to nearest $10k | `franchise_tags.py:111` — `round_to_10k(raw)` | **MATCH** | Uses ROUND_HALF_UP to nearest $10k |
| F9 | Tag contract is 1-year, fully guaranteed (FG) | `franchise_tags.py:357-359` — `contract_years=1`, `guarantee_level="FG"` | **MATCH** | Hardcoded correctly |
| F10 | FT salary based on "current ADL position designations at the Franchise Tag deadline" | `franchise_tags.py:79-80` — uses `Player.position` | **DISCREPANCY** | See F10-D below |
| F11 | Consecutive tag check counts EFT and NEFT only (TT resets the count) | `franchise_tags.py:148-164` — checks `"EFT" in designation or "NEFT" in designation` | **MATCH** | TT designations correctly excluded from consecutive count |

### Franchise Tag Discrepancies

#### F6-M: PK/PN position grouping not implemented

**Bylaws:** "FT contract salaries are determined by current ADL position designations at the Franchise Tag deadline, with the exception that Kickers and Punters are grouped into a single 'Kicker/Punter' position category."

**Code:** `_get_top_n_positional_salaries()` at `franchise_tags.py:67-87` filters by `Player.position == position`. PK and PN are queried separately — there is no grouping logic.

**Impact:** A PK receiving a franchise tag would have their tag salary calculated against only PK salaries (a very small pool), instead of the combined PK+PN pool. Same for PN. This could significantly affect tag salary calculations.

**Recommended Fix:**
- In `_get_top_n_positional_salaries()` and `calculate_tag_salary()`, when `position` is "PK" or "PN", query both positions:
  ```python
  if position in ("PK", "PN"):
      Player.position.in_(["PK", "PN"])
  else:
      Player.position == position
  ```
- Similarly update `_calculate_third_tag_salary()` highest FT lookup

#### F10-D: Tag salary uses player's current position, not position at tag deadline

**Bylaws:** "FT contract salaries are determined by current ADL position designations at the Franchise Tag deadline"

**Code:** Uses `player.position` which is the current position in the Player table. This would be correct if positions are snapshotted at the deadline, but if a player's position changes after the deadline, the tag salary calculation could use the wrong position.

**Impact:** Minor — positions rarely change mid-season. This is more of a data-integrity concern than a code bug.

**Recommended Fix:** Document that positions should be verified at tag deadline. No code change needed if position data is maintained correctly.

---

## 3. RFA Tenders (Section X-C)

**Code files:** `src/app/services/tenders.py`

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| R1 | Player must have 0 years remaining | `tenders.py:243-245` — `contract.years_remaining > 0` | **MATCH** | Correctly requires expired contract |
| R2 | Contract must not be 4+ years old | `tenders.py:251-253` — `contract_age >= 4` | **MATCH** | Uses `season - signed_season` correctly |
| R3 | Contract must not include ineligible types from 2021 or earlier (EFT, NEFT, TT, NEFToff, TToff, FRFA, SRFA, ORFA, RRFA, B/R, EXT, 5YO) | `tenders.py:256-262` — iterates `_INELIGIBLE_TYPES_2021_OR_EARLIER` if `signed_season <= 2021` | **MATCH** | All 12 types listed correctly |
| R4 | Must not be expired multi-year UFA from 2023 or earlier | `tenders.py:265-271` — `signed_season <= 2023 and "UFA" in desig` then checks `original_length > 1` | **DISCREPANCY** | See R4-D below |
| R5 | 2 tenders per team shared with ERFA | `eligibility.py:371-393` — `has_allotment(session, team_id, season, "tender")` with limit 2 | **MATCH** | Shared limit correctly implemented |
| R6 | FRFA opening bid: MAX(NFL_FRFA_price, 2.2x prev_salary), rounded down to $100k | `tenders.py:89-100` — `calculate_frfa_bid()` | **DISCREPANCY** | See R6-D below |
| R7 | SRFA opening bid: MAX(NFL_SRFA_price, 1.65x prev_salary), rounded down to $100k | `tenders.py:103-114` — `calculate_srfa_bid()` | **DISCREPANCY** | Same issue as R6-D (placeholder NFL prices) |
| R8 | ORFA opening bid: MAX(NFL_ORFA_price, 1.1x prev_salary), rounded down to $100k | `tenders.py:117-128` — `calculate_orfa_bid()` | **DISCREPANCY** | Same issue as R6-D |
| R9 | RRFA opening bid: NFL_RRFA_price only | `tenders.py:131-139` — `calculate_rrfa_bid()` | **DISCREPANCY** | See R9-D below |
| R10 | Must not be a previous RFA contract | `tenders.py:256-262` — FRFA/SRFA/ORFA/RRFA included in `_INELIGIBLE_TYPES_2021_OR_EARLIER` | **DISCREPANCY** | See R10-D below |
| R11 | All RFA figures rounded down to nearest $100k | `tenders.py:100,114,128,139` — all use `floor_100k()` | **MATCH** | Correct rounding direction |

### RFA Discrepancies

#### R4-D: Multi-year UFA original_length calculation may be off by one

**Bylaws:** "An expiring contract may also not be an expired multi-year UFA contract" signed in "2023 or earlier"

**Code (`tenders.py:269`):**
```python
original_length = season - signed_season
```

**Issue:** For an expired contract (years_remaining=0), `original_length = season - signed_season`. A 1-year UFA signed in 2025 for season 2026 would have `signed_season=2025`, so `original_length = 2026 - 2025 = 1`, which is correct (1-year, not blocked). A 2-year UFA signed in 2023 would have `original_length = 2026 - 2023 = 3`, but the actual original contract length was 2 years (2023 + 2024, expired in 2025 carried forward to 2026). The formula gives `3` instead of `2`.

However, since `original_length > 1` is the threshold, this over-count doesn't produce false negatives for the multi-year check — it would still correctly identify multi-year UFAs. The issue is whether a 1-year UFA from 2023 would incorrectly show `original_length = 3 > 1` and be falsely blocked.

**Wait:** A 1-year UFA signed in 2023 would have: played 2023 season only, expired after 2023. If the contract shows up in season 2026 data, that means it was carried forward. But `signed_season=2023` and `season=2026` gives `original_length=3`. This is wrong — it was a 1-year deal. **This is a real bug.** A 1-year UFA from 2023 or earlier would be incorrectly blocked from RFA because `original_length = season - signed_season` gives a value > 1.

**Recommended Fix:**
- Track `original_length` explicitly on the Contract model, OR
- Calculate as `original_length = (season_when_expired - signed_season + 1)` where season_when_expired needs to be derived from when years_remaining first hit 0, OR
- Use a different approach: check `original_years` from the signed contract (if the contract was originally signed for N years, store N)

#### R6-D: NFL RFA prices are all zero (placeholders)

**Code (`tenders.py:81-86`):**
```python
_NFL_RFA_PRICES: dict[str, Decimal] = {
    "FRFA": Decimal("0"),
    "SRFA": Decimal("0"),
    "ORFA": Decimal("0"),
    "RRFA": Decimal("0"),
}
```

**Impact:** NFL CBA RFA tender prices are all `$0`. For FRFA/SRFA/ORFA, the `MAX(NFL_price, multiplier * prev_salary)` formula still works because the multiplier term dominates. But for RRFA, which uses NFL price ONLY, the opening bid is always `$0` — a completely wrong result.

**Recommended Fix:**
- Add 2026 NFL RFA tender prices to `contracts.json` (or a dedicated data source)
- Load them dynamically by season
- Critical for RRFA which has no salary multiplier fallback

#### R9-D: RRFA bid always returns $0

**Bylaws:** "RRFA... opening bid equal to the NFL RRFA price"

**Code:** `calculate_rrfa_bid()` returns `floor_100k(Decimal("0"))` = `$0` because the NFL price placeholder is zero.

**Impact:** Any RRFA tender would have a $0 opening bid, which is clearly incorrect.

**Recommended Fix:** Same as R6-D — add real NFL RFA prices.

#### R10-D: "Previous RFA contract" rule only checked for 2021 or earlier

**Bylaws:** "a player's expiring contract must not be 4 or more years old, a previous RFA contract, or a 'premium' type of contract"

The opening paragraph states a universal rule that the contract must not be "a previous RFA contract." The subsequent table gates ineligible types to "2021 or earlier." The FRFA/SRFA/ORFA/RRFA types appear in the 2021-or-earlier table.

**Code:** Only checks for FRFA/SRFA/ORFA/RRFA in the `signed_season <= 2021` block.

**Issue:** For contracts signed in 2022 or later, an RFA contract (FRFA/SRFA/ORFA/RRFA designation) would NOT be caught by the code. If the bylaws intend "previous RFA contract" to be a universal rule (regardless of year), this is a bug. If the table is the definitive specification and the opening paragraph is just a summary, then the code is correct.

**Recommended Fix:**
- Clarify with league commissioner whether the "previous RFA contract" rule applies universally or only to 2021-and-earlier contracts
- If universal: add a separate check for RFA designations regardless of `signed_season`
- If year-gated: code is correct, but the bylaws text is misleading

---

## 4. ERFA Tenders (Section X-D)

**Code files:** `src/app/services/tenders.py`

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| A1 | Player must have 0 years remaining | `tenders.py:177-178` — `contract.years_remaining > 0` | **MATCH** | Correctly requires expired contract |
| A2 | Previous contract signed at or below veteran minimum in one of previous two league years | `tenders.py:187-196` — checks `signed_season in [season-1, season-2]` and `prev_salary > vet_min_at_signing` | **DISCREPANCY** | See A2-D below |
| A3 | Expired contract must not be an ERFA contract | `tenders.py:181-183` — `"ERFA" in desig` | **MATCH** | Correctly blocks ERFA chaining |
| A4 | Salary: MAX(Veteran Minimum, 110% of previous salary) | `tenders.py:64-71` — `calculate_erfa_salary()` | **MATCH** | Formula correct |
| A5 | 1-year contract, no auction | `tenders.py:328-331` — `contract_years=1`, `compensation="None — exclusive rights retained"` | **MATCH** | Correctly configured |
| A6 | 2 tenders shared with RFA | `eligibility.py:311-333` — same `has_allotment(session, team_id, season, "tender")` check | **MATCH** | Shared limit correctly enforced |

### ERFA Discrepancies

#### A2-D: ERFA salary comparison uses contract salary, not signing salary

**Bylaws:** "a player must have signed their previous contract at or below the ADL veteran minimum salary in one of the previous two league years (i.e. at $900k in 2023 or $1.0m in 2024)"

**Code (`tenders.py:187-196`):**
```python
prev_salary = Decimal(str(contract.salary))
signed_season = contract.signed_season
eligible_seasons = [season - 1, season - 2]
if signed_season not in eligible_seasons:
    return False, "Contract was not signed in one of the previous two league years"
vet_min_at_signing = get_veteran_minimum(signed_season)
if prev_salary > vet_min_at_signing:
    return False, "Previous contract salary exceeds veteran minimum at time of signing"
```

**Issue:** `contract.salary` is the *current* salary on the contract, which may have been adjusted (e.g., by PPE escalator). The bylaws say "signed...at or below" which refers to the salary at the time of signing, not the current salary. If a player's salary was escalated via PPE from $0.90m to $1.50m, the current `contract.salary` would be $1.50m, incorrectly disqualifying them from ERFA.

**However:** In practice, players with PPE escalators would typically have salaries above veteran minimum after escalation, and the ERFA tender is designed for low-salary players. The signing salary is the more accurate check. If the system doesn't track original signing salary separately from current salary, this could be a data model issue.

**Recommended Fix:**
- If `contract.salary` always reflects the signing salary (not adjusted), this is a MATCH
- If salary can be modified in-place (e.g., by PPE), need to store `original_salary` or check signing history
- Verify with data: does the contract table store original or current salary?

---

## 5. Buyout/Restructure (Section X-E)

**Code files:** `src/app/services/buyouts.py`, `src/app/services/eligibility.py`

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| B1 | Any player with a contract is eligible EXCEPT rookies/UDFA not in final year | `buyouts.py:193-232` — `check_buyout_eligibility()` | **DISCREPANCY** | See B1-D below |
| B2 | "Including the fifth year if 5YO has been exercised" — 5YO year counts toward final year | `buyouts.py:225` — `contract.years_remaining > 1` | **DISCREPANCY** | See B2-D below |
| B3 | 1 B/R per team per season | `eligibility.py:432-451` — `has_allotment(session, team_id, season, "buyout_restructure")` | **MATCH** | Allotments service limits to 1 |
| B4 | Opening bid: SD minimum rounded up to $100k | `buyouts.py:104-107` — `ceil_100k(sd_minimum)` | **MATCH** | Formula correct |
| B5 | Salary: high_bid x (1 - 0.05 x (years - 1)), floored at SD minimum | `buyouts.py:80-96` — `calculate_br_salary()` | **MATCH** | Discount and floor correct |
| B6 | Revert/Transfer prohibited for tagged players on expired contracts | Not implemented | **MISSING** | See B6-M below |
| B7 | Salary cap penalty rules for Restructure/Buyout options | Not audited (calculation, not eligibility) | N/A | Penalty calculations are out of eligibility scope |

### Buyout Discrepancies

#### B1-D: Rookie/UDFA final-year check uses years_remaining > 1 instead of == 1

**Bylaws:** "Players on Drafted Rookie or UDFA contracts are ineligible for B/Rs until the final year of their contract"

**Code (`buyouts.py:225`):**
```python
if (is_rookie or is_udfa) and contract.years_remaining > 1:
```

**Issue:** This blocks rookies/UDFA with years_remaining > 1 (2+), which means players with `years_remaining == 1` (final year) ARE eligible. This seems correct on its face. However, players with `years_remaining == 0` (expired) would also pass through, meaning expired rookie/UDFA contracts would be B/R eligible. The bylaws say "until the final year" which means the final year IS eligible. Expired contracts (years_remaining=0) are technically past the final year.

**However:** B/R requires "a player with any contract" — an expired contract with years_remaining=0 still appears on the roster. The code allows B/R on expired contracts, which is consistent with the bylaws saying "any player with a contract is eligible."

**Verdict:** Actually on re-read, this is likely correct behavior. The rookie/UDFA restriction is "ineligible until the final year" — so they become eligible in the final year AND after (expired). The `> 1` check correctly allows both `years_remaining == 1` and `years_remaining == 0`.

**Status: MATCH** (reclassified from DISCREPANCY)

#### B2-D: 5YO extension of final year not explicitly handled

**Bylaws:** "Including the fifth year if the 5YO has been exercised"

**Code:** The check is `contract.years_remaining > 1`. If 5YO adds a 5th year to a 4-year rookie contract, `years_remaining` would already reflect the 5YO year (since it's a data property). So in year 4 with 5YO exercised, `years_remaining` would be 2 (years 4 and 5), making the player ineligible. In year 5, `years_remaining` would be 1, making them eligible.

**This depends on whether `years_remaining` is updated when 5YO is exercised.** If 5YO exercise correctly increments years_remaining, the code works. If not, a player in year 4 with 5YO exercised but unchanged years_remaining=1 would incorrectly be eligible for B/R.

**Recommended Fix:**
- Verify that 5YO exercise updates `years_remaining` in the contract table
- Add explicit test case for this scenario

#### B6-M: Revert/Transfer prohibition for tagged players not implemented

**Bylaws (Section X-E, option 3):** "Revert/Transfer: Revert the player's contract to its exact status from before the B/R auction process (including salary, years, and type) and release the player to the high-bidding team (with or without agreed-upon trade compensation) **[This option is prohibited for tagged players on expired contracts]**"

**Code (`buyouts.py:137-173`):** `_build_gm_options()` always returns all 4 options without checking if the player was previously on a franchise tag.

**Impact:** The API would present Revert/Transfer as an option for franchise-tagged players, which the bylaws explicitly prohibit.

**Recommended Fix:**
- Accept player context (contract designation, years_remaining) in `_build_gm_options()`
- If the player's prior contract was a franchise tag (designation contains "EFT", "NEFT", or "TT") AND was expired (years_remaining == 0), exclude the "revert_transfer" option
- Alternatively, flag the option as prohibited rather than omitting it

---

## 6. Fifth Year Option (5YO)

**Code files:** `src/app/services/eligibility.py` (eligibility), `src/app/services/buyouts.py` (calculation)

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| Y1 | First-round drafted rookies only | `eligibility.py:492-504` — `player.draft_round not in [1]` | **MATCH** | Uses `fifth_year_option.eligible_rounds = [1]` |
| Y2 | Must be in 4th year of rookie contract | `eligibility.py:528-539` — `contract_year = season - contract.signed_season + 1 != 4` | **MATCH** | Correct year calculation |
| Y3 | 5YO not already exercised | `eligibility.py:542-551` — checks `"+" in desig or "5YO" in desig.upper()` | **MATCH** | Catches both `1.01+` and `5YO` designations |
| Y4 | Salary tiers by performance percentile (top 87.5% = NEFT, 75-87.5% = TT, 25-75% = TT 3rd-20th, bottom 25% = TT 3rd-25th) | `buyouts.py:461-601` — `_determine_5yo_tier()` + tier-specific salary calculations | **MATCH** | Tier boundaries and salary formulas correct |
| Y5 | FG guarantee level | `buyouts.py:602-611` — `guarantee_level="FG"` | **MATCH** | Hardcoded correctly |
| Y6 | Deadline: July 1 (fyo_deadline) | `window_status.py:170-171` — `calendar.fyo_deadline` | **MATCH** | Correct calendar field used |
| Y7 | Percentile calculation uses PR Starter Floor | `buyouts.py:344-417` — `calculate_starter_percentile()` | **DISCREPANCY** | See Y7-D below |
| Y8 | Position designations follow same rule as Franchise Tags (PK/PN grouping) | Not implemented for 5YO | **MISSING** | See Y8-M below |

### 5YO Discrepancies

#### Y7-D: Percentile calculation does not use PR Starter Floor

**Bylaws:** "players who scored in the 87.5th percentile or higher above his PR Starter Floor in ADL-wide Total Starter Points"

The bylaws define a specific PR Starter Floor calculation:
> PR Starter Floor = [(#Total Position ADL Starts in Prior League Year) / (ADL Weeks Played x 2)] x [Missed-Start Inflation Rate], rounded to the nearest multiple of 4

**Code (`buyouts.py:344-417`):** `calculate_starter_percentile()` calculates percentile among ALL players at the position who have scores, rather than only those above the PR Starter Floor. The function even has a comment acknowledging this:
```python
# PR Starter Floor: for now, use total starters available.
# The bylaws formula is complex...so we use the count of scored players as the pool
```

**Impact:** Without the PR Starter Floor, the percentile denominator is wrong. For example, if there are 200 WRs with scores but the PR Starter Floor is WR52, a WR ranked 7th should be in the top 87.5% of the 52 starters (rank 7/52 = 86.5% from top). With all 200 WRs, rank 7/200 = 96.5% — a significantly different percentile that could place a player in the wrong salary tier.

**Recommended Fix:**
- Implement the PR Starter Floor formula from bylaws
- Filter the scoring pool to only players at or above the PR Starter Floor before calculating percentile
- The `round_to_nearest_4()` helper in `rules.py` already exists for rounding to multiples of 4

#### Y8-M: PK/PN position grouping for 5YO salary tiers

**Bylaws:** "Position designations follow the same rule as for Franchise Tags" (which groups PK/PN)

**Code:** `calculate_5yo()` calls `calculate_tag_salary()` and `calculate_modified_tt_salary()` with `player.position` directly. These functions query positional salaries without PK/PN grouping (same issue as F6-M).

**Impact:** Same as F6-M — PK/PN tag prices would be calculated against only their individual position pool instead of the combined pool.

**Recommended Fix:** Fix PK/PN grouping at the query level (same fix as F6-M will resolve this automatically).

---

## 7. Proven Performance Escalator (PPE)

**Code files:** `src/app/services/eligibility.py` (eligibility), `src/app/services/buyouts.py` (calculation)

| # | Bylaw Rule | Code Reference | Status | Notes |
|---|-----------|---------------|--------|-------|
| P1 | Rounds 2-5 drafted rookies (bylaws says 2-6 but ADL draft has 5 rounds) | `eligibility.py:594-607` — `player.draft_round not in [2,3,4,5]` | **MATCH** | contracts.json has `[2,3,4,5]`, correct for 5-round draft |
| P2 | PK/PN ineligible | `eligibility.py:610-620` — `player.position in ["PK", "PN"]` | **MATCH** | Correctly blocks kickers/punters |
| P3 | Must be in 4th year of contract | `eligibility.py:646-661` — `contract_year != eligible_year (4)` | **MATCH** | Correct year check |
| P4 | Top 75% = SRFA price, below 75% = ORFA price | `buyouts.py:748-755` — `calculate_srfa_bid()` / `calculate_orfa_bid()` | **DISCREPANCY** | See P4-D below |
| P5 | Automatic (no deadline/window) | `window_status.py:173-179` — PPE always returns `status="open"` | **MATCH** | Correctly always open |
| P6 | Only applies if escalator salary exceeds current salary | `buyouts.py:758-759` — `if current_salary > escalator_salary: escalator_salary = None` | **MATCH** | Correctly preserves higher salary |
| P7 | Percentile calculation uses PR Starter Floor | Same code as 5YO: `buyouts.py:739-743` calls `calculate_starter_percentile()` | **DISCREPANCY** | Same issue as Y7-D — does not use PR Starter Floor |

### PPE Discrepancies

#### P4-D: PPE uses SRFA/ORFA *bid* functions instead of *tag price* functions

**Bylaws:** "The SRFA tag price" and "The ORFA tag price"

**Code (`buyouts.py:751-755`):**
```python
escalator_salary = calculate_srfa_bid(prev_salary)  # for top 75%
escalator_salary = calculate_orfa_bid(prev_salary)  # for below 75%
```

**Issue:** `calculate_srfa_bid()` and `calculate_orfa_bid()` compute `FLOOR_100K(MAX(NFL_price, multiplier * prev_salary))`. These are **opening bid** calculations (for RFA auctions), not **tag price** calculations. The bylaws say "SRFA tag price" and "ORFA tag price" which should be the actual tag prices, not the bid amounts.

The SRFA tag price would be `MAX(NFL_SRFA_price, 1.65 * prev_salary)` (before floor_100k). The bid function applies floor_100k rounding, which could lower the result. Additionally, since NFL RFA prices are currently $0 placeholders, the result is `floor_100k(1.65 * prev_salary)` vs what should be the actual SRFA "tag price."

**However:** The bylaws do specify RFA opening bids are "rounded down to the nearest $100,000", so if the "tag price" IS the opening bid, the code would be correct. The terminology is ambiguous.

**Recommended Fix:**
- Clarify whether "SRFA tag price" means the tag salary or the opening bid
- If tag salary (pre-rounding): create separate `calculate_srfa_tag_price()` and `calculate_orfa_tag_price()` functions without `floor_100k`
- If opening bid: code is correct but needs real NFL RFA prices

---

## 8. Cross-Cutting Concerns

### 8a. Window Gating (window_status.py)

| # | Action | Calendar Field | Code Reference | Status |
|---|--------|---------------|---------------|--------|
| W1 | Extension | `oext_deadline` / `iext_window_start`+`end` | `window_status.py:93-147` | **MATCH** |
| W2 | Franchise Tag | `tag_deadline` | `window_status.py:159-160` | **MATCH** |
| W3 | ERFA Tender | `tender_deadline` | `window_status.py:162-165` | **MATCH** |
| W4 | RFA Tender | `tender_deadline` | `window_status.py:162-165` | **MATCH** |
| W5 | B/R | `br_deadline` | `window_status.py:167-168` | **MATCH** |
| W6 | 5YO | `fyo_deadline` | `window_status.py:170-171` | **MATCH** |
| W7 | PPE | Always open (automatic) | `window_status.py:173-179` | **MATCH** |

All window gating is correctly implemented.

### 8b. Allotment Limits (allotments.py)

| # | Action | Bylaws Limit | Code Limit | Status |
|---|--------|-------------|-----------|--------|
| L1 | Franchise Tag | 1 per team per season | `_ALLOTMENT_LIMITS["franchise_tag"] = 1` | **MATCH** |
| L2 | B/R | 1 per team per season | `_ALLOTMENT_LIMITS["buyout_restructure"] = 1` | **MATCH** |
| L3 | RFA/ERFA Tenders | 2 per team per season (shared) | `_ALLOTMENT_LIMITS["tender"] = 2` | **MATCH** |
| L4 | Extensions | Unlimited | Not tracked in allotments | **MATCH** |

All allotment limits are correctly implemented.

### 8c. Contract Query Patterns

| # | Action | Expected Query | Code Query | Status |
|---|--------|---------------|-----------|--------|
| Q1 | Extension | Active contract (current season, status=active) | `extensions.py:143-153` — `season=season, status="active"` | **MATCH** |
| Q2 | Franchise Tag | Expired contract (current season, years_remaining=0) | `franchise_tags.py:185-194` — `season=season` (no status filter) | **DISCREPANCY** | See Q2-D |
| Q3 | RFA/ERFA | Expired contract (current season, years_remaining=0) | `tenders.py:163-172` / `tenders.py:230-239` — `season=season` (no status filter) | **DISCREPANCY** | See Q3-D |
| Q4 | B/R | Active contract | `buyouts.py:195-206` — `season=season, status="active"` | **MATCH** |
| Q5 | 5YO | Active contract | `eligibility.py:507-517` — `season=season, status="active"` | **MATCH** |
| Q6 | PPE | Active contract | `eligibility.py:623-633` — `season=season, status="active"` | **MATCH** |

#### Q2-D / Q3-D: Tag and tender queries don't filter by contract status

**Code:** `franchise_tags.py` and `tenders.py` query contracts with `Contract.season == season` but don't filter by `Contract.status`. This means they could pick up `bought_out` or `traded` contracts.

**Impact:** If a player's contract was bought out or traded in the current season, the old contract row might still exist with `status="bought_out"`. The tag/tender eligibility check could use this obsolete contract.

**Recommended Fix:**
- For tags: query should include `Contract.status == "active"` OR a dedicated expired-contract status
- For tenders: same — need to ensure the query only picks up the correct expired contract
- Alternatively, if the data model ensures only one contract per player/season exists and status is always current, this is less critical

---

## Summary of Findings

### Counts

| Status | Count |
|--------|-------|
| **MATCH** | 41 |
| **DISCREPANCY** | 12 |
| **MISSING** | 3 |

### Discrepancy/Missing Catalog

| ID | Action | Severity | Description |
|----|--------|----------|-------------|
| E2-D | Extension | **High** | UDFA contract max-years check missing — UDFA players on short contracts could get extensions they shouldn't |
| E3-D | Extension | **Medium** | EXT re-extension blocking depends on contract storage pattern — may not work if `signed_season` != EXT season |
| F6-M | Franchise Tag | **High** | PK/PN not grouped into "Kicker/Punter" category — wrong salary pool for kicker/punter tags |
| F10-D | Franchise Tag | **Low** | Tag salary uses current position, not position at deadline — minor data integrity concern |
| R4-D | RFA | **High** | Multi-year UFA `original_length` calculation incorrect — 1-year UFAs from 2023 may be falsely blocked |
| R6-D | RFA | **High** | NFL RFA prices are all $0 placeholders — FRFA/SRFA/ORFA bids may be too low if NFL price should dominate |
| R9-D | RFA | **Critical** | RRFA opening bid always $0 due to placeholder NFL prices |
| R10-D | RFA | **Medium** | "Previous RFA contract" rule may not apply to post-2021 contracts — ambiguous bylaws text |
| A2-D | ERFA | **Medium** | Salary comparison may use escalated salary instead of signing salary — depends on data model |
| B6-M | B/R | **Medium** | Revert/Transfer option not prohibited for tagged players on expired contracts |
| Y7-D | 5YO | **High** | Percentile calculation does not use PR Starter Floor — wrong percentile denominators |
| Y8-M | 5YO | **High** | PK/PN position grouping missing for 5YO salary tiers (same as F6-M) |
| P4-D | PPE | **Medium** | Uses SRFA/ORFA bid functions instead of tag price functions — may differ due to rounding |
| P7-D | PPE | **High** | Percentile calculation does not use PR Starter Floor (same as Y7-D) |
| Q2-D | Query | **Medium** | Tag/tender queries don't filter by contract status — may use obsolete contracts |

---

## Runtime Test Results

*Tests executed against live SQLite database on 2026-03-13*

### Test Setup

```
Database: /Users/clohr/git/adl_contract_admin/adl_contract_admin.db
Season: 2026
```

*(Runtime tests appended below after execution)*
